"""端到端自动化测试：自带三个本地模拟 HTTP 数据源，无需外部依赖。

运行：
    python3 -m unittest discover -s tests -v
或：
    python3 run_tests.py
"""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
from pathlib import Path

from crane_sync.engine import AbortSync, SyncConfig, SyncEngine
from crane_sync.models import InspectionOrder, normalize_gap
from crane_sync.mock_server import MockServer, build_datasets
from crane_sync.storage import Storage

ROOT = Path(__file__).resolve().parents[1]


def start_server(**overrides) -> MockServer:
    defaults = dict(
        gateway_quota=120,
        gateway_window=1.0,
        window_quota=120,
        window_window=1.0,
        token_ttl=300.0,
        cursor_rotate_after=None,
    )
    defaults.update(overrides)
    server = MockServer(("127.0.0.1", 0), **defaults)
    import threading

    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def make_config(base_url: str, state_dir: str, **overrides) -> SyncConfig:
    cfg = dict(
        base_url=base_url,
        state_dir=state_dir,
        snapshot_format="jsonl",
        page_size=10,
        gateway_quota=120,
        gateway_window=1.0,
        window_source_quota=120,
        window_source_window=1.0,
        cooldown_seconds=0.2,
        window_minutes=30,
        window_horizon="2026-11-12T00:00:00",
    )
    cfg.update(overrides)
    return SyncConfig(**cfg)


def admin_stats(base_url: str) -> dict:
    with urllib.request.urlopen(f"{base_url}/mock/admin/stats", timeout=5) as r:
        return json.loads(r.read().decode())


def inject_window_change(base_url: str, change: dict) -> dict:
    req = urllib.request.Request(
        f"{base_url}/mock/admin/window-change",
        data=json.dumps(change).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read().decode())


def unique_order_count(rows: list[dict]) -> int:
    """已发布快照每个单号只保留最高修订，故条数=唯一单号数。"""
    return len({r["order_id"] for r in rows})


class UnitNormalizationTests(unittest.TestCase):
    def test_mm_and_um_normalized_to_mm(self):
        self.assertAlmostEqual(normalize_gap(8.01, "mm"), 8.01)
        self.assertAlmostEqual(normalize_gap(8010, "um"), 8.01)
        self.assertAlmostEqual(normalize_gap(1, "μm"), 0.001)
        self.assertAlmostEqual(normalize_gap(5, None), 5.0)

    def test_order_ingest_normalizes_mixed_units(self):
        payload = {
            "order_id": "QC-0001",
            "revision": 1,
            "inspected_at": "2026-11-10T08:00:00",
            "gaps": [
                {"point": "rail", "value": 8000, "unit": "um"},
                {"point": "trolley", "value": 12.5, "unit": "mm"},
            ],
        }
        order = InspectionOrder.from_payload(payload, "offset")
        self.assertEqual([g["unit"] for g in order.gaps], ["mm", "mm"])
        self.assertAlmostEqual(order.gaps[0]["value_mm"], 8.0)
        self.assertAlmostEqual(order.gaps[1]["value_mm"], 12.5)

    def test_unknown_unit_rejected(self):
        with self.assertRaises(ValueError):
            normalize_gap(1, "inch")


class EndToEndTests(unittest.TestCase):
    def setUp(self):
        self.server = start_server()
        self.base_url = (
            f"http://{self.server.server_address[0]}:{self.server.server_address[1]}"
        )
        self.tmp = tempfile.TemporaryDirectory()
        self.state_dir = self.tmp.name

    def tearDown(self):
        self.server.shutdown()
        self.tmp.cleanup()

    def test_full_sync_publishes_three_snapshots(self):
        engine = SyncEngine(make_config(self.base_url, self.state_dir))
        try:
            results = asyncio.run(engine.run())
        finally:
            engine.close()

        self.assertEqual(set(results), {"offset", "cursor", "window"})
        # 发布快照每个单号只保留最高修订，故期望条数=各源唯一单号数
        unique_sizes = {
            k: unique_order_count(v)
            for k, v in self.server.gateway.datasets.items()
        }
        for name, info in results.items():
            self.assertEqual(info["record_count"], unique_sizes[name])
            self.assertTrue(Path(info["file_path"]).exists())

        storage = Storage(self.state_dir)
        try:
            for name in ("offset", "cursor", "window"):
                rows = storage.read_snapshot(name)
                self.assertEqual(len(rows), unique_sizes[name])
                # 单源快照内单号唯一（已折叠为最高修订）
                self.assertEqual(
                    len({r["order_id"] for r in rows}), len(rows)
                )
                # 所有间隙必须统一为毫米
                for row in rows:
                    for g in row["gaps"]:
                        self.assertEqual(g["unit"], "mm")
            merged = storage.read_merged()
        finally:
            storage.close()

        # 合并视图：每张单号只保留最高修订
        by_id: dict[str, dict] = {}
        for row in merged:
            by_id.setdefault(row["order_id"], []).append(row)
        for order_id, rows in by_id.items():
            self.assertEqual(len(rows), 1, f"{order_id} 出现多条")
            max_rev = max(
                r["revision"]
                for r in merged
                if r["order_id"] == order_id
            )
            self.assertEqual(rows[0]["revision"], max_rev)

        # 数值核对：微米读数已正确换算
        any_um_source = next(
            iter(self.server.gateway.datasets.values())
        )
        um_row = next(
            r for r in any_um_source if r["gaps"][0]["unit"] == "um"
        )
        storage = Storage(self.state_dir)
        try:
            merged = storage.read_merged()
        finally:
            storage.close()
        stored = next(r for r in merged if r["order_id"] == um_row["order_id"])
        self.assertAlmostEqual(
            stored["gaps"][0]["value_mm"],
            um_row["gaps"][0]["value"] / 1000.0,
            places=6,
        )

    def test_parquet_format_when_available(self):
        try:
            import pyarrow  # noqa: F401
            import pyarrow.parquet  # noqa: F401
        except ImportError:
            self.skipTest("环境未安装 pyarrow，跳过 Parquet 链路测试")
        engine = SyncEngine(
            make_config(self.base_url, self.state_dir, snapshot_format="parquet")
        )
        try:
            results = asyncio.run(engine.run())
        finally:
            engine.close()
        for name, info in results.items():
            self.assertEqual(info["format"], "parquet")
            path = Path(info["file_path"])
            self.assertTrue(path.exists())
            self.assertEqual(path.suffix, ".parquet")
            import pyarrow.parquet as pq

            table = pq.read_table(str(path))
            self.assertEqual(table.num_rows, info["record_count"])
            # 间隙列里单位统一为毫米
            for cell in table.column("gaps_json").to_pylist():
                self.assertIn('"unit": "mm"', cell)

    def test_parquet_without_pyarrow_reports_clear_error(self):
        # 模拟 pyarrow 缺失：发布时应给出可操作的错误信息
        from unittest import mock

        storage = Storage(self.state_dir)
        try:
            storage.upsert_records(
                "offset",
                [
                    InspectionOrder.from_payload(
                        {
                            "order_id": "QC-0001",
                            "revision": 1,
                            "inspected_at": "2026-11-10T08:00:00",
                            "gaps": [{"point": "rail", "value": 8.0, "unit": "mm"}],
                        },
                        "offset",
                    )
                ],
            )
            real_import = __builtins__.__import__ if not isinstance(
                __builtins__, dict
            ) else __builtins__["__import__"]

            def fake_import(name, *a, **k):
                if name.startswith("pyarrow"):
                    raise ImportError("simulated missing pyarrow")
                return real_import(name, *a, **k)

            with mock.patch("builtins.__import__", side_effect=fake_import):
                with self.assertRaises(RuntimeError) as ctx:
                    storage.publish_snapshot("offset", fmt="parquet")
            self.assertIn("pyarrow", str(ctx.exception))
        finally:
            storage.close()

    def test_mixed_units_appear_on_same_page(self):
        # 单页数据里确实混用了 mm 与 um（前置条件校验）
        offset_rows = self.server.gateway.datasets["offset"]
        page = offset_rows[:10]
        units = {g["unit"] for r in page for g in r["gaps"]}
        self.assertEqual(units, {"mm", "um"})

    def test_abort_and_resume_keeps_progress(self):
        page_log: dict[str, int] = {}

        async def abort_hook(source: str, pages: int):
            page_log[source] = pages
            if sum(page_log.values()) >= 4:
                raise AbortSync("模拟强制中止")

        cfg = make_config(self.base_url, self.state_dir, on_page=abort_hook)
        engine = SyncEngine(cfg)
        with self.assertRaises(AbortSync):
            asyncio.run(engine.run())
        engine.close()

        # 中止时没有任何完整快照发布
        storage = Storage(self.state_dir)
        self.assertEqual(storage.published_sources(), [])
        # 但已有部分进度与暂存记录落盘
        self.assertIsNotNone(storage.get_progress("offset"))
        self.assertGreater(storage.count_staging("offset"), 0)
        storage.close()

        # 重跑：从已保存进度续读，最终全部发布
        engine2 = SyncEngine(make_config(self.base_url, self.state_dir))
        try:
            results = asyncio.run(engine2.run())
        finally:
            engine2.close()
        unique_sizes = {
            k: unique_order_count(v)
            for k, v in self.server.gateway.datasets.items()
        }
        for name, info in results.items():
            self.assertEqual(info["record_count"], unique_sizes[name])

    def test_stale_cursor_rereads_new_manifest(self):
        # 清单在第 2 次游标调用后轮换，已保存游标随即过期
        server = start_server(cursor_rotate_after=2, token_ttl=300.0)
        base_url = (
            f"http://{server.server_address[0]}:{server.server_address[1]}"
        )
        tmp = tempfile.TemporaryDirectory()
        try:
            engine = SyncEngine(make_config(base_url, tmp.name))
            results = asyncio.run(engine.run())
            engine.close()
            # 最终快照必须基于新版清单（v2，记录与顺序均变化）
            self.assertEqual(server.gateway.cursor_version, 2)
            v2_rows = server.gateway.cursor_manifest()
            # 发布投影：每个单号只保留最高修订；顺序按该单号最高修订
            # 在清单中的行位置（position），与存储层 SQL 保持一致
            best_idx: dict[str, tuple[int, int]] = {}
            for idx, r in enumerate(v2_rows):
                oid = r["order_id"]
                if oid not in best_idx or r["revision"] > best_idx[oid][1]:
                    best_idx[oid] = (idx, r["revision"])
            expected_pairs = [
                (oid, rev)
                for oid, (_idx, rev) in sorted(
                    best_idx.items(), key=lambda kv: kv[1][0]
                )
            ]
            self.assertEqual(
                results["cursor"]["record_count"], len(expected_pairs)
            )
            storage = Storage(tmp.name)
            snap = storage.read_snapshot("cursor")
            # 快照的 (单号, 修订) 与"每单号最高修订"投影一致且同序
            self.assertEqual(
                [(r["order_id"], r["revision"]) for r in snap],
                expected_pairs,
            )
            storage.close()
        finally:
            server.shutdown()
            tmp.cleanup()

    def test_old_snapshot_still_readable_during_cursor_reread(self):
        # 第一轮：正常发布
        engine = SyncEngine(make_config(self.base_url, self.state_dir))
        asyncio.run(engine.run())
        engine.close()
        storage = Storage(self.state_dir)
        before = storage.read_snapshot("cursor")
        self.assertTrue(before)
        storage.close()

        # 服务端轮换清单，并把客户端游标进度伪造成旧版游标
        req = urllib.request.Request(
            f"{self.base_url}/mock/admin/rotate-cursor", method="POST"
        )
        urllib.request.urlopen(req, timeout=5).read()
        storage = Storage(self.state_dir)
        storage.set_progress(
            "cursor",
            {"manifest_version": "v1", "next_cursor": "v1:5:x", "done": False},
        )
        storage.close()

        # 重读期间旧快照始终可读
        storage = Storage(self.state_dir)
        self.assertEqual(len(storage.read_snapshot("cursor")), len(before))
        storage.close()

        engine2 = SyncEngine(make_config(self.base_url, self.state_dir))
        asyncio.run(engine2.run())
        engine2.close()

        storage = Storage(self.state_dir)
        after = storage.read_snapshot("cursor")
        v2_rows = self.server.gateway.cursor_manifest()
        self.assertEqual(len(after), unique_order_count(v2_rows))
        storage.close()

    def test_window_overlap_deduplicated(self):
        engine = SyncEngine(make_config(self.base_url, self.state_dir))
        asyncio.run(engine.run())
        engine.close()
        storage = Storage(self.state_dir)
        rows = storage.read_snapshot("window")
        # 暂存区：窗口重叠返回的边界记录按主键去重，条数=数据集修订行数
        staging_count = storage.count_staging("window")
        storage.close()
        raw = self.server.gateway.datasets["window"]
        self.assertEqual(staging_count, len(raw))
        # 已发布快照：进一步按单号折叠为最高修订，条数=唯一单号数
        self.assertEqual(len(rows), unique_order_count(raw))
        order_ids = [r["order_id"] for r in rows]
        self.assertEqual(len(order_ids), len(set(order_ids)))

    def test_shared_gateway_quota_and_token_renewal(self):
        # 紧额度必然出现 429 冷却；令牌长 TTL，由管理员强制失效恰好一次，
        # 验证"同一失效令牌只续期一次"
        server = start_server(
            gateway_quota=3, gateway_window=1.0, token_ttl=300.0
        )
        base_url = (
            f"http://{server.server_address[0]}:{server.server_address[1]}"
        )
        tmp = tempfile.TemporaryDirectory()
        try:
            expired = {"done": False}

            async def expire_after_first_page(source: str, pages: int):
                # 第一页成功后强制令牌失效，下一次请求必然 401；
                # 两个共享网关来源可能同时 401，正好检验只续期一次
                if not expired["done"]:
                    expired["done"] = True
                    loop = asyncio.get_running_loop()

                    def _post():
                        req = urllib.request.Request(
                            f"{base_url}/mock/admin/expire-token", method="POST"
                        )
                        urllib.request.urlopen(req, timeout=5).read()

                    await loop.run_in_executor(None, _post)

            cfg = make_config(
                base_url,
                tmp.name,
                gateway_quota=3,
                gateway_window=1.0,
                on_page=expire_after_first_page,
            )
            engine = SyncEngine(cfg)
            results = asyncio.run(engine.run())
            engine.close()
            stats = admin_stats(base_url)
            # 初次签发 + 强制失效后的一次续期（同一令牌不重复续）
            self.assertEqual(stats["gateway"]["renewals"], 2)
            self.assertGreaterEqual(stats["gateway"]["rejected_429"], 1)
            # 限流/续期之后数据仍然完整（快照条数=各源唯一单号数）
            unique_sizes = {
                k: unique_order_count(v)
                for k, v in server.gateway.datasets.items()
            }
            for name in ("offset", "cursor"):
                self.assertEqual(
                    results[name]["record_count"], unique_sizes[name]
                )
        finally:
            server.shutdown()
            tmp.cleanup()

    def test_concurrent_sources_both_progress(self):
        # 小额度制造等待；检查网关额度实际在两个来源之间持续、均衡发放
        server = start_server(gateway_quota=2, gateway_window=0.5)
        base_url = (
            f"http://{server.server_address[0]}:{server.server_address[1]}"
        )
        tmp = tempfile.TemporaryDirectory()
        try:
            cfg = make_config(
                base_url,
                tmp.name,
                gateway_quota=2,
                gateway_window=0.5,
                sources=("offset", "cursor"),
            )
            engine = SyncEngine(cfg)
            asyncio.run(engine.run())
            grants = engine.gateway.quota.grant_log
            engine.close()

            data_grants = [g for g in grants if g in ("offset", "cursor")]
            self.assertGreaterEqual(len(data_grants), 4)
            # 两个来源都拿到了额度
            self.assertIn("offset", data_grants)
            self.assertIn("cursor", data_grants)
            n_off = data_grants.count("offset")
            n_cur = data_grants.count("cursor")
            # 发放量基本均衡：任一方不超过另一方的 2 倍（不被饿死）
            self.assertLessEqual(n_off, 2 * n_cur)
            self.assertLessEqual(n_cur, 2 * n_off)
            # 两个来源在序列中交替出现：各自都不是只在开头/结尾集中出现
            self.assertIn("offset", data_grants[1:])
            self.assertIn("cursor", data_grants[:-1])
            # 数据完整（快照已按单号折叠为最高修订）
            storage = Storage(tmp.name)
            unique_sizes = {
                k: unique_order_count(v)
                for k, v in server.gateway.datasets.items()
            }
            self.assertEqual(
                len(storage.read_snapshot("offset")), unique_sizes["offset"]
            )
            self.assertEqual(
                len(storage.read_snapshot("cursor")), unique_sizes["cursor"]
            )
            storage.close()
        finally:
            server.shutdown()
            tmp.cleanup()


class WindowChangeCatchUpTests(unittest.TestCase):
    """时间窗源的迟到变更追赶：补发 / 更正 / 撤回、续跑、过期重建。"""

    def setUp(self):
        self.server = start_server()
        self.base_url = (
            f"http://{self.server.server_address[0]}:{self.server.server_address[1]}"
        )
        self.tmp = tempfile.TemporaryDirectory()
        self.state_dir = self.tmp.name

    def tearDown(self):
        self.server.shutdown()
        self.tmp.cleanup()

    def _sync(self, **overrides) -> dict:
        engine = SyncEngine(make_config(self.base_url, self.state_dir, **overrides))
        try:
            return asyncio.run(engine.run())
        finally:
            engine.close()

    def _window_snapshot(self) -> dict[str, dict]:
        storage = Storage(self.state_dir)
        try:
            return {r["order_id"]: r for r in storage.read_snapshot("window")}
        finally:
            storage.close()

    def test_baseline_sets_change_cursor_without_rescanning_next_round(self):
        self._sync()
        storage = Storage(self.state_dir)
        progress = storage.get_progress("window")
        storage.close()
        self.assertEqual(progress["mode"], "changes")
        self.assertEqual(
            progress["since"], self.server.gateway.window_log.high_watermark
        )
        scans_after_baseline = admin_stats(self.base_url)["window_scans"]
        self.assertGreater(scans_after_baseline, 0)

        # 日常一轮：即便没有任何变更，也只走变更流，不再扫历史时间窗
        self._sync()
        stats = admin_stats(self.base_url)
        self.assertEqual(stats["window_scans"], scans_after_baseline)
        self.assertGreater(stats["window_change_pages"], 0)

    def test_append_correct_retract_are_applied(self):
        self._sync()
        base = self._window_snapshot()
        # 取一个有多个修订的单用于撤回其最高修订，验证回落到次高
        by_track: dict[str, list[dict]] = {}
        for row in self.server.gateway.window_log.rows():
            by_track.setdefault(row["order_id"], []).append(row)
        multi_id, multi_rows = next(
            (oid, rows) for oid, rows in by_track.items() if len(rows) >= 2
        )
        top_rev = max(r["revision"] for r in multi_rows)
        second_rev = max(r["revision"] for r in multi_rows if r["revision"] < top_rev)
        rev1 = next(
            r for r in self.server.gateway.window_log.rows()
            if r["revision"] == 1 and r["order_id"] != multi_id
        )

        inject_window_change(
            self.base_url,
            {
                "type": "append",
                "order_id": "QC-9999",
                "revision": 1,
                "inspected_at": "2026-11-09T00:00:00",
                "gaps": [{"point": "rail", "value": 9000, "unit": "um"}],
            },
        )
        inject_window_change(
            self.base_url,
            {
                "type": "correct",
                "order_id": rev1["order_id"],
                "revision": 1,
                "inspected_at": rev1["inspected_at"],
                "gaps": [{"point": "rail", "value": 12345, "unit": "um"}],
            },
        )
        inject_window_change(
            self.base_url,
            {"type": "retract", "order_id": multi_id, "revision": top_rev},
        )

        self._sync()
        snap = self._window_snapshot()

        # 补发：新单号出现，单位已归一为 mm
        self.assertIn("QC-9999", snap)
        self.assertAlmostEqual(snap["QC-9999"]["gaps"][0]["value_mm"], 9.0)

        # 更正：同单号同修订，读数被更新
        self.assertAlmostEqual(
            snap[rev1["order_id"]]["gaps"][0]["value_mm"], 12.345
        )

        # 撤回最高修订：回落到次高修订，单号仍在
        self.assertEqual(snap[multi_id]["revision"], second_rev)
        self.assertEqual(base[multi_id]["revision"], top_rev)

        # 撤回"唯一修订"的单号：整单消失
        only_rev1 = next(
            oid for oid, row in base.items()
            if row["revision"] == 1 and oid != rev1["order_id"]
        )
        inject_window_change(
            self.base_url,
            {"type": "retract", "order_id": only_rev1, "revision": 1},
        )
        self._sync()
        self.assertNotIn(only_rev1, self._window_snapshot())

    def test_each_round_caps_at_startup_watermark(self):
        self._sync()
        inject_window_change(
            self.base_url,
            {"type": "append", "order_id": "QC-8888", "revision": 1,
             "inspected_at": "2026-11-09T00:00:00",
             "gaps": [{"point": "rail", "value": 8.0, "unit": "mm"}]},
        )
        # 本轮启动后、采集过程中再加入一条变更：应留到下一轮
        added_during: dict[str, bool] = {"done": False}

        async def add_late(source: str, pages: int):
            if source == "window" and not added_during["done"]:
                added_during["done"] = True
                inject_window_change(
                    self.base_url,
                    {"type": "append", "order_id": "QC-7777", "revision": 1,
                     "inspected_at": "2026-11-09T00:00:00",
                     "gaps": [{"point": "rail", "value": 8.0, "unit": "mm"}]},
                )

        self._sync(page_size=1, on_page=add_late)
        snap = self._window_snapshot()
        self.assertIn("QC-8888", snap)   # 启动前已存在的变更被采到
        self.assertNotIn("QC-7777", snap)  # 启动后才出现的留到下一轮

        self._sync()
        self.assertIn("QC-7777", self._window_snapshot())

    def test_resume_change_pages_does_not_refetch_saved_pages(self):
        self._sync()
        for i in range(5):
            inject_window_change(
                self.base_url,
                {"type": "append", "order_id": f"QC-LATE-{i}", "revision": 1,
                 "inspected_at": "2026-11-09T00:00:00",
                 "gaps": [{"point": "rail", "value": 8.0 + i, "unit": "mm"}]},
            )

        pages: list[int] = []

        async def abort_after_two(source: str, page: int):
            if source != "window":
                return
            pages.append(page)
            if len(pages) >= 2:
                raise AbortSync("模拟变更追赶中途中止")

        with self.assertRaises(AbortSync):
            self._sync(page_size=2, on_page=abort_after_two)

        storage = Storage(self.state_dir)
        progress = storage.get_progress("window")
        # 已消费 2 页（每页 2 条），续点=4
        self.assertEqual(progress["mode"], "changes")
        self.assertEqual(progress["since"], 4)
        self.assertEqual(progress["done"], False)
        self.assertIsNotNone(progress["until"])
        storage.close()

        before = admin_stats(self.base_url)
        # 续跑：沿用原 until，从 since=4 拉下一页，已保存页面不再重拉
        self._sync(page_size=2)
        after = admin_stats(self.base_url)
        self.assertEqual(after["window_scans"], before["window_scans"])
        # 续跑直接沿用原 until，只拉下一页变更（4->5），不重取高水位、不重拉已存页
        self.assertEqual(
            after["window_change_pages"] - before["window_change_pages"], 1
        )

        snap = self._window_snapshot()
        for i in range(5):
            self.assertIn(f"QC-LATE-{i}", snap)
        storage = Storage(self.state_dir)
        self.assertTrue(storage.get_progress("window")["done"])
        storage.close()

    def test_expired_change_cursor_rebuilds_baseline(self):
        server = start_server(window_change_retention=3)
        base_url = (
            f"http://{server.server_address[0]}:{server.server_address[1]}"
        )
        tmp = tempfile.TemporaryDirectory()
        try:
            engine = SyncEngine(make_config(base_url, tmp.name))
            asyncio.run(engine.run())
            engine.close()

            storage = Storage(tmp.name)
            before = storage.read_snapshot("window")
            old_progress = storage.get_progress("window")
            storage.close()

            # 制造超过保留窗口的变更，使已保存续点过期。
            # 时间落在基线扫描窗口 [window_start, horizon] 内，因此全量重扫
            # 能覆盖到它们——这也是供应商窗口的语义：窗口返回当前所有记录。
            for i in range(6):
                inject_window_change(
                    base_url,
                    {"type": "append", "order_id": f"QC-NEW-{i}", "revision": 1,
                     "inspected_at": "2026-11-10T09:00:00",
                     "gaps": [{"point": "rail", "value": 8.0, "unit": "mm"}]},
                )
            self.assertGreater(
                server.gateway.window_log.oldest_seq,
                old_progress["since"] + 1,
            )

            scans_before = admin_stats(base_url)["window_scans"]

            # 重建期间（发布前）旧快照仍可读
            storage = Storage(tmp.name)
            self.assertEqual(
                len(storage.read_snapshot("window")), len(before)
            )
            storage.close()

            engine = SyncEngine(make_config(base_url, tmp.name))
            results = asyncio.run(engine.run())
            engine.close()
            # 其他来源照常发布
            self.assertIn("offset", results)
            self.assertIn("cursor", results)

            # 触发了全量重扫（基线重建）
            scans_after = admin_stats(base_url)["window_scans"]
            self.assertGreater(scans_after, scans_before)

            storage = Storage(tmp.name)
            try:
                snap = {
                    r["order_id"]: r for r in storage.read_snapshot("window")
                }
                progress = storage.get_progress("window")
            finally:
                storage.close()
            # 重建后的快照 = 供应商当前状态（补发的都在，撤回的不残留）；
            # 快照每单号只保留最高修订，故以唯一单号数比较
            current_ids = {r["order_id"] for r in server.gateway.window_log.rows()}
            self.assertEqual(set(snap), current_ids)
            for i in range(6):
                self.assertIn(f"QC-NEW-{i}", snap)
            self.assertEqual(progress["mode"], "changes")
            self.assertEqual(progress["done"], True)
        finally:
            server.shutdown()
            tmp.cleanup()

    def test_legacy_state_without_change_cursor_rebuilds(self):
        # 模拟旧版本状态：只有 last_to，没有 mode/since
        self._sync()
        storage = Storage(self.state_dir)
        storage.set_progress(
            "window", {"last_to": "2026-11-11T00:00:00", "done": True}
        )
        storage.close()
        scans_before = admin_stats(self.base_url)["window_scans"]
        self._sync()
        # 重建为完整基线：重新全量扫描
        self.assertGreater(admin_stats(self.base_url)["window_scans"], scans_before)
        storage = Storage(self.state_dir)
        self.assertEqual(storage.get_progress("window")["mode"], "changes")
        storage.close()
        current_ids = {
            r["order_id"] for r in self.server.gateway.window_log.rows()
        }
        self.assertEqual(set(self._window_snapshot()), current_ids)


class SubprocessCliTests(unittest.TestCase):
    """用启动脚本跑真实子进程：serve -> sync -> read。"""

    def test_cli_serve_sync_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "mock.json"
            state_dir = Path(tmp) / "state"
            env = dict(os.environ, PYTHONPATH=str(ROOT))
            serve = subprocess.Popen(
                [
                    sys.executable, "-m", "crane_sync", "serve",
                    "--config-out", str(cfg_path),
                    "--state-dir", str(state_dir),
                    "--gateway-quota", "30",
                    "--window-quota", "30",
                ],
                cwd=str(ROOT),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                deadline = time.time() + 10
                while not cfg_path.exists() and time.time() < deadline:
                    time.sleep(0.05)
                self.assertTrue(cfg_path.exists(), "模拟源未及时写出配置")

                sync = subprocess.run(
                    [sys.executable, "-m", "crane_sync", "sync",
                     "--config", str(cfg_path)],
                    cwd=str(ROOT), env=env, text=True,
                    capture_output=True, timeout=30,
                )
                self.assertEqual(sync.returncode, 0, sync.stderr)
                self.assertIn("同步完成", sync.stdout)

                read = subprocess.run(
                    [sys.executable, "-m", "crane_sync", "read",
                     "--state-dir", str(state_dir), "--merged"],
                    cwd=str(ROOT), env=env, text=True,
                    capture_output=True, timeout=15,
                )
                self.assertEqual(read.returncode, 0, read.stderr)
                self.assertIn("合并快照", read.stdout)
                self.assertIn("QC-0001", read.stdout)
                self.assertIn("mm", read.stdout)
            finally:
                serve.terminate()
                try:
                    serve.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    serve.kill()


if __name__ == "__main__":
    unittest.main(verbosity=2)
