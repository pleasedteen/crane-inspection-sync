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
        window_start="2026-11-10T08:00:00",
        window_horizon="2026-11-12T00:00:00",
    )
    cfg.update(overrides)
    return SyncConfig(**cfg)


def run_sync(base_url: str, state_dir: str, **overrides) -> dict:
    engine = SyncEngine(make_config(base_url, state_dir, **overrides))
    try:
        return asyncio.run(engine.run())
    finally:
        engine.close()


def admin_stats(base_url: str) -> dict:
    with urllib.request.urlopen(f"{base_url}/mock/admin/stats", timeout=5) as r:
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


class WindowIncrementalTests(unittest.TestCase):
    """时间窗源增量同步：探测、版本钉选、补送/修订补齐与续跑。"""

    LATE_ORDER = {
        "order_id": "QC-LATE1",
        "revision": 1,
        # 迟到单：时间戳落在第一轮早已扫过的窗口里
        "inspected_at": "2026-11-10T09:15:00",
        "gaps": [{"point": "rail", "value": 9.0, "unit": "mm"}],
    }

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

    def test_unchanged_source_costs_one_probe_only(self):
        run_sync(self.base_url, self.state_dir)
        probes_after_first = self.server.window_probe_requests
        data_after_first = self.server.window_data_requests
        self.assertGreaterEqual(data_after_first, 1)

        # 源端无变化再跑一轮：只允许新增一次探测，不得再拉任何窗口
        run_sync(self.base_url, self.state_dir)
        self.assertEqual(
            self.server.window_probe_requests, probes_after_first + 1
        )
        self.assertEqual(self.server.window_data_requests, data_after_first)

        # 再跑一轮仍是一次探测
        run_sync(self.base_url, self.state_dir)
        self.assertEqual(
            self.server.window_probe_requests, probes_after_first + 2
        )
        self.assertEqual(self.server.window_data_requests, data_after_first)

    def test_late_order_is_backfilled_with_incremental_windows_only(self):
        results1 = run_sync(self.base_url, self.state_dir)
        full_windows = self.server.window_data_requests
        storage = Storage(self.state_dir)
        before = storage.read_snapshot("window")
        before_ids = {r["order_id"] for r in before}
        storage.close()
        self.assertNotIn("QC-LATE1", before_ids)

        # 供应商补送一张迟到单（旧时间戳）
        self.server.gateway.deliver_late_order(dict(self.LATE_ORDER))
        self.server.window_data_requests = 0
        results2 = run_sync(self.base_url, self.state_dir)

        # 第二轮只拉增量窗口，远少于首轮全量窗口
        incremental_windows = self.server.window_data_requests
        self.assertLess(incremental_windows, full_windows)
        self.assertGreaterEqual(incremental_windows, 1)

        storage = Storage(self.state_dir)
        after = storage.read_snapshot("window")
        storage.close()
        ids = {r["order_id"] for r in after}
        self.assertIn("QC-LATE1", ids)
        # 原有单据一条不少；快照按单号折叠，迟到单只出现一次
        self.assertTrue(before_ids.issubset(ids))
        self.assertEqual(len(ids), len(after))
        # 重新发布产生了新快照 ID
        self.assertNotEqual(
            results1["window"]["snapshot_id"], results2["window"]["snapshot_id"]
        )

        # 再跑一轮：源端无变化，只剩一次探测
        self.server.window_data_requests = 0
        run_sync(self.base_url, self.state_dir)
        self.assertEqual(self.server.window_data_requests, 0)

    def test_revision_keeps_highest_revision_and_mm_units(self):
        storage = Storage(self.state_dir)
        target_id = self.server.gateway.window_current_rows()[0]["order_id"]
        storage.close()

        run_sync(self.base_url, self.state_dir)
        storage = Storage(self.state_dir)
        rev_before = next(
            r["revision"]
            for r in storage.read_snapshot("window")
            if r["order_id"] == target_id
        )
        storage.close()

        # 供应商修订已扫过的单：微米单位，验证毫米归一
        self.server.gateway.revise_window_order(
            target_id, [{"point": "rail", "value": 7500, "unit": "um"}]
        )
        self.server.window_data_requests = 0
        run_sync(self.base_url, self.state_dir)
        self.assertGreaterEqual(self.server.window_data_requests, 1)

        storage = Storage(self.state_dir)
        snap = storage.read_snapshot("window")
        storage.close()
        target_rows = [r for r in snap if r["order_id"] == target_id]
        self.assertEqual(len(target_rows), 1)  # 单号仍唯一
        row = target_rows[0]
        self.assertEqual(row["revision"], rev_before + 1)  # 只保留最高修订
        self.assertEqual(row["gaps"][0]["unit"], "mm")
        self.assertAlmostEqual(row["gaps"][0]["value_mm"], 7.5, places=6)

    def test_mid_round_change_waits_for_next_round(self):
        run_sync(self.base_url, self.state_dir)
        storage = Storage(self.state_dir)
        before_ids = {r["order_id"] for r in storage.read_snapshot("window")}
        storage.close()

        # 先制造一个变动，让本轮探测到 v2 并开始拉窗口；
        # 拉到第一个窗口时再补送一张单（v3），本轮已钉住 v2，不应看到它
        late = dict(self.LATE_ORDER)
        late["order_id"] = "QC-MIDROUND"
        self.server.gateway.deliver_late_order(
            dict(self.LATE_ORDER, order_id="QC-PREROUND")
        )

        async def deliver_mid_round(source: str, pages: int):
            if source == "window" and pages == 1 and not hook_state["fired"]:
                hook_state["fired"] = True
                self.server.gateway.deliver_late_order(late)

        hook_state = {"fired": False}
        run_sync(
            self.base_url,
            self.state_dir,
            sources=("window",),
            on_page=deliver_mid_round,
        )
        self.assertTrue(hook_state["fired"])

        storage = Storage(self.state_dir)
        ids = {r["order_id"] for r in storage.read_snapshot("window")}
        storage.close()
        self.assertIn("QC-PREROUND", ids)       # 探测前的变动本轮补齐
        self.assertNotIn("QC-MIDROUND", ids)    # 途中补送留到下轮
        self.assertTrue(before_ids.issubset(ids))

        # 下一轮只探测一次即发现 v3，增量补齐途中补送的单
        self.server.window_data_requests = 0
        run_sync(self.base_url, self.state_dir, sources=("window",))
        self.assertGreaterEqual(self.server.window_data_requests, 1)
        storage = Storage(self.state_dir)
        ids = {r["order_id"] for r in storage.read_snapshot("window")}
        storage.close()
        self.assertIn("QC-MIDROUND", ids)

    def test_kill_resume_uses_frozen_plan_and_keeps_snapshot_readable(self):
        # 第一轮先发布一份可读快照
        run_sync(self.base_url, self.state_dir)
        storage = Storage(self.state_dir)
        before = storage.read_snapshot("window")
        before_ids = {r["order_id"] for r in before}
        storage.close()

        self.server.gateway.deliver_late_order(
            dict(self.LATE_ORDER, order_id="QC-RESUME")
        )
        page_log: dict[str, int] = {}

        async def abort_hook(source: str, pages: int):
            if source != "window":
                return
            page_log[source] = pages
            if pages >= 2:
                raise AbortSync("模拟强杀")

        with self.assertRaises(AbortSync):
            run_sync(
                self.base_url,
                self.state_dir,
                sources=("window",),
                on_page=abort_hook,
            )
        self.assertGreaterEqual(page_log.get("window", 0), 2)

        # 强杀后：未完成轮次的冻结计划已落盘，上一份快照仍可读
        storage = Storage(self.state_dir)
        progress = storage.get_progress("window")
        self.assertTrue(progress["active"])
        self.assertIn("data_version", progress)
        readable = {r["order_id"] for r in storage.read_snapshot("window")}
        self.assertEqual(readable, before_ids)
        storage.close()

        probes_before = self.server.window_probe_requests
        self.server.window_data_requests = 0
        # 续跑：沿用冻结计划，不重新探测
        run_sync(self.base_url, self.state_dir, sources=("window",))
        self.assertEqual(self.server.window_probe_requests, probes_before)
        self.assertGreaterEqual(self.server.window_data_requests, 1)

        storage = Storage(self.state_dir)
        progress = storage.get_progress("window")
        self.assertFalse(progress["active"])
        ids = {r["order_id"] for r in storage.read_snapshot("window")}
        storage.close()
        self.assertIn("QC-RESUME", ids)
        self.assertTrue(before_ids.issubset(ids))

    def test_window_quota_independent_from_gateway(self):
        # 时间窗源独立额度收紧：429 冷却重试后仍完整，且不影响网关来源
        server = start_server(window_quota=1, window_window=0.3)
        base_url = (
            f"http://{server.server_address[0]}:{server.server_address[1]}"
        )
        tmp = tempfile.TemporaryDirectory()
        try:
            # 缩短配置范围，窗口数少、测试更快；
            # 客户端额度比服务端宽松， burst 请求必然撞上服务端 429，
            # 验证冷却重试与独立额度（网关来源不受牵连）
            results = run_sync(
                base_url,
                tmp.name,
                window_source_quota=3,
                window_source_window=0.3,
                window_horizon="2026-11-10T12:00:00",
            )
            self.assertGreaterEqual(server.window_rejected, 1)
            raw = server.gateway.window_current_rows()
            expected = {
                r["order_id"]
                for r in raw
                if r["inspected_at"] <= "2026-11-10T12:00:00"
            }
            storage = Storage(tmp.name)
            got = {r["order_id"] for r in storage.read_snapshot("window")}
            storage.close()
            self.assertEqual(got, expected)
            # 共享网关的两个来源不受时间窗额度拖累，正常发布
            self.assertIn("offset", results)
            self.assertIn("cursor", results)
        finally:
            server.shutdown()
            tmp.cleanup()

    def test_change_outside_configured_range_is_skipped(self):
        run_sync(self.base_url, self.state_dir)
        # 补送一张晚于配置 horizon 的单：探测得到变化，但无窗口请求
        late = dict(
            self.LATE_ORDER,
            order_id="QC-FUTURE",
            inspected_at="2026-11-13T09:00:00",
        )
        self.server.gateway.deliver_late_order(late)
        probes_before = self.server.window_probe_requests
        self.server.window_data_requests = 0
        run_sync(self.base_url, self.state_dir)
        self.assertEqual(self.server.window_probe_requests, probes_before + 1)
        self.assertEqual(self.server.window_data_requests, 0)
        storage = Storage(self.state_dir)
        ids = {r["order_id"] for r in storage.read_snapshot("window")}
        storage.close()
        self.assertNotIn("QC-FUTURE", ids)

        # 再跑一轮：该版本已记账为完成，源端"无变化"，仍只有一次探测
        self.server.window_data_requests = 0
        run_sync(self.base_url, self.state_dir)
        self.assertEqual(self.server.window_data_requests, 0)


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
