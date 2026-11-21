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
import sqlite3
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


class OffsetZoneTests(unittest.TestCase):
    """偏移源按作业区拆分：拆区并发、强杀续跑、增删区与统一发布。"""

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

    def make(self, zones, **overrides):
        return make_config(
            self.base_url, self.state_dir, sources=("offset",),
            offset_zones=zones, **overrides,
        )

    def zone_rows(self) -> dict[str, int]:
        rows = self.server.gateway.datasets["offset"]
        return {
            z: sum(1 for r in rows if r.get("zone") == z)
            for z in {r.get("zone") for r in rows}
        }

    def expected_best(self, zones: list[str]) -> dict[str, int]:
        """给定参与发布的作业区，每单号最高修订。"""
        best: dict[str, int] = {}
        for r in self.server.gateway.datasets["offset"]:
            if r.get("zone") in zones:
                best[r["order_id"]] = max(best.get(r["order_id"], 0), r["revision"])
        return best

    def stored_staging(self) -> dict[str, int]:
        conn = sqlite3.connect(str(Path(self.state_dir) / "sync_state.db"))
        try:
            rows = conn.execute(
                "SELECT zone, COUNT(*) FROM records WHERE source='offset' "
                "GROUP BY zone"
            ).fetchall()
        finally:
            conn.close()
        return dict(rows)

    def test_zone_split_publishes_single_offset_snapshot(self):
        engine = SyncEngine(self.make(("A", "B", "C", "D")))
        try:
            results = asyncio.run(engine.run())
        finally:
            engine.close()

        # 仍只有一份 offset 快照，跨区按单号折叠为最高修订
        self.assertEqual(set(results), {"offset"})
        info = results["offset"]
        expected = self.expected_best(["A", "B", "C", "D"])
        self.assertEqual(info["record_count"], len(expected))
        self.assertTrue(Path(info["file_path"]).exists())
        self.assertEqual(set(info["zones"]), {"A", "B", "C", "D"})
        # 各区请求都真的发到了带 zone 的接口
        for zone in ("A", "B", "C", "D"):
            self.assertGreaterEqual(
                self.server.gateway.zone_requests.get(zone, 0), 1
            )

        storage = Storage(self.state_dir)
        try:
            snap = storage.read_snapshot("offset")
        finally:
            storage.close()
        by_id = {r["order_id"]: r for r in snap}
        self.assertEqual(len(by_id), len(snap), "发布快照中单号应唯一")
        self.assertEqual(
            {oid: r["revision"] for oid, r in by_id.items()}, expected
        )
        for row in snap:
            self.assertEqual(row["source"], "offset")
            for g in row["gaps"]:
                self.assertEqual(g["unit"], "mm")
        # 跨区复检单号取到别区更高修订
        self.assertEqual(by_id["QC-0007"]["revision"], 4)
        self.assertEqual(by_id["QC-0010"]["revision"], 4)

    def test_slow_zone_does_not_block_others(self):
        self.server.gateway.zone_delay_config = {"A": 0.4}
        engine = SyncEngine(
            self.make(("A", "B", "C", "D"), page_size=2)
        )
        started = time.monotonic()
        try:
            results = asyncio.run(engine.run())
        finally:
            engine.close()
        elapsed = time.monotonic() - started
        log = self.server.gateway.zone_request_log
        a_times = [r["finished"] - r["started"] for r in log if r["zone"] == "A"]
        self.assertTrue(a_times)
        a_first = next(r for r in log if r["zone"] == "A")
        others_done_before_a = [
            r for r in log
            if r["zone"] in ("B", "C", "D")
            and r.get("finished") is not None
            and r["finished"] < a_first["finished"]
        ]
        # 慢区还在等回包时，其他区已采完若干页
        self.assertGreaterEqual(len(others_done_before_a), 2)
        # 共享额度/令牌：所有区走同一网关
        self.assertEqual(
            results["offset"]["record_count"],
            len(self.expected_best(["A", "B", "C", "D"])),
        )
        self.assertGreaterEqual(elapsed, 0.4)

    def test_shared_gateway_quota_across_zones(self):
        # 紧额度下四个区共享同一网关（令牌续期也占额度），最终仍完整
        server = start_server(gateway_quota=3, gateway_window=0.3, token_ttl=300.0)
        base_url = (
            f"http://{server.server_address[0]}:{server.server_address[1]}"
        )
        tmp = tempfile.TemporaryDirectory()
        try:
            engine = SyncEngine(
                make_config(
                    base_url, tmp.name, sources=("offset",),
                    offset_zones=("A", "B", "C", "D"),
                    gateway_quota=3, gateway_window=0.3,
                )
            )
            results = asyncio.run(engine.run())
            grants = engine.gateway.quota.grant_log
            engine.close()
            data_owners = [g for g in grants if str(g).startswith("offset:")]
            for zone in ("A", "B", "C", "D"):
                self.assertIn(f"offset:{zone}", data_owners)
            stats = admin_stats(base_url)
            self.assertEqual(stats["gateway"]["renewals"], 1)
            self.assertGreaterEqual(stats["gateway"]["rejected_429"], 1)
            expected = self.expected_best(["A", "B", "C", "D"])
            self.assertEqual(results["offset"]["record_count"], len(expected))
        finally:
            server.shutdown()
            tmp.cleanup()

    def test_kill_then_resume_and_add_remove_zones(self):
        # 第一轮：只采 A、B；A 采齐后强杀，B 中途停止
        pages: dict[str, int] = {}

        async def abort_after_a(source: str, done: int):
            pages[source] = done
            if source == "offset:A" and done >= 3:
                raise AbortSync("模拟强杀")

        cfg = self.make(("A", "B"), page_size=2, on_page=abort_after_a)
        engine = SyncEngine(cfg)
        with self.assertRaises(AbortSync):
            asyncio.run(engine.run())
        engine.close()

        with sqlite3.connect(str(Path(self.state_dir) / "sync_state.db")) as conn:
            prog = dict(
                conn.execute(
                    "SELECT source, data FROM progress WHERE source LIKE 'offset:%'"
                ).fetchall()
            )
        self.assertTrue(json.loads(prog["offset:zone:A"])["done"])
        self.assertFalse(json.loads(prog["offset:zone:B"])["done"])
        # 中止时没有发布任何快照
        storage = Storage(self.state_dir)
        self.assertEqual(storage.published_sources(), [])
        storage.close()

        # 第二轮：增删区，A 零请求，B 续读，D 从头采集
        self.server.gateway.zone_requests.clear()
        self.server.gateway.reset_zone_log()
        engine = SyncEngine(self.make(("A", "B", "D"), page_size=2))
        try:
            results = asyncio.run(engine.run())
        finally:
            engine.close()

        reqs = self.server.gateway.zone_requests
        self.assertNotIn("A", reqs)
        self.assertGreaterEqual(reqs.get("B", 0), 1)
        self.assertGreaterEqual(reqs.get("D", 0), 1)
        zones = results["offset"]["zones"]
        self.assertTrue(zones["A"]["skipped"])
        self.assertEqual(zones["A"]["pages"], 0)
        # B 的第一次请求从原进度续读，而不是 offset=0
        b_offsets = [
            r["offset"] for r in self.server.gateway.zone_request_log
            if r["zone"] == "B"
        ]
        self.assertTrue(b_offsets)
        self.assertNotEqual(b_offsets[0], 0)
        # 新增 D 从 offset=0 开始
        d_offsets = [
            r["offset"] for r in self.server.gateway.zone_request_log
            if r["zone"] == "D"
        ]
        self.assertEqual(d_offsets[0], 0)
        # 最终仍按 A/B/D 统一发布一份快照，单号折叠为最高修订
        expected = self.expected_best(["A", "B", "D"])
        storage = Storage(self.state_dir)
        try:
            snap = storage.read_snapshot("offset")
        finally:
            storage.close()
        self.assertEqual(
            {r["order_id"]: r["revision"] for r in snap}, expected
        )

    def test_remove_zone_publishes_highest_of_remaining(self):
        engine = SyncEngine(self.make(("A", "B", "C", "D")))
        try:
            asyncio.run(engine.run())
        finally:
            engine.close()
        storage = Storage(self.state_dir)
        try:
            before = {r["order_id"]: r["revision"] for r in storage.read_snapshot("offset")}
        finally:
            storage.close()
        self.assertEqual(before["QC-0010"], 4)  # 更高修订来自 B 区

        # 撤下 B、D：已采齐的 A/C 零请求，快照按剩余区最高修订发布
        self.server.gateway.zone_requests.clear()
        engine = SyncEngine(self.make(("A", "C")))
        try:
            results = asyncio.run(engine.run())
        finally:
            engine.close()
        self.assertEqual(self.server.gateway.zone_requests, {})
        self.assertTrue(all(z["skipped"] for z in results["offset"]["zones"].values()))
        storage = Storage(self.state_dir)
        try:
            after = storage.read_snapshot("offset")
        finally:
            storage.close()
        after_map = {r["order_id"]: r["revision"] for r in after}
        self.assertNotIn("QC-0010", after_map)  # 该单只属于撤下的 D/B
        self.assertEqual(after_map["QC-0007"], 4)  # A 区的更高修订仍在
        # A 撤下后 QC-0007 应回落到 C 区 rev3
        engine = SyncEngine(self.make(("C",)))
        try:
            asyncio.run(engine.run())
        finally:
            engine.close()
        storage = Storage(self.state_dir)
        try:
            only_c = {r["order_id"]: r["revision"] for r in storage.read_snapshot("offset")}
        finally:
            storage.close()
        self.assertEqual(only_c["QC-0007"], 3)
        # 撤下的区释放暂存占用
        self.assertNotIn("B", self.stored_staging())
        self.assertNotIn("D", self.stored_staging())

    def test_readding_zone_starts_fresh_only_for_that_zone(self):
        engine = SyncEngine(self.make(("A", "B", "C")))
        try:
            asyncio.run(engine.run())
        finally:
            engine.close()
        staged_before = self.stored_staging()
        self.assertEqual(staged_before["A"], self.zone_rows()["A"])

        # 配置不变：开启全新一轮，全部区重新采集
        self.server.gateway.zone_requests.clear()
        engine = SyncEngine(self.make(("A", "B", "C")))
        try:
            asyncio.run(engine.run())
        finally:
            engine.close()
        self.assertEqual(
            set(self.server.gateway.zone_requests), {"A", "B", "C"}
        )

        # 改配为 A/B/C/D：已采齐区零请求，暂存只随 D 的数据量增长
        self.server.gateway.zone_requests.clear()
        engine = SyncEngine(self.make(("A", "B", "C", "D")))
        try:
            asyncio.run(engine.run())
        finally:
            engine.close()
        self.assertEqual(set(self.server.gateway.zone_requests), {"D"})
        staged_after = self.stored_staging()
        self.assertEqual(staged_after["D"], self.zone_rows()["D"])
        for zone in ("A", "B", "C"):
            self.assertEqual(staged_after[zone], staged_before[zone])
        # 总暂存增量恰好等于新增区的原始行数
        self.assertEqual(
            sum(staged_after.values()) - sum(staged_before.values()),
            self.zone_rows()["D"],
        )

    def test_old_snapshot_readable_until_round_completes(self):
        engine = SyncEngine(self.make(("A", "B", "C", "D")))
        try:
            asyncio.run(engine.run())
        finally:
            engine.close()
        storage = Storage(self.state_dir)
        before = storage.read_snapshot("offset")
        storage.close()

        # 新的一轮中途强杀：旧快照必须仍可读且内容不变
        async def abort_hook(source: str, done: int):
            raise AbortSync("kill on first page")

        cfg = self.make(("A", "B", "C", "D"), on_page=abort_hook)
        engine = SyncEngine(cfg)
        with self.assertRaises(AbortSync):
            asyncio.run(engine.run())
        engine.close()

        storage = Storage(self.state_dir)
        try:
            mid = storage.read_snapshot("offset")
            merged = storage.read_merged()
        finally:
            storage.close()
        self.assertEqual(
            [(r["order_id"], r["revision"]) for r in mid],
            [(r["order_id"], r["revision"]) for r in before],
        )
        self.assertTrue(merged)
        # 其他供应商（如有）的已发布快照仍各自独立可读
        self.assertIn("offset", Storage(self.state_dir).published_sources())

    def test_legacy_single_endpoint_mode_still_works(self):
        # 不配置作业区：沿用旧单接口（不带 zone），行为与历史一致
        engine = SyncEngine(
            make_config(self.base_url, self.state_dir, sources=("offset",))
        )
        try:
            results = asyncio.run(engine.run())
        finally:
            engine.close()
        expected = len({r["order_id"] for r in self.server.gateway.datasets["offset"]})
        self.assertEqual(results["offset"]["record_count"], expected)
        self.assertEqual(self.server.gateway.zone_requests, {})
        storage = Storage(self.state_dir)
        try:
            rows = storage.read_snapshot("offset")
        finally:
            storage.close()
        self.assertEqual(len(rows), expected)
        self.assertTrue(all(r["source"] == "offset" for r in rows))


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

    def test_cli_serve_sync_read_with_offset_zones(self):
        """内置模拟源一键验证：拆区发布、配置增加区后只补新区。"""
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
                    "--offset-zones", "A,B,C",
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
                cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
                self.assertEqual(cfg["offset_zones"], ["A", "B", "C"])

                sync = subprocess.run(
                    [sys.executable, "-m", "crane_sync", "sync",
                     "--config", str(cfg_path)],
                    cwd=str(ROOT), env=env, text=True,
                    capture_output=True, timeout=30,
                )
                self.assertEqual(sync.returncode, 0, sync.stderr)
                self.assertIn("同步完成", sync.stdout)
                self.assertIn("offset", sync.stdout)
                for zone in ("A", "B", "C"):
                    self.assertIn(f"作业区 {zone}", sync.stdout)

                read = subprocess.run(
                    [sys.executable, "-m", "crane_sync", "read",
                     "--state-dir", str(state_dir), "--source", "offset"],
                    cwd=str(ROOT), env=env, text=True,
                    capture_output=True, timeout=15,
                )
                self.assertEqual(read.returncode, 0, read.stderr)
                self.assertIn("来源 offset 的最新已发布快照", read.stdout)
                self.assertIn("mm", read.stdout)

                # 改配增加 D 区：A/B/C 已采齐零请求，只补 D
                stats_before = admin_stats(
                    f"http://127.0.0.1:{cfg['base_url'].rsplit(':', 1)[1]}"
                )
                sync2 = subprocess.run(
                    [sys.executable, "-m", "crane_sync", "sync",
                     "--config", str(cfg_path),
                     "--offset-zones", "A,B,C,D"],
                    cwd=str(ROOT), env=env, text=True,
                    capture_output=True, timeout=30,
                )
                self.assertEqual(sync2.returncode, 0, sync2.stderr)
                self.assertIn("作业区 D", sync2.stdout)
                self.assertEqual(sync2.stdout.count("已采齐跳过"), 3)
                stats_after = admin_stats(
                    f"http://127.0.0.1:{cfg['base_url'].rsplit(':', 1)[1]}"
                )
                reqs_before = stats_before["offset_zone_requests"]
                reqs_after = stats_after["offset_zone_requests"]
                for zone in ("A", "B", "C"):
                    self.assertEqual(
                        reqs_after.get(zone, 0), reqs_before.get(zone, 0)
                    )
                self.assertGreaterEqual(reqs_after.get("D", 0), 1)
            finally:
                serve.terminate()
                try:
                    serve.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    serve.kill()


if __name__ == "__main__":
    unittest.main(verbosity=2)
