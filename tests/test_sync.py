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


def admin_post(base_url: str, path: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload or {}).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url}{path}", data=data, method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read().decode())


async def wait_until(predicate, timeout: float = 5.0, interval: float = 0.02) -> None:
    """轮询直到 predicate() 为真，超时抛出 AssertionError。"""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("等待条件未在超时内满足")


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


class LimiterGatewayUnitTests(unittest.TestCase):
    """额度队列取消清理与共享令牌在途续期去重的单元级验证。"""

    def test_quota_cancelled_waiter_does_not_leak_queue(self):
        from crane_sync.limits import WindowQuota

        quota = WindowQuota(limit=1, window_seconds=10.0)

        async def scenario():
            # 占满唯一额度
            await quota.acquire("holder")
            # 两个来源排队等待，随后取消
            waiter = asyncio.ensure_future(quota.acquire("cancelled-src"))
            await asyncio.sleep(0.05)
            self.assertIn("cancelled-src", quota._waiting)
            waiter.cancel()
            try:
                await waiter
            except asyncio.CancelledError:
                pass
            # 排队痕迹必须被清理，否则该来源会被永久记为等待者
            self.assertNotIn("cancelled-src", quota._waiting)
            self.assertEqual(quota._waiting, {})

        asyncio.run(scenario())

    def test_same_generation_renewed_once_even_across_cancellation(self):
        from crane_sync.limits import CooldownGate, SharedGateway, WindowQuota

        async def scenario():
            issued = {"count": 0}
            renewed = asyncio.Event()
            release = asyncio.Event()

            async def provider():
                # 模拟在途续期挂起，等待测试显式放行
                issued["count"] += 1
                if issued["count"] >= 2:
                    renewed.set()
                    await release.wait()
                return f"tok-{issued['count']}"

            gateway = SharedGateway(
                WindowQuota(100, 10.0), CooldownGate(), provider
            )
            # 首次签发
            token, gen1 = await gateway._current_token()
            self.assertEqual((token, gen1), ("tok-1", 1))

            # 两个来源同时为世代 1 触发续期，其中一个等待者随即被取消
            first = asyncio.ensure_future(gateway._renew_token(gen1))
            await wait_until(renewed.is_set)
            second = asyncio.ensure_future(gateway._renew_token(gen1))
            await asyncio.sleep(0.05)
            second.cancel()
            try:
                await second
            except asyncio.CancelledError:
                pass
            # 取消等待者不得取消在途续期任务
            self.assertEqual(issued["count"], 2)
            self.assertEqual(set(gateway._renewal_tasks), {gen1})

            # 放行在途任务：新令牌经完成回调进入网关
            release.set()
            new_token = await first
            self.assertEqual(new_token, "tok-2")
            await asyncio.sleep(0)  # 让 done 回调执行
            self.assertEqual(gateway._token, "tok-2")
            self.assertEqual(gateway._generation, 2)
            self.assertIn(gen1, gateway._renewed_generation)

            # 下一轮（模拟重跑）再为世代 1 请求续期：直接复用，不再发请求
            reused = await gateway._renew_token(gen1)
            self.assertEqual(reused, "tok-2")
            self.assertEqual(issued["count"], 2)

        asyncio.run(scenario())

    def test_failed_renewal_allows_retry_once(self):
        from crane_sync.limits import CooldownGate, SharedGateway, WindowQuota

        async def scenario():
            calls = {"n": 0}

            async def provider():
                # 首次签发成功；世代 1 的第一次续期失败，第二次成功
                calls["n"] += 1
                if calls["n"] == 2:
                    raise RuntimeError("网关暂时不可用")
                return f"tok-{calls['n']}"

            gateway = SharedGateway(
                WindowQuota(100, 10.0), CooldownGate(), provider
            )
            token, gen = await gateway._current_token()
            self.assertEqual((token, gen), ("tok-1", 1))

            bad = asyncio.ensure_future(gateway._renew_token(gen))
            with self.assertRaises(RuntimeError):
                await bad
            await asyncio.sleep(0)  # 让 done 回调清理失败标记
            self.assertNotIn(gen, gateway._renewed_generation)
            self.assertNotIn(gen, gateway._renewal_tasks)

            again = await gateway._renew_token(gen)
            self.assertEqual(again, "tok-3")
            self.assertEqual(gateway._generation, 2)
            self.assertIn(gen, gateway._renewed_generation)

        asyncio.run(scenario())


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


class MultiRoundSessionTests(unittest.TestCase):
    """同一引擎多轮运行、取消后立即重跑、在途请求挂起场景。"""

    def setUp(self):
        self.server = start_server()
        self.base_url = (
            f"http://{self.server.server_address[0]}:{self.server.server_address[1]}"
        )
        self.tmp = tempfile.TemporaryDirectory()
        self.state_dir = self.tmp.name

    def tearDown(self):
        # 放行所有可能仍挂起的请求，避免线程泄漏干扰后续用例
        self.server.hang.release()
        self.server.shutdown()
        self.tmp.cleanup()

    def test_same_engine_runs_multiple_rounds_in_one_loop(self):
        # 回归：旧实现每轮 run() 都 shutdown 执行器，第二轮必然 RuntimeError
        engine = SyncEngine(make_config(self.base_url, self.state_dir))
        try:
            async def scenario():
                first = await engine.run()
                second = await engine.run()
                return first, second

            first, second = asyncio.run(scenario())
        finally:
            engine.close()

        self.assertEqual(set(first), {"offset", "cursor", "window"})
        self.assertEqual(set(second), {"offset", "cursor", "window"})
        # 第二轮是完整重跑：每源都发布了新的快照
        for name in ("offset", "cursor", "window"):
            self.assertNotEqual(
                first[name]["snapshot_id"], second[name]["snapshot_id"]
            )
            self.assertEqual(
                second[name]["record_count"], first[name]["record_count"]
            )

    def test_cancel_rerun_keeps_quota_cooldown_and_checkpoint(self):
        # 紧额度必然出现真实 429 冷却；取消后立即重跑，验证额度窗口、
        # 冷却状态与已落盘续点全部沿用同一套对象/数据。
        server = start_server(gateway_quota=3, gateway_window=1.0)
        base_url = (
            f"http://{server.server_address[0]}:{server.server_address[1]}"
        )
        tmp = tempfile.TemporaryDirectory()
        try:
            cfg = make_config(
                base_url, tmp.name, gateway_quota=3, gateway_window=1.0
            )
            engine = SyncEngine(cfg)
            saved_offset_holder: dict[str, int] = {}
            try:
                async def scenario():
                    quota = engine.gateway.quota
                    cooldown = engine.gateway.cooldown
                    saw_cooldown = {"value": False}

                    # 第一轮：offset 两页后中止；小额度下其他来源的请求
                    # 被 429 拒绝时会把冷却写进共享 CooldownGate
                    async def abort_after_two(source, pages):
                        if cooldown.remaining() > 0:
                            saw_cooldown["value"] = True
                        if source == "offset" and pages >= 2:
                            raise AbortSync("中止第一轮")

                    engine.config.on_page = abort_after_two
                    with self.assertRaises(AbortSync):
                        await engine.run()

                    # 取消后额度窗口里已有发放记录，且对象就是同一个
                    self.assertGreater(len(quota._hits), 0)
                    self.assertTrue(
                        saw_cooldown["value"],
                        "紧额度下本轮应实际观察到 429 冷却",
                    )

                    storage = Storage(tmp.name)
                    try:
                        checkpoint = storage.get_progress("offset")
                    finally:
                        storage.close()
                    self.assertIsNotNone(checkpoint)
                    saved_offset = checkpoint["next_offset"]
                    self.assertEqual(saved_offset, 20)  # 两页 * page_size 10
                    saved_offset_holder["value"] = saved_offset

                    # 立即重跑：同一引擎、同一事件循环、同一套限额对象
                    engine.config.on_page = None
                    results = await engine.run()
                    self.assertIs(engine.gateway.quota, quota)
                    self.assertIs(engine.gateway.cooldown, cooldown)
                    return results

                results = asyncio.run(scenario())
            finally:
                engine.close()

            unique_sizes = {
                k: unique_order_count(v)
                for k, v in server.gateway.datasets.items()
            }
            for name, info in results.items():
                self.assertEqual(info["record_count"], unique_sizes[name])
            # 续点有效：重跑不会再从头请求 offset=0
            offsets = server.hang.snapshot()["offset_params"]
            self.assertNotIn(0, offsets[1:])
            self.assertIn(saved_offset_holder["value"], offsets)
        finally:
            server.hang.release()
            server.shutdown()
            tmp.cleanup()

    def test_hung_pagination_does_not_block_other_sources_or_rerun(self):
        engine = SyncEngine(
            make_config(self.base_url, self.state_dir, http_timeout=30.0)
        )
        try:
            async def scenario():
                # 让 offset 的下一页请求在服务端挂起（挂起不占网关额度）
                async def arm_hang(source, pages):
                    if source == "offset" and pages == 1:
                        await asyncio.get_running_loop().run_in_executor(
                            None,
                            lambda: admin_post(
                                self.base_url,
                                "/mock/admin/hang",
                                {"path": "/offset/orders", "count": 1},
                            ),
                        )

                engine.config.on_page = arm_hang
                run_task = asyncio.ensure_future(engine.run())

                # 等待 offset 请求真正进入挂起
                await wait_until(lambda: self.server.hang.waiting() >= 1)

                storage = Storage(self.state_dir)
                try:
                    progress_before = storage.get_progress("offset")
                finally:
                    storage.close()
                self.assertIsNotNone(progress_before)

                # 取消整个 run：旧轮任务被 drain，挂起的旧分页留在服务端
                run_task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await run_task

                # 取消后旧轮不再派发 offset 请求
                calls_at_cancel = self.server.hang.snapshot()["path_calls"].get(
                    "/offset/orders", 0
                )
                await asyncio.sleep(0.15)
                self.assertEqual(
                    self.server.hang.snapshot()["path_calls"].get(
                        "/offset/orders", 0
                    ),
                    calls_at_cancel,
                )
                # 旧轮也没有再落盘（offset 进度停在取消前的值，且未发布）
                storage = Storage(self.state_dir)
                try:
                    self.assertEqual(
                        storage.get_progress("offset"), progress_before
                    )
                    self.assertNotIn("offset", storage.published_sources())
                finally:
                    storage.close()

                # 立即重跑：挂起的旧分页不阻塞新轮，cursor/window 可先行
                engine.config.on_page = None
                results = await engine.run()
                return results

            results = asyncio.run(scenario())
        finally:
            engine.close()

        unique_sizes = {
            k: unique_order_count(v)
            for k, v in self.server.gateway.datasets.items()
        }
        for name, info in results.items():
            self.assertEqual(info["record_count"], unique_sizes[name])
        # 重跑沿用续点：取消前已完成第一页（offset=10），重跑不会再请求 offset=0
        offsets = self.server.hang.snapshot()["offset_params"]
        self.assertEqual(offsets[0], 0)
        self.assertNotIn(0, offsets[1:])
        # 旧的挂起请求最终被放行过（tearDown/重跑过程中），且没有产生重复续点请求
        self.assertEqual(offsets.count(0), 1)

    def test_task_cancel_then_immediate_rerun_completes(self):
        # 不借助挂起：直接从外部 cancel run() 协程，引擎必须能立即重跑。
        engine = SyncEngine(make_config(self.base_url, self.state_dir))
        try:
            async def scenario():
                async def cancel_soon(source, pages):
                    if source == "offset" and pages >= 1:
                        # 在下一个事件循环节拍从外部取消本轮 run
                        task.cancel()

                engine.config.on_page = cancel_soon
                task = asyncio.ensure_future(engine.run())
                with self.assertRaises(asyncio.CancelledError):
                    await task

                # 引擎已解锁，可立即开始下一轮
                self.assertFalse(engine._running)
                engine.config.on_page = None
                results = await engine.run()
                return results

            results = asyncio.run(scenario())
        finally:
            engine.close()

        unique_sizes = {
            k: unique_order_count(v)
            for k, v in self.server.gateway.datasets.items()
        }
        for name, info in results.items():
            self.assertEqual(info["record_count"], unique_sizes[name])
        storage = Storage(self.state_dir)
        try:
            self.assertEqual(
                set(storage.published_sources()),
                {"offset", "cursor", "window"},
            )
        finally:
            storage.close()

    def test_hung_token_renewal_reused_after_cancel_rerun(self):
        # 第一轮：第一页后强制令牌失效，并让 401 触发的续期请求挂起；
        # 取消后立即重跑——在途续期完成后必须被新轮复用，同一失效令牌
        # 整个过程只续期一次。
        engine = SyncEngine(
            make_config(self.base_url, self.state_dir, http_timeout=30.0)
        )
        try:
            async def scenario():
                armed = {"done": False}

                async def expire_and_hang(source, pages):
                    if armed["done"]:
                        return
                    armed["done"] = True
                    loop = asyncio.get_running_loop()

                    def _admin():
                        admin_post(
                            self.base_url, "/mock/admin/expire-token"
                        )
                        admin_post(
                            self.base_url,
                            "/mock/admin/hang",
                            {"path": "/gateway/token", "count": 1},
                        )

                    await loop.run_in_executor(None, _admin)

                engine.config.on_page = expire_and_hang
                run_task = asyncio.ensure_future(engine.run())

                # 等待续期 POST 在服务端挂起
                await wait_until(
                    lambda: self.server.hang.snapshot()["path_calls"].get(
                        "/gateway/token", 0
                    ) >= 2  # 初次签发 + 一次续期
                )
                await wait_until(lambda: self.server.hang.waiting() >= 1)

                renewals_at_cancel = admin_stats(self.base_url)["gateway"][
                    "renewals"
                ]
                # 此刻只有初次签发完成，续期还挂着
                self.assertEqual(renewals_at_cancel, 1)
                old_generation = engine.gateway._generation

                # 取消：续期任务不在取消范围内，继续在途
                run_task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await run_task

                # 立即重跑（先不释放挂起）：新轮等待同一个在途续期，
                # 不会自己再发一个 token POST
                engine.config.on_page = None
                rerun = asyncio.ensure_future(engine.run())
                await asyncio.sleep(0.2)
                token_calls_before_release = self.server.hang.snapshot()[
                    "path_calls"
                ].get("/gateway/token", 0)
                self.assertFalse(rerun.done(), "续期挂起期间重跑应继续等待")

                # 放行：在途续期落地，重跑直接复用新令牌跑完全程
                await asyncio.get_running_loop().run_in_executor(
                    None, lambda: admin_post(self.base_url, "/mock/admin/release")
                )
                results = await rerun

                # 整个过程：初次签发 1 次 + 同一失效世代续期 1 次
                stats = admin_stats(self.base_url)
                self.assertEqual(stats["gateway"]["renewals"], 2)
                self.assertEqual(
                    self.server.hang.snapshot()["path_calls"].get(
                        "/gateway/token", 0
                    ),
                    token_calls_before_release,
                )
                # 网关状态已被在途任务的完成回调更新
                self.assertEqual(engine.gateway._generation, old_generation + 1)
                self.assertIn(old_generation, engine.gateway._renewed_generation)
                return results

            results = asyncio.run(scenario())
        finally:
            engine.close()

        unique_sizes = {
            k: unique_order_count(v)
            for k, v in self.server.gateway.datasets.items()
        }
        for name, info in results.items():
            self.assertEqual(info["record_count"], unique_sizes[name])


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
