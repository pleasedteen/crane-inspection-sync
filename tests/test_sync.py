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
            # 确保偏移源至少落盘一页后再中止，断言才有意义
            # （其他来源推进更快时，不能先被它们的页数触发）
            if page_log.get("offset", 0) >= 1 and sum(page_log.values()) >= 4:
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


class SnapshotBatchingTests(unittest.TestCase):
    """快照分批归档必须与一次性折叠结果完全一致。"""

    def test_batched_publish_keeps_every_order_with_many_revisions(self):
        # 30 个单号 x 12 个修订，批大小小于单号数/修订数，
        # 覆盖"同一单号多条修订跨批"的边界
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(tmp)
            try:
                for rev in range(1, 13):
                    storage.upsert_records(
                        "offset",
                        [
                            InspectionOrder(
                                f"QC-{i:04d}", rev, "offset", "2026-11-10T08:00:00",
                                [{"point": "rail", "value_mm": float(rev), "unit": "mm"}],
                            )
                            for i in range(30)
                        ],
                    )
                for batch in (1, 4, 7, 200):
                    info = storage.publish_snapshot("offset", "jsonl", batch)
                    rows = storage.read_snapshot("offset")
                    self.assertEqual(info["record_count"], 30, f"batch={batch}")
                    self.assertEqual(len(rows), 30, f"batch={batch}")
                    self.assertEqual(
                        {r["order_id"] for r in rows},
                        {f"QC-{i:04d}" for i in range(30)},
                    )
                    # 每个单号只保留最高修订
                    self.assertEqual({r["revision"] for r in rows}, {12})
            finally:
                storage.close()


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
                serve.stdout.close()
                serve.stderr.close()

    def test_cli_bulk_body_serve_sync_read(self):
        """内置模拟源直接提供同秒 128 MiB 大正文，命令行全链路可跑通。"""
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "mock.json"
            state_dir = Path(tmp) / "state"
            env = dict(os.environ, PYTHONPATH=str(ROOT))
            serve = subprocess.Popen(
                [
                    sys.executable, "-m", "crane_sync", "serve",
                    "--config-out", str(cfg_path),
                    "--state-dir", str(state_dir),
                    "--offset-bulk-count", "128",
                    "--offset-record-bytes", str(128 * 1024),
                    "--stream-chunk", "8",
                    "--gateway-quota", "2000",
                    "--window-quota", "2000",
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
                # 大正文需要的页大小比默认配置大，覆盖为一次读完整轮
                raw = json.loads(cfg_path.read_text(encoding="utf-8"))
                raw.update(page_size=128)
                cfg_path.write_text(
                    json.dumps(raw, ensure_ascii=False), encoding="utf-8"
                )

                sync = subprocess.run(
                    [sys.executable, "-m", "crane_sync", "sync",
                     "--config", str(cfg_path)],
                    cwd=str(ROOT), env=env, text=True,
                    capture_output=True, timeout=120,
                )
                self.assertEqual(sync.returncode, 0, sync.stderr)
                self.assertIn("offset", sync.stdout)
                read = subprocess.run(
                    [sys.executable, "-m", "crane_sync", "read",
                     "--state-dir", str(state_dir), "--source", "offset"],
                    cwd=str(ROOT), env=env, text=True,
                    capture_output=True, timeout=30,
                )
                self.assertEqual(read.returncode, 0, read.stderr)
                self.assertIn("共 128 张巡检单", read.stdout)
                self.assertIn("BULK-00000", read.stdout)
            finally:
                serve.terminate()
                try:
                    serve.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    serve.kill()
                serve.stdout.close()
                serve.stderr.close()



# ----------------------------------------------------------------------
# 大正文（同一秒集中到达）与来源隔离
# ----------------------------------------------------------------------
BULK_TARGET_BYTES = 128 * 1024 * 1024
BULK_RECORDS = 1024


class BulkBodyTests(unittest.TestCase):
    """默认 JSON Lines 同步 128 MiB 同秒正文，峰值 RSS 增量 ≤ 64 MiB。"""

    @classmethod
    def setUpClass(cls):
        cls.server = start_server(
            offset_bulk_count=BULK_RECORDS,
            offset_record_bytes=BULK_TARGET_BYTES // BULK_RECORDS,
            stream_chunk=8,
        )
        cls.base_url = (
            f"http://{cls.server.server_address[0]}:{cls.server.server_address[1]}"
        )

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def _config(self, state_dir: str, mode: str) -> dict:
        return {
            "mode": mode,
            "base_url": self.base_url,
            "state_dir": state_dir,
            "page_size": BULK_RECORDS,
            "gateway_quota": 100000,
            "gateway_window": 1.0,
            "window_source_quota": 100000,
            "window_source_window": 1.0,
            "snapshot_format": "jsonl",
            "sources": [] if mode == "idle" else ["offset"],
        }

    def _run_probe(self, cfg: dict) -> dict:
        proc = subprocess.run(
            [sys.executable, str(ROOT / "tests" / "_sync_probe.py"),
             json.dumps(cfg)],
            cwd=str(ROOT),
            env=dict(os.environ, PYTHONPATH=str(ROOT)),
            capture_output=True,
            text=True,
            timeout=180,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return json.loads(proc.stdout.strip().splitlines()[-1])

    def test_peak_rss_bounded_for_128mib_page(self):
        with tempfile.TemporaryDirectory() as tmp:
            idle = self._run_probe(self._config(str(Path(tmp) / "idle"), "idle"))
            full = self._run_probe(self._config(str(Path(tmp) / "sync"), "sync"))
        self.assertEqual(full["results"]["offset"]["record_count"], BULK_RECORDS)
        delta = full["hwm"] - idle["hwm"]
        self.assertLessEqual(
            delta,
            64 * 1024 * 1024,
            f"同步端峰值 RSS 增量 {delta / 2**20:.1f} MiB 超过 64 MiB",
        )

    def test_bulk_snapshot_content_complete(self):
        with tempfile.TemporaryDirectory() as tmp:
            full = self._run_probe(self._config(tmp, "sync"))
            info = full["results"]["offset"]
            path = Path(info["file_path"])
            self.assertTrue(path.exists())
            lines = path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), BULK_RECORDS)
            first = json.loads(lines[0])
            self.assertEqual(first["source"], "offset")
            self.assertGreater(len(first["gaps"]), 0)
            # 同一秒集中到达：所有正文的时间戳相同
            stamps = {json.loads(line)["inspected_at"] for line in lines}
            self.assertEqual(len(stamps), 1)
            # 快照记录数与发布元数据一致
            storage = Storage(tmp)
            try:
                self.assertEqual(
                    len(storage.read_snapshot("offset")), BULK_RECORDS
                )
            finally:
                storage.close()


class SourceIsolationTests(unittest.TestCase):
    """一个来源写快照卡住时，其他来源仍能落盘并独立发布。"""

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

    def test_stuck_publish_does_not_block_other_sources(self):
        import threading

        stuck = threading.Event()
        release = threading.Event()
        published_at: dict[str, float] = {}
        start = time.monotonic()

        def publish_hook(source: str) -> None:
            if source == "offset":
                stuck.set()
                release.wait(15)
            published_at[source] = time.monotonic() - start

        cfg = make_config(
            self.base_url, self.state_dir, publish_hook=publish_hook
        )
        engine = SyncEngine(cfg)
        result: dict = {}

        def run() -> None:
            try:
                result["value"] = asyncio.run(engine.run())
            finally:
                engine.close()

        worker = threading.Thread(target=run)
        worker.start()
        try:
            self.assertTrue(stuck.wait(15), "offset 未进入发布阶段")
            # offset 卡住期间，另外两个来源必须完成落盘并各自发布
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                storage = Storage(self.state_dir)
                try:
                    published = storage.published_sources()
                    progress = {
                        name: storage.get_progress(name)
                        for name in ("cursor", "window")
                    }
                finally:
                    storage.close()
                if set(published) == {"cursor", "window"}:
                    break
                time.sleep(0.05)
            self.assertEqual(set(published), {"cursor", "window"})
            for name, prog in progress.items():
                self.assertIsNotNone(prog, f"{name} 没有落盘进度")
                self.assertTrue(prog.get("done"), f"{name} 未读完")
            # 卡住的来源在此期间确实没有发布，其他来源则各自按时发布
            self.assertNotIn("offset", published_at)
            self.assertLess(published_at["cursor"], 10)
            self.assertLess(published_at["window"], 10)
            storage = Storage(self.state_dir)
            try:
                # 其他来源的快照内容完整，可独立读取
                self.assertTrue(storage.read_snapshot("cursor"))
                self.assertTrue(storage.read_snapshot("window"))
                with self.assertRaises(RuntimeError):
                    storage.read_snapshot("offset")
            finally:
                storage.close()
        finally:
            release.set()
            worker.join(30)
        self.assertFalse(worker.is_alive(), "同步线程未在放行后结束")
        self.assertEqual(
            set(result["value"]), {"offset", "cursor", "window"}
        )
        # 放行后 offset 也完成发布，且发布时间明显晚于其他来源
        storage = Storage(self.state_dir)
        try:
            self.assertTrue(storage.read_snapshot("offset"))
            stamps = {
                name: storage.latest_snapshot_info(name)["published_at"]
                for name in ("offset", "cursor", "window")
            }
        finally:
            storage.close()
        self.assertGreater(stamps["offset"], stamps["cursor"])
        self.assertGreater(stamps["offset"], stamps["window"])


class AbortRecoveryTests(unittest.TestCase):
    """中止后续读：已发布快照可读，重跑补全且结果与一次跑完一致。"""

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

    def test_abort_then_immediate_resume(self):
        # 第一次只跑完 offset 并发布，随后在 cursor/window 途中中止
        page_log: dict[str, int] = {}

        async def abort_hook(source: str, pages: int):
            page_log[source] = pages
            if source == "offset":
                return
            if sum(v for k, v in page_log.items() if k != "offset") >= 2:
                raise AbortSync("模拟强制中止")

        engine = SyncEngine(
            make_config(
                self.base_url, self.state_dir, on_page=abort_hook
            )
        )
        with self.assertRaises(AbortSync):
            asyncio.run(engine.run())
        engine.close()

        storage = Storage(self.state_dir)
        try:
            published_before = storage.published_sources()
            # 中止点之前的进度已落盘
            self.assertIsNotNone(storage.get_progress("offset"))
            # 尚未完成的来源没有发布半成品
            for name in ("cursor", "window"):
                if name not in published_before:
                    with self.assertRaises(RuntimeError):
                        storage.read_snapshot(name)
        finally:
            storage.close()

        # 立即重跑：从已保存进度续读，最终与一次跑完的结果一致
        engine = SyncEngine(make_config(self.base_url, self.state_dir))
        try:
            results = asyncio.run(engine.run())
        finally:
            engine.close()
        unique_sizes = {
            k: unique_order_count(v)
            for k, v in self.server.gateway.datasets.items()
        }
        self.assertEqual(set(results), {"offset", "cursor", "window"})
        for name, info in results.items():
            self.assertEqual(info["record_count"], unique_sizes[name])
        storage = Storage(self.state_dir)
        try:
            for name in ("offset", "cursor", "window"):
                rows = storage.read_snapshot(name)
                self.assertEqual(len(rows), unique_sizes[name])
                # 每单号仅一条（最高修订）
                self.assertEqual(
                    len({r["order_id"] for r in rows}), len(rows)
                )
        finally:
            storage.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
