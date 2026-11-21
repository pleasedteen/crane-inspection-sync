"""诊断能力测试：请求尝试日志、200 条上限、强停对账与 CLI 输出。"""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from crane_sync.diagnostics import build_report, render_report
from crane_sync.engine import SyncConfig, SyncEngine
from crane_sync.mock_server import MockServer
from crane_sync.storage import ATTEMPT_HISTORY_CAP, Storage

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
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def make_config(base_url: str, state_dir: str) -> SyncConfig:
    return SyncConfig(
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


class AttemptHistoryTests(unittest.TestCase):
    def test_finished_attempts_capped_but_inflight_kept(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(tmp)
            run_id = storage.begin_run()
            # 大量已结束请求
            for i in range(ATTEMPT_HISTORY_CAP + 40):
                aid = storage.begin_attempt(
                    run_id, "offset", "paging", "/offset/orders", f"offset={i}"
                )
                storage.finish_attempt(aid, "ok", 200, True)
            self.assertLessEqual(
                storage.count_attempts("finished"), ATTEMPT_HISTORY_CAP
            )
            # 当前等待（inflight）不受上限影响
            live = storage.begin_attempt(
                run_id, "cursor", "paging", "/cursor/orders", "cursor=v1:0:x"
            )
            self.assertIsNotNone(storage.attempt(live))
            self.assertEqual(storage.count_attempts("inflight"), 1)
            # 续点独立保留，不随请求上限淘汰
            storage.set_progress("offset", {"next_offset": 40, "done": False})
            self.assertEqual(
                storage.get_progress("offset"),
                {"next_offset": 40, "done": False},
            )
            storage.close()

    def test_reconcile_marks_vanished_process_attempt_unknown(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(tmp)
            # 伪造一次已被强停的运行（pid 必然不存在）
            storage._conn.execute(
                "INSERT INTO runs(run_id,pid,started_at,status,detail) "
                "VALUES('deadrun', 99999999, '2026-01-01T00:00:00', "
                "'running', '{}')"
            )
            aid = storage.begin_attempt(
                "deadrun", "window", "paging", "/window/orders",
                "from=a&to=b", {"page": 2},
            )
            marked = storage.reconcile_attempts()
            self.assertTrue(any(m["attempt_id"] == aid for m in marked))
            rec = storage.attempt(aid)
            self.assertEqual(rec["state"], "unknown")
            self.assertEqual(rec["outcome"], "unknown")
            self.assertFalse(rec["reply_confirmed"])
            self.assertEqual(rec["progress_before"], None)
            # 运行状态被收敛为 interrupted，便于重跑后辨认
            self.assertEqual(storage.get_run("deadrun")["status"], "interrupted")
            # 第二次对账不应重复标记
            self.assertEqual(storage.reconcile_attempts(), [])
            storage.close()

    def test_reconcile_leaves_live_inflight_alone(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(tmp)
            run_id = storage.begin_run()  # 当前进程、当前运行
            aid = storage.begin_attempt(
                run_id, "offset", "paging", "/offset/orders", "offset=0"
            )
            self.assertEqual(storage.reconcile_attempts(), [])
            self.assertEqual(storage.attempt(aid)["state"], "inflight")
            storage.finish_run(run_id)
            storage.close()


class DiagnoseAfterSyncTests(unittest.TestCase):
    def setUp(self):
        self.server = start_server()
        host, port = self.server.server_address
        self.base_url = f"http://{host}:{port}"
        self.tmp = tempfile.TemporaryDirectory()
        self.state_dir = self.tmp.name

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.tmp.cleanup()

    def _sync(self):
        engine = SyncEngine(make_config(self.base_url, self.state_dir))
        try:
            return asyncio.run(engine.run())
        finally:
            engine.close()

    def test_report_distinguishes_sources_and_progress(self):
        self._sync()
        storage = Storage(self.state_dir)
        try:
            report = build_report(storage, attempt_limit=20)
        finally:
            storage.close()

        by_source = {s["source"]: s for s in report["sources"]}
        for name in ("offset", "cursor", "window"):
            src = by_source[name]
            self.assertIsNotNone(src["progress"], f"{name} 缺续点")
            self.assertIsNotNone(src["publication"], f"{name} 未发布")
            # 已确认回包最终形成当前可续读进度
            relations = {a.get("progress_relation") for a in src["recent_attempts"]}
            self.assertIn("current", relations)
            # 状态可见（完成后为 done）
            self.assertEqual(src["state"]["state"], "done")
        # 网关令牌请求单独成源，不混入数据来源
        self.assertIn("gateway", by_source)
        # 总数不超过上限 + 在途
        self.assertLessEqual(
            report["attempt_count"],
            ATTEMPT_HISTORY_CAP + report["inflight_count"],
        )
        text = render_report(report)
        self.assertIn("[offset]", text)
        self.assertIn("进度已落盘（当前续点）", text)
        self.assertIn("续点", text)
        # 状态字典覆盖三种等待，供人在同步过程中辨识
        from crane_sync.diagnostics import STATE_LABELS
        self.assertEqual(STATE_LABELS["waiting_quota"], "等额度")
        self.assertEqual(STATE_LABELS["cooldown"], "冷却")
        self.assertEqual(STATE_LABELS["waiting_reply"], "等 HTTP 回包")

    def test_cli_diagnose_json_and_text(self):
        self._sync()
        env = dict(os.environ, PYTHONPATH=str(ROOT))
        js = subprocess.run(
            [sys.executable, "-m", "crane_sync", "diagnose",
             "--state-dir", self.state_dir, "--json"],
            cwd=str(ROOT), env=env, text=True, capture_output=True, timeout=20,
        )
        self.assertEqual(js.returncode, 0, js.stderr)
        report = json.loads(js.stdout)
        self.assertIn("sources", report)
        self.assertGreaterEqual(len(report["sources"]), 3)
        self.assertEqual(report["inflight_count"], 0)

        text = subprocess.run(
            [sys.executable, "-m", "crane_sync", "diagnose",
             "--state-dir", self.state_dir, "--source", "offset"],
            cwd=str(ROOT), env=env, text=True, capture_output=True, timeout=20,
        )
        self.assertEqual(text.returncode, 0, text.stderr)
        self.assertIn("offset", text.stdout)
        self.assertIn("续点", text.stdout)


class SelfTestCommandTests(unittest.TestCase):
    def test_builtin_selftest_covers_wait_kill_resume(self):
        env = dict(os.environ, PYTHONPATH=str(ROOT))
        start = time.monotonic()
        proc = subprocess.run(
            [sys.executable, "-m", "crane_sync", "selftest"],
            cwd=str(ROOT), env=env, text=True, capture_output=True, timeout=90,
        )
        elapsed = time.monotonic() - start
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("三种等待可区分", proc.stdout)
        self.assertIn("结果未知", proc.stdout)
        self.assertIn("自检通过", proc.stdout)
        self.assertLess(elapsed, 60, "一键自检超时")


if __name__ == "__main__":
    unittest.main(verbosity=2)
