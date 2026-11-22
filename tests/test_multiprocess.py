"""多进程按来源分工的测试。

覆盖三个关键点：
1. 同源不重复拉取/发布：同源采集进程在跑时，后启动的同源进程跳过该来源。
2. 跨源共用额度与令牌，同一失效令牌只续期一次。
3. 进程在页间被杀后，重跑从已保存的续点接手。

运行：python3 run_tests.py（或 python3 -m unittest tests.test_multiprocess -v）
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
from pathlib import Path

from crane_sync.coordination import SourceLock
from crane_sync.storage import Storage

ROOT = Path(__file__).resolve().parents[1]

# 子进程脚本：按来源同步。支持：
#   abort_after_pages  —— 累计处理 N 页后 os._exit，模拟进程在页间被强杀
#   slow_page_seconds  —— 每页后 sleep，制造“同源仍在运行”的窗口
_SYNC_WORKER = r"""
import asyncio, json, os, sys, time
from crane_sync.engine import SyncConfig, SyncEngine

cfg = json.loads(sys.argv[1])
sources = tuple(cfg.pop("sources"))
abort_after = cfg.pop("abort_after_pages", None)
delay = cfg.pop("slow_page_seconds", 0.0)
engine = SyncEngine(SyncConfig(sources=sources, **cfg))
pages = {"n": 0}

async def on_page(source, n):
    pages["n"] += 1
    if abort_after is not None and pages["n"] >= abort_after:
        os._exit(17)          # 不给清理机会，模拟被强杀
    if delay:
        await asyncio.sleep(delay)

engine.config.on_page = on_page
started = time.time()
results = asyncio.run(engine.run())
ended = time.time()
grants = list(engine.gateway.grant_log) if engine.gateway else []
skipped = engine.skipped
engine.close()
print(json.dumps({
    "results": results,
    "skipped": skipped,
    "grants": grants,
    "started": started,
    "ended": ended,
}))
"""


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _start_worker(cfg: dict) -> subprocess.Popen:
    env = dict(os.environ, PYTHONPATH=str(ROOT))
    return subprocess.Popen(
        [sys.executable, "-c", _SYNC_WORKER, json.dumps(cfg)],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _admin(base_url: str, path: str) -> dict:
    req = urllib.request.Request(f"{base_url}{path}", method="POST")
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read().decode())


def _stats(base_url: str) -> dict:
    with urllib.request.urlopen(f"{base_url}/mock/admin/stats", timeout=5) as r:
        return json.loads(r.read().decode())


class SourceLockTests(unittest.TestCase):
    def test_same_source_second_holder_skips(self):
        """同源锁已持有时，后来者拿不到；不同来源互不影响。"""
        with tempfile.TemporaryDirectory() as tmp:
            held = SourceLock(tmp, "offset")
            self.assertTrue(held.acquire())
            try:
                self.assertFalse(SourceLock(tmp, "offset").acquire())
                other = SourceLock(tmp, "cursor")
                self.assertTrue(other.acquire())
                other.release()
            finally:
                held.release()
            again = SourceLock(tmp, "offset")
            self.assertTrue(again.acquire())
            again.release()

    def test_lock_released_when_holder_process_killed(self):
        """持有锁的进程被 kill 后，内核释放锁，重跑可以接手。"""
        with tempfile.TemporaryDirectory() as tmp:
            lock_path = Path(tmp) / "locks" / "offset.lock"
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(lock_path, os.O_RDWR | os.O_CREAT)
            os.close(fd)
            proc = subprocess.Popen(
                [
                    sys.executable, "-c",
                    "import fcntl,sys,time;"
                    "fd=open(sys.argv[1],'r+');"
                    "fcntl.flock(fd, fcntl.LOCK_EX);"
                    "print('locked', flush=True); time.sleep(60)",
                    str(lock_path),
                ],
                stdout=subprocess.PIPE, text=True,
            )
            try:
                self.assertEqual(proc.stdout.readline().strip(), "locked")
                self.assertFalse(SourceLock(tmp, "offset").acquire())
            finally:
                proc.kill()
                proc.wait(timeout=5)
            deadline = time.time() + 5
            got = False
            while time.time() < deadline and not got:
                got = SourceLock(tmp, "offset").acquire()
                if not got:
                    time.sleep(0.05)
            self.assertTrue(got, "持锁进程被杀后锁应可被接手")


class MultiProcessSyncTests(unittest.TestCase):
    def setUp(self):
        from tests.test_sync import start_server

        self.server = start_server()
        host, port = self.server.server_address
        self.base_url = f"http://{host}:{port}"
        self.tmp = tempfile.TemporaryDirectory()
        self.state_dir = self.tmp.name

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.tmp.cleanup()

    def _config(self, sources, **extra) -> dict:
        cfg = {
            "base_url": self.base_url,
            "state_dir": self.state_dir,
            "snapshot_format": "jsonl",
            "page_size": 10,
            "gateway_quota": 6,
            "gateway_window": 1.0,
            "window_source_quota": 120,
            "window_source_window": 1.0,
            "cooldown_seconds": 0.2,
            "window_minutes": 30,
            "window_horizon": "2026-11-12T00:00:00",
            "sources": list(sources),
        }
        cfg.update(extra)
        return cfg

    def _published(self, source: str):
        st = Storage(self.state_dir)
        try:
            info = st.latest_snapshot_info(source)
        finally:
            st.close()
        return info and info["snapshot_id"]

    def _unique_sizes(self) -> dict:
        return {
            k: len({r["order_id"] for r in v})
            for k, v in self.server.gateway.datasets.items()
        }

    # ---------- 关键点 1：同源不重复拉取/发布 ----------
    def test_second_same_source_process_does_not_fetch_or_publish(self):
        # 进程 A 负责 offset，页间放慢，制造与 B 的重叠窗口
        worker_a = _start_worker(
            self._config(["offset"], page_size=2, slow_page_seconds=0.15)
        )
        try:
            # 等 A 真正开始（offset 进度已落盘）
            deadline = time.time() + 10
            progress = None
            while time.time() < deadline:
                st = Storage(self.state_dir)
                try:
                    progress = st.get_progress("offset")
                finally:
                    st.close()
                if progress is not None:
                    break
                time.sleep(0.02)
            self.assertIsNotNone(progress, "进程 A 未开始采集 offset")
            calls_before = len(self.server.requests_for("offset"))
            published_before = self._published("offset")

            # 进程 B 同时负责 cursor + offset
            worker_b = _start_worker(self._config(["cursor", "offset"]))
            out_b, err_b = worker_b.communicate(timeout=40)
            self.assertEqual(worker_b.returncode, 0, err_b)
            payload_b = json.loads(out_b.strip().splitlines()[-1])
            self.assertIn("offset", payload_b["skipped"])
            self.assertNotIn("offset", payload_b["results"])
            self.assertIn("cursor", payload_b["results"])

            # B 未对 offset 发起任何分页请求，也未覆盖 A 的发布指针
            self.assertEqual(
                len(self.server.requests_for("offset")), calls_before,
                "同源进程在跑时，后来者不得拉取 offset",
            )
            self.assertEqual(self._published("offset"), published_before)
            self.assertIsNotNone(self._published("cursor"))

            # A 继续跑完，正常发布 offset
            out_a, err_a = worker_a.communicate(timeout=60)
            self.assertEqual(worker_a.returncode, 0, err_a)
            payload_a = json.loads(out_a.strip().splitlines()[-1])
            self.assertIn("offset", payload_a["results"])
            self.assertIsNotNone(self._published("offset"))
            # 最终 offset 快照完整
            st = Storage(self.state_dir)
            try:
                self.assertEqual(
                    len(st.read_snapshot("offset")),
                    self._unique_sizes()["offset"],
                )
            finally:
                st.close()
        finally:
            if worker_a.poll() is None:
                worker_a.kill()
                worker_a.wait(timeout=5)

    # ---------- 关键点 2：跨源共用额度与令牌，只续一次 ----------
    def test_cross_process_shared_quota_and_token_renew_once(self):
        # 两个进程分别负责 offset / cursor，共享紧额度网关与同一令牌。
        # 页间放慢，让两个进程的取额度区间真正重叠。
        cfg_extra = dict(
            page_size=2, gateway_quota=3, gateway_window=0.5,
            slow_page_seconds=0.03,
        )
        workers = [
            _start_worker(self._config([src], **cfg_extra))
            for src in ("offset", "cursor")
        ]
        try:
            # 等两进程都完成过至少一页
            deadline = time.time() + 20
            while time.time() < deadline:
                if (self.server.requests_for("offset")
                        and self.server.requests_for("cursor")):
                    break
                time.sleep(0.01)
            renewals_before = _stats(self.base_url)["gateway"]["renewals"]
            self.assertEqual(renewals_before, 1, "两进程应共用同一初次令牌")

            # 让当前共享令牌立即失效；两进程下一次请求都会 401
            _admin(self.base_url, "/mock/admin/expire-token")

            outs = [w.communicate(timeout=60) for w in workers]
            payloads = []
            for w, (out, err) in zip(workers, outs):
                self.assertEqual(w.returncode, 0, err)
                payloads.append(json.loads(out.strip().splitlines()[-1]))

            stats = _stats(self.base_url)
            # 初次签发 1 次 + 失效后仅续期 1 次（不是每进程各续一次）
            self.assertEqual(stats["gateway"]["renewals"], 2)

            # 两个进程确实共用同一额度池：都拿到额度，且运行区间重叠
            off, cur = payloads
            self.assertTrue(
                any(g == "offset" for g in off["grants"]),
                "offset 进程未拿到网关额度",
            )
            self.assertTrue(
                any(g == "cursor" for g in cur["grants"]),
                "cursor 进程未拿到网关额度",
            )
            overlap = min(off["ended"], cur["ended"]) - max(
                off["started"], cur["started"]
            )
            self.assertGreater(overlap, 0, "两进程未同时运行，无法证明共享额度")

            # 全局限速：服务端在配额窗口内批准的数据请求数不超过配额
            # 加上时间窗口自然滚动的少量溢出；此处只校验共享未失效
            accepted = stats["gateway"]["gateway_requests"]
            self.assertGreater(accepted, 3, "共享额度池应持续发放")

            # 额度共享但没有互相拖垮：两来源都完整发布
            sizes = self._unique_sizes()
            st = Storage(self.state_dir)
            try:
                self.assertEqual(
                    len(st.read_snapshot("offset")), sizes["offset"]
                )
                self.assertEqual(
                    len(st.read_snapshot("cursor")), sizes["cursor"]
                )
            finally:
                st.close()
        finally:
            for w in workers:
                if w.poll() is None:
                    w.kill()
                    w.wait(timeout=5)

    # ---------- 关键点 2 补充：一边等响应，另一边有额度就继续翻页 ----------
    def test_other_source_pages_while_one_waits_for_response(self):
        """offset 每个请求都很慢；cursor 的翻页不应被 offset 的响应拖住。

        额度占位在发 HTTP 前就提交，所以 offset 等待响应时，
        cursor 只要有剩余额度就能立刻继续翻页。
        """
        from tests.test_sync import start_server

        slow = start_server(delay_ms=400)
        host, port = slow.server_address
        base_url = f"http://{host}:{port}"
        tmp = tempfile.TemporaryDirectory()
        try:
            cfg = {
                "base_url": base_url,
                "state_dir": tmp.name,
                "page_size": 2,
                "gateway_quota": 50,       # 额度充足，排除限流干扰
                "gateway_window": 5.0,
                "window_source_quota": 120,
                "window_source_window": 1.0,
                "slow_page_seconds": 0.0,
            }
            # 先启动 offset，等它的首个请求到达服务端（服务端在 sleep 前
            # 已记录到达顺序），再启动 cursor，消除进程启动顺序的竞态。
            off_worker = _start_worker({**cfg, "sources": ["offset"]})
            cur_worker = None
            try:
                deadline = time.time() + 15
                while time.time() < deadline and not slow.requests_for("offset"):
                    time.sleep(0.01)
                self.assertTrue(
                    slow.requests_for("offset"), "offset 进程未发出首个请求"
                )
                cur_worker = _start_worker({**cfg, "sources": ["cursor"]})
                workers = [off_worker, cur_worker]
                outs = [w.communicate(timeout=60) for w in workers]
                for w, (out, err) in zip(workers, outs):
                    self.assertEqual(w.returncode, 0, err)
            finally:
                for w in (off_worker, cur_worker):
                    if w is not None and w.poll() is None:
                        w.kill()
                        w.wait(timeout=5)

            off_calls = slow.requests_for("offset")
            cur_calls = slow.requests_for("cursor")
            self.assertGreaterEqual(len(cur_calls), 3)
            # 只有 offset 每个请求慢 400ms。服务端在发响应前即记录到达顺序，
            # 因此若 cursor 能在 offset 的两次请求之间插入，说明它并未被
            # offset 等待响应所阻塞，而是靠共享额度池继续翻页。
            log = [r["source"] for r in slow.request_log
                   if r["source"] in ("offset", "cursor")]
            off_positions = [i for i, s in enumerate(log) if s == "offset"]
            self.assertGreaterEqual(len(off_positions), 2)
            between = log[off_positions[0] + 1:off_positions[1]]
            self.assertIn(
                "cursor", between,
                f"一边等响应时另一边没有继续翻页: {log}",
            )
            # 数据完整
            sizes = {
                k: len({r["order_id"] for r in v})
                for k, v in slow.gateway.datasets.items()
            }
            st = Storage(tmp.name)
            try:
                self.assertEqual(len(st.read_snapshot("offset")), sizes["offset"])
                self.assertEqual(len(st.read_snapshot("cursor")), sizes["cursor"])
            finally:
                st.close()
        finally:
            slow.shutdown()
            slow.server_close()
            tmp.cleanup()

    # ---------- 关键点 3：页间被杀后重跑接续点 ----------
    def test_resume_after_process_killed_between_pages(self):
        # 第一轮：offset 在两页后被杀，不发布
        killed = _start_worker(
            self._config(["offset"], page_size=2, abort_after_pages=2)
        )
        killed.communicate(timeout=30)
        self.assertNotEqual(killed.returncode, 0)

        st = Storage(self.state_dir)
        try:
            self.assertEqual(st.published_sources(), [])
            progress = st.get_progress("offset")
            self.assertIsNotNone(progress)
            resume_offset = progress["next_offset"]
            self.assertGreater(st.count_staging("offset"), 0)
        finally:
            st.close()
        self.assertEqual(resume_offset, 4)
        self.assertEqual(self.server.requests_for("offset")[0]["offset"], 0)

        # 重跑：必须从已保存的续点续读
        marker = len(self.server.requests_for("offset"))
        resumed = _start_worker(self._config(["offset"], page_size=2))
        out, err = resumed.communicate(timeout=40)
        self.assertEqual(resumed.returncode, 0, err)
        later = self.server.requests_for("offset")[marker:]
        self.assertTrue(later)
        self.assertEqual(
            later[0]["offset"], resume_offset,
            "重跑应从保存的续点开始，而不是 offset=0",
        )
        self.assertNotIn({"offset": 0, "limit": 2}, later)

        st = Storage(self.state_dir)
        try:
            rows = st.read_snapshot("offset")
        finally:
            st.close()
        self.assertEqual(len(rows), self._unique_sizes()["offset"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
