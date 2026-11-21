"""一键诊断自检：用内置模拟源验证"等待 / 强停 / 续跑"。

流程（全部在本机完成，不依赖外部网络）：

1. 启动内置三个模拟源，并让数据响应变慢，制造真实的"请求在途"与
   "等额度/冷却"状态；
2. 启动一次真实 sync 子进程，轮询诊断接口，确认能看到各来源当前是
   等额度、冷却还是等 HTTP 回包；
3. 在请求在途时 SIGKILL 强停该进程（模拟断电/被杀）；
4. 再跑诊断：未确认回包的在途请求被标记为"结果未知"，
   同时能查清被杀前最后一次确认的分页进度；
5. 重跑 sync 至完成：从已确认的续点续读并发布快照；
6. 完成后诊断仍能查到被杀那次的旧尝试，且已结束请求不超过 200 条。
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from .diagnostics import build_report, render_report
from .mock_server import serve_forever
from .storage import ATTEMPT_HISTORY_CAP, Storage

ROOT = Path(__file__).resolve().parents[1]

WAIT_STATES = {"waiting_quota", "cooldown"}


class SelfTestFailure(RuntimeError):
    pass


def _ok(step: str, detail: str = "") -> None:
    suffix = f"  {detail}" if detail else ""
    print(f"  [通过] {step}{suffix}", flush=True)


def _write_config(path: Path, base_url: str, state_dir: Path) -> None:
    cfg = {
        "base_url": base_url,
        "state_dir": str(state_dir),
        "snapshot_format": "jsonl",
        "page_size": 4,
        "gateway_quota": 2,
        "gateway_window": 1.0,
        "window_source_quota": 2,
        "window_source_window": 1.0,
        "cooldown_seconds": 0.5,
        "window_minutes": 1440,
        "window_horizon": "2026-11-12T00:00:00",
    }
    path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


def _spawn_sync(cfg_path: Path, env: dict) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "crane_sync", "sync", "--config", str(cfg_path)],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _run_cli(args: list[str], env: dict, timeout: float = 30) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "crane_sync", *args],
        cwd=str(ROOT),
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout,
    )


def _live_snapshot(state_dir: Path) -> tuple[dict, dict, int]:
    """读取当前各来源状态、各源进度与在途请求数（不触发对账）。"""
    storage = Storage(state_dir)
    try:
        states = storage.all_source_states()
        progress = {
            s: storage.get_progress(s) for s in ("offset", "cursor", "window")
        }
        inflight = storage.count_attempts("inflight")
    finally:
        storage.close()
    return states, progress, inflight


def run_selftest(keep: bool = False) -> int:
    tmp = tempfile.TemporaryDirectory(prefix="crane_sync_selftest_")
    tmp_path = Path(tmp.name)
    state_dir = tmp_path / "state"
    cfg_path = tmp_path / "mock.json"
    env = dict(os.environ, PYTHONPATH=str(ROOT))

    print("=" * 68)
    print("crane_sync 诊断自检：等待 → 强停 → 续跑（内置模拟源）")
    print("=" * 68)

    server = serve_forever(
        {
            "host": "127.0.0.1",
            "port": 0,
            "gateway_quota": 2,
            "gateway_window": 1.0,
            "window_quota": 2,
            "window_window": 1.0,
            "token_ttl": 300.0,
            "response_delay": 0.5,
        }
    )
    host, port = server.server_address
    base_url = f"http://{host}:{port}"
    _write_config(cfg_path, base_url, state_dir)
    print(f"模拟源: {base_url}   状态目录: {state_dir}")

    proc: subprocess.Popen | None = None
    killed_run_id: str | None = None
    try:
        # ---------- 1) 启动同步，观察等待状态 ----------
        print("\n[1/6] 启动同步子进程，观察各来源等待状态…")
        proc = _spawn_sync(cfg_path, env)
        seen_wait: dict[str, str] = {}
        seen_any: set[str] = set()
        seen_inflight_while_waiting = False
        deadline = time.time() + 30
        while time.time() < deadline:
            if proc.poll() is not None:
                raise SelfTestFailure(
                    "同步进程在强停前已退出，未能演示强停 "
                    f"(returncode={proc.returncode})"
                )
            states, progress, inflight = _live_snapshot(state_dir)
            for src, st in states.items():
                seen_any.add(st["state"])
                if st["state"] in WAIT_STATES:
                    seen_wait[src] = st["state"]
            if inflight:
                seen_inflight_while_waiting = True
            # 至少看到过一种等待状态 + 一个在途请求 + 一处续点，即可强停
            if seen_wait and inflight and any(progress.values()):
                break
            time.sleep(0.05)
        else:
            raise SelfTestFailure("等待阶段超时：没有观察到等额度/冷却或在途请求")

        storage = Storage(state_dir)
        try:
            running = storage.get_run_by_status("running")
        finally:
            storage.close()
        if running:
            killed_run_id = running["run_id"]
        _ok(
            "观察到等待状态",
            "，".join(f"{k}={v}" for k, v in sorted(seen_wait.items())),
        )
        _ok("观察到各来源阶段", "，".join(sorted(seen_any)))
        if not (seen_any & {"waiting_quota", "cooldown"}):
            raise SelfTestFailure("未观察到等额度/冷却状态")
        if "waiting_reply" not in seen_any:
            raise SelfTestFailure("未观察到等 HTTP 回包状态，无法区分三种等待")
        if not seen_inflight_while_waiting:
            raise SelfTestFailure("未观察到在途 HTTP 请求")
        _ok("三种等待可区分", "等额度/冷却/等 HTTP 回包")
        _ok("观察到在途 HTTP 请求", "强停时将留下未确认回包")

        # ---------- 2) 强停 ----------
        print("\n[2/6] 在请求在途时 SIGKILL 强停…")
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=10)
        _ok("进程已被强制终止", f"pid={proc.pid}, returncode={proc.returncode}")
        proc = None

        # ---------- 3) 强停后诊断 ----------
        print("\n[3/6] 强停后运行 diagnose，确认在途请求=结果未知…")
        diag = _run_cli(["diagnose", "--state-dir", str(state_dir), "--json"], env)
        if diag.returncode != 0:
            raise SelfTestFailure(f"diagnose 失败: {diag.stderr}")
        report = json.loads(diag.stdout)
        unknown = report["unknown_attempts"]
        if not unknown:
            raise SelfTestFailure("强停后没有任何请求被标记为结果未知")
        if killed_run_id:
            same_run = [a for a in unknown if a["run_id"] == killed_run_id]
            if not same_run:
                raise SelfTestFailure("结果未知的请求不属于被强停的那次运行")
        _ok("在途请求已记为结果未知", f"{len(unknown)} 条")
        _ok(
            "仍可查到强停前的续点",
            "，".join(
                f"{s['source']}={s['progress']}"
                for s in report["sources"]
                if s["progress"]
            )
            or "（无）",
        )
        human = _run_cli(["diagnose", "--state-dir", str(state_dir)], env)
        if human.returncode != 0 or "结果未知" not in human.stdout:
            raise SelfTestFailure("diagnose 文本输出缺少‘结果未知’标注")
        _ok("人类可读诊断包含结果未知标注")

        # ---------- 4) 重跑续读 ----------
        print("\n[4/6] 重跑 sync：从已确认续点继续…")
        rerun = _run_cli(["sync", "--config", str(cfg_path)], env, timeout=120)
        if rerun.returncode != 0:
            raise SelfTestFailure(
                f"续跑 sync 失败（{rerun.returncode}）:\n{rerun.stderr}"
            )
        _ok("续跑同步完成", "已发布快照")

        # ---------- 5) 完成后诊断 ----------
        print("\n[5/6] 完成后诊断：续点已推进、旧尝试仍可查…")
        final = _run_cli(["diagnose", "--state-dir", str(state_dir), "--json"], env)
        if final.returncode != 0:
            raise SelfTestFailure(f"完成后 diagnose 失败: {final.stderr}")
        freport = json.loads(final.stdout)
        if freport["inflight_count"] != 0:
            raise SelfTestFailure("完成后仍有在途请求")
        published = {
            s["source"] for s in freport["sources"] if s["publication"]
        }
        expected = {"offset", "cursor", "window"}
        if not expected.issubset(published):
            raise SelfTestFailure(f"完成后的快照不完整: {published}")
        _ok("三个来源均已发布快照", "，".join(sorted(published)))

        current_progress = {
            s["source"]: s["progress"] for s in freport["sources"] if s["progress"]
        }
        if not all(current_progress.get(s) for s in expected):
            raise SelfTestFailure("完成后仍有来源没有续点")
        _ok(
            "各源续点可查",
            "，".join(f"{k}={v}" for k, v in sorted(current_progress.items())),
        )

        confirmed_current = [
            a
            for s in freport["sources"]
            for a in s["recent_attempts"]
            if a.get("progress_relation") == "current"
        ]
        if not confirmed_current:
            raise SelfTestFailure("没有任何已确认分页响应对应到当前可续读进度")
        _ok(
            "已确认分页响应形成可续读进度",
            f"{len(confirmed_current)} 条与当前续点一致",
        )

        still_unknown = freport["unknown_attempts"]
        if killed_run_id and not any(a["run_id"] == killed_run_id for a in still_unknown):
            raise SelfTestFailure("重跑后查不到强停那次的旧尝试")
        _ok("重跑后仍能查清旧尝试是否落盘", f"{len(still_unknown)} 条结果未知")

        # ---------- 6) 上限检查 ----------
        print("\n[6/6] 检查已结束请求保留上限…")
        finished = freport["attempt_count"] - freport["inflight_count"]
        if finished > ATTEMPT_HISTORY_CAP:
            raise SelfTestFailure(
                f"已结束请求 {finished} 条，超过上限 {ATTEMPT_HISTORY_CAP}"
            )
        _ok(
            "已结束请求不超过上限",
            f"{finished} ≤ {ATTEMPT_HISTORY_CAP}，当前等待与续点不受影响",
        )

        print("\n" + "-" * 68)
        print("自检通过：等待状态可观测、强停留下结果未知、重跑可续读。")
        print("-" * 68)
        print("\n最终诊断报告：")
        storage = Storage(state_dir)
        try:
            print(render_report(build_report(storage)))
        finally:
            storage.close()
        return 0
    except SelfTestFailure as exc:
        print(f"\n[失败] {exc}", file=sys.stderr)
        return 1
    except subprocess.TimeoutExpired as exc:
        print(f"\n[失败] 子进程超时: {exc}", file=sys.stderr)
        return 1
    finally:
        if proc is not None and proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
        server.shutdown()
        server.server_close()
        if keep:
            print(f"\n已保留状态目录以便排查: {state_dir}")
        else:
            tmp.cleanup()


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="crane_sync selftest")
    parser.add_argument("--keep", action="store_true", help="保留临时状态目录")
    args = parser.parse_args(argv)
    return run_selftest(keep=args.keep)


if __name__ == "__main__":
    raise SystemExit(main())
