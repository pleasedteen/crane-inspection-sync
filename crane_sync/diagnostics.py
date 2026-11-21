"""本地诊断：请求尝试日志与各来源当前状态。

诊断信息全部落在 state/sync_state.db，因此：

- 任意时刻（包括同步进程仍在运行、或在另一个终端）都能读取；
- 进程被强停后，发出请求前落盘的 inflight 记录仍在，
  重跑/诊断时按 pid 存活情况把它们标为"结果未知"；
- 已结束请求按上限滚动保留（默认最近 200 条），当前等待与
  各来源续点不受上限影响。
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from .limits import HttpStatusError
from .storage import ATTEMPT_HISTORY_CAP, Storage

# 供诊断展示的状态中文名
STATE_LABELS = {
    "starting": "启动中",
    "processing": "处理回包",
    "publishing": "发布快照",
    "waiting_quota": "等额度",
    "cooldown": "冷却",
    "renewing_token": "续令牌",
    "waiting_reply": "等 HTTP 回包",
    "done": "已完成",
    "idle": "空闲",
    "aborted": "已中止",
    "failed": "失败",
}

OUTCOME_LABELS = {
    "pending": "进行中",
    "ok": "已确认回包",
    "http_429": "HTTP 429 限流",
    "http_401": "HTTP 401 令牌失效",
    "http_410": "HTTP 410 游标过期",
    "http_error": "HTTP 错误",
    "error": "网络/其他错误",
    "unknown": "结果未知（进程强停）",
}

PHASE_LABELS = {
    "paging": "分页",
    "manifest": "清单",
    "token": "令牌",
}


class AttemptRecorder:
    """把每个 HTTP 请求与来源当前状态写入 SQLite，供诊断命令读取。

    所有落库操作都吞掉异常：诊断失败绝不能影响同步本身。
    """

    def __init__(self, storage: Storage, run_id: str):
        self.storage = storage
        self.run_id = run_id

    # ---- 来源状态 ----
    def probe(self, owner: object, state: str, detail: dict[str, Any]) -> None:
        """limits 探针回调：等额度 / 冷却 / 等回包 / 续令牌。"""
        source = owner[1] if isinstance(owner, tuple) else owner
        try:
            self.storage.set_source_state(
                str(source), state, self.run_id, detail
            )
        except Exception:
            pass

    def set_state(self, source: str, state: str, **detail: Any) -> None:
        try:
            self.storage.set_source_state(source, state, self.run_id, detail)
        except Exception:
            pass

    # ---- 请求尝试 ----
    def begin(
        self,
        source: str,
        phase: str,
        endpoint: str,
        request_key: str,
        detail: dict[str, Any] | None = None,
    ) -> str | None:
        try:
            return self.storage.begin_attempt(
                self.run_id, source, phase, endpoint, request_key, detail
            )
        except Exception:
            return None

    def finish_ok(
        self, attempt_id: str | None, http_status: int = 200,
        detail: dict[str, Any] | None = None,
    ) -> None:
        if not attempt_id:
            return
        try:
            self.storage.finish_attempt(
                attempt_id,
                "ok",
                http_status=http_status,
                reply_confirmed=True,
                detail=detail,
            )
        except Exception:
            pass

    def finish_error(
        self,
        attempt_id: str | None,
        outcome: str,
        http_status: int | None,
        reply_confirmed: bool,
        detail: dict[str, Any] | None = None,
    ) -> None:
        if not attempt_id:
            return
        try:
            self.storage.finish_attempt(
                attempt_id,
                outcome,
                http_status=http_status,
                reply_confirmed=reply_confirmed,
                detail=detail,
            )
        except Exception:
            pass

    def mark_progress(
        self, attempt_id: str | None, source: str, progress: dict | None
    ) -> None:
        """回包处理完后登记可续读进度是否已落盘。"""
        if not attempt_id:
            return
        try:
            self.storage.mark_attempt_progress(attempt_id, source, progress)
        except Exception:
            pass

    # ---- 便捷异常分类 ----
    @staticmethod
    def classify(exc: BaseException) -> tuple[str, int | None, bool, dict]:
        """把 HTTP 异常映射为 (outcome, status, 是否收到回包, 细节)。"""
        from .limits import AuthError, GatewayError, RateLimited, StaleCursor

        if isinstance(exc, RateLimited):
            return "http_429", 429, True, {"retry_after": exc.retry_after}
        if isinstance(exc, AuthError):
            return "http_401", 401, True, {"error": str(exc)}
        if isinstance(exc, StaleCursor):
            return "http_410", 410, True, {"error": str(exc)}
        if isinstance(exc, HttpStatusError):
            return "http_error", exc.status, True, {"error": exc.body}
        if isinstance(exc, GatewayError):
            return "error", None, False, {"error": str(exc)}
        return "error", None, False, {"error": repr(exc)}


@dataclass
class SourceReport:
    source: str
    state: dict | None
    progress: dict | None
    progress_updated_at: str | None
    publication: dict | None
    inflight: list[dict]
    recent: list[dict]

    def as_dict(self) -> dict:
        return {
            "source": self.source,
            "state": self.state,
            "progress": self.progress,
            "progress_updated_at": self.progress_updated_at,
            "publication": self.publication,
            "inflight": self.inflight,
            "recent_attempts": self.recent,
        }


def _pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except (PermissionError, OverflowError, ValueError):
        return True
    return True


def _progress_relation(attempt: dict, current: dict | None) -> str:
    """已确认回包是否形成了"当前可续读"的进度。"""
    if not attempt.get("reply_confirmed"):
        return "no_reply"
    if not attempt.get("progress_written"):
        return "not_written"
    if current is not None and attempt.get("progress") == current:
        return "current"
    return "superseded"


def build_report(
    storage: Storage,
    sources: list[str] | None = None,
    attempt_limit: int = 10,
) -> dict:
    """汇总各来源状态、续点、当前在途请求与近期尝试。

    读取前顺便对账：属于已消失进程的 inflight 尝试会被标为结果未知，
    因此"重跑后查旧尝试是否落盘"得到的是稳定结论。
    """
    reconciled = storage.reconcile_attempts()
    published = set(storage.published_sources())
    if sources:
        names = list(dict.fromkeys(sources))
    else:
        names = sorted(
            set(published)
            | set(storage.all_source_states())
            | {"offset", "cursor", "window", "gateway"}
        )

    reports: list[SourceReport] = []
    for name in names:
        state = storage.get_source_state(name)
        progress = storage.get_progress(name)
        conn_row = storage.progress_updated_at(name)
        pub = storage.latest_snapshot_info(name)
        attempts = storage.attempts_for_source(name, limit=attempt_limit)
        inflight = [a for a in attempts if a["state"] == "inflight"]
        recent = [a for a in attempts if a["state"] != "inflight"]
        for a in recent:
            a["progress_relation"] = _progress_relation(a, progress)
        reports.append(
            SourceReport(
                source=name,
                state=state,
                progress=progress,
                progress_updated_at=conn_row,
                publication=pub,
                inflight=inflight,
                recent=recent,
            )
        )

    recent_global = storage.recent_attempts(attempt_limit)
    progress_by_source = {
        r.source: r.progress for r in reports
    }
    for a in recent_global:
        if a["state"] != "inflight":
            a["progress_relation"] = _progress_relation(
                a, progress_by_source.get(a["source"])
            )

    return {
        "attempt_history_cap": ATTEMPT_HISTORY_CAP,
        "attempt_count": storage.count_attempts(),
        "inflight_count": storage.count_attempts("inflight"),
        "reconciled_now": reconciled,
        "sources": [r.as_dict() for r in reports],
        "recent_attempts": recent_global,
        # 结果未知的旧尝试单独列全，避免被"最近 N 条"截断而查不清
        "unknown_attempts": storage.attempts_by_state("unknown"),
        "inflight_attempts": storage.attempts_by_state("inflight"),
    }


# ----------------------------------------------------------------------
# 人类可读输出
# ----------------------------------------------------------------------
def _fmt_state(state: dict | None) -> str:
    if not state:
        return "无记录"
    label = STATE_LABELS.get(state["state"], state["state"])
    bits = [label]
    if state.get("since"):
        bits.append(f"自 {state['since']}")
    detail = state.get("detail") or {}
    if detail.get("remaining") is not None:
        bits.append(f"剩余 {detail['remaining']}s")
    if detail.get("page") is not None:
        bits.append(f"第 {detail['page']} 页")
    if detail.get("token_generation") is not None:
        bits.append(f"令牌世代 {detail['token_generation']}")
    pid = state.get("pid")
    if pid:
        alive = "存活" if _pid_alive(pid) else "已退出"
        bits.append(f"pid {pid} {alive}")
    return "，".join(bits)


def _fmt_attempt(a: dict) -> str:
    outcome = OUTCOME_LABELS.get(a["outcome"], a["outcome"])
    bits = [
        a["started_at"],
        f"{a['source']}/{PHASE_LABELS.get(a['phase'], a['phase'])}",
        f"{a['endpoint']}?{a['request_key']}" if a["request_key"] else a["endpoint"],
        outcome,
    ]
    if a.get("http_status") and a["outcome"] not in (
        "http_429", "http_401", "http_410", "http_error"
    ):
        bits.append(f"HTTP {a['http_status']}")
    if a["state"] == "inflight":
        bits.append("【仍在途】")
    elif a["outcome"] == "ok":
        if a.get("progress_written"):
            rel = a.get("progress_relation")
            rel_text = {
                "current": "进度已落盘（当前续点）",
                "superseded": "进度已落盘（已被后续页覆盖）",
                "not_written": "进度未落盘",
            }.get(rel, "进度已落盘")
            bits.append(rel_text)
        else:
            bits.append("已确认回包但进度未落盘")
    elif a["outcome"] == "unknown":
        bits.append("无可用进度")
    return "  ".join(bits)


def render_report(report: dict) -> str:
    lines: list[str] = []
    lines.append(
        f"=== 同步诊断（已结束请求保留上限 {report['attempt_history_cap']} 条，"
        f"当前共 {report['attempt_count']} 条，在途 {report['inflight_count']} 条）==="
    )
    if report["reconciled_now"]:
        lines.append(
            f"对账：{len(report['reconciled_now'])} 条旧的在途请求已标为结果未知"
        )
        for a in report["reconciled_now"]:
            lines.append("  " + _fmt_attempt({**a, "state": "unknown"}))
    for src in report["sources"]:
        lines.append("")
        lines.append(f"[{src['source']}] 当前状态：{_fmt_state(src['state'])}")
        if src["progress"] is None:
            lines.append("  续点：无（下一轮从头开始）")
        else:
            lines.append(
                f"  续点：{src['progress']}（更新于 {src['progress_updated_at']}）"
            )
        pub = src["publication"]
        if pub:
            lines.append(
                f"  已发布快照：{pub['snapshot_id']} 记录数={pub['record_count']} "
                f"发布于 {pub['published_at']}"
            )
        else:
            lines.append("  已发布快照：无")
        if src["inflight"]:
            lines.append("  当前在途请求：")
            for a in src["inflight"]:
                lines.append("    " + _fmt_attempt(a))
        recent = src["recent_attempts"]
        if recent:
            lines.append(f"  近期已结束请求（最多 {len(recent)} 条）：")
            for a in recent:
                lines.append("    " + _fmt_attempt(a))
        elif not src["inflight"]:
            lines.append("  近期已结束请求：无")

    lines.append("")
    lines.append("=== 全局最近请求（跨来源）===")
    if not report["recent_attempts"]:
        lines.append("  无请求记录")
    for a in report["recent_attempts"]:
        lines.append("  " + _fmt_attempt(a))

    unknown = report.get("unknown_attempts") or []
    if unknown:
        lines.append("")
        lines.append(f"=== 结果未知的旧尝试（{len(unknown)} 条，可核对当时是否落盘）===")
        for a in unknown:
            bits = _fmt_attempt(a)
            if a.get("progress_before") is not None:
                bits += f"  请求前续点={a['progress_before']}"
            lines.append("  " + bits)
    return "\n".join(lines)
