"""三个本地模拟 HTTP 数据源。

一个 ThreadingHTTPServer 进程暴露三组前缀：

- /offset/**     偏移分页源（?limit=&offset=），走共享网关令牌
- /cursor/**     游标分页源（清单版次 + 游标），走共享网关令牌
- /window/**     时间窗分页源（?from=&to=）+ 迟到变更流（/window/changes），独立额度
- /gateway/token POST 取/续共享令牌（续期使旧令牌失效，且占用额度）
- /mock/admin/** 测试辅助：强制令牌过期、轮换游标清单、注入时间窗变更、查看计数

游标语义：游标属于某一版清单，清单过期后旧游标返回 410，
客户端只能从新版清单开头重读；新版的记录集合与顺序都可能变化。
时间窗语义：相邻窗口在边界上重叠，同一条边界记录会被两个窗口重复返回；
同一 inspected_at 时刻允许存在多张单。
变更流语义：时间窗供应商会对"旧巡检单的某个修订"补发、更正或撤回，
这些迟到变更通过 /window/changes 按递增序号发布，客户端只需从续点
追赶变更，无需重扫历史时间窗；变更日志超出保留范围后旧续点返回 410。
"""
from __future__ import annotations

import copy
import json
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


# ----------------------------------------------------------------------
# 模拟数据生成
# ----------------------------------------------------------------------
def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def build_datasets(seed: int = 20261118) -> dict:
    """生成三套数据集，单号/修订号在供应商之间共用。"""
    base = datetime(2026, 11, 10, 8, 0, 0)
    all_orders: dict[str, list[dict]] = {}
    for i in range(1, 41):  # 40 个单号
        order_id = f"QC-{i:04d}"
        n_rev = 3 if i % 7 == 0 else (2 if i % 4 == 0 else 1)
        versions = []
        for rev in range(1, n_rev + 1):
            # 同一时刻可以有多张单：i%5 的单共享相同 inspected_at
            ts = base + timedelta(minutes=15 * (i - i % 5) + 30 * (rev - 1))
            unit = "um" if (i + rev) % 2 == 0 else "mm"  # 同页混用单位
            rail = 8000 + i * 10 + rev * 3 if unit == "um" else 8.0 + i * 0.01
            trolley = 12000 - i * 5 if unit == "um" else 12.0 - i * 0.005
            versions.append(
                {
                    "order_id": order_id,
                    "revision": rev,
                    "inspected_at": _iso(ts),
                    "gaps": [
                        {"point": "rail", "value": round(rail, 4), "unit": unit},
                        {
                            "point": "trolley",
                            "value": round(trolley, 4),
                            "unit": unit,
                        },
                    ],
                }
            )
        all_orders[order_id] = versions

    def pick(mod: int, rem: int) -> list[dict]:
        rows = []
        for i, (oid, versions) in enumerate(all_orders.items()):
            if i % mod == rem:
                rows.extend(versions)
        return rows

    offset_rows = pick(3, 0)
    cursor_rows = pick(3, 1)
    window_rows = pick(2, 0)
    return {
        "offset": offset_rows,
        "cursor": cursor_rows,
        "window": window_rows,
    }


# ----------------------------------------------------------------------
# 时间窗变更流（补发 / 更正 / 撤回）
# ----------------------------------------------------------------------
class WindowChangeLog:
    """时间窗源当前状态 + 递增变更日志。

    供应商会针对"旧巡检单的某个修订"发布三类迟到变更：

    - ``append``：补发一个新修订（单号/修订此前不存在）
    - ``correct``：更正某个已有修订的读数（单号/修订不变）
    - ``retract``：撤回某个修订（该修订从当前状态中消失）

    变更按全局递增序号 seq 发布；客户端保存已消费到的 seq 作为续点，
    只需追赶后续变更，无需重扫历史时间窗。日志可配置保留条数，
    续点早于保留范围时返回 410，客户端须重建基线。
    """

    TYPES = ("append", "correct", "retract")

    def __init__(self, rows: list[dict], retention: int | None = None):
        self.retention = retention if retention and retention > 0 else None
        self._lock = threading.Lock()
        self._rows: dict[tuple[str, int], dict] = {
            (r["order_id"], int(r["revision"])): copy.deepcopy(r) for r in rows
        }
        self._log: list[dict] = []
        self._seq = 0

    # ---- 当前状态 ----
    def rows(self) -> list[dict]:
        with self._lock:
            return [copy.deepcopy(r) for r in self._rows.values()]

    def row_count(self) -> int:
        with self._lock:
            return len(self._rows)

    # ---- 变更日志 ----
    @property
    def high_watermark(self) -> int:
        with self._lock:
            return self._seq

    @property
    def oldest_seq(self) -> int:
        """日志中仍可读到的最早序号；日志为空时为 high_watermark+1。"""
        with self._lock:
            return self._log[0]["seq"] if self._log else self._seq + 1

    def _record_event(self, etype: str, order_id: str, revision: int,
                      row: dict | None) -> dict:
        self._seq += 1
        event = {
            "seq": self._seq,
            "type": etype,
            "order_id": order_id,
            "revision": revision,
        }
        if row is not None:
            event["order"] = copy.deepcopy(row)
        self._log.append(event)
        if self.retention is not None:
            while len(self._log) > self.retention:
                self._log.pop(0)
        return copy.deepcopy(event)

    def apply(self, change: dict) -> dict:
        """注入一条变更并更新当前状态，返回带 seq 的事件。"""
        etype = change.get("type")
        if etype not in self.TYPES:
            raise ValueError(f"未知变更类型: {etype!r}")
        order_id = str(change["order_id"])
        revision = int(change["revision"])
        key = (order_id, revision)
        with self._lock:
            if etype == "retract":
                self._rows.pop(key, None)
                return self._record_event("retract", order_id, revision, None)
            if etype == "append" and key in self._rows:
                raise ValueError(f"{order_id} rev{revision} 已存在，补发会冲突")
            if etype == "correct" and key not in self._rows:
                raise ValueError(f"{order_id} rev{revision} 不存在，无法更正")
            row = {
                "order_id": order_id,
                "revision": revision,
                "inspected_at": str(change["inspected_at"]),
                "gaps": copy.deepcopy(change.get("gaps", [])),
            }
            self._rows[key] = row
            return self._record_event(etype, order_id, revision, row)

    def changes_since(self, since: int, until: int, limit: int) -> dict:
        """返回 (since, until] 内的变更页；续点早于保留范围时由调用方处理。"""
        with self._lock:
            oldest = self._log[0]["seq"] if self._log else self._seq + 1
            if since < oldest - 1:
                raise _ChangeCursorExpired(since=since, oldest_seq=oldest)
            cap = min(until, self._seq)
            pending = [e for e in self._log if since < e["seq"] <= cap]
            page = pending[:limit]
            next_since = page[-1]["seq"] if page else max(since, cap)
            has_more = bool(pending[limit:])
            return {
                "changes": copy.deepcopy(page),
                "next_since": next_since,
                "has_more": has_more,
                "high_watermark": self._seq,
            }

    def manifest(self) -> dict:
        with self._lock:
            return {
                "high_watermark": self._seq,
                "oldest_seq": self._log[0]["seq"] if self._log else self._seq + 1,
                "retention": self.retention,
            }


class _ChangeCursorExpired(Exception):
    def __init__(self, since: int, oldest_seq: int):
        super().__init__(
            f"change cursor {since} is older than retention {oldest_seq}"
        )
        self.since = since
        self.oldest_seq = oldest_seq


# ----------------------------------------------------------------------
# 服务端额度与令牌状态
# ----------------------------------------------------------------------
class SlidingWindow:
    def __init__(self, limit: int, window: float):
        self.limit = limit
        self.window = window
        self._hits: deque[float] = deque()
        self._lock = threading.Lock()

    def allow(self) -> tuple[bool, float]:
        now = time.monotonic()
        with self._lock:
            while self._hits and now - self._hits[0] >= self.window:
                self._hits.popleft()
            if len(self._hits) < self.limit:
                self._hits.append(now)
                return True, 0.0
            retry = self.window - (now - self._hits[0])
            return False, max(0.05, retry)


class GatewayState:
    def __init__(
        self,
        quota_limit: int,
        quota_window: float,
        token_ttl: float,
        cursor_rotate_after: int | None,
        datasets: dict,
        window_change_retention: int | None = None,
    ):
        self.quota = SlidingWindow(quota_limit, quota_window)
        self.token_ttl = token_ttl
        self._lock = threading.Lock()
        self._token_seq = 0
        self._current_token: str | None = None
        self._token_expires = 0.0
        # 游标清单版次
        self._cursor_version = 1
        self._cursor_rows = self._build_cursor_manifest(datasets["cursor"], 1)
        self._cursor_calls = 0
        self._cursor_rotate_after = cursor_rotate_after
        self.datasets = datasets
        # 时间窗源：当前状态 + 迟到变更日志
        self.window_log = WindowChangeLog(
            datasets["window"], retention=window_change_retention
        )
        self.stats = {"gateway_requests": 0, "renewals": 0, "rejected_429": 0}

    def _build_cursor_manifest(self, rows: list[dict], version: int) -> list[dict]:
        if version == 1:
            return list(rows)
        # 新版清单：删除部分记录、追加新记录并调整顺序（记录与顺序都可能变化）
        changed = [r for r in rows if int(r["order_id"].split("-")[1]) % 11 != 0]
        changed.sort(key=lambda r: (r["inspected_at"], r["order_id"], r["revision"]))
        return changed

    # ---- 令牌 ----
    def issue_token(self) -> dict:
        with self._lock:
            self._token_seq += 1
            self._current_token = f"tok-{self._token_seq}"
            self._token_expires = time.monotonic() + self.token_ttl
            self.stats["renewals"] += 1
            return {"token": self._current_token, "expires_in": self.token_ttl}

    def force_token_expiry(self) -> None:
        with self._lock:
            self._token_expires = 0.0

    def check_token(self, token: str | None) -> bool:
        with self._lock:
            return (
                token is not None
                and token == self._current_token
                and time.monotonic() < self._token_expires
            )

    # ---- 额度 ----
    def charge(self) -> tuple[bool, float]:
        ok, retry = self.quota.allow()
        with self._lock:
            if ok:
                self.stats["gateway_requests"] += 1
            else:
                self.stats["rejected_429"] += 1
        return ok, retry

    # ---- 游标清单 ----
    def note_cursor_call(self) -> None:
        with self._lock:
            if self._cursor_version != 1:
                return  # 已轮换到新版，保持稳定终态，后续调用不再改变清单
            self._cursor_calls += 1
            if (
                self._cursor_rotate_after is not None
                and self._cursor_calls >= self._cursor_rotate_after
            ):
                self._cursor_version = 2
                self._cursor_rows = self._build_cursor_manifest(
                    self.datasets["cursor"], 2
                )

    @property
    def cursor_version(self) -> int:
        return self._cursor_version

    def cursor_manifest(self) -> list[dict]:
        return list(self._cursor_rows)

    def rotate_cursor(self) -> int:
        with self._lock:
            if self._cursor_version == 1:
                self._cursor_version = 2
                self._cursor_rows = self._build_cursor_manifest(
                    self.datasets["cursor"], 2
                )
            return self._cursor_version


# ----------------------------------------------------------------------
# HTTP handler
# ----------------------------------------------------------------------
class _Handler(BaseHTTPRequestHandler):
    server_version = "CraneMock/1.0"

    def log_message(self, fmt, *args):  # 静默常规访问日志
        pass

    @property
    def state(self) -> "MockServer":
        return self.server  # type: ignore[assignment]

    def _send_json(self, code: int, payload: dict | list, headers: dict | None = None):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        for k, v in (headers or {}).items():
            self.send_header(k, str(v))
        self.end_headers()
        self.wfile.write(body)

    def _gateway_authorized(self) -> bool:
        """额度+令牌校验；失败时已写出响应，返回 False。"""
        ok, retry = self.state.gateway.charge()
        if not ok:
            self._send_json(
                429,
                {"error": "gateway quota exhausted"},
                {"Retry-After": round(retry, 3)},
            )
            return False
        auth = self.headers.get("Authorization", "")
        token = auth.removeprefix("Bearer ").strip() if auth else None
        if not self.state.gateway.check_token(token):
            self._send_json(401, {"error": "invalid or expired token"})
            return False
        return True

    # ---------------- GET 路由 ----------------
    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        qs = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        try:
            if path == "/offset/orders":
                return self._handle_offset(qs)
            if path == "/cursor/manifest":
                return self._handle_cursor_manifest(qs)
            if path == "/cursor/orders":
                return self._handle_cursor_orders(qs)
            if path == "/window/orders":
                return self._handle_window(qs)
            if path == "/window/changes":
                return self._handle_window_changes(qs)
            if path == "/mock/admin/stats":
                return self._send_json(200, self.state.admin_stats())
            self._send_json(404, {"error": "not found", "path": path})
        except Exception as exc:  # 模拟源自身不应崩溃
            self._send_json(500, {"error": f"mock failure: {exc}"})

    def do_POST(self):  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/gateway/token":
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length) if length else b"{}"
            try:
                payload = json.loads(raw or b"{}")
            except json.JSONDecodeError:
                payload = {}
            # 续期同样占用网关额度
            ok, retry = self.state.gateway.charge()
            if not ok:
                return self._send_json(
                    429,
                    {"error": "gateway quota exhausted"},
                    {"Retry-After": round(retry, 3)},
                )
            result = self.state.gateway.issue_token()
            result["note"] = (
                "renewed" if payload.get("grant_type") == "refresh_token" else "issued"
            )
            return self._send_json(200, result)
        if parsed.path == "/mock/admin/rotate-cursor":
            version = self.state.gateway.rotate_cursor()
            return self._send_json(200, {"cursor_version": version})
        if parsed.path == "/mock/admin/expire-token":
            self.state.gateway.force_token_expiry()
            return self._send_json(200, {"expired": True})
        if parsed.path == "/mock/admin/window-change":
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length) if length else b"{}"
            try:
                change = json.loads(raw or b"{}")
            except json.JSONDecodeError:
                return self._send_json(400, {"error": "invalid json"})
            try:
                event = self.state.gateway.window_log.apply(change)
            except (KeyError, TypeError, ValueError) as exc:
                return self._send_json(400, {"error": str(exc)})
            return self._send_json(200, event)
        self._send_json(404, {"error": "not found", "path": parsed.path})

    # ---------------- 三个来源 ----------------
    def _handle_offset(self, qs: dict):
        if not self._gateway_authorized():
            return
        rows = self.state.gateway.datasets["offset"]
        limit = int(qs.get("limit", 10))
        offset = int(qs.get("offset", 0))
        page = rows[offset : offset + limit]
        self._send_json(
            200,
            {
                "orders": page,
                "next_offset": offset + limit if offset + limit < len(rows) else None,
                "total": len(rows),
            },
        )

    def _handle_cursor_manifest(self, qs: dict):
        if not self._gateway_authorized():
            return
        self.state.gateway.note_cursor_call()
        rows = self.state.gateway.cursor_manifest()
        version = self.state.gateway.cursor_version
        # 清单只给 ID/修订顺序，正文仍走 /cursor/orders
        entries = [
            {"order_id": r["order_id"], "revision": r["revision"]} for r in rows
        ]
        self._send_json(
            200,
            {
                "manifest_version": f"v{version}",
                "entries": entries,
                "next_cursor": f"v{version}:0:{len(entries)}",
            },
        )

    def _handle_cursor_orders(self, qs: dict):
        if not self._gateway_authorized():
            return
        self.state.gateway.note_cursor_call()
        version = self.state.gateway.cursor_version
        cursor = qs.get("cursor")
        limit = int(qs.get("limit", 10))
        if cursor is None:
            return self._send_json(400, {"error": "cursor required"})
        try:
            cur_ver, start, _token = cursor.split(":")
            if int(cur_ver.lstrip("v")) != version:
                return self._send_json(
                    410,
                    {
                        "error": "cursor belongs to stale manifest",
                        "current_manifest": f"v{version}",
                    },
                )
            start_idx = int(start)
        except (ValueError, AttributeError):
            return self._send_json(400, {"error": "malformed cursor"})
        rows = self.state.gateway.cursor_manifest()
        page = rows[start_idx : start_idx + limit]
        end = start_idx + len(page)
        next_cursor = f"v{version}:{end}:x" if end < len(rows) else None
        self._send_json(
            200,
            {
                "manifest_version": f"v{version}",
                "orders": page,
                "next_cursor": next_cursor,
            },
        )

    def _handle_window(self, qs: dict):
        # 时间窗源：独立额度，无令牌
        ok, retry = self.state.window_quota.allow()
        if not ok:
            self.state.window_rejected += 1
            return self._send_json(
                429,
                {"error": "window source quota exhausted"},
                {"Retry-After": round(retry, 3)},
            )
        self.state.window_scans += 1
        # 数据来源是供应商"当前状态"，包含所有已补发/更正/撤回后的结果
        rows = self.state.gateway.window_log.rows()
        frm = qs.get("from")
        to = qs.get("to")
        if not frm or not to:
            return self._send_json(400, {"error": "from/to required"})
        # 闭区间 [from, to]：相邻窗口在边界上重叠，边界记录会重复返回
        selected = [r for r in rows if frm <= r["inspected_at"] <= to]
        selected.sort(key=lambda r: (r["inspected_at"], r["order_id"], r["revision"]))
        self._send_json(
            200,
            {
                "orders": selected,
                "count": len(selected),
                "window": {"from": frm, "to": to},
            },
        )

    def _handle_window_changes(self, qs: dict):
        """迟到变更流：只读变更日志，不重扫时间窗。"""
        ok, retry = self.state.window_quota.allow()
        if not ok:
            self.state.window_rejected += 1
            return self._send_json(
                429,
                {"error": "window source quota exhausted"},
                {"Retry-After": round(retry, 3)},
            )
        self.state.window_change_pages += 1
        log = self.state.gateway.window_log
        if qs.get("manifest") is not None:
            return self._send_json(200, log.manifest())
        try:
            since = int(qs.get("since", 0))
            until = int(qs["until"]) if "until" in qs else log.high_watermark
            limit = int(qs.get("limit", 100))
        except (TypeError, ValueError):
            return self._send_json(
                400, {"error": "since/until/limit must be integers"}
            )
        try:
            page = log.changes_since(since, until, limit)
        except _ChangeCursorExpired as exc:
            # 续点早于日志保留范围：客户端须重建基线
            return self._send_json(
                410,
                {
                    "error": "change cursor expired",
                    "since": exc.since,
                    "oldest_seq": exc.oldest_seq,
                },
            )
        page["since"] = since
        page["until"] = until
        return self._send_json(200, page)


class MockServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        addr: tuple[str, int],
        gateway_quota: int,
        gateway_window: float,
        window_quota: int,
        window_window: float,
        token_ttl: float,
        cursor_rotate_after: int | None,
        window_change_retention: int | None = None,
    ):
        super().__init__(addr, _Handler)
        datasets = build_datasets()
        self.gateway = GatewayState(
            gateway_quota,
            gateway_window,
            token_ttl,
            cursor_rotate_after,
            datasets,
            window_change_retention=window_change_retention,
        )
        self.window_quota = SlidingWindow(window_quota, window_window)
        self.window_rejected = 0
        # 观测：全量时间窗扫描页数 vs 变更流页数
        self.window_scans = 0
        self.window_change_pages = 0

    def admin_stats(self) -> dict:
        ds = self.gateway.datasets
        sizes = {k: len(v) for k, v in ds.items()}
        # 时间窗源给的是"当前状态"，含补发/更正/撤回后的结果
        sizes["window"] = self.gateway.window_log.row_count()
        return {
            "gateway": self.gateway.stats,
            "window_rejected_429": self.window_rejected,
            "window_scans": self.window_scans,
            "window_change_pages": self.window_change_pages,
            "dataset_sizes": sizes,
            "cursor_version": self.gateway.cursor_version,
            "window_change_watermark": self.gateway.window_log.high_watermark,
        }


def serve_forever(config: dict) -> MockServer:
    server = MockServer(
        (config.get("host", "127.0.0.1"), config.get("port", 0)),
        config.get("gateway_quota", 12),
        config.get("gateway_window", 5.0),
        config.get("window_quota", 18),
        config.get("window_window", 5.0),
        config.get("token_ttl", 300.0),
        config.get("cursor_rotate_after"),
        config.get("window_change_retention"),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
