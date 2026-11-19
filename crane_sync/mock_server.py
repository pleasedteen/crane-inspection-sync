"""三个本地模拟 HTTP 数据源。

一个 ThreadingHTTPServer 进程暴露三组前缀：

- /offset/**     偏移分页源（?limit=&offset=），走共享网关令牌
- /cursor/**     游标分页源（清单版次 + 游标），走共享网关令牌
- /window/**     时间窗分页源（?from=&to=），独立额度，无需令牌
- /gateway/token POST 取/续共享令牌（续期使旧令牌失效，且占用额度）
- /mock/admin/** 测试辅助：强制令牌过期、轮换游标清单、查看计数

游标语义：游标属于某一版清单，清单过期后旧游标返回 410，
客户端只能从新版清单开头重读；新版的记录集合与顺序都可能变化。
时间窗语义：相邻窗口在边界上重叠，同一条边界记录会被两个窗口重复返回；
同一 inspected_at 时刻允许存在多张单。
"""
from __future__ import annotations

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
        rows = self.state.gateway.datasets["window"]
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
    ):
        super().__init__(addr, _Handler)
        datasets = build_datasets()
        self.gateway = GatewayState(
            gateway_quota,
            gateway_window,
            token_ttl,
            cursor_rotate_after,
            datasets,
        )
        self.window_quota = SlidingWindow(window_quota, window_window)
        self.window_rejected = 0

    def admin_stats(self) -> dict:
        ds = self.gateway.datasets
        return {
            "gateway": self.gateway.stats,
            "window_rejected_429": self.window_rejected,
            "dataset_sizes": {k: len(v) for k, v in ds.items()},
            "cursor_version": self.gateway.cursor_version,
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
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
