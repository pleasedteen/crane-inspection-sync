"""极简 JSON HTTP 客户端（仅用标准库，在线程池中执行以支持并发）。"""
from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from .limits import AuthError, GatewayError, RateLimited, StaleCursor


class HttpClient:
    def __init__(self, executor: asyncio.Executor, timeout: float = 10.0):
        self._executor = executor
        self._timeout = timeout

    def _blocking_get(
        self, url: str, params: dict, token: str | None
    ) -> dict[str, Any]:
        query = urllib.parse.urlencode(params)
        full = f"{url}?{query}" if query else url
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        req = urllib.request.Request(full, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            retry_after = 0.0
            if exc.headers.get("Retry-After"):
                try:
                    retry_after = float(exc.headers["Retry-After"])
                except ValueError:
                    retry_after = 1.0
            if exc.code == 401:
                raise AuthError(body) from exc
            if exc.code == 410:
                raise StaleCursor(body) from exc
            if exc.code == 429:
                raise RateLimited(retry_after) from exc
            raise GatewayError(f"HTTP {exc.code}: {body}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise GatewayError(f"网络错误: {exc}") from exc

    def _blocking_post(self, url: str, payload: dict) -> dict[str, Any]:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            retry_after = 0.0
            if exc.headers.get("Retry-After"):
                try:
                    retry_after = float(exc.headers["Retry-After"])
                except ValueError:
                    retry_after = 1.0
            # 令牌续期同样受网关额度约束，可能收到 429
            if exc.code == 429:
                raise RateLimited(retry_after) from exc
            raise GatewayError(f"HTTP {exc.code}: {body}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise GatewayError(f"网络错误: {exc}") from exc

    async def get(
        self, url: str, params: dict | None = None, token: str | None = None
    ) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor, self._blocking_get, url, params or {}, token
        )

    async def post(self, url: str, payload: dict) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor, self._blocking_post, url, payload
        )
