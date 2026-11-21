"""极简 JSON HTTP 客户端（仅用标准库，在线程池中执行以支持并发）。

大响应支持流式处理：``get_stream`` 把响应文件对象交给一个同步回调，
在同一个线程里边读边解析，不需要把整份正文先读进内存。
"""
from __future__ import annotations

import asyncio
import json
import threading
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, TypeVar

from .limits import AuthError, GatewayError, RateLimited, StaleCursor

T = TypeVar("T")


class StreamAborted(Exception):
    """流式读取中途放弃：不把已被取消的响应继续读完/写库。"""


class HttpClient:
    def __init__(
        self,
        executor: asyncio.Executor,
        timeout: float = 10.0,
        on_submit: Callable[[], None] | None = None,
        on_done: Callable[[], None] | None = None,
    ):
        self._executor = executor
        self._timeout = timeout
        # 线程池任务计数：用于中止后等待在途 I/O 收尾（on_done 在线程真正
        # 结束时调用，即使等待它的协程已被取消）
        self._on_submit = on_submit
        self._on_done = on_done
        self._stream_lock = threading.Lock()
        self._active_streams: set[threading.Event] = set()

    async def run_blocking(self, fn: Callable[..., T], *args: Any) -> T:
        """把阻塞调用（HTTP 或数据库 I/O）放到线程池，避免卡住事件循环。"""
        loop = asyncio.get_running_loop()
        if self._on_submit is not None:
            self._on_submit()
        future = self._executor.submit(fn, *args)
        if self._on_done is not None:
            future.add_done_callback(lambda _f: self._on_done())
        try:
            return await asyncio.wrap_future(future, loop=loop)
        except asyncio.CancelledError:
            # 终止本客户端的在途流式读取（停止继续读大响应/写库），
            # 不等线程自己跑完才把取消传给调用方
            self.cancel_sync()
            raise

    def _request(self, url: str, params: dict, token: str | None) -> urllib.request.Request:
        query = urllib.parse.urlencode(params)
        full = f"{url}?{query}" if query else url
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return urllib.request.Request(full, headers=headers)

    @staticmethod
    def _raise_for_http_error(exc: urllib.error.HTTPError) -> None:
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

    def _blocking_get(
        self, url: str, params: dict, token: str | None
    ) -> dict[str, Any]:
        req = self._request(url, params, token)
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            self._raise_for_http_error(exc)
            raise AssertionError("unreachable")  # pragma: no cover
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise GatewayError(f"网络错误: {exc}") from exc

    def _blocking_get_stream(
        self,
        url: str,
        params: dict,
        token: str | None,
        process: Callable[[Any], T],
        cancel: threading.Event | None = None,
    ) -> T:
        req = self._request(url, params, token)
        if cancel is not None:
            with self._stream_lock:
                self._active_streams.add(cancel)
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                if cancel is not None and cancel.is_set():
                    raise StreamAborted("流式读取已被取消")
                return process(resp)
        except urllib.error.HTTPError as exc:
            self._raise_for_http_error(exc)
            raise AssertionError("unreachable")  # pragma: no cover
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise GatewayError(f"网络错误: {exc}") from exc
        finally:
            if cancel is not None:
                with self._stream_lock:
                    self._active_streams.discard(cancel)

    def cancel_sync(self) -> None:
        """取消本客户端当前所有在途的流式读取。

        由 ``run_blocking`` 在协程被取消时调用：流式解析会在下一个读块处
        放弃，不再继续读大响应、也不再继续写库。
        """
        with self._stream_lock:
            events = list(self._active_streams)
        for event in events:
            event.set()

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
        return await self.run_blocking(
            self._blocking_get, url, params or {}, token
        )

    async def get_stream(
        self,
        url: str,
        params: dict | None,
        token: str | None,
        process: Callable[[Any], T],
        cancel: threading.Event | None = None,
    ) -> T:
        """流式 GET：``process(resp)`` 在线程池中同步消费响应正文。

        ``cancel`` 置位后本次读取尽快放弃，用于中止后立即重跑。
        """
        return await self.run_blocking(
            self._blocking_get_stream, url, params or {}, token, process, cancel
        )

    async def post(self, url: str, payload: dict) -> dict[str, Any]:
        return await self.run_blocking(self._blocking_post, url, payload)
