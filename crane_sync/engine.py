"""同步引擎：并发编排三个来源。

- 偏移源/游标源共享网关额度与令牌；时间窗源独立额度。
- 三个来源作为独立 asyncio 任务并发推进；某个来源等待响应或冷却时，
  其他可请求的来源照常拿额度推进。
- 每个来源读完自己的完整一轮清单后独立发布快照。
- 存储 I/O（暂存写入、快照落盘）都在线程池中执行：某个来源写快照
  卡住时，事件循环与其他来源的采集/落盘/发布不受影响。
- 支持外部中止（AbortSync）：已落盘的分页进度保留，重跑续读。
"""
from __future__ import annotations

import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable

from .fetchers import CursorFetcher, OffsetFetcher, WindowFetcher
from .http import HttpClient
from .limits import CooldownGate, IndependentLimiter, SharedGateway, WindowQuota
from .storage import Storage

logger = logging.getLogger("crane_sync")


class AbortSync(Exception):
    """由测试钩子抛出，模拟同步被强制中止。"""


@dataclass
class SyncConfig:
    base_url: str
    state_dir: str
    snapshot_format: str = "jsonl"          # jsonl | parquet
    page_size: int = 10
    gateway_quota: int = 12                 # 每窗口共享请求数
    gateway_window: float = 5.0             # 窗口秒数
    window_source_quota: int = 18
    window_source_window: float = 5.0
    cooldown_seconds: float = 0.5           # 收到 429 后的客户端冷却
    window_minutes: int = 30
    window_horizon: str = "2026-11-12T00:00:00"
    sources: tuple[str, ...] = ("offset", "cursor", "window")
    on_page: Callable[[str, int], Awaitable[None]] | None = None
    # 测试钩子：某来源发布快照前调用（在存储线程中执行，可阻塞）
    publish_hook: Callable[[str], None] | None = None
    # 流式解析每条正文的入库批大小（条数上限 + 字节上限）
    orders_batch: int = 128
    orders_batch_bytes: int = 256 * 1024
    # 快照发布时的分批行数（JSONL 写盘/快照表归档）
    publish_batch: int = 200


class SyncEngine:
    def __init__(self, config: SyncConfig):
        self.config = config
        self.storage = Storage(Path(config.state_dir))
        # HTTP 与存储 I/O 使用同一个线程池：任何一个阻塞调用都不会占用
        # 事件循环；不同来源的 I/O 可以真正并行。
        self._executor = ThreadPoolExecutor(
            max_workers=12, thread_name_prefix="crane-io"
        )
        self._inflight = 0
        self._inflight_lock = threading.Lock()
        self._inflight_idle: threading.Condition | None = None
        self.client = HttpClient(
            self._executor,
            on_submit=self._note_inflight_start,
            on_done=self._note_inflight_done,
        )

        # 共享网关（offset + cursor）
        gateway_quota = WindowQuota(config.gateway_quota, config.gateway_window)
        gateway_cooldown = CooldownGate()
        self.gateway = SharedGateway(
            gateway_quota, gateway_cooldown, self._issue_gateway_token
        )
        # 时间窗源独立额度
        window_quota = WindowQuota(
            config.window_source_quota, config.window_source_window
        )
        self.window_limiter = IndependentLimiter(window_quota, CooldownGate())

    # ---------- 在途 I/O 计数 ----------
    def _note_inflight_start(self) -> None:
        with self._inflight_lock:
            self._inflight += 1

    def _note_inflight_done(self) -> None:
        with self._inflight_lock:
            self._inflight -= 1
            if self._inflight <= 0 and self._inflight_idle is not None:
                self._inflight_idle.notify_all()

    def wait_io_idle(self, timeout: float = 30.0) -> bool:
        """等待线程池中的在途 I/O 全部结束（含被取消的协程触发的写入）。

        中止后立即重跑时，先等上一轮真正落盘完成，避免两轮交错写同一个
        暂存表/进度。返回是否在超时前等到空闲。
        """
        with self._inflight_lock:
            if self._inflight_idle is None:
                self._inflight_idle = threading.Condition(self._inflight_lock)
            return self._inflight_idle.wait_for(
                lambda: self._inflight <= 0, timeout=timeout
            )

    async def _issue_gateway_token(self) -> str:
        """取/续令牌。

        调用方（SharedGateway）已先申请过一次额度；若 POST 撞上服务端
        窗口边界的 429，则按 Retry-After 冷却并重新申请额度后再发，
        保证续期请求与数据请求一样严格遵守网关额度。
        """
        from .limits import GatewayError, RateLimited

        payload = {
            "grant_type": (
                "refresh_token" if self.gateway._generation else "client_credentials"
            )
        }
        attempts = 0
        while True:
            attempts += 1
            if attempts > 8:
                raise GatewayError("令牌续期多次被限流，网关持续不可用")
            try:
                data = await self.client.post(
                    f"{self.config.base_url}/gateway/token", payload
                )
                logger.info("取得新网关令牌（世代 %s）", data.get("token"))
                return data["token"]
            except RateLimited as exc:
                logger.info("令牌续期遇到 429，冷却 %.2fs 并重新申请额度", exc.retry_after)
                await self.gateway.cooldown.cooldown(exc.retry_after)
                await self.gateway.quota.acquire(("token", "retry"))

    def _build_fetchers(self):
        cfg = self.config
        common = dict(
            page_size=cfg.page_size,
            snapshot_format=cfg.snapshot_format,
            on_page=cfg.on_page,
            publish_hook=cfg.publish_hook,
            storage_call=self.client.run_blocking,
            orders_batch=cfg.orders_batch,
            orders_batch_bytes=cfg.orders_batch_bytes,
            publish_batch=cfg.publish_batch,
        )
        fetchers = {}
        if "offset" in cfg.sources:
            fetchers["offset"] = OffsetFetcher(
                self.storage, self.client, cfg.base_url, self.gateway, **common
            )
        if "cursor" in cfg.sources:
            fetchers["cursor"] = CursorFetcher(
                self.storage, self.client, cfg.base_url, self.gateway, **common
            )
        if "window" in cfg.sources:
            fetchers["window"] = WindowFetcher(
                self.storage,
                self.client,
                cfg.base_url,
                self.window_limiter,
                window_minutes=cfg.window_minutes,
                horizon=cfg.window_horizon,
                **common,
            )
        return fetchers

    async def run(self) -> dict[str, dict]:
        fetchers = self._build_fetchers()
        results: dict[str, dict] = {}
        tasks = {
            name: asyncio.create_task(f.run(), name=f"fetch-{name}")
            for name, f in fetchers.items()
        }
        pending = set(tasks.values())
        try:
            while pending:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_EXCEPTION
                )
                for task in done:
                    name = task.get_name().removeprefix("fetch-")
                    exc = task.exception()
                    if exc is not None:
                        # 任一来源失败/中止：取消其他仍在等待的来源
                        for t in pending:
                            t.cancel()
                        if pending:
                            await asyncio.gather(*pending, return_exceptions=True)
                        raise exc
                    results[name] = task.result()
        finally:
            # 不再接受新任务；等线程池中已提交的写入真正结束再返回，
            # 保证调用方看到的中止点是"已落盘的进度 + 无半成品"。
            self._executor.shutdown(wait=False, cancel_futures=True)
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.wait_io_idle, 30.0)
        return results

    def close(self) -> None:
        self.storage.close()
