"""同步引擎：并发编排来源，支持按来源拆分到多个进程。

- 偏移源/游标源共享跨进程网关额度与令牌；时间窗源独立额度。
- 每个来源在执行前先拿一把“来源锁”（state_dir/locks/<source>.lock）。
  同源已有进程在跑时，本进程跳过该来源，既不拉取也不发布。
- 配置的多个来源在本进程内作为独立 asyncio 任务并发推进；某个来源等待
  响应或冷却时，其他可请求的来源照常拿额度推进。
- 每个来源读完自己的完整一轮清单后独立发布快照。
- 支持外部中止（AbortSync）：已落盘的分页进度保留，重跑续读。
"""
from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable

from .coordination import CrossProcessGateway, SourceLock
from .fetchers import CursorFetcher, OffsetFetcher, WindowFetcher
from .http import HttpClient
from .limits import CooldownGate, IndependentLimiter, WindowQuota
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
    gateway_quota: int = 12                 # 每窗口共享请求数（跨进程）
    gateway_window: float = 5.0             # 窗口秒数
    window_source_quota: int = 18
    window_source_window: float = 5.0
    cooldown_seconds: float = 0.5           # 收到 429 后的客户端冷却
    window_minutes: int = 30
    window_horizon: str = "2026-11-12T00:00:00"
    sources: tuple[str, ...] = ("offset", "cursor", "window")
    # 等待同源锁的秒数；0 表示不等待：同源已有进程在跑时直接跳过该来源
    lock_wait: float = 0.0
    on_page: Callable[[str, int], Awaitable[None]] | None = None


class SyncEngine:
    def __init__(self, config: SyncConfig):
        self.config = config
        self.storage = Storage(Path(config.state_dir))
        self._executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="http")
        self.client = HttpClient(self._executor)

        # 共享网关（offset + cursor）：额度/令牌/冷却持久化，跨进程共享
        self.gateway: CrossProcessGateway | None = None
        if {"offset", "cursor"} & set(config.sources):
            self.gateway = CrossProcessGateway(
                Path(config.state_dir),
                self.client,
                config.base_url,
                config.gateway_quota,
                config.gateway_window,
            )
        # 时间窗源独立额度（来源锁保证同一时刻只有一个进程采集它）
        window_quota = WindowQuota(
            config.window_source_quota, config.window_source_window
        )
        self.window_limiter = IndependentLimiter(window_quota, CooldownGate())

        # 本进程跳过的来源：{来源: 原因}
        self.skipped: dict[str, str] = {}
        self._locks: dict[str, SourceLock] = {}

    def _build_fetchers(self):
        cfg = self.config
        common = dict(
            page_size=cfg.page_size,
            snapshot_format=cfg.snapshot_format,
            on_page=cfg.on_page,
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

    # ---------- 按来源抢锁 ----------
    def _claim(self, fetchers: dict) -> dict:
        """为每个配置来源抢锁；抢不到（同源进程在跑）则跳过该来源。"""
        claimed: dict = {}
        self.skipped.clear()
        for name, fetcher in fetchers.items():
            lock = SourceLock(self.config.state_dir, name)
            ok = lock.acquire(
                blocking=self.config.lock_wait > 0,
                timeout=self.config.lock_wait or None,
            )
            if not ok:
                reason = "同源采集进程正在运行，跳过（不拉取、不发布）"
                self.skipped[name] = reason
                logger.warning("[%s] %s", name, reason)
                continue
            self._locks[name] = lock
            claimed[name] = fetcher
            logger.info("[%s] 取得来源锁 %s", name, lock.path)
        return claimed

    def _release_locks(self) -> None:
        for lock in self._locks.values():
            lock.release()
        self._locks.clear()

    async def run(self) -> dict[str, dict]:
        fetchers = self._build_fetchers()
        claimed = self._claim(fetchers)
        results: dict[str, dict] = {}
        tasks = {
            name: asyncio.create_task(f.run(), name=f"fetch-{name}")
            for name, f in claimed.items()
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
            self._release_locks()
            self._executor.shutdown(wait=False)
        return results

    def close(self) -> None:
        self._release_locks()
        if self.gateway is not None:
            self.gateway.close()
        self.storage.close()
