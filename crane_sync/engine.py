"""同步引擎：并发编排三个来源，支持同一引擎连续多轮与取消后立即重跑。

- 偏移源/游标源共享网关额度与令牌；时间窗源独立额度。
- 三个来源作为独立 asyncio 任务并发推进；某个来源等待响应或冷却时，
  其他可请求的来源照常拿额度推进。
- 每个来源读完自己的完整一轮清单后独立发布快照。
- HTTP 执行器、网关（额度/冷却/令牌世代/在途续期）在引擎生命周期内
  只创建一次：run() 可以在同一事件循环里连续调用多轮，取消后立刻
  重跑也沿用未耗尽的额度窗口、冷却剩余时间与已取得的令牌。
- 每轮 run() 对应一个 SyncSession（按世代编号）。中止/取消只 drain
  当前会话的任务：旧轮协程一旦感知到取消便不再派发请求、不再落盘；
  已在途的 HTTP 调用与令牌续期任务不被强制终止，其结果可被后续轮次
  复用（见 limits.SharedGateway）。
"""
from __future__ import annotations

import asyncio
import logging
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
    http_timeout: float = 10.0              # 单次 HTTP 调用超时


@dataclass
class SyncSession:
    """一次 run() 的会话标识与取消状态。

    generation 单调递增，旧轮任务即使被唤醒，也能通过 cancelled()
    识别自己已作废，从而不再派发请求或写入进度/快照。
    """

    generation: int
    cancelled: bool = False

    def cancel(self) -> None:
        self.cancelled = True

    def ensure_active(self) -> None:
        if self.cancelled:
            raise asyncio.CancelledError("同步会话已取消")


class SyncEngine:
    def __init__(self, config: SyncConfig):
        self.config = config
        self.storage = Storage(Path(config.state_dir))
        # 执行器在引擎生命周期内复用：run() 多轮共用，关闭引擎时才回收
        self._executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="http")
        self.client = HttpClient(self._executor, timeout=config.http_timeout)

        # 共享网关（offset + cursor）：额度窗口、冷却与令牌世代跨轮保留
        gateway_quota = WindowQuota(config.gateway_quota, config.gateway_window)
        gateway_cooldown = CooldownGate()
        self.gateway = SharedGateway(
            gateway_quota, gateway_cooldown, self._issue_gateway_token
        )
        # 时间窗源独立额度，同样跨轮保留
        window_quota = WindowQuota(
            config.window_source_quota, config.window_source_window
        )
        self.window_limiter = IndependentLimiter(window_quota, CooldownGate())

        self._session_generation = 0
        self._running = False

    async def _issue_gateway_token(self) -> str:
        """取/续令牌。

        调用方（SharedGateway）已先申请过一次额度；若 POST 撞上服务端
        窗口边界的 429，则按 Retry-After 冷却并重新申请额度后再发，
        保证续期请求与数据请求一样严格遵守网关额度。

        本协程通常作为"在途任务"运行（见 SharedGateway）：等待它的
        同步轮被取消不会连带取消它，任务完成后新令牌自动进入网关。
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

    def _build_fetchers(self, session: SyncSession):
        cfg = self.config
        common = dict(
            page_size=cfg.page_size,
            snapshot_format=cfg.snapshot_format,
            on_page=cfg.on_page,
            session=session,
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

    async def _drain(self, tasks: set[asyncio.Task]) -> None:
        """等待本轮剩余任务全部结束，吞掉取消/中止类异常。"""
        if not tasks:
            return
        await asyncio.gather(*tasks, return_exceptions=True)

    async def run(self) -> dict[str, dict]:
        if self._running:
            raise RuntimeError("同一引擎不允许并发 run()，请等待上一轮结束")
        self._running = True
        self._session_generation += 1
        session = SyncSession(self._session_generation)
        fetchers = self._build_fetchers(session)
        tasks = {
            asyncio.create_task(f.run(), name=f"fetch-{name}")
            for name, f in fetchers.items()
        }
        pending: set[asyncio.Task] = set(tasks)
        results: dict[str, dict] = {}
        try:
            while pending:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_EXCEPTION
                )
                for task in done:
                    name = task.get_name().removeprefix("fetch-")
                    exc = task.exception()
                    if exc is not None:
                        # 任一来源失败/中止：标记会话作废并取消其他来源
                        session.cancel()
                        for t in pending:
                            t.cancel()
                        await self._drain(pending)
                        raise exc
                    results[name] = task.result()
        except asyncio.CancelledError:
            # 调用方直接取消 run()（任务级取消）：同样只结束当前会话，
            # 在途 HTTP / 令牌任务保留，引擎仍可立即开始下一轮
            session.cancel()
            for t in pending:
                t.cancel()
            await self._drain(pending)
            raise
        finally:
            self._running = False
            # 注意：不在此关闭执行器/网关，多轮 run() 与取消后立即重跑
            # 都依赖它们继续存活；资源回收统一在 close() 完成。
        return results

    def close(self) -> None:
        # cancel_futures：仍在线程池队列里的 HTTP 调用不必再执行；
        # 已在途的阻塞 urlopen 会自然结束或随进程退出而释放
        self._executor.shutdown(wait=False, cancel_futures=True)
        self.storage.close()
