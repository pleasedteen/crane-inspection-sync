"""同步引擎：并发编排三个来源。

- 偏移源/游标源共享网关额度与令牌；时间窗源独立额度。
- 三个来源作为独立 asyncio 任务并发推进；某个来源等待响应或冷却时，
  其他可请求的来源照常拿额度推进。
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

from .diagnostics import AttemptRecorder
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


class SyncEngine:
    def __init__(self, config: SyncConfig):
        self.config = config
        self.storage = Storage(Path(config.state_dir))
        self._executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="http")
        self.client = HttpClient(self._executor)
        # 请求尝试诊断：先对账旧进程遗留的在途请求，再登记本次运行
        self.storage.reconcile_attempts()
        self.run_id = self.storage.begin_run()
        self.recorder = AttemptRecorder(self.storage, self.run_id)

        # 共享网关（offset + cursor）
        gateway_quota = WindowQuota(config.gateway_quota, config.gateway_window)
        gateway_cooldown = CooldownGate()
        self.gateway = SharedGateway(
            gateway_quota,
            gateway_cooldown,
            self._issue_gateway_token,
            probe=self.recorder.probe,
        )
        # 时间窗源独立额度
        window_quota = WindowQuota(
            config.window_source_quota, config.window_source_window
        )
        self.window_limiter = IndependentLimiter(
            window_quota, CooldownGate(), probe=self.recorder.probe
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
            attempt_id = self.recorder.begin(
                "gateway",
                "token",
                "/gateway/token",
                payload["grant_type"],
                {"generation_before": self.gateway._generation},
            )
            self.recorder.set_state("gateway", "waiting_reply", endpoint="/gateway/token")
            try:
                data = await self.client.post(
                    f"{self.config.base_url}/gateway/token", payload
                )
            except RateLimited as exc:
                outcome, status, confirmed, detail = AttemptRecorder.classify(exc)
                self.recorder.finish_error(
                    attempt_id, outcome, status, confirmed, detail
                )
                logger.info("令牌续期遇到 429，冷却 %.2fs 并重新申请额度", exc.retry_after)
                await self.gateway.cooldown.cooldown(exc.retry_after)
                await self.gateway.quota.acquire(("token", "retry"))
                continue
            except BaseException as exc:
                outcome, status, confirmed, detail = AttemptRecorder.classify(exc)
                self.recorder.finish_error(
                    attempt_id, outcome, status, confirmed, detail
                )
                raise
            self.recorder.finish_ok(
                attempt_id, detail={"token": data.get("token")}
            )
            logger.info("取得新网关令牌（世代 %s）", data.get("token"))
            return data["token"]

    def _build_fetchers(self):
        cfg = self.config
        common = dict(
            page_size=cfg.page_size,
            snapshot_format=cfg.snapshot_format,
            on_page=cfg.on_page,
            recorder=self.recorder,
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
                                await asyncio.gather(
                                    *pending, return_exceptions=True
                                )
                            # 取走同一批其余任务的结果，避免
                            # "Task exception was never retrieved" 噪音
                            for t in done:
                                if t is not task and not t.cancelled():
                                    t.exception()
                            raise exc
                        results[name] = task.result()
                        self.recorder.set_state(name, "done")
            except BaseException:
                self.storage.finish_run(self.run_id, "aborted")
                self._mark_remaining_aborted(tasks, results)
                raise
            self.storage.finish_run(self.run_id, "completed")
            self.recorder.set_state("gateway", "done")
            return results
        finally:
            self._executor.shutdown(wait=False)

    def _mark_remaining_aborted(self, tasks, results) -> None:
        for name in tasks:
            if name not in results:
                self.recorder.set_state(name, "aborted")
        self.recorder.set_state("gateway", "aborted")

    def close(self) -> None:
        self.storage.close()
