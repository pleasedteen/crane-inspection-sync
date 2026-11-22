"""同步引擎：并发编排三个来源。

- 偏移源/游标源共享网关额度与令牌；时间窗源独立额度。
- 三个来源作为独立 asyncio 任务并发推进；某个来源等待响应或冷却时，
  其他可请求的来源照常拿额度推进。
- 每个来源读完自己的完整一轮清单后独立发布快照。
- 每次单源发布都会触发一次合并快照交付：由独立的合并发布器把当前各
  来源最新已发布快照合并（与 read --merged 相同的投影与配置格式），
  原子发布为可定位的合并快照文件。合并写出与来源采集互不阻塞：一路
  合并写出卡住时，其他来源照常拉完并先发布；写出恢复后才读取最新
  已发布指针，等待期间完成的发布会被纳入本次合并。
- 支持外部中止（AbortSync）：已落盘的分页进度保留，重跑续读；合并
  指针未切换前，读者始终只看到上一份完整合并结果。
"""
from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
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
    # 测试钩子：每次合并快照写出前调用（模拟合并写出卡住/被强停）
    on_merged_write: Callable[[int], Awaitable[None]] | None = None


class SyncEngine:
    def __init__(self, config: SyncConfig):
        self.config = config
        self.storage = Storage(Path(config.state_dir))
        self._executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="http")
        self.client = HttpClient(self._executor)

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

        # 合并发布状态（在 run() 里按事件循环重建）
        self.merged_info: dict | None = None       # 最近一次合并发布结果
        self.merged_history: list[dict] = []       # 本轮全部合并发布（按序）
        self._pub_seq = 0                          # 单源发布计数（触发号）
        self._merged_done_seq = 0                  # 合并已反映到的触发号
        self._merged_calls = 0                     # 合并写出次数（钩子入参）
        self._merged_stop = False                  # 全部来源完成，收尾后退出
        self._merged_event: asyncio.Event | None = None

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
            on_publish=self._note_source_published,
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

    # ---------- 合并快照发布 ----------
    def _note_source_published(self, source: str, info: dict) -> None:
        """单源发布完成（采集器回调）：唤醒合并发布器。"""
        assert self._merged_event is not None
        self._pub_seq += 1
        self._merged_event.set()

    async def _merged_worker(self) -> None:
        """合并发布器：每次单源发布后交付一份最新的可定位合并快照。

        与来源采集并发推进；写出动作若曾被卡住，恢复后才读取最新已
        发布指针，等待期间完成的单源发布会被纳入同一次合并。
        """
        assert self._merged_event is not None
        while True:
            await self._merged_event.wait()
            self._merged_event.clear()
            # 触发号是唯一事实来源：把积压的发布一次合并到位
            while self._merged_done_seq < self._pub_seq:
                await self._publish_merged_once()
            if self._merged_stop:
                return

    async def _publish_merged_once(self) -> None:
        # 测试钩子：模拟合并快照写出阶段卡住/被强停
        hook = self.config.on_merged_write
        if hook is not None:
            await hook(self._merged_calls)
        self._merged_calls += 1
        # 卡住恢复后才取号并读取各来源最新已发布指针：
        # 等待期间完成的单源发布会被纳入本次合并
        seq = self._pub_seq
        info = self.storage.publish_merged(self.config.snapshot_format)
        logger.info(
            "[merged] 已发布合并快照 %s（%s 条）文件=%s",
            info["snapshot_id"], info["record_count"], info["file_path"],
        )
        self.merged_info = info
        self.merged_history.append(info)
        self._merged_done_seq = max(self._merged_done_seq, seq)

    async def run(self) -> dict[str, dict]:
        fetchers = self._build_fetchers()
        results: dict[str, dict] = {}
        # 合并发布状态随事件循环重建
        self.merged_info = None
        self.merged_history = []
        self._pub_seq = 0
        self._merged_done_seq = 0
        self._merged_calls = 0
        self._merged_stop = False
        self._merged_event = asyncio.Event()
        worker = asyncio.create_task(
            self._merged_worker(), name="merged-publisher"
        )
        tasks = {
            name: asyncio.create_task(f.run(), name=f"fetch-{name}")
            for name, f in fetchers.items()
        }
        fetch_pending = set(tasks.values())
        pending = fetch_pending | {worker}
        error: BaseException | None = None
        try:
            while pending:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    if task is worker:
                        # 合并发布器只可能因异常退出（如写出阶段被强停）
                        exc = task.exception()
                        if exc is not None and error is None:
                            error = exc
                        continue
                    fetch_pending.discard(task)
                    name = task.get_name().removeprefix("fetch-")
                    exc = task.exception()
                    if exc is not None:
                        if error is None:
                            error = exc
                    else:
                        results[name] = task.result()
                if error is not None:
                    # 任一来源失败/中止：取消其他仍在等待的来源与合并发布器
                    for t in pending:
                        t.cancel()
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)
                    pending = set()
                elif not fetch_pending and not self._merged_stop:
                    # 全部来源完成：合并发布器把最后的单源发布
                    # 反映进合并快照后自行退出
                    self._merged_stop = True
                    self._merged_event.set()
        finally:
            if not worker.done():
                worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            self._executor.shutdown(wait=False)
        if error is not None:
            raise error
        return results

    def close(self) -> None:
        self.storage.close()
