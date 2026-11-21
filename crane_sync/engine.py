"""同步引擎：并发编排三个来源。

- 偏移源/游标源共享网关额度与令牌；时间窗源独立额度。
- 三个来源作为独立 asyncio 任务并发推进；某个来源等待响应或冷却时，
  其他可请求的来源照常拿额度推进。
- 每个来源读完自己的完整一轮清单后独立发布快照。
- 偏移源可按作业区拆分：每个区一个任务，共享原网关额度/令牌；
  全部区采齐后仍只发布一份 offset 快照。
- 支持外部中止（AbortSync）：已落盘的分页进度保留，重跑续读。
"""
from __future__ import annotations

import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable

from .fetchers import CursorFetcher, OffsetFetcher, WindowFetcher
from .http import HttpClient
from .limits import CooldownGate, IndependentLimiter, SharedGateway, WindowQuota
from .storage import OFFSET_ROUND_STATE_KEY, Storage, zone_progress_key

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
    # 偏移源作业区列表；留空即使用旧单接口（不带 zone 参数）
    offset_zones: tuple[str, ...] = ()
    on_page: Callable[[str, int], Awaitable[None]] | None = None

    def __post_init__(self) -> None:
        # 允许从 JSON 传入列表，也容忍 "A,B,C" 这样的字符串
        self.sources = tuple(self.sources)
        zones = self.offset_zones or ()
        if isinstance(zones, str):
            zones = zones.split(",")
        self.offset_zones = tuple(
            str(z).strip() for z in zones if str(z).strip()
        )


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

    def _build_fetchers(self) -> dict[str, object]:
        cfg = self.config
        common = dict(
            page_size=cfg.page_size,
            snapshot_format=cfg.snapshot_format,
            on_page=cfg.on_page,
        )
        fetchers: dict[str, object] = {}
        if "offset" in cfg.sources:
            if cfg.offset_zones:
                for zone in cfg.offset_zones:
                    fetchers[f"offset:{zone}"] = OffsetFetcher(
                        self.storage,
                        self.client,
                        cfg.base_url,
                        self.gateway,
                        zone=zone,
                        **common,
                    )
            else:
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

    # ---------- 偏移源作业区轮次 ----------
    def _load_round(self) -> dict | None:
        raw = self.storage.get_state(OFFSET_ROUND_STATE_KEY)
        if not raw:
            return None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if not isinstance(data, dict) or "zones" not in data:
            return None
        return data

    def _save_round(self, data: dict) -> None:
        self.storage.set_state(
            OFFSET_ROUND_STATE_KEY, json.dumps(data, ensure_ascii=False)
        )

    def _prepare_offset_round(self) -> list[str]:
        """确定本轮参与发布的作业区，并只重置真正需要重采的区。

        语义：
        - 上一轮已完整发布，且配置的作业区集合不变 → 开启全新一轮，
          清空这些区的进度与暂存，全部重采；
        - 其余情况（强杀后续跑 / 增删作业区）→ 延续本轮：
          已采齐的区保留进度与暂存（零请求），未采完的沿原进度续读；
          新增区清掉旧痕迹后从头采集；撤下的区移出本轮并释放暂存。
        """
        cfg_zones = list(self.config.offset_zones)
        rnd = self._load_round()
        if rnd is None:
            for zone in cfg_zones:
                self.storage.reset_progress(zone_progress_key(zone))
                self.storage.clear_staging("offset", zone=zone)
            self._save_round({"zones": cfg_zones, "published": False})
            return cfg_zones

        prev_zones = list(rnd.get("zones", []))
        if rnd.get("published") and set(prev_zones) == set(cfg_zones):
            # 周期性的全新一轮
            for zone in cfg_zones:
                self.storage.reset_progress(zone_progress_key(zone))
                self.storage.clear_staging("offset", zone=zone)
            self._save_round({"zones": cfg_zones, "published": False})
            return cfg_zones

        added = [z for z in cfg_zones if z not in prev_zones]
        removed = [z for z in prev_zones if z not in cfg_zones]
        for zone in added:
            # 新加入本轮的区必须从头采集
            self.storage.reset_progress(zone_progress_key(zone))
            self.storage.clear_staging("offset", zone=zone)
        for zone in removed:
            # 撤下的区不再参与发布，释放其暂存占用
            self.storage.reset_progress(zone_progress_key(zone))
            self.storage.clear_staging("offset", zone=zone)
        self._save_round({"zones": cfg_zones, "published": False})
        logger.info(
            "[offset] 延续本轮作业区（保留=%s 新增=%s 撤下=%s）",
            [z for z in cfg_zones if z not in added],
            added,
            removed,
        )
        return cfg_zones

    def _publish_offset_zones(self, zone_results: dict[str, dict]) -> dict:
        """所有区采齐后统一发布一份 offset 快照。

        使用本轮参与发布的作业区集合：撤下的区即使暂存残留也不进入快照；
        快照内同一单号按剩余区的最高修订折叠，间隙已是毫米。
        """
        rnd = self._load_round() or {"zones": []}
        zones = list(rnd.get("zones", []))
        info = self.storage.publish_snapshot(
            "offset", self.config.snapshot_format, zones=zones
        )
        self._save_round({**rnd, "published": True})
        logger.info(
            "[offset] 作业区 %s 全部采齐，已发布统一快照 %s（%s 条）",
            ",".join(zones) or "（无）", info["snapshot_id"],
            info["record_count"],
        )
        info = dict(info)
        info["zones"] = zone_results
        return info

    async def run(self) -> dict[str, dict]:
        zone_mode = bool(self.config.offset_zones) and "offset" in self.config.sources
        if zone_mode:
            self._prepare_offset_round()
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
                first_exc: BaseException | None = None
                for task in done:
                    name = task.get_name().removeprefix("fetch-")
                    exc = task.exception()
                    if exc is not None:
                        # 先记下错误，继续取完其余已完成任务的结果/异常，
                        # 避免"Task exception was never retrieved"告警
                        if first_exc is None:
                            first_exc = exc
                        continue
                    results[name] = task.result()
                if first_exc is not None:
                    # 任一来源失败/中止：取消其他仍在等待的来源
                    for t in pending:
                        t.cancel()
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)
                    raise first_exc
        finally:
            self._executor.shutdown(wait=False)
        if zone_mode:
            zone_results = {
                name.removeprefix("offset:"): results.pop(name)
                for name in list(results)
                if name.startswith("offset:")
            }
            results["offset"] = self._publish_offset_zones(zone_results)
        return results

    def close(self) -> None:
        self.storage.close()
