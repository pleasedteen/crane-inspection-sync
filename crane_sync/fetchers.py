"""三个来源的采集器。

每个采集器：
- 每成功读完一页就把分页进度落盘（强制中止后可续读）；
- 记录写入暂存表（按 来源+作业区+单号+修订号 去重，间隙已归一为毫米）；
- 读完整轮清单后独立发布原子快照，发布后才对读者可见。

偏移源可按作业区拆分为多个并发采集任务：各区共享同一网关额度/令牌，
各自落盘独立进度，全部采齐后由引擎统一发布一份 offset 快照。
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable

from .http import HttpClient
from .limits import IndependentLimiter, SharedGateway
from .models import InspectionOrder
from .storage import Storage, zone_progress_key

logger = logging.getLogger("crane_sync")


class BaseFetcher:
    name = "base"
    # 暂存/发布使用的来源名；作业区模式下 name 带区后缀，source 仍是家长来源
    source = "base"

    def __init__(
        self,
        storage: Storage,
        client: HttpClient,
        base_url: str,
        page_size: int = 10,
        snapshot_format: str = "jsonl",
        on_page: Callable[[str, int], Awaitable[None]] | None = None,
    ):
        self.storage = storage
        self.client = client
        self.base_url = base_url.rstrip("/")
        self.page_size = page_size
        self.snapshot_format = snapshot_format
        # 测试钩子：每处理完一页调用一次，便于做"中途中止"测试
        self.on_page = on_page
        self._position = 0

    async def _page_hook(self) -> None:
        if self.on_page is not None:
            await self.on_page(self.hook_name, self.pages_done)

    @property
    def hook_name(self) -> str:
        """测试钩子收到的名称；作业区模式带区后缀，其余等于来源名。"""
        return self.name

    @property
    def pages_done(self) -> int:
        return self._pages

    def _ingest(self, payloads: list[dict], zone: str = "") -> int:
        orders = [InspectionOrder.from_payload(p, self.source) for p in payloads]
        track = self._track_sequence()
        if track:
            # 清单中的每一行（含同一单号的每个修订）都有唯一位置
            for order in orders:
                order.position = self._position
                self._position += 1
        return self.storage.upsert_records(
            self.source, orders, track_position=track, zone=zone
        )

    def _track_sequence(self) -> bool:
        """是否按来源清单行序维护 position（游标源必须为 True）。"""
        return False

    async def run(self) -> dict:  # pragma: no cover - 抽象方法
        raise NotImplementedError


class OffsetFetcher(BaseFetcher):
    """偏移分页源：?limit=&offset=[&zone=]，进度即下一页 offset。

    两种运行模式：

    - 旧单接口模式（zone=None）：不带 zone 参数，进度键仍为 ``offset``，
      读完整轮后自行发布快照，与历史配置完全兼容。
    - 作业区模式（zone 非空）：每个作业区一个采集任务，各自用自己的进度键
      与暂存分区；不自行发布，由引擎在所有区采齐后统一发布一份快照。
      本轮已采齐的区再次启动时直接跳过，不发出任何请求。
    """

    name = "offset"
    source = "offset"

    def __init__(
        self,
        storage,
        client,
        base_url,
        gateway: SharedGateway,
        zone: str | None = None,
        **kw,
    ):
        super().__init__(storage, client, base_url, **kw)
        self.gateway = gateway
        self.zone = zone
        self._pages = 0
        if zone is not None:
            # 进度键、日志与网关排队/公平性都按作业区区分
            self.name = f"offset:{zone}"

    async def run(self) -> dict:
        if self.zone is not None:
            return await self._run_zone()
        return await self._run_legacy()

    async def _run_legacy(self) -> dict:
        progress = self.storage.get_progress(self.source) or {}
        saved_offset = progress.get("next_offset", 0)
        if saved_offset is None:
            # 上一轮已完整读完，新一轮从头开始
            self.storage.reset_progress(self.source)
            offset = 0
        else:
            offset = int(saved_offset)
        self._pages = 0
        logger.info("[offset] 从 offset=%s 开始（或续读）", offset)
        while True:
            async def _send(token, offset=offset):
                return await self.client.get(
                    f"{self.base_url}/offset/orders",
                    {"limit": self.page_size, "offset": offset},
                    token=token,
                )

            data = await self.gateway.request(_send, owner=self.name)
            written = self._ingest(data.get("orders", []))
            offset = data["next_offset"]
            self._pages += 1
            self.storage.set_progress(
                self.source, {"next_offset": offset, "done": offset is None}
            )
            logger.info(
                "[offset] 第 %s 页完成，本页写入 %s 条，下一页 offset=%s",
                self._pages, written, offset,
            )
            await self._page_hook()
            if offset is None:
                break
        info = self.storage.publish_snapshot(self.source, self.snapshot_format)
        logger.info("[offset] 已发布快照 %s（%s 条）", info["snapshot_id"],
                    info["record_count"])
        return info

    async def _run_zone(self) -> dict:
        """采一个作业区；本轮已采齐则零请求返回。

        进度键为 ``offset:zone:<zone>``，只在本区采集时读写；
        是否需从头采集由引擎的轮次状态决定（引擎在启动各种子前会重置
        需要重采的区的进度）。
        """
        assert self.zone is not None
        key = zone_progress_key(self.zone)
        progress = self.storage.get_progress(key) or {}
        self._pages = 0
        if progress.get("done"):
            staged = self.storage.count_staging("offset", zone=self.zone)
            logger.info(
                "[offset:%s] 本轮已采齐，跳过采集（零请求，暂存 %s 条）",
                self.zone, staged,
            )
            return {
                "zone": self.zone,
                "skipped": True,
                "pages": 0,
                "staged": staged,
            }

        offset = int(progress.get("next_offset") or 0)
        logger.info("[offset:%s] 从 offset=%s 开始（或续读）", self.zone, offset)
        while True:
            async def _send(token, offset=offset):
                return await self.client.get(
                    f"{self.base_url}/offset/orders",
                    {
                        "limit": self.page_size,
                        "offset": offset,
                        "zone": self.zone,
                    },
                    token=token,
                )

            data = await self.gateway.request(_send, owner=self.name)
            written = self._ingest(data.get("orders", []), zone=self.zone)
            offset = data["next_offset"]
            self._pages += 1
            self.storage.set_progress(
                key,
                {
                    "next_offset": offset,
                    "done": offset is None,
                },
            )
            logger.info(
                "[offset:%s] 第 %s 页完成，本页写入 %s 条，下一页 offset=%s",
                self.zone, self._pages, written, offset,
            )
            await self._page_hook()
            if offset is None:
                break
        staged = self.storage.count_staging("offset", zone=self.zone)
        logger.info("[offset:%s] 本轮采齐（暂存 %s 条）", self.zone, staged)
        return {
            "zone": self.zone,
            "skipped": False,
            "pages": self._pages,
            "staged": staged,
        }


class CursorFetcher(BaseFetcher):
    """游标分页源。

    游标绑定清单版次。已保存游标对应的清单过期（410）时，
    丢弃旧进度、从新版清单开头重读；重读期间已发布旧快照仍可读。
    """

    name = "cursor"
    source = "cursor"

    def __init__(self, storage, client, base_url, gateway: SharedGateway, **kw):
        super().__init__(storage, client, base_url, **kw)
        self.gateway = gateway
        self._pages = 0

    def _track_sequence(self) -> bool:
        return True

    async def _load_manifest(self, token: str | None) -> dict:
        return await self.client.get(
            f"{self.base_url}/cursor/manifest", token=token
        )

    async def _once(self) -> dict:
        from .limits import StaleCursor

        progress = self.storage.get_progress(self.source) or {}
        cursor = progress.get("next_cursor")  # None 表示从头开始
        self._pages = 0
        self._position = 0  # 每次完整重读都从清单第 0 行开始编号
        if cursor:
            logger.info("[cursor] 尝试从已保存游标续读: %s", cursor)
        else:
            logger.info("[cursor] 无有效游标，从清单开头读取")

        while True:
            async def _send(token):
                if cursor is None:
                    return await self._load_manifest(token)
                return await self.client.get(
                    f"{self.base_url}/cursor/orders",
                    {"cursor": cursor, "limit": self.page_size},
                    token=token,
                )

            data = await self.gateway.request(_send, owner=self.name)

            # 清单入口响应
            if "entries" in data:
                cursor = data["next_cursor"]
                self.storage.set_progress(
                    self.source,
                    {
                        "manifest_version": data["manifest_version"],
                        "next_cursor": cursor,
                        "done": False,
                    },
                )
                logger.info(
                    "[cursor] 取得清单 %s，%s 个条目，起始游标=%s",
                    data["manifest_version"], len(data["entries"]), cursor,
                )
                continue

            written = self._ingest(data.get("orders", []))
            cursor = data.get("next_cursor")
            self._pages += 1
            self.storage.set_progress(
                self.source,
                {
                    "manifest_version": data["manifest_version"],
                    "next_cursor": cursor,
                    "done": cursor is None,
                },
            )
            logger.info(
                "[cursor] 第 %s 页完成（清单 %s），写入 %s 条，下一游标=%s",
                self._pages, data["manifest_version"], written, cursor,
            )
            await self._page_hook()
            if cursor is None:
                break

        info = self.storage.publish_snapshot(self.source, self.snapshot_format)
        logger.info("[cursor] 已发布快照 %s（%s 条）", info["snapshot_id"],
                    info["record_count"])
        return info

    async def run(self) -> dict:
        """带 410 过期处理的入口：清单过期则清空进度从头重读。"""
        from .limits import StaleCursor

        try:
            return await self._once()
        except StaleCursor as exc:
            logger.warning("[cursor] 清单已过期（%s），丢弃旧游标从头重读", exc)
            self.storage.reset_progress(self.source)
            # 新版清单记录集合可能变化，清掉旧暂存；已发布快照仍可读
            self.storage.clear_staging(self.source)
            self._position = 0
            return await self._once()


class WindowFetcher(BaseFetcher):
    """时间窗分页源。

    相邻窗口闭区间重叠，边界记录会重复返回；同一时刻可有多张单。
    进度记录最后成功窗口的 to；去重交给暂存表主键
    （source+order_id+revision），重复记录自然被合并。
    """

    name = "window"
    source = "window"

    def __init__(
        self,
        storage,
        client,
        base_url,
        limiter: IndependentLimiter,
        window_minutes: int = 30,
        horizon: str | None = None,
        **kw,
    ):
        super().__init__(storage, client, base_url, **kw)
        self.limiter = limiter
        self.window_minutes = window_minutes
        # 模拟源数据上界；实际项目中可由配置或探测接口给出
        self.horizon = horizon or "2026-11-12T00:00:00"
        self._pages = 0

    def _add_minutes(self, iso_ts: str, minutes: int) -> str:
        dt = datetime.strptime(iso_ts, "%Y-%m-%dT%H:%M:%S")
        return (dt + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%S")

    async def run(self) -> dict:
        progress = self.storage.get_progress(self.source) or {}
        start = progress.get("last_to") or "2026-11-10T08:00:00"
        self._pages = 0
        logger.info("[window] 从时间窗起点 %s 开始（或续读）", start)
        while start <= self.horizon:
            end = self._add_minutes(start, self.window_minutes)
            frm, to = start, end

            async def _send(_token, frm=frm, to=to):
                return await self.client.get(
                    f"{self.base_url}/window/orders", {"from": frm, "to": to}
                )

            data = await self.limiter.request(_send, owner=self.name)
            written = self._ingest(data.get("orders", []))
            self._pages += 1
            self.storage.set_progress(
                self.source, {"last_to": to, "done": to >= self.horizon}
            )
            logger.info(
                "[window] 窗口 [%s, %s] 完成，本页 %s 条，新增/更新 %s 条",
                frm, to, data.get("count", 0), written,
            )
            await self._page_hook()
            # 下一窗口与当前窗口首尾相接（闭区间天然重叠一条边界记录）
            start = end
        info = self.storage.publish_snapshot(self.source, self.snapshot_format)
        logger.info("[window] 已发布快照 %s（%s 条）", info["snapshot_id"],
                    info["record_count"])
        return info
