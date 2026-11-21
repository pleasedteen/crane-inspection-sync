"""三个来源的采集器。

每个采集器：
- 每成功读完一页就把分页进度落盘（强制中止后可续读）；
- 记录写入暂存表（按 来源+单号+修订号 去重，间隙已归一为毫米）；
- 读完整轮清单后独立发布原子快照，发布后才对读者可见。

大页正文（同一秒集中到达的百 MiB 级巡检单）按 JSON 数组逐条流式解析并
分批入库，既不整页驻留内存，也不在页面读完后才一次性写库。
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable, TypeVar

from .http import HttpClient
from .limits import IndependentLimiter, SharedGateway
from .models import InspectionOrder
from .storage import Storage
from .stream_json import iter_json_object

logger = logging.getLogger("crane_sync")
T = TypeVar("T")

# 流式解析时每次交给存储层的记录条数上限与字节上限（先到者为准）。
# 批太大峰值内存随页内记录变大，批太小则数据库事务过于频繁。
DEFAULT_ORDERS_BATCH = 128
DEFAULT_ORDERS_BATCH_BYTES = 256 * 1024


class BaseFetcher:
    name = "base"

    def __init__(
        self,
        storage: Storage,
        client: HttpClient,
        base_url: str,
        page_size: int = 10,
        snapshot_format: str = "jsonl",
        on_page: Callable[[str, int], Awaitable[None]] | None = None,
        publish_hook: Callable[[str], None] | None = None,
        storage_call: Callable[..., Awaitable[Any]] | None = None,
        orders_batch: int = DEFAULT_ORDERS_BATCH,
        orders_batch_bytes: int = DEFAULT_ORDERS_BATCH_BYTES,
        publish_batch: int = 200,
    ):
        self.storage = storage
        self.client = client
        self.base_url = base_url.rstrip("/")
        self.page_size = page_size
        self.snapshot_format = snapshot_format
        # 测试钩子：每处理完一页调用一次，便于做"中途中止"测试
        self.on_page = on_page
        # 测试钩子：发布前调用，可阻塞以模拟"写快照卡住"
        self.publish_hook = publish_hook
        # 把阻塞的存储 I/O 放到线程池，避免卡住事件循环（None=同步执行）
        self.storage_call = storage_call
        self.orders_batch = max(1, int(orders_batch))
        self.orders_batch_bytes = max(0, int(orders_batch_bytes))
        self.publish_batch = max(1, int(publish_batch))
        self._pages = 0
        self._position = 0

    async def _run(self, fn: Callable[..., T], *args: Any) -> T:
        if self.storage_call is None:
            return fn(*args)
        return await self.storage_call(fn, *args)

    async def _page_hook(self) -> None:
        if self.on_page is not None:
            await self.on_page(self.name, self.pages_done)

    @property
    def pages_done(self) -> int:
        return self._pages

    def _ingest(self, payloads: list[dict]) -> int:
        orders = [InspectionOrder.from_payload(p, self.name) for p in payloads]
        track = self._track_sequence()
        if track:
            # 清单中的每一行（含同一单号的每个修订）都有唯一位置
            for order in orders:
                order.position = self._position
                self._position += 1
        return self.storage.upsert_records(
            self.name, orders, track_position=track
        )

    def _track_sequence(self) -> bool:
        """是否按来源清单行序维护 position（游标源必须为 True）。"""
        return False

    async def _stream_page(
        self,
        url: str,
        params: dict,
        token: str | None,
        seen: dict[str, int],
        manifest: bool = False,
    ) -> dict:
        """流式读取一页：正文逐条解析、分批入库，返回其余字段。

        ``seen["written"]`` 累加本页已入库条数；清单响应只统计条目数，
        条目本身不驻留内存。
        """
        if manifest:
            def process(resp):
                return iter_json_object(resp, count_entries=True)

            return await self.client.get_stream(url, params, token, process)

        batch_size = self.orders_batch
        batch_bytes = self.orders_batch_bytes

        def on_orders(batch: list[dict]) -> None:
            seen["written"] += self._ingest(batch)

        def process(resp):
            return iter_json_object(
                resp,
                on_orders=on_orders,
                orders_batch=batch_size,
                orders_batch_bytes=batch_bytes,
            )

        return await self.client.get_stream(url, params, token, process)

    async def _publish(self) -> dict:
        if self.publish_hook is not None:
            # 在存储线程里执行，模拟"某个来源写快照卡住"时其他来源不受影响
            await self._run(self.publish_hook, self.name)
        info = await self._run(
            self.storage.publish_snapshot,
            self.name,
            self.snapshot_format,
            self.publish_batch,
        )
        logger.info("[%s] 已发布快照 %s（%s 条）", self.name,
                    info["snapshot_id"], info["record_count"])
        return info

    async def run(self) -> dict:  # pragma: no cover - 抽象方法
        raise NotImplementedError


class OffsetFetcher(BaseFetcher):
    """偏移分页源：?limit=&offset=，进度即下一页 offset。"""

    name = "offset"

    def __init__(self, storage, client, base_url, gateway: SharedGateway, **kw):
        super().__init__(storage, client, base_url, **kw)
        self.gateway = gateway

    async def run(self) -> dict:
        progress = await self._run(self.storage.get_progress, self.name) or {}
        saved_offset = progress.get("next_offset", 0)
        if saved_offset is None:
            # 上一轮已完整读完，新一轮从头开始
            await self._run(self.storage.reset_progress, self.name)
            offset = 0
        else:
            offset = int(saved_offset)
        self._pages = 0
        logger.info("[offset] 从 offset=%s 开始（或续读）", offset)
        while True:
            seen = {"written": 0}

            async def _send(token, offset=offset):
                return await self._stream_page(
                    f"{self.base_url}/offset/orders",
                    {"limit": self.page_size, "offset": offset},
                    token,
                    seen,
                )

            data = await self.gateway.request(_send, owner=self.name)
            written = seen["written"]
            offset = data["next_offset"]
            self._pages += 1
            await self._run(
                self.storage.set_progress,
                self.name,
                {"next_offset": offset, "done": offset is None},
            )
            logger.info(
                "[offset] 第 %s 页完成，本页写入 %s 条，下一页 offset=%s",
                self._pages, written, offset,
            )
            await self._page_hook()
            if offset is None:
                break
        return await self._publish()


class CursorFetcher(BaseFetcher):
    """游标分页源。

    游标绑定清单版次。已保存游标对应的清单过期（410）时，
    丢弃旧进度、从新版清单开头重读；重读期间已发布旧快照仍可读。
    """

    name = "cursor"

    def __init__(self, storage, client, base_url, gateway: SharedGateway, **kw):
        super().__init__(storage, client, base_url, **kw)
        self.gateway = gateway

    def _track_sequence(self) -> bool:
        return True

    async def _once(self) -> dict:
        progress = await self._run(self.storage.get_progress, self.name) or {}
        cursor = progress.get("next_cursor")  # None 表示从头开始
        self._pages = 0
        self._position = 0  # 每次完整重读都从清单第 0 行开始编号
        if cursor:
            logger.info("[cursor] 尝试从已保存游标续读: %s", cursor)
        else:
            logger.info("[cursor] 无有效游标，从清单开头读取")

        while True:
            seen = {"written": 0}

            async def _send(token, cursor=cursor):
                if cursor is None:
                    return await self._stream_page(
                        f"{self.base_url}/cursor/manifest", {}, token, seen,
                        manifest=True,
                    )
                return await self._stream_page(
                    f"{self.base_url}/cursor/orders",
                    {"cursor": cursor, "limit": self.page_size},
                    token,
                    seen,
                )

            data = await self.gateway.request(_send, owner=self.name)

            # 清单入口响应（只保留条目数，不保留条目本身）
            if "entries_count" in data:
                cursor = data["next_cursor"]
                await self._run(
                    self.storage.set_progress,
                    self.name,
                    {
                        "manifest_version": data["manifest_version"],
                        "next_cursor": cursor,
                        "done": False,
                    },
                )
                logger.info(
                    "[cursor] 取得清单 %s，%s 个条目，起始游标=%s",
                    data["manifest_version"], data["entries_count"], cursor,
                )
                continue

            written = seen["written"]
            cursor = data.get("next_cursor")
            self._pages += 1
            await self._run(
                self.storage.set_progress,
                self.name,
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

        return await self._publish()

    async def run(self) -> dict:
        """带 410 过期处理的入口：清单过期则清空进度从头重读。"""
        from .limits import StaleCursor

        try:
            return await self._once()
        except StaleCursor as exc:
            logger.warning("[cursor] 清单已过期（%s），丢弃旧游标从头重读", exc)
            await self._run(self.storage.reset_progress, self.name)
            # 新版清单记录集合可能变化，清掉旧暂存；已发布快照仍可读
            await self._run(self.storage.clear_staging, self.name)
            self._position = 0
            return await self._once()


class WindowFetcher(BaseFetcher):
    """时间窗分页源。

    相邻窗口闭区间重叠，边界记录会重复返回；同一时刻可有多张单。
    进度记录最后成功窗口的 to；去重交给暂存表主键
    （source+order_id+revision），重复记录自然被合并。
    """

    name = "window"

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

    def _add_minutes(self, iso_ts: str, minutes: int) -> str:
        dt = datetime.strptime(iso_ts, "%Y-%m-%dT%H:%M:%S")
        return (dt + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%S")

    async def run(self) -> dict:
        progress = await self._run(self.storage.get_progress, self.name) or {}
        start = progress.get("last_to") or "2026-11-10T08:00:00"
        self._pages = 0
        logger.info("[window] 从时间窗起点 %s 开始（或续读）", start)
        while start <= self.horizon:
            end = self._add_minutes(start, self.window_minutes)
            frm, to = start, end
            seen = {"written": 0}

            async def _send(_token, frm=frm, to=to):
                return await self._stream_page(
                    f"{self.base_url}/window/orders",
                    {"from": frm, "to": to},
                    None,
                    seen,
                )

            data = await self.limiter.request(_send, owner=self.name)
            self._pages += 1
            await self._run(
                self.storage.set_progress,
                self.name,
                {"last_to": to, "done": to >= self.horizon},
            )
            logger.info(
                "[window] 窗口 [%s, %s] 完成，本页 %s 条，新增/更新 %s 条",
                frm, to, data.get("count", seen["written"]), seen["written"],
            )
            await self._page_hook()
            # 下一窗口与当前窗口首尾相接（闭区间天然重叠一条边界记录）
            start = end
        return await self._publish()
