"""三个来源的采集器。

每个采集器：
- 每成功读完一页就把分页进度落盘（强制中止后可续读）；
- 记录写入暂存表（按 来源+单号+修订号 去重，间隙已归一为毫米）；
- 读完整轮清单后独立发布原子快照，发布后才对读者可见。
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable

from .http import HttpClient
from .limits import IndependentLimiter, SharedGateway
from .models import InspectionOrder
from .storage import Storage

logger = logging.getLogger("crane_sync")


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

    async def run(self) -> dict:  # pragma: no cover - 抽象方法
        raise NotImplementedError


class OffsetFetcher(BaseFetcher):
    """偏移分页源：?limit=&offset=，进度即下一页 offset。"""

    name = "offset"

    def __init__(self, storage, client, base_url, gateway: SharedGateway, **kw):
        super().__init__(storage, client, base_url, **kw)
        self.gateway = gateway
        self._pages = 0

    async def run(self) -> dict:
        progress = self.storage.get_progress(self.name) or {}
        saved_offset = progress.get("next_offset", 0)
        if saved_offset is None:
            # 上一轮已完整读完，新一轮从头开始
            self.storage.reset_progress(self.name)
            offset = 0
        else:
            offset = int(saved_offset)
        self._pages = 0
        logger.info("[offset] 从 offset=%s 开始（或续读）", offset)
        while True:
            async def _send(token):
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
                self.name, {"next_offset": offset, "done": offset is None}
            )
            logger.info(
                "[offset] 第 %s 页完成，本页写入 %s 条，下一页 offset=%s",
                self._pages, written, offset,
            )
            await self._page_hook()
            if offset is None:
                break
        info = self.storage.publish_snapshot(self.name, self.snapshot_format)
        logger.info("[offset] 已发布快照 %s（%s 条）", info["snapshot_id"],
                    info["record_count"])
        return info


class CursorFetcher(BaseFetcher):
    """游标分页源。

    游标绑定清单版次。已保存游标对应的清单过期（410）时，
    丢弃旧进度、从新版清单开头重读；重读期间已发布旧快照仍可读。
    """

    name = "cursor"

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

        progress = self.storage.get_progress(self.name) or {}
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
                    self.name,
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

        info = self.storage.publish_snapshot(self.name, self.snapshot_format)
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
            self.storage.reset_progress(self.name)
            # 新版清单记录集合可能变化，清掉旧暂存；已发布快照仍可读
            self.storage.clear_staging(self.name)
            self._position = 0
            return await self._once()


class WindowFetcher(BaseFetcher):
    """时间窗分页源（增量同步）。

    供应商会补送迟到的巡检单、修订已经扫过的单，且同一时刻可有多张单。
    同步语义：

    - 每轮先探测一次源端（GET /window/status），拿到源端确认范围与数据版本；
      之后数据请求都钉住该版本，**每轮以启动时源端确认的范围/版本为准**，
      途中补送/修订留到下轮再处理。
    - 源端相对上轮已完成版本无变化（rescan_from 为空）时，本轮只有这一次
      探测请求，不再拉任何窗口。
    - 有变化时只拉增量：从 rescan_from 与配置起点的较大者开始，到
      min(配置 horizon, 源端确认上界) 为止，按窗口步进；相邻窗口闭区间
      重叠的边界记录由暂存表主键（source+order_id+revision）天然去重。
    - 窗口计划与进度逐页落盘：强杀重跑时未完成轮次直接按冻结的范围/版本
      续跑，不再重复探测；发布完成前上一份已发布快照始终可读。
    - 探测与数据请求共用该源的独立额度，不与共享网关互相阻塞。
    """

    name = "window"
    # 源端尚无数据时给出的配置内默认起点
    DEFAULT_START = "2026-11-10T08:00:00"

    def __init__(
        self,
        storage,
        client,
        base_url,
        limiter: IndependentLimiter,
        window_minutes: int = 30,
        horizon: str | None = None,
        window_start: str | None = None,
        **kw,
    ):
        super().__init__(storage, client, base_url, **kw)
        self.limiter = limiter
        self.window_minutes = window_minutes
        # 配置范围：只有落在 [window_start, horizon] 内的变动才需要同步
        self.window_start = window_start or self.DEFAULT_START
        self.horizon = horizon or "2026-11-12T00:00:00"
        self._pages = 0

    def _parse_ts(self, iso_ts: str) -> datetime:
        return datetime.strptime(iso_ts, "%Y-%m-%dT%H:%M:%S")

    def _fmt_ts(self, dt: datetime) -> str:
        return dt.strftime("%Y-%m-%dT%H:%M:%S")

    def _add_minutes(self, iso_ts: str, minutes: int) -> str:
        return self._fmt_ts(self._parse_ts(iso_ts) + timedelta(minutes=minutes))

    def _floor_to_grid(self, iso_ts: str) -> str:
        """把时间戳向下对齐到窗口网格（默认 30 分钟整点）。"""
        dt = self._parse_ts(iso_ts)
        floored = dt.replace(
            minute=(dt.minute // self.window_minutes) * self.window_minutes,
            second=0,
            microsecond=0,
        )
        return self._fmt_ts(floored)

    async def _probe(self, since_version: int | None) -> dict:
        """探测一次源端：数据版本、确认范围与增量下界（占独立额度）。"""
        params: dict[str, Any] = {}
        if since_version is not None:
            params["since_version"] = since_version
        data = await self.limiter.request(
            lambda _t: self.client.get(
                f"{self.base_url}/window/status", params
            ),
            owner=self.name,
        )
        logger.info(
            "[window] 探测完成：源端版本=%s 确认范围=[%s, %s] 增量下界=%s",
            data.get("data_version"), data.get("confirmed_from"),
            data.get("confirmed_to"), data.get("rescan_from"),
        )
        return data

    def _build_plan(self, status: dict) -> dict | None:
        """按探测结果生成本轮计划；配置范围与源端确认范围取交集。"""
        version = int(status["data_version"])
        lower = max(self.window_start, status.get("rescan_from") or self.window_start)
        confirmed_to = status.get("confirmed_to")
        upper = min(self.horizon, confirmed_to or self.horizon)
        if lower > upper:
            return None
        return {
            "data_version": version,
            "scan_from": self._floor_to_grid(lower),
            "scan_to": upper,
        }

    async def _run_plan(self, plan: dict) -> None:
        """按冻结的版本/范围逐窗口拉取，每窗落盘，强杀后可从此处续跑。"""
        version = plan["data_version"]
        scan_to = plan["scan_to"]
        start = plan.get("next_from", plan["scan_from"])
        self._pages = plan.get("windows_done", 0)
        logger.info(
            "[window] 本轮版本钉住 v%s，窗口范围 [%s, %s]（续读起点 %s）",
            version, plan["scan_from"], scan_to, start,
        )
        while start <= scan_to:
            end = self._add_minutes(start, self.window_minutes)
            frm, to = start, end

            async def _send(_token, frm=frm, to=to, version=version):
                return await self.client.get(
                    f"{self.base_url}/window/orders",
                    {"from": frm, "to": to, "version": version},
                )

            data = await self.limiter.request(_send, owner=self.name)
            written = self._ingest(data.get("orders", []))
            self._pages += 1
            next_from = end
            plan["next_from"] = next_from
            plan["windows_done"] = self._pages
            self.storage.set_progress(self.name, plan)
            logger.info(
                "[window] 窗口 [%s, %s]（v%s）完成，本页 %s 条，新增/更新 %s 条",
                frm, to, version, data.get("count", 0), written,
            )
            await self._page_hook()
            # 下一窗口与当前窗口首尾相接（闭区间天然重叠一条边界记录）
            start = next_from

    async def run(self) -> dict:
        progress = self.storage.get_progress(self.name) or {}

        # 强杀续跑：上一轮窗口计划尚未完成，沿用启动时冻结的版本与范围，
        # 不再重新探测（源端途中的变化留到下一轮）。
        if progress.get("active"):
            logger.info("[window] 发现未完成轮次，按已冻结计划续跑")
            await self._run_plan(progress)
            info = self.storage.publish_snapshot(self.name, self.snapshot_format)
            self.storage.set_progress(
                self.name,
                {
                    "active": False,
                    "last_version": progress["data_version"],
                    "last_to": progress["scan_to"],
                },
            )
            logger.info("[window] 续跑完成，已发布快照 %s（%s 条）",
                        info["snapshot_id"], info["record_count"])
            return info

        since_version = progress.get("last_version")
        status = await self._probe(since_version)
        current_version = int(status["data_version"])

        # 源端自上轮已完成版本以来无变化：本轮只有上面这一次探测
        if status.get("rescan_from") is None:
            logger.info(
                "[window] 源端无变化（版本 v%s），本轮仅探测一次，跳过拉取",
                current_version,
            )
            existing = self.storage.latest_snapshot_info(self.name)
            if existing is not None:
                # 上一份已发布快照继续可读，无需重新发布
                return existing
            # 尚无快照（理论上仅空数据源首轮）：发布空快照占位
            info = self.storage.publish_snapshot(self.name, self.snapshot_format)
            self.storage.set_progress(
                self.name,
                {"active": False, "last_version": current_version, "last_to": None},
            )
            return info

        plan = self._build_plan(status)
        if plan is None:
            # 变动落在配置范围之外：确认记账，不产生数据请求
            logger.info("[window] 变动不在配置范围 [%s, %s] 内，跳过",
                        self.window_start, self.horizon)
            existing = self.storage.latest_snapshot_info(self.name)
            self.storage.set_progress(
                self.name,
                {
                    "active": False,
                    "last_version": current_version,
                    "last_to": progress.get("last_to"),
                },
            )
            if existing is not None:
                return existing
            return self.storage.publish_snapshot(self.name, self.snapshot_format)

        # 冻结本轮版本与范围并立即落盘；之后源端再变化也不影响本轮
        plan.update({"active": True, "windows_done": 0, "next_from": plan["scan_from"]})
        self.storage.set_progress(self.name, plan)
        await self._run_plan(plan)
        # 发布期间上一份快照仍可读，指针切换是原子的
        info = self.storage.publish_snapshot(self.name, self.snapshot_format)
        self.storage.set_progress(
            self.name,
            {
                "active": False,
                "last_version": current_version,
                "last_to": plan["scan_to"],
            },
        )
        logger.info("[window] 已发布快照 %s（%s 条）",
                    info["snapshot_id"], info["record_count"])
        return info
