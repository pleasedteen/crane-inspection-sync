"""三个来源的采集器。

每个采集器：
- 每成功读完一页就把分页进度落盘（强制中止后可续读）；
- 记录写入暂存表（按 来源+单号+修订号 去重，间隙已归一为毫米）；
- 读完整轮清单后独立发布原子快照，发布后才对读者可见。

时间窗采集器还支持"迟到变更追赶"：首次/基线失效时全量扫描时间窗，
此后只从保存的变更续点追赶补发/更正/撤回，不再重扫旧时间窗。
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
    """时间窗分页源 + 迟到变更追赶。

    供应商会对旧巡检单的某个修订**补发 / 更正 / 撤回**。首次同步（或
    基线失效时）用时间窗全量扫描建立基线；此后每轮只需从保存的变更续点
    ``since`` 追赶变更日志，不再重扫历史时间窗。

    - **每轮上限**：启动时读取变更高水位作为本轮 ``until``，只追到该上限，
      采完（含分页续读）再一次性发布；上限之后的新变更留给下一轮。
    - **页级续点**：每读完一页变更即保存 ``since``，重启后从下一页继续，
      不重拉已保存的页面。未完成的一轮沿用其原 ``until``。
    - **重建**：旧状态没有变更续点，或续点早于日志保留范围（HTTP 410）时，
      清空该源暂存并全量重扫时间窗建立新基线。重建期间上一版已发布快照
      仍可读，其他来源并发采集不受影响。
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
        window_start: str | None = None,
        **kw,
    ):
        super().__init__(storage, client, base_url, **kw)
        self.limiter = limiter
        self.window_minutes = window_minutes
        # 模拟源数据上界；实际项目中可由配置或探测接口给出
        self.horizon = horizon or "2026-11-12T00:00:00"
        self.window_start = window_start or "2026-11-10T08:00:00"
        self._pages = 0

    def _add_minutes(self, iso_ts: str, minutes: int) -> str:
        dt = datetime.strptime(iso_ts, "%Y-%m-%dT%H:%M:%S")
        return (dt + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%S")

    # ---------------- 公共入口 ----------------
    async def run(self) -> dict:
        from .limits import StaleCursor

        progress = self.storage.get_progress(self.name) or {}
        mode = progress.get("mode")
        if mode == "changes" and "since" in progress:
            try:
                return await self._run_changes(progress)
            except StaleCursor as exc:
                logger.warning(
                    "[window] 变更续点已过期（%s），重建基线并全量重扫时间窗", exc
                )
                return await self._run_baseline({})
        # 旧状态（无 mode/续点）或正在重建的基线
        return await self._run_baseline(progress if mode == "baseline" else {})

    # ---------------- 变更追赶 ----------------
    async def _fetch_manifest(self) -> dict:
        async def _send(_token):
            return await self.client.get(
                f"{self.base_url}/window/changes", {"manifest": 1}
            )

        return await self.limiter.request(_send, owner=self.name)

    async def _fetch_watermark(self) -> int:
        return int((await self._fetch_manifest())["high_watermark"])

    async def _fetch_changes(self, since: int, until: int) -> dict:
        async def _send(_token):
            return await self.client.get(
                f"{self.base_url}/window/changes",
                {"since": since, "until": until, "limit": self.page_size},
            )

        return await self.limiter.request(_send, owner=self.name)

    def _apply_changes(self, events: list[dict]) -> None:
        """按序应用一页变更：撤回即时删除，补发/更正先收集再批量写入。

        同一页内可能出现"先补发后撤回"等组合，因此严格保持日志顺序：
        每次遇到需要写入的记录先 flush 之前的删除/写入批次，保证最终状态正确。
        """
        pending_writes: list[InspectionOrder] = []

        def flush() -> None:
            if pending_writes:
                self.storage.upsert_records(self.name, list(pending_writes))
                pending_writes.clear()

        for event in events:
            etype = event.get("type")
            if etype == "retract":
                flush()
                self.storage.delete_record(
                    self.name, str(event["order_id"]), int(event["revision"])
                )
            else:  # append / correct：都按键 upsert
                pending_writes.append(
                    InspectionOrder.from_payload(event["order"], self.name)
                )
        flush()

    async def _run_changes(self, progress: dict) -> dict:
        since = int(progress["since"])
        if progress.get("done") is False and "until" in progress:
            # 重启续跑未完成的一轮：沿用启动时记录的变更上限
            until = int(progress["until"])
            logger.info(
                "[window] 续跑变更追赶：since=%s until=%s（沿用本轮上限）",
                since, until,
            )
        else:
            # 新一轮：以此刻的变更高水位为本轮上限
            until = await self._fetch_watermark()
            logger.info(
                "[window] 开始新一轮变更追赶：since=%s until=%s（启动上限）",
                since, until,
            )
        self._pages = 0
        since = await self._drain_changes(since, until)
        self.storage.set_progress(
            self.name,
            {"mode": "changes", "since": since, "until": until, "done": True},
        )
        info = self.storage.publish_snapshot(self.name, self.snapshot_format)
        logger.info("[window] 已发布快照 %s（%s 条）", info["snapshot_id"],
                    info["record_count"])
        return info

    async def _drain_changes(self, since: int, until: int) -> int:
        """把 (since, until] 的变更分页追完；每页落盘续点，返回最终续点。"""
        while True:
            data = await self._fetch_changes(since, until)
            self._apply_changes(data.get("changes", []))
            next_since = int(data["next_since"])
            done = not data.get("has_more", False)
            self._pages += 1
            self.storage.set_progress(
                self.name,
                {
                    "mode": "changes",
                    "since": next_since,
                    "until": until,
                    "done": done,
                },
            )
            logger.info(
                "[window] 变更第 %s 页：本页 %s 条，since=%s -> %s，%s",
                self._pages, len(data.get("changes", [])), since, next_since,
                "本轮追赶完成" if done else "继续下一页",
            )
            await self._page_hook()
            since = next_since
            if done:
                return since

    # ---------------- 基线（全量时间窗扫描） ----------------
    async def _run_baseline(self, progress: dict) -> dict:
        from .limits import StaleCursor

        self._pages = 0
        resuming = (
            progress.get("last_to") is not None
            and progress.get("baseline_until") is not None
        )
        if resuming:
            # 中止后继续未完成的全量扫描，不重拉已保存的窗口
            start = progress["last_to"]
            until = int(progress["baseline_until"])
            baseline_since = int(progress.get("baseline_since", 0) or 0)
            logger.info(
                "[window] 续跑基线扫描：从 %s 继续，基线上限 until=%s", start, until
            )
        else:
            # 新建基线：旧暂存可能含已撤回的记录，先清空；
            # 已发布快照在重新发布前始终可读
            self.storage.clear_staging(self.name)
            manifest = await self._fetch_manifest()
            until = int(manifest["high_watermark"])
            # 时间窗扫描只覆盖 [window_start, horizon]；更早/更晚被补发的记录
            # 只能靠变更日志补齐。日志已滚出保留范围时只能从当前最早处开始。
            baseline_since = max(0, int(manifest["oldest_seq"]) - 1)
            start = self.window_start
            self.storage.set_progress(
                self.name,
                {
                    "mode": "baseline",
                    "baseline_until": until,
                    "baseline_since": baseline_since,
                    "last_to": None,
                    "done": False,
                },
            )
            logger.info("[window] 建立基线：从 %s 全量扫描，基线上限 until=%s",
                        start, until)

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
            start = end
            done = start > self.horizon
            self.storage.set_progress(
                self.name,
                {
                    "mode": "baseline",
                    "baseline_until": until,
                    "baseline_since": baseline_since,
                    "last_to": start,
                    "done": done,
                },
            )
            logger.info(
                "[window] 窗口 [%s, %s] 完成，本页 %s 条，新增/更新 %s 条",
                frm, to, data.get("count", 0), written,
            )
            await self._page_hook()

        # 全量扫描已覆盖整个时间窗，但变更日志中还可能有时间窗覆盖不到的
        # 变化（例如更早时间被补发的新修订、之后被撤回的旧修订）。从 0 追到
        # 本轮上限，把基线补齐到"启动时的当前状态"：撤回正确删除、补发/更正
        # 正确覆盖。上限之后的新变更留给下一轮。
        since = baseline_since
        try:
            since = await self._drain_changes(since, until)
        except StaleCursor:
            # 基线扫描耗时较久，补齐阶段起点已滚出日志保留范围：
            # 从当前仍可读的最早变更继续（更早的变化已被全量扫描覆盖）。
            manifest = await self._fetch_manifest()
            fallback = max(0, int(manifest["oldest_seq"]) - 1)
            logger.warning(
                "[window] 基线补齐续点已过期，改从 %s 继续补齐", fallback
            )
            since = await self._drain_changes(fallback, until)

        self.storage.set_progress(
            self.name,
            {"mode": "changes", "since": since, "until": until, "done": True},
        )
        info = self.storage.publish_snapshot(self.name, self.snapshot_format)
        logger.info("[window] 基线完成并发布快照 %s（%s 条）",
                    info["snapshot_id"], info["record_count"])
        return info
