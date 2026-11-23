"""请求额度、冷却与共享访问令牌控制（全异步）。

规则要点：
- 偏移源与游标源共用同一个网关：共享每分钟请求额度和同一访问令牌；
  数据请求与令牌续期都消耗额度；续期后旧令牌立即失效。
- 时间窗源使用独立额度。
- 额度用滑动窗口实现，配合 asyncio.Condition 的 FIFO 等待队列，
  保证"额度允许时各来源持续获得采集机会"，不会有来源被饿死。
- 任一来源收到 429 后按其要求的 retry_after 进入冷却，冷却期间
  其他来源照常推进。
- 同一失效令牌只续期一次（按令牌世代记录是否已续期）。
- 首次签发与每次续期都以"在途任务"形式运行：等待者被取消（如同步轮
  被整体中止）不会连带取消令牌任务；任务完成后自动把新令牌纳入网关，
  下一轮同步可直接复用，不会因上一轮中止而重复续期同一失效世代。
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")


class CooldownGate:
    """冷却门：冷却期内 wait() 阻塞，冷却结束自动放行。"""

    def __init__(self, clock: Callable[[], float] = time.monotonic):
        self._clock = clock
        self._until = 0.0

    async def cooldown(self, seconds: float) -> None:
        self._until = max(self._until, self._clock() + max(0.0, seconds))

    def remaining(self) -> float:
        return max(0.0, self._until - self._clock())

    async def wait_ready(self) -> None:
        remaining = self.remaining()
        if remaining > 0:
            await asyncio.sleep(remaining)


class WindowQuota:
    """滑动窗口请求额度。窗口内最多 limit 次请求。

    acquire(owner) 按"来源"轮询发放：可发放时优先选择与上次不同
    且排队最久的来源，保证额度允许时每个来源都能持续获得采集机会，
    不会出现一个来源把窗口额度全部占满的情况。
    """

    def __init__(
        self,
        limit: int,
        window_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.limit = limit
        self.window = window_seconds
        self._clock = clock
        self._hits: deque[float] = deque()
        self._cond = asyncio.Condition()
        # 每个来源的排队序号队列；队首即该来源最早等待者
        self._waiting: dict[object, deque[int]] = {}
        self._ticket = 0
        self._last_owner: object = None
        # 实际发放序列，供测试/观测验证各来源都持续获得采集机会
        self.grant_log: list[object] = []

    async def acquire(self, owner: object = "default") -> None:
        async with self._cond:
            my_ticket = self._ticket
            self._ticket += 1
            queue = self._waiting.setdefault(owner, deque())
            queue.append(my_ticket)
            try:
                while True:
                    now = self._clock()
                    while self._hits and now - self._hits[0] >= self.window:
                        self._hits.popleft()
                    # 排队最久的来源们（按其队首序号），并与上次获得者错开
                    candidates = sorted(
                        (q[0], own) for own, q in self._waiting.items() if q
                    )
                    head_owner = candidates[0][1] if candidates else owner
                    chosen = next(
                        (own for _, own in candidates if own != self._last_owner),
                        head_owner,
                    )
                    if (
                        chosen == owner
                        and queue[0] == my_ticket
                        and len(self._hits) < self.limit
                    ):
                        self._hits.append(now)
                        self._last_owner = owner
                        self.grant_log.append(owner)
                        queue.popleft()
                        if not queue:
                            self._waiting.pop(owner, None)
                        self._cond.notify_all()
                        return
                    if self._hits and len(self._hits) >= self.limit:
                        wait_for = self.window - (now - self._hits[0])
                    else:
                        wait_for = None
                    if wait_for is not None and wait_for > 0:
                        try:
                            await asyncio.wait_for(
                                self._cond.wait(), timeout=wait_for
                            )
                        except asyncio.TimeoutError:
                            pass
                    else:
                        await self._cond.wait()
            except BaseException:
                # 取消时清理排队痕迹，避免泄漏
                if my_ticket in queue:
                    queue.remove(my_ticket)
                if not queue:
                    self._waiting.pop(owner, None)
                self._cond.notify_all()
                raise


class AuthError(Exception):
    """令牌失效（HTTP 401）。"""


class RateLimited(Exception):
    """被服务端限流（HTTP 429）。"""

    def __init__(self, retry_after: float):
        super().__init__(f"rate limited, retry_after={retry_after}")
        self.retry_after = retry_after


class StaleCursor(Exception):
    """游标绑定的清单已过期（HTTP 410），必须从新版清单开头重读。"""


class GatewayError(Exception):
    """网关其他错误（4xx/5xx、网络错误）。"""


class SharedGateway:
    """偏移源/游标源共用的网关：共享额度 + 共享令牌。

    令牌的首次签发与每次续期都包装成独立的在途任务（asyncio.Task）。
    等待该任务的请求协程被取消（例如同步轮被中止）时，任务本身继续
    运行：任务一旦完成就通过 done 回调把新令牌落进网关状态，随后任何
    来源、任何轮次都能直接复用；同一失效世代的续期任务最多创建一个。
    """

    def __init__(
        self,
        quota: WindowQuota,
        cooldown: CooldownGate,
        token_provider: Callable[[], Awaitable[str]],
        clock: Callable[[], float] = time.monotonic,
    ):
        self.quota = quota
        self.cooldown = cooldown
        self._token_provider = token_provider
        self._clock = clock
        self._token_lock = asyncio.Lock()
        self._token: str | None = None
        self._generation = 0          # 令牌世代，续期一次 +1
        self._renewed_generation: set[int] = set()
        # 在途的首次签发 / 各失效世代续期任务；取消等待者不会取消任务
        self._issuing_task: asyncio.Task[str] | None = None
        self._renewal_tasks: dict[int, asyncio.Task[str]] = {}

    def _spawn_issue_task(self) -> asyncio.Task[str]:
        task = asyncio.ensure_future(self._token_provider())

        def _adopt_issue(done: asyncio.Task[str]) -> None:
            # 事件循环单线程内的普通属性写，无需加锁；任务只完成一次
            if self._issuing_task is done:
                self._issuing_task = None
            if done.cancelled() or done.exception() is not None:
                return
            token = done.result()
            if self._token is None:
                self._token = token
                self._generation += 1

        task.add_done_callback(_adopt_issue)
        return task

    async def _current_token(self) -> tuple[str, int]:
        async with self._token_lock:
            if self._token is not None:
                return self._token, self._generation
            # 首次取令牌；续期请求本身也占额度，由调用方先占坑，
            # provider 内部不再 acquire。在途签发任务可跨取消复用。
            if self._issuing_task is None or self._issuing_task.done():
                self._issuing_task = self._spawn_issue_task()
            task = self._issuing_task
        # shield：等待者被取消（同步轮中止）不得连带取消在途签发任务
        token = await asyncio.shield(task)
        async with self._token_lock:
            # 极端情况下回调还没跑（同批次回调顺序），以结果为准
            if self._token is None:
                self._token = token
                self._generation += 1
            return self._token, self._generation

    def _spawn_renewal_task(self, failed_generation: int) -> asyncio.Task[str]:
        task = asyncio.ensure_future(self._token_provider())

        def _adopt_renewal(done: asyncio.Task[str], gen: int = failed_generation) -> None:
            if self._renewal_tasks.get(gen) is done:
                self._renewal_tasks.pop(gen, None)
            if done.cancelled() or done.exception() is not None:
                # 失败不留标记：后续请求允许为该世代重新发起续期
                self._renewed_generation.discard(gen)
                return
            new_token = done.result()
            self._token = new_token
            self._generation += 1
            self._renewed_generation.add(gen)

        task.add_done_callback(_adopt_renewal)
        return task

    async def _renew_token(self, failed_generation: int) -> str:
        async with self._token_lock:
            if failed_generation in self._renewed_generation:
                # 该世代令牌已经续期过（可能由上一轮在途任务完成），
                # 直接用当前新令牌，不重复续期
                assert self._token is not None
                return self._token
            existing = self._renewal_tasks.get(failed_generation)
            if existing is None or existing.done():
                existing = self._spawn_renewal_task(failed_generation)
                self._renewal_tasks[failed_generation] = existing
            task = existing
        # 锁外等待，并 shield：等待者被取消不影响在途续期任务，
        # 任务完成后由 done 回调把新令牌纳入网关，供后续轮次复用
        return await asyncio.shield(task)

    async def request(
        self,
        send: Callable[[str | None], Awaitable[T]],
        owner: object = "default",
    ) -> T:
        """发送一次带令牌的请求，自动处理冷却、额度、401 续期与 429。

        send(token) 由调用方提供，执行真正的 HTTP 调用；
        本方法保证同一失效令牌最多触发一次续期。
        """
        attempts = 0
        while True:
            attempts += 1
            if attempts > 6:
                raise GatewayError("重试次数过多，网关持续不可用")
            await self.cooldown.wait_ready()
            await self.quota.acquire(owner)
            token, generation = await self._current_token()
            try:
                return await send(token)
            except AuthError:
                # 续期要占额度：先排队拿额度，再调 provider
                await self.cooldown.wait_ready()
                await self.quota.acquire(("token", owner))
                await self._renew_token(generation)
                continue
            except RateLimited as exc:
                await self.cooldown.cooldown(exc.retry_after)
                continue


class IndependentLimiter:
    """时间窗源使用的独立额度 + 冷却。"""

    def __init__(self, quota: WindowQuota, cooldown: CooldownGate):
        self.quota = quota
        self.cooldown = cooldown

    async def request(self, send: Callable[[None], Awaitable[T]],
                      owner: object = "default") -> T:
        attempts = 0
        while True:
            attempts += 1
            if attempts > 6:
                raise GatewayError("重试次数过多，来源持续不可用")
            await self.cooldown.wait_ready()
            await self.quota.acquire(owner)
            try:
                return await send(None)
            except RateLimited as exc:
                await self.cooldown.cooldown(exc.retry_after)
                continue
