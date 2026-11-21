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
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")

# 诊断探针：probe(owner, state, detail)
# state ∈ waiting_quota | cooldown | waiting_reply | processing | ...
Probe = Callable[[object, str, dict], None]


def _emit(probe: Probe | None, owner: object, state: str, **detail) -> None:
    if probe is not None:
        try:
            probe(owner, state, detail)
        except Exception:  # 观测失败绝不能影响同步
            pass


class CooldownGate:
    """冷却门：冷却期内 wait() 阻塞，冷却结束自动放行。"""

    def __init__(self, clock: Callable[[], float] = time.monotonic):
        self._clock = clock
        self._until = 0.0

    async def cooldown(self, seconds: float) -> None:
        self._until = max(self._until, self._clock() + max(0.0, seconds))

    def remaining(self) -> float:
        return max(0.0, self._until - self._clock())

    async def wait_ready(
        self, on_wait: Callable[[float], None] | None = None
    ) -> None:
        remaining = self.remaining()
        if remaining > 0:
            if on_wait is not None:
                on_wait(remaining)
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


class HttpStatusError(GatewayError):
    """携带 HTTP 状态码的网关错误，便于诊断区分"HTTP 回包"类型。"""

    def __init__(self, status: int, body: str):
        super().__init__(f"HTTP {status}: {body}")
        self.status = status
        self.body = body


class SharedGateway:
    """偏移源/游标源共用的网关：共享额度 + 共享令牌。"""

    def __init__(
        self,
        quota: WindowQuota,
        cooldown: CooldownGate,
        token_provider: Callable[[], Awaitable[str]],
        clock: Callable[[], float] = time.monotonic,
        probe: Probe | None = None,
    ):
        self.quota = quota
        self.cooldown = cooldown
        self._token_provider = token_provider
        self._clock = clock
        self._token_lock = asyncio.Lock()
        self._token: str | None = None
        self._generation = 0          # 令牌世代，续期一次 +1
        self._renewed_generation: set[int] = set()
        self.probe = probe

    async def _current_token(self) -> tuple[str, int]:
        async with self._token_lock:
            if self._token is None:
                # 首次取令牌；续期请求本身也占额度，由调用方先占坑，
                # 这里直接走 provider（provider 内部不再 acquire）。
                self._token = await self._token_provider()
                self._generation += 1
            return self._token, self._generation

    async def _renew_token(self, failed_generation: int) -> str:
        async with self._token_lock:
            if failed_generation in self._renewed_generation:
                # 该世代令牌已经续期过，直接用当前新令牌，不重复续期
                assert self._token is not None
                return self._token
            new_token = await self._token_provider()
            self._token = new_token
            self._generation += 1
            self._renewed_generation.add(failed_generation)
            return new_token

    def generation(self) -> int:
        return self._generation

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
            _emit(self.probe, owner, "waiting_quota", attempt=attempts)
            await self.cooldown.wait_ready(
                lambda secs: _emit(
                    self.probe, owner, "cooldown", remaining=round(secs, 3)
                )
            )
            await self.quota.acquire(owner)
            token, generation = await self._current_token()
            _emit(
                self.probe,
                owner,
                "waiting_reply",
                token_generation=generation,
                attempt=attempts,
            )
            try:
                return await send(token)
            except AuthError:
                # 续期要占额度：先排队拿额度，再调 provider
                _emit(self.probe, owner, "renewing_token",
                      token_generation=generation)
                await self.cooldown.wait_ready(
                    lambda secs: _emit(
                        self.probe, owner, "cooldown", remaining=round(secs, 3),
                        during="token_renewal",
                    )
                )
                await self.quota.acquire(("token", owner))
                await self._renew_token(generation)
                continue
            except RateLimited as exc:
                _emit(self.probe, owner, "cooldown",
                      remaining=round(exc.retry_after, 3))
                await self.cooldown.cooldown(exc.retry_after)
                continue


class IndependentLimiter:
    """时间窗源使用的独立额度 + 冷却。"""

    def __init__(
        self,
        quota: WindowQuota,
        cooldown: CooldownGate,
        probe: Probe | None = None,
    ):
        self.quota = quota
        self.cooldown = cooldown
        self.probe = probe

    async def request(self, send: Callable[[None], Awaitable[T]],
                      owner: object = "default") -> T:
        attempts = 0
        while True:
            attempts += 1
            if attempts > 6:
                raise GatewayError("重试次数过多，来源持续不可用")
            _emit(self.probe, owner, "waiting_quota", attempt=attempts)
            await self.cooldown.wait_ready(
                lambda secs: _emit(
                    self.probe, owner, "cooldown", remaining=round(secs, 3)
                )
            )
            await self.quota.acquire(owner)
            _emit(self.probe, owner, "waiting_reply", attempt=attempts)
            try:
                return await send(None)
            except RateLimited as exc:
                _emit(self.probe, owner, "cooldown",
                      remaining=round(exc.retry_after, 3))
                await self.cooldown.cooldown(exc.retry_after)
                continue
