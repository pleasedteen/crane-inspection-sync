"""跨进程协调：按来源的独占锁 + 共享网关额度/令牌。

多个 sync 进程共用同一个状态目录时：

- **按来源分工**：每个来源一把 ``flock`` 独占锁。同源采集进程还在运行时，
  后启动的同源进程拿不到锁，直接跳过该来源（不拉取、不发布）。进程被杀时
  内核自动释放锁，不会留下死锁。
- **共享额度**：网关的滑动窗口额度存进 SQLite（``gateway_quota`` 表），
  每个进程在发请求前用 ``BEGIN IMMEDIATE`` 原子地占一个名额；额度不足时
  短轮询等待。占名额的事务在 HTTP 请求开始前就已提交，所以一个进程等待
  响应时，另一个进程只要有额度就能继续翻页。
- **共享令牌**：令牌、世代号存进 SQLite；取令牌/续期用一把跨进程文件锁
  串行化。续期前先比对世代号，同一失效令牌只会触发一次续期，其他进程
  直接复用新令牌。

额度时间戳使用 ``time.time()``（墙钟），因为 ``time.monotonic()`` 在多进程
之间不可比较。
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Awaitable, Callable, TypeVar

try:  # pragma: no cover - Linux/macOS 走此分支
    import fcntl
except ImportError:  # pragma: no cover - 非 POSIX 平台退化为无锁
    fcntl = None

from .limits import AuthError, GatewayError, RateLimited

logger = logging.getLogger("crane_sync")

T = TypeVar("T")

SOURCE_NAMES = ("offset", "cursor", "window")


# ----------------------------------------------------------------------
# 按来源的跨进程独占锁
# ----------------------------------------------------------------------
class SourceLock:
    """基于 ``flock`` 的跨进程独占锁。

    ``acquire()`` 默认非阻塞：锁已被其他进程持有时立即返回 False。
    进程退出或被 ``SIGKILL`` 时，文件描述符由内核关闭，锁随之释放。
    """

    def __init__(self, state_dir: str | Path, name: str):
        self.name = name
        self.path = Path(state_dir) / "locks" / f"{name}.lock"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fd: int | None = None

    def acquire(self, blocking: bool = False, timeout: float | None = None) -> bool:
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        if fcntl is None:  # pragma: no cover
            self._fd = fd
            return True

        if blocking:
            deadline = None if timeout is None else time.monotonic() + timeout
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except OSError:
                    if deadline is not None and time.monotonic() >= deadline:
                        os.close(fd)
                        return False
                    time.sleep(0.02)
        else:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                os.close(fd)
                return False

        # 记录持有者 PID，便于排查；锁本身由 flock 保证
        os.ftruncate(fd, 0)
        os.write(fd, f"{os.getpid()}\n".encode("ascii"))
        self._fd = fd
        return True

    def release(self) -> None:
        if self._fd is None:
            return
        try:
            if fcntl is not None:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
        except OSError:  # pragma: no cover
            pass
        try:
            os.close(self._fd)
        except OSError:  # pragma: no cover
            pass
        self._fd = None

    def __enter__(self) -> "SourceLock":
        self.acquire(blocking=True)
        return self

    def __exit__(self, *exc) -> None:
        self.release()


# ----------------------------------------------------------------------
# 跨进程共享网关状态（SQLite）
# ----------------------------------------------------------------------
class SharedGatewayStore:
    """网关额度、冷却、令牌的跨进程持久状态。"""

    def __init__(self, state_dir: str | Path, quota_limit: int, quota_window: float):
        self.limit = quota_limit
        self.window = quota_window
        self.db_path = Path(state_dir) / "sync_state.db"
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(self.db_path),
            timeout=30.0,
            check_same_thread=False,
            isolation_level=None,
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS gateway_quota(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    owner TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_gateway_quota_ts
                    ON gateway_quota(ts);
                CREATE TABLE IF NOT EXISTS gateway_waiters(
                    owner TEXT PRIMARY KEY,
                    seen REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS gateway_meta(
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS gateway_token(
                    id INTEGER PRIMARY KEY CHECK(id=1),
                    token TEXT,
                    generation INTEGER NOT NULL DEFAULT 0,
                    updated_at REAL NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS gateway_cooldown(
                    id INTEGER PRIMARY KEY CHECK(id=1),
                    until REAL NOT NULL DEFAULT 0
                );
                INSERT OR IGNORE INTO gateway_token(id,token,generation,updated_at)
                    VALUES(1,NULL,0,0);
                INSERT OR IGNORE INTO gateway_cooldown(id,until)
                    VALUES(1,0);
                """
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---------- 事务 ----------
    @contextmanager
    def _tx(self):
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    # ---------- 额度 ----------
    def try_acquire(self, owner: str, now: float) -> tuple[bool, float]:
        """尝试占一个额度名额。

        返回 ``(是否拿到, 建议重试秒数)``。事务只覆盖查询与占位，
        HTTP 请求不在事务内，因此不同进程可以真正并行发请求。
        """
        with self._tx() as db:
            db.execute(
                "DELETE FROM gateway_quota WHERE ts <= ?", (now - self.window,)
            )
            # 活跃等待者：持续轮询的进程会不断刷新 seen；阈值取小一些，
            # 这样某进程被强杀后残留的等待者很快失效，不会拖慢存活进程。
            db.execute(
                "DELETE FROM gateway_waiters WHERE seen <= ?", (now - 0.5,)
            )
            db.execute(
                "INSERT INTO gateway_waiters(owner,seen) VALUES(?,?) "
                "ON CONFLICT(owner) DO UPDATE SET seen=excluded.seen",
                (owner, now),
            )
            rows = db.execute(
                "SELECT ts FROM gateway_quota ORDER BY ts"
            ).fetchall()
            if len(rows) < self.limit:
                last_owner = self._get_meta(db, "last_owner")
                other = db.execute(
                    "SELECT 1 FROM gateway_waiters WHERE owner<>? LIMIT 1",
                    (owner,),
                ).fetchone()
                # 与其他来源错开：上一名额刚被自己拿走且另有来源在等，
                # 先让出一次，保证每个来源都能持续获得采集机会。
                if last_owner == owner and other is not None:
                    return False, 0.02
                db.execute(
                    "INSERT INTO gateway_quota(ts,owner) VALUES(?,?)",
                    (now, owner),
                )
                self._set_meta(db, "last_owner", owner)
                db.execute(
                    "DELETE FROM gateway_waiters WHERE owner=?", (owner,)
                )
                return True, 0.0
            retry = self.window - (now - rows[0][0])
            return False, max(0.01, retry)

    # ---------- 冷却 ----------
    def set_cooldown(self, seconds: float, now: float) -> None:
        until = now + max(0.0, seconds)
        with self._tx() as db:
            row = db.execute(
                "SELECT until FROM gateway_cooldown WHERE id=1"
            ).fetchone()
            if row and row[0] > until:
                until = row[0]
            db.execute(
                "UPDATE gateway_cooldown SET until=? WHERE id=1", (until,)
            )

    def cooldown_remaining(self, now: float) -> float:
        with self._lock:
            row = self._conn.execute(
                "SELECT until FROM gateway_cooldown WHERE id=1"
            ).fetchone()
        return max(0.0, row[0] - now) if row else 0.0

    # ---------- 令牌 ----------
    def read_token(self) -> tuple[str | None, int]:
        with self._lock:
            row = self._conn.execute(
                "SELECT token,generation FROM gateway_token WHERE id=1"
            ).fetchone()
        return (row[0], row[1]) if row else (None, 0)

    def write_token(self, token: str, generation: int, now: float) -> None:
        with self._tx() as db:
            db.execute(
                "UPDATE gateway_token SET token=?, generation=?, updated_at=? "
                "WHERE id=1",
                (token, generation, now),
            )

    # ---------- meta ----------
    @staticmethod
    def _get_meta(db: sqlite3.Connection, key: str) -> str | None:
        row = db.execute(
            "SELECT value FROM gateway_meta WHERE key=?", (key,)
        ).fetchone()
        return row[0] if row else None

    @staticmethod
    def _set_meta(db: sqlite3.Connection, key: str, value: str) -> None:
        db.execute(
            "INSERT INTO gateway_meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )


class CrossProcessGateway:
    """偏移源/游标源跨进程共用的网关。

    与 ``limits.SharedGateway`` 的 ``request(send, owner)`` 接口一致，
    但额度、令牌、冷却都持久化到状态目录，供多个进程共享。
    """

    def __init__(
        self,
        state_dir: str | Path,
        client,
        base_url: str,
        quota_limit: int,
        quota_window: float,
        token_timeout: float = 30.0,
    ):
        self.client = client
        self.base_url = base_url.rstrip("/")
        self.store = SharedGatewayStore(state_dir, quota_limit, quota_window)
        self.limit = quota_limit
        self.window = quota_window
        self.token_timeout = token_timeout
        self._token_lock = SourceLock(state_dir, "gateway-token")
        # 观测：本进程实际拿到的额度序列
        self.grant_log: list[object] = []

    @property
    def quota(self) -> "CrossProcessGateway":
        """兼容 limits.SharedGateway 的观测接口（``gateway.quota.grant_log``）。"""
        return self

    def close(self) -> None:
        with self.store._lock:
            try:
                self.store._conn.execute(
                    "DELETE FROM gateway_waiters WHERE owner IN (?,?)",
                    ("offset", "cursor"),
                )
            except sqlite3.Error:  # pragma: no cover
                pass
        self.store.close()

    # ---------- 对外请求 ----------
    async def request(
        self,
        send: Callable[[str | None], Awaitable[T]],
        owner: object = "default",
    ) -> T:
        attempts = 0
        while True:
            attempts += 1
            if attempts > 8:
                raise GatewayError("重试次数过多，网关持续不可用")
            await self._wait_cooldown()
            await self._acquire(str(owner))
            token, generation = await self._ensure_token(str(owner))
            try:
                return await send(token)
            except AuthError:
                # 同一失效世代只续一次；其他进程可能已经续好
                await self._renew_token(generation, str(owner))
            except RateLimited as exc:
                self.store.set_cooldown(exc.retry_after, time.time())

    # ---------- 额度 ----------
    async def _wait_cooldown(self) -> None:
        while True:
            remaining = self.store.cooldown_remaining(time.time())
            if remaining <= 0:
                return
            await asyncio.sleep(min(remaining, 0.2))

    async def _acquire(self, owner: str) -> None:
        while True:
            await self._wait_cooldown()
            ok, retry = self.store.try_acquire(owner, time.time())
            if ok:
                self.grant_log.append(owner)
                return
            # 加一点抖动，避免多个进程同一时刻醒来抢同一个名额
            delay = min(max(retry, 0.01), 0.05) * random.uniform(0.8, 1.2)
            await asyncio.sleep(delay)

    # ---------- 令牌 ----------
    async def _ensure_token(self, owner: str) -> tuple[str, int]:
        token, generation = self.store.read_token()
        if token:
            return token, generation

        loop = asyncio.get_running_loop()
        acquired = await loop.run_in_executor(
            None, self._token_lock.acquire, True, self.token_timeout
        )
        if not acquired:
            raise GatewayError("等待跨进程令牌锁超时")
        try:
            # 等锁期间可能已有其他进程签发，先复用
            token, generation = self.store.read_token()
            if token:
                return token, generation
            new_token = await self._issue_token(owner, "client_credentials")
            generation += 1
            self.store.write_token(new_token, generation, time.time())
            return new_token, generation
        finally:
            self._token_lock.release()

    async def _renew_token(self, failed_generation: int, owner: str) -> None:
        loop = asyncio.get_running_loop()
        acquired = await loop.run_in_executor(
            None, self._token_lock.acquire, True, self.token_timeout
        )
        if not acquired:
            raise GatewayError("等待跨进程令牌锁超时")
        try:
            token, generation = self.store.read_token()
            if token and generation != failed_generation:
                # 该失效令牌已被其他进程续期，直接复用新令牌
                logger.info(
                    "[gateway] 令牌世代 %s 已由其他进程续期到 %s，复用",
                    failed_generation,
                    generation,
                )
                return
            new_token = await self._issue_token(owner, "refresh_token")
            generation += 1
            self.store.write_token(new_token, generation, time.time())
            logger.info("[gateway] 令牌续期完成，新世代 %s", generation)
        finally:
            self._token_lock.release()

    async def _issue_token(self, owner: str, grant_type: str) -> str:
        attempts = 0
        while True:
            attempts += 1
            if attempts > 8:
                raise GatewayError("令牌续期多次被限流，网关持续不可用")
            # 续期请求同样占用网关额度
            await self._acquire(f"token:{owner}")
            try:
                data = await self.client.post(
                    f"{self.base_url}/gateway/token",
                    {"grant_type": grant_type},
                )
                return data["token"]
            except RateLimited as exc:
                logger.info(
                    "令牌请求遇到 429，冷却 %.2fs 后重试", exc.retry_after
                )
                self.store.set_cooldown(exc.retry_after, time.time())
