"""持久化层。

职责：
- 分页进度（中止重跑后续读）
- 各来源暂存记录（按 来源+单号+修订号 去重）
- 快照原子发布：先把完整快照写成不可见临时文件并 rename，
  再在一个数据库事务里切换"已发布"指针；读者只会看到完整快照。
- 合并读取：每个来源只取其最新已发布快照，单号只保留最高修订。

快照文件格式支持 JSON Lines（零依赖）与 Parquet（需 pyarrow）。

并发与内存：
- 每个线程使用独立的 SQLite 连接（WAL 模式，读不阻塞写），
  写事务另有进程内锁串行化；
- 快照发布先分批归档到独立快照表，再从表逐批流式写文件——
  写文件期间不持有任何数据库写锁，某个来源落盘卡住时其他来源
  照常写暂存与发布；
- JSON Lines 快照逐行写出，峰值内存与快照总大小无关。
"""
from __future__ import annotations

import json
import os
import random
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from .models import InspectionOrder

# 快照表分批归档 / 分批写盘的默认批大小（按行数与字节数）。
# 单行可能很大（同一秒内的长正文），两种上限先到者为准，
# 以限定发布阶段的峰值内存。
DEFAULT_COPY_BATCH = 128
DEFAULT_WRITE_BATCH = 200
DEFAULT_BATCH_BYTES = 256 * 1024


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class Storage:
    def __init__(self, state_dir: str | Path):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        (self.state_dir / "snapshots").mkdir(exist_ok=True)
        self.db_path = self.state_dir / "sync_state.db"
        # 每个线程一条连接；SQLite 连接不是线程安全的
        self._local = threading.local()
        self._conns: list[sqlite3.Connection] = []
        self._conns_lock = threading.Lock()
        self._write_lock = threading.RLock()
        self._publish_locks: dict[str, threading.Lock] = {}
        self._publish_locks_guard = threading.Lock()
        self._closed = False
        self._init_schema()

    # ---------- 连接 ----------
    def _new_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self.db_path),
            check_same_thread=False,
            isolation_level=None,
            timeout=30.0,
        )
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @property
    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            if self._closed:
                raise RuntimeError("Storage 已关闭")
            conn = self._new_connection()
            self._local.conn = conn
            with self._conns_lock:
                self._conns.append(conn)
        return conn

    def _publish_lock(self, source: str) -> threading.Lock:
        with self._publish_locks_guard:
            lock = self._publish_locks.get(source)
            if lock is None:
                lock = threading.Lock()
                self._publish_locks[source] = lock
            return lock

    # ---------- schema ----------
    def _init_schema(self) -> None:
        with self._write_lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS state(
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS progress(
                    source TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS records(
                    source TEXT NOT NULL,
                    order_id TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    inspected_at TEXT NOT NULL,
                    gaps_json TEXT NOT NULL,
                    position INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(source, order_id, revision)
                );
                CREATE TABLE IF NOT EXISTS publications(
                    source TEXT PRIMARY KEY,
                    snapshot_id TEXT NOT NULL,
                    snapshot_table TEXT NOT NULL,
                    file_path TEXT,
                    format TEXT NOT NULL,
                    record_count INTEGER NOT NULL,
                    published_at TEXT NOT NULL
                );
                """
            )
            # 旧版本数据库的轻量迁移
            cols = {
                r[1]
                for r in self._conn.execute("PRAGMA table_info(records)").fetchall()
            }
            if "position" not in cols and "seq" not in cols:
                self._conn.execute(
                    "ALTER TABLE records ADD COLUMN position INTEGER NOT NULL DEFAULT 0"
                )

    def close(self) -> None:
        with self._conns_lock:
            self._closed = True
            conns, self._conns = self._conns, []
        for conn in conns:
            try:
                conn.close()
            except sqlite3.Error:
                pass
        self._local = threading.local()

    # ---------- generic state ----------
    def get_state(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM state WHERE key=?", (key,)
        ).fetchone()
        return row[0] if row else None

    def set_state(self, key: str, value: str) -> None:
        with self._write_lock:
            self._conn.execute(
                "INSERT INTO state(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    # ---------- progress ----------
    def get_progress(self, source: str) -> dict | None:
        row = self._conn.execute(
            "SELECT data FROM progress WHERE source=?", (source,)
        ).fetchone()
        return json.loads(row[0]) if row else None

    def set_progress(self, source: str, data: dict) -> None:
        with self._write_lock:
            self._conn.execute(
                "INSERT INTO progress(source,data,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(source) DO UPDATE SET data=excluded.data, "
                "updated_at=excluded.updated_at",
                (source, json.dumps(data, ensure_ascii=False), _utcnow()),
            )

    def reset_progress(self, source: str) -> None:
        with self._write_lock:
            self._conn.execute("DELETE FROM progress WHERE source=?", (source,))

    # ---------- staging records ----------
    def upsert_records(
        self,
        source: str,
        orders: list[InspectionOrder],
        position_start: int = 0,
        track_position: bool = False,
    ) -> int:
        """写入暂存区。

        track_position=True 时，每条记录按其在清单中的行号写入 position，
        游标源重读新版清单即按新清单逐行覆盖顺序；
        track_position=False 时 position 恒为 0，按单号/修订排序即可。
        已存在且本次未带位置信息的记录，保留原 position。
        """
        if not orders:
            return 0
        now = _utcnow()
        rows = []
        for idx, o in enumerate(orders):
            if track_position:
                # 游标源全量重读：每条记录按新清单行号无条件覆盖位置
                pos = o.position if o.position is not None else position_start + idx
            else:
                pos = 0
            rows.append(
                (
                    o.order_id,
                    o.revision,
                    source,
                    o.inspected_at,
                    json.dumps(o.gaps, ensure_ascii=False),
                    pos,
                    now,
                    1 if track_position else 0,
                )
            )
        with self._write_lock:
            cur = self._conn.executemany(
                "INSERT INTO records(order_id,revision,source,inspected_at,"
                "gaps_json,position,updated_at) VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(source,order_id,revision) DO UPDATE SET "
                "inspected_at=excluded.inspected_at, gaps_json=excluded.gaps_json, "
                "position=CASE WHEN ?=1 THEN excluded.position ELSE records.position END, "
                "updated_at=excluded.updated_at",
                # 每条 8 个参数：7 个插入列 + 末尾 position 更新开关
                rows,
            )
            return cur.rowcount

    def count_staging(self, source: str) -> int:
        return self._conn.execute(
            "SELECT COUNT(*) FROM records WHERE source=?", (source,)
        ).fetchone()[0]

    def clear_staging(self, source: str) -> None:
        """清空某来源暂存区（已发布的快照表不受影响）。

        游标清单整体过期、记录集合可能变化时，重读前调用，
        避免新版清单已删除的旧记录残留进新快照。
        """
        with self._write_lock:
            self._conn.execute("DELETE FROM records WHERE source=?", (source,))

    # ---------- snapshot publish ----------
    def published_sources(self) -> list[str]:
        return [
            r[0]
            for r in self._conn.execute(
                "SELECT source FROM publications ORDER BY source"
            ).fetchall()
        ]

    def latest_snapshot_info(self, source: str) -> dict | None:
        row = self._conn.execute(
            "SELECT snapshot_id,snapshot_table,file_path,format,record_count,"
            "published_at FROM publications WHERE source=?",
            (source,),
        ).fetchone()
        if not row:
            return None
        return {
            "snapshot_id": row[0],
            "snapshot_table": row[1],
            "file_path": row[2],
            "format": row[3],
            "record_count": row[4],
            "published_at": row[5],
        }

    _SNAPSHOT_SELECT = (
        "SELECT order_id,revision,source,inspected_at,gaps_json,position "
        "FROM {table} ORDER BY position, order_id"
    )

    def _create_snapshot_table(
        self, source: str, table: str, batch: int = DEFAULT_COPY_BATCH
    ) -> int:
        """把某来源暂存区折叠为"每单号最高修订"并分批归档到快照表。

        按"单号"分批（同一单号的全部修订必然落在同一批，折叠结果与
        一次性 SQL 完全一致），每批一个短事务：大批快照归档期间其他
        来源仍能提交自己的写事务。返回归档行数。
        """
        with self._write_lock:
            self._conn.execute(f"DROP TABLE IF EXISTS {table}")
            self._conn.execute(
                f"CREATE TABLE {table}("
                "order_id TEXT NOT NULL, revision INTEGER NOT NULL, "
                "source TEXT NOT NULL, inspected_at TEXT NOT NULL, "
                "gaps_json TEXT NOT NULL, position INTEGER NOT NULL)"
            )
        total = 0
        last_oid: str | None = None
        id_sql = (
            "SELECT DISTINCT order_id FROM records WHERE source=? "
            "AND (? IS NULL OR order_id>?) ORDER BY order_id LIMIT ?"
        )
        while True:
            with self._write_lock:
                self._conn.execute("BEGIN IMMEDIATE")
                try:
                    batch_ids = [
                        r[0]
                        for r in self._conn.execute(
                            id_sql, (source, last_oid, last_oid, batch)
                        ).fetchall()
                    ]
                    rows = []
                    if batch_ids:
                        # 直接用本批的单号列表取各单最高修订：
                        # 不能用子查询再 LIMIT 一次，否则同一单号的多条修订
                        # 会吃掉限额，导致部分单号漏出快照
                        placeholders = ",".join("?" * len(batch_ids))
                        rows = self._conn.execute(
                            "WITH ranked AS ("
                            "SELECT order_id,revision,source,inspected_at,"
                            "gaps_json,position,ROW_NUMBER() OVER ("
                            "PARTITION BY order_id ORDER BY revision DESC) AS rn "
                            "FROM records WHERE source=? "
                            f"AND order_id IN ({placeholders})) "
                            "SELECT order_id,revision,source,inspected_at,"
                            "gaps_json,position FROM ranked WHERE rn=1",
                            (source, *batch_ids),
                        ).fetchall()
                    if rows:
                        self._conn.executemany(
                            f"INSERT INTO {table} VALUES(?,?,?,?,?,?)", rows
                        )
                    self._conn.execute("COMMIT")
                except BaseException:
                    self._conn.execute("ROLLBACK")
                    raise
            if not batch_ids:
                break
            total += len(rows)
            last_oid = batch_ids[-1]
        return total

    def _drop_snapshot_table(self, table: str) -> None:
        if not table:
            return
        try:
            with self._write_lock:
                self._conn.execute("DROP TABLE IF EXISTS " + table)
        except sqlite3.Error:
            pass

    def _write_snapshot_file(
        self,
        source: str,
        table: str,
        snapshot_id: str,
        fmt: str,
        batch: int = DEFAULT_WRITE_BATCH,
    ) -> str:
        """从快照表流式写出文件；不持有数据库写锁。"""
        out_dir = self.state_dir / "snapshots" / source
        out_dir.mkdir(parents=True, exist_ok=True)
        select = self._SNAPSHOT_SELECT.format(table=table)

        if fmt == "jsonl":
            final = out_dir / f"{snapshot_id}.jsonl"
            tmp = final.with_suffix(".jsonl.tmp")
            try:
                with open(tmp, "w", encoding="utf-8") as f:
                    cur = self._conn.execute(select)
                    while True:
                        rows = cur.fetchmany(batch)
                        if not rows:
                            break
                        parts = []
                        chunk_bytes = 0
                        for r in rows:
                            # gaps_json 已是合法 JSON 数组文本，直接拼入，
                            # 避免整份快照物化为 Python 对象
                            parts.append(
                                '{"order_id":'
                                + json.dumps(r[0], ensure_ascii=False)
                                + ',"revision":'
                                + str(int(r[1]))
                                + ',"source":'
                                + json.dumps(r[2], ensure_ascii=False)
                                + ',"inspected_at":'
                                + json.dumps(r[3], ensure_ascii=False)
                                + ',"gaps":'
                                + r[4]
                                + "}\n"
                            )
                            chunk_bytes += len(parts[-1])
                            if chunk_bytes >= DEFAULT_BATCH_BYTES and len(parts) > 1:
                                f.write("".join(parts))
                                parts = []
                                chunk_bytes = 0
                        f.write("".join(parts))
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, final)
            except BaseException:
                try:
                    os.remove(tmp)
                except OSError:
                    pass
                raise
            return str(final)

        if fmt == "parquet":
            try:
                import pyarrow as pa
                import pyarrow.parquet as pq
            except ImportError as exc:  # pragma: no cover - 环境相关
                raise RuntimeError(
                    "选择 Parquet 格式需要安装 pyarrow：pip install pyarrow"
                ) from exc

            final = out_dir / f"{snapshot_id}.parquet"
            tmp = final.with_suffix(".parquet.tmp")
            schema = pa.schema(
                [
                    ("order_id", pa.string()),
                    ("revision", pa.int64()),
                    ("source", pa.string()),
                    ("inspected_at", pa.string()),
                    ("gaps_json", pa.string()),
                ]
            )
            try:
                with pq.ParquetWriter(str(tmp), schema) as writer:
                    # 间隙是嵌套结构，序列化为 JSON 字符串列以便 Parquet 存储
                    cur = self._conn.execute(select)
                    while True:
                        rows = cur.fetchmany(batch)
                        if not rows:
                            break
                        columns = {
                            "order_id": [r[0] for r in rows],
                            "revision": [int(r[1]) for r in rows],
                            "source": [r[2] for r in rows],
                            "inspected_at": [r[3] for r in rows],
                            "gaps_json": [r[4] for r in rows],
                        }
                        writer.write_table(pa.table(columns, schema=schema))
                os.replace(tmp, final)
            except BaseException:
                try:
                    os.remove(tmp)
                except OSError:
                    pass
                raise
            return str(final)
        raise ValueError(f"不支持的快照格式: {fmt}")

    def publish_snapshot(
        self,
        source: str,
        fmt: str = "jsonl",
        batch: int = DEFAULT_WRITE_BATCH,
    ) -> dict:
        """发布某来源当前暂存区的完整快照（原子切换）。

        发布投影中每个巡检单号只保留最高修订（暂存区仍保留全量记录）。
        文件写盘在数据库锁之外进行：写盘卡住只影响该来源本身。
        """
        with self._publish_lock(source):
            stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
            snapshot_id = f"{stamp}_{random.getrandbits(24):06x}"
            table_name = f"snap_{source}_{snapshot_id}"
            published = False
            try:
                batch = max(1, int(batch))
                record_count = self._create_snapshot_table(
                    source, table_name, batch
                )
                file_path = self._write_snapshot_file(
                    source, table_name, snapshot_id, fmt, batch
                )
                with self._write_lock:
                    old = self.latest_snapshot_info(source)
                    self._conn.execute("BEGIN IMMEDIATE")
                    try:
                        self._conn.execute(
                            "INSERT INTO publications("
                            "source,snapshot_id,snapshot_table,file_path,format,"
                            "record_count,published_at) VALUES(?,?,?,?,?,?,?) "
                            "ON CONFLICT(source) DO UPDATE SET "
                            "snapshot_id=excluded.snapshot_id,"
                            "snapshot_table=excluded.snapshot_table,"
                            "file_path=excluded.file_path,format=excluded.format,"
                            "record_count=excluded.record_count,"
                            "published_at=excluded.published_at",
                            (
                                source,
                                snapshot_id,
                                table_name,
                                file_path,
                                fmt,
                                record_count,
                                _utcnow(),
                            ),
                        )
                        self._conn.execute("COMMIT")
                    except BaseException:
                        self._conn.execute("ROLLBACK")
                        raise
                published = True
            finally:
                if not published:
                    # 发布未完成：清理不可见的半成品
                    self._drop_snapshot_table(table_name)
            # 旧快照在新指针提交后再删除；读者永远不会看到半成品
            if old and old["snapshot_id"] != snapshot_id:
                self._drop_snapshot_table(old["snapshot_table"])
                old_file = old.get("file_path")
                if old_file:
                    try:
                        os.remove(old_file)
                    except OSError:
                        pass
        info = self.latest_snapshot_info(source)
        assert info is not None
        return info

    # ---------- snapshot read ----------
    def _select_rows(self, table: str) -> list[dict]:
        cur = self._conn.execute(
            f"SELECT order_id,revision,source,inspected_at,gaps_json "
            f"FROM {table} ORDER BY position, order_id, revision"
        )
        return [
            {
                "order_id": r[0],
                "revision": r[1],
                "source": r[2],
                "inspected_at": r[3],
                "gaps": json.loads(r[4]),
            }
            for r in cur.fetchall()
        ]

    def _retry_missing_table(self, fn, attempts: int = 6):
        """发布切换指针时会删除旧快照表，读者重取指针后重试即可。"""
        for i in range(attempts):
            try:
                return fn()
            except sqlite3.OperationalError as exc:
                if "no such table" not in str(exc) or i == attempts - 1:
                    raise
                time.sleep(0.02)

    def read_snapshot(self, source: str) -> list[dict]:
        """读取某来源最新已发布快照。未发布时抛错。"""

        def _read() -> list[dict]:
            info = self.latest_snapshot_info(source)
            if info is None:
                raise RuntimeError(f"来源 {source} 尚未发布过快照")
            return self._select_rows(info["snapshot_table"])

        return self._retry_missing_table(_read)

    def read_merged(self, sources: list[str] | None = None) -> list[dict]:
        """跨来源合并：只取各来源最新已发布快照，单号仅保留最高修订。"""

        def _read() -> list[dict]:
            published = self.published_sources()
            if sources:
                published = [s for s in published if s in sources]
            if not published:
                raise RuntimeError("还没有任何已发布快照")
            parts = []
            for s in published:
                info = self.latest_snapshot_info(s)
                assert info is not None
                parts.append(
                    "SELECT order_id,revision,source,inspected_at,gaps_json,position "
                    f"FROM {info['snapshot_table']}"
                )
            union = " UNION ALL ".join(parts)
            sql = (
                "WITH all_rows AS (" + union + "), "
                "ranked AS ("
                "SELECT *, ROW_NUMBER() OVER ("
                "PARTITION BY order_id ORDER BY revision DESC, source) AS rn "
                "FROM all_rows) "
                "SELECT order_id,revision,source,inspected_at,gaps_json "
                "FROM ranked WHERE rn=1 ORDER BY order_id"
            )
            cur = self._conn.execute(sql)
            return [
                {
                    "order_id": r[0],
                    "revision": r[1],
                    "source": r[2],
                    "inspected_at": r[3],
                    "gaps": json.loads(r[4]),
                }
                for r in cur.fetchall()
            ]

        return self._retry_missing_table(_read)
