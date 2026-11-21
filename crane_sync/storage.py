"""持久化层。

职责：
- 分页进度（中止重跑后续读）
- 各来源暂存记录（按 来源+作业区+单号+修订号 去重）
- 快照原子发布：先把完整快照写成不可见临时文件并 rename，
  再在一个数据库事务里切换"已发布"指针；读者只会看到完整快照。
- 合并读取：每个来源只取其最新已发布快照，单号只保留最高修订。

偏移源可以按作业区拆分：每个作业区有独立的分页进度和独立暂存行，
但最终仍只发布一份 offset 快照（跨区按单号取最高修订）。
快照文件格式支持 JSON Lines（零依赖）与 Parquet（需 pyarrow）。
"""
from __future__ import annotations

import json
import os
import random
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

from .models import InspectionOrder

# 偏移源各作业区的分页进度键前缀；旧单接口模式仍用 "offset"。
OFFSET_ZONE_PROGRESS_PREFIX = "offset:zone:"
OFFSET_ROUND_STATE_KEY = "offset_round"


def zone_progress_key(zone: str) -> str:
    return f"{OFFSET_ZONE_PROGRESS_PREFIX}{zone}"


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class Storage:
    def __init__(self, state_dir: str | Path):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        (self.state_dir / "snapshots").mkdir(exist_ok=True)
        self.db_path = self.state_dir / "sync_state.db"
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(self.db_path), check_same_thread=False, isolation_level=None
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    # ---------- schema ----------
    def _init_schema(self) -> None:
        with self._lock:
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
                    zone TEXT NOT NULL DEFAULT '',
                    order_id TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    inspected_at TEXT NOT NULL,
                    gaps_json TEXT NOT NULL,
                    position INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(source, zone, order_id, revision)
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
            if "zone" not in cols:
                # 旧表主键不含 zone，无法容纳同一单号出现在多个作业区；
                # 重建表并把旧数据归入默认空作业区。
                self._conn.executescript(
                    """
                    BEGIN;
                    CREATE TABLE records_migrated(
                        source TEXT NOT NULL,
                        zone TEXT NOT NULL DEFAULT '',
                        order_id TEXT NOT NULL,
                        revision INTEGER NOT NULL,
                        inspected_at TEXT NOT NULL,
                        gaps_json TEXT NOT NULL,
                        position INTEGER NOT NULL DEFAULT 0,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY(source, zone, order_id, revision)
                    );
                    INSERT INTO records_migrated(
                        source,zone,order_id,revision,inspected_at,
                        gaps_json,position,updated_at
                    )
                    SELECT source,'',order_id,revision,inspected_at,
                           gaps_json,position,updated_at FROM records;
                    DROP TABLE records;
                    ALTER TABLE records_migrated RENAME TO records;
                    COMMIT;
                    """
                )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---------- generic state ----------
    def get_state(self, key: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM state WHERE key=?", (key,)
            ).fetchone()
            return row[0] if row else None

    def set_state(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO state(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    # ---------- progress ----------
    def get_progress(self, source: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT data FROM progress WHERE source=?", (source,)
            ).fetchone()
            return json.loads(row[0]) if row else None

    def set_progress(self, source: str, data: dict) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO progress(source,data,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(source) DO UPDATE SET data=excluded.data, "
                "updated_at=excluded.updated_at",
                (source, json.dumps(data, ensure_ascii=False), _utcnow()),
            )

    def reset_progress(self, source: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM progress WHERE source=?", (source,))

    # ---------- staging records ----------
    def upsert_records(
        self,
        source: str,
        orders: list[InspectionOrder],
        position_start: int = 0,
        track_position: bool = False,
        zone: str = "",
    ) -> int:
        """写入暂存区。

        zone 用于偏移源按作业区隔离暂存行：同一单号可以在不同作业区各存一份，
        发布时再跨区按单号取最高修订。
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
                    zone,
                    o.inspected_at,
                    json.dumps(o.gaps, ensure_ascii=False),
                    pos,
                    now,
                    1 if track_position else 0,
                )
            )
        with self._lock:
            cur = self._conn.executemany(
                "INSERT INTO records(order_id,revision,source,zone,inspected_at,"
                "gaps_json,position,updated_at) VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(source,zone,order_id,revision) DO UPDATE SET "
                "inspected_at=excluded.inspected_at, gaps_json=excluded.gaps_json, "
                "position=CASE WHEN ?=1 THEN excluded.position ELSE records.position END, "
                "updated_at=excluded.updated_at",
                # 每条 9 个参数：8 个插入列 + 末尾 position 更新开关
                rows,
            )
            return cur.rowcount

    def count_staging(self, source: str, zone: str | None = None) -> int:
        with self._lock:
            if zone is None:
                return self._conn.execute(
                    "SELECT COUNT(*) FROM records WHERE source=?", (source,)
                ).fetchone()[0]
            return self._conn.execute(
                "SELECT COUNT(*) FROM records WHERE source=? AND zone=?",
                (source, zone),
            ).fetchone()[0]

    def clear_staging(self, source: str, zone: str | None = None) -> None:
        """清空某来源暂存区（已发布的快照表不受影响）。

        zone 为 None 时清空该来源全部分区；指定 zone 时只清该作业区。
        游标清单整体过期、记录集合可能变化时，重读前调用，
        避免新版清单已删除的旧记录残留进新快照。
        """
        with self._lock:
            if zone is None:
                self._conn.execute(
                    "DELETE FROM records WHERE source=?", (source,)
                )
            else:
                self._conn.execute(
                    "DELETE FROM records WHERE source=? AND zone=?", (source, zone)
                )

    def _projection_sql(self, source: str, zones: list[str] | None) -> tuple[str, list]:
        """发布投影 SQL：单号只保留最高修订，可选限定作业区集合。"""
        if zones is None:
            where = "source=? AND zone=''"
            params: list = [source]
        elif not zones:
            # 空集合：没有任何作业区参与发布，结果为空
            where = "source=? AND 1=0"
            params = [source]
        else:
            marks = ",".join("?" for _ in zones)
            where = f"source=? AND zone IN ({marks})"
            params = [source, *zones]
        sql = (
            "WITH ranked AS ("
            "SELECT order_id,revision,source,inspected_at,gaps_json,position,"
            "ROW_NUMBER() OVER ("
            "PARTITION BY order_id ORDER BY revision DESC) AS rn "
            f"FROM records WHERE {where}) "
            "SELECT order_id,revision,source,inspected_at,gaps_json,position "
            "FROM ranked WHERE rn=1 ORDER BY position, order_id"
        )
        return sql, params

    def latest_revision_rows(
        self, source: str, zones: list[str] | None = None
    ) -> list[dict]:
        """发布投影：每个巡检单号只保留最高修订，并保持来源清单顺序。"""
        sql, params = self._projection_sql(source, zones)
        with self._lock:
            cur = self._conn.execute(sql, params)
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

    def _latest_revision_rows(self, source: str) -> list[dict]:
        return self.latest_revision_rows(source, zones=None)

    # ---------- snapshot publish ----------
    def published_sources(self) -> list[str]:
        with self._lock:
            return [
                r[0]
                for r in self._conn.execute(
                    "SELECT source FROM publications ORDER BY source"
                ).fetchall()
            ]

    def latest_snapshot_info(self, source: str) -> dict | None:
        with self._lock:
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

    def _write_snapshot_file(
        self, source: str, snapshot_id: str, rows: list[dict], fmt: str
    ) -> str:
        out_dir = self.state_dir / "snapshots" / source
        out_dir.mkdir(parents=True, exist_ok=True)
        if fmt == "jsonl":
            final = out_dir / f"{snapshot_id}.jsonl"
            tmp = final.with_suffix(".jsonl.tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, final)
            return str(final)
        if fmt == "parquet":
            try:
                import pyarrow as pa  # noqa: F401
                import pyarrow.parquet as pq
            except ImportError as exc:  # pragma: no cover - 环境相关
                raise RuntimeError(
                    "选择 Parquet 格式需要安装 pyarrow：pip install pyarrow"
                ) from exc
            import pyarrow as pa

            final = out_dir / f"{snapshot_id}.parquet"
            tmp = final.with_suffix(".parquet.tmp")
            # 间隙是嵌套结构，序列化为 JSON 字符串列以便 Parquet 存储
            flat = [
                {
                    "order_id": r["order_id"],
                    "revision": r["revision"],
                    "source": r["source"],
                    "inspected_at": r["inspected_at"],
                    "gaps_json": json.dumps(r["gaps"], ensure_ascii=False),
                }
                for r in rows
            ]
            table = pa.Table.from_pylist(flat)
            pq.write_table(table, str(tmp))
            os.replace(tmp, final)
            return str(final)
        raise ValueError(f"不支持的快照格式: {fmt}")

    def publish_snapshot(
        self, source: str, fmt: str = "jsonl", zones: list[str] | None = None
    ) -> dict:
        """发布某来源当前暂存区的完整快照（原子切换）。

        发布投影中每个巡检单号只保留最高修订（暂存区仍保留全量记录）。
        zones 为 None 时只发布 ``zone=''`` 的旧单接口数据（历史行为）；
        传入作业区列表时，把所列作业区的暂存数据合并成一份快照。
        """
        rows = self.latest_revision_rows(source, zones=zones)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
        snapshot_id = f"{stamp}_{random.getrandbits(24):06x}"
        table_name = f"snap_{source}_{snapshot_id}"
        file_path = self._write_snapshot_file(source, snapshot_id, rows, fmt)
        projection_sql, projection_params = self._projection_sql(source, zones)
        with self._lock:
            old = self.latest_snapshot_info(source)
            self._conn.execute("BEGIN")
            try:
                # 与 latest_revision_rows 等价的 SQL：仅保留每单号最高修订
                self._conn.execute(
                    f"CREATE TABLE {table_name} AS " + projection_sql,
                    projection_params,
                )
                self._conn.execute(
                    "INSERT INTO publications(source,snapshot_id,snapshot_table,"
                    "file_path,format,record_count,published_at) "
                    "VALUES(?,?,?,?,?,?,?) "
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
                        len(rows),
                        _utcnow(),
                    ),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            # 旧快照在新指针提交后再删除；读者永远不会看到半成品
            if old and old["snapshot_id"] != snapshot_id:
                self._conn.execute(
                    "DROP TABLE IF EXISTS " + old["snapshot_table"]
                )
                old_file = old.get("file_path")
                if old_file:
                    try:
                        os.remove(old_file)
                    except OSError:
                        pass
        info = self.latest_snapshot_info(source)
        assert info is not None
        return info

    def _snapshot_table(self, source: str, snapshot_id: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT snapshot_table FROM publications "
                "WHERE source=? AND snapshot_id=?",
                (source, snapshot_id),
            ).fetchone()
            return row[0] if row else None

    def read_snapshot(self, source: str) -> list[dict]:
        """读取某来源最新已发布快照。未发布时抛错。"""
        info = self.latest_snapshot_info(source)
        if info is None:
            raise RuntimeError(f"来源 {source} 尚未发布过快照")
        table = self._snapshot_table(source, info["snapshot_id"])
        assert table is not None
        with self._lock:
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

    def read_merged(self, sources: list[str] | None = None) -> list[dict]:
        """跨来源合并：只取各来源最新已发布快照，单号仅保留最高修订。"""
        published = self.published_sources()
        if sources:
            published = [s for s in published if s in sources]
        if not published:
            raise RuntimeError("还没有任何已发布快照")
        parts = []
        for s in published:
            info = self.latest_snapshot_info(s)
            assert info is not None
            tbl = self._snapshot_table(s, info["snapshot_id"])
            parts.append(
                "SELECT order_id,revision,source,inspected_at,gaps_json,position "
                f"FROM {tbl}"
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
        with self._lock:
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
