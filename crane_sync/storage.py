"""持久化层。

职责：
- 分页进度（中止重跑后续读）
- 各来源暂存记录（按 来源+单号+修订号 去重）
- 快照原子发布：先把完整快照写成不可见临时文件并 rename，
  再在一个数据库事务里切换"已发布"指针；读者只会看到完整快照。
- 合并读取：每个来源只取其最新已发布快照，单号只保留最高修订。

快照文件格式支持 JSON Lines（零依赖）与 Parquet（需 pyarrow）。
"""
from __future__ import annotations

import json
import os
import random
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .models import InspectionOrder

# 已结束请求（confirmed/failed/unknown/interrupted）最多保留的条数。
# 未结束的当前等待请求与 progress 表中的各源续点不受此上限影响。
ATTEMPT_HISTORY_CAP = 200

_ATTEMPT_COLUMNS = (
    "attempt_id,run_id,source,phase,endpoint,request_key,state,outcome,"
    "http_status,detail,reply_confirmed,progress_written,progress_json,"
    "progress_before,progress_at,started_at,updated_at,finished_at"
)

# 本进程内仍在运行的 run_id；用于同进程多次打开 Storage 时不误判旧运行已死。
_ACTIVE_RUN_IDS: set[str] = set()


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pid_alive(pid: int) -> bool:
    """本机进程是否仍存活（用于识别被强停的旧同步进程）。"""
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except (PermissionError, OverflowError, ValueError):
        return True
    return True


def _attempt_row(row) -> dict:
    return {
        "attempt_id": row[0],
        "run_id": row[1],
        "source": row[2],
        "phase": row[3],
        "endpoint": row[4],
        "request_key": row[5],
        "state": row[6],
        "outcome": row[7],
        "http_status": row[8],
        "detail": json.loads(row[9]) if row[9] else {},
        "reply_confirmed": bool(row[10]),
        "progress_written": bool(row[11]),
        "progress": json.loads(row[12]) if row[12] else None,
        "progress_before": json.loads(row[13]) if row[13] else None,
        "progress_at": row[14],
        "started_at": row[15],
        "updated_at": row[16],
        "finished_at": row[17],
    }


class Storage:
    def __init__(self, state_dir: str | Path):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        (self.state_dir / "snapshots").mkdir(exist_ok=True)
        self.db_path = self.state_dir / "sync_state.db"
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(self.db_path),
            check_same_thread=False,
            isolation_level=None,
            timeout=5.0,  # 与正在运行的 sync 并发时短暂等待写锁
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
                -- 每次 sync 进程一行的运行登记；正常退出写 finished_at，
                -- 被强停则保持 running，由下一次打开时按 pid 存活情况对账。
                CREATE TABLE IF NOT EXISTS runs(
                    run_id TEXT PRIMARY KEY,
                    pid INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    status TEXT NOT NULL,
                    detail TEXT
                );
                -- 每个 HTTP 请求一行的尝试日志（含已结束请求与当前在途请求）。
                CREATE TABLE IF NOT EXISTS attempts(
                    attempt_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    request_key TEXT NOT NULL,
                    state TEXT NOT NULL,          -- inflight | finished | unknown
                    outcome TEXT NOT NULL,        -- pending|ok|http_429|http_401|http_410|error|unknown
                    http_status INTEGER,
                    detail TEXT NOT NULL DEFAULT '{}',
                    reply_confirmed INTEGER NOT NULL DEFAULT 0,
                    progress_written INTEGER NOT NULL DEFAULT 0,
                    progress_json TEXT,
                    progress_before TEXT,
                    progress_at TEXT,
                    started_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    finished_at TEXT
                );
                CREATE INDEX IF NOT EXISTS attempts_started_idx
                    ON attempts(started_at);
                CREATE INDEX IF NOT EXISTS attempts_finished_idx
                    ON attempts(finished_at);
                -- 每个来源当前阻塞在何处（等额度/冷却/等 HTTP 回包）。
                CREATE TABLE IF NOT EXISTS runtime(
                    source TEXT PRIMARY KEY,
                    run_id TEXT,
                    pid INTEGER,
                    state TEXT NOT NULL,
                    since TEXT NOT NULL,
                    detail TEXT NOT NULL DEFAULT '{}',
                    updated_at TEXT NOT NULL
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

    def progress_updated_at(self, source: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT updated_at FROM progress WHERE source=?", (source,)
            ).fetchone()
        return row[0] if row else None

    def reset_progress(self, source: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM progress WHERE source=?", (source,))

    # ---------- runs / attempts / runtime（本地诊断） ----------
    def begin_run(self) -> str:
        """登记一次 sync 进程；pid 用于强停后判定"结果未知"。"""
        run_id = uuid.uuid4().hex[:12]
        _ACTIVE_RUN_IDS.add(run_id)
        with self._lock:
            self._conn.execute(
                "INSERT INTO runs(run_id,pid,started_at,status,detail) "
                "VALUES(?,?,?,'running','{}')",
                (run_id, os.getpid(), _utcnow()),
            )
        return run_id

    def finish_run(self, run_id: str, status: str = "completed") -> None:
        _ACTIVE_RUN_IDS.discard(run_id)
        with self._lock:
            self._conn.execute(
                "UPDATE runs SET finished_at=?,status=? WHERE run_id=?",
                (_utcnow(), status, run_id),
            )

    def get_run(self, run_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT run_id,pid,started_at,finished_at,status,detail "
                "FROM runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
        if not row:
            return None
        return {
            "run_id": row[0],
            "pid": row[1],
            "started_at": row[2],
            "finished_at": row[3],
            "status": row[4],
            "detail": json.loads(row[5]) if row[5] else {},
        }

    def get_run_by_status(self, status: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT run_id FROM runs WHERE status=? "
                "ORDER BY started_at DESC, rowid DESC LIMIT 1",
                (status,),
            ).fetchone()
        return self.get_run(row[0]) if row else None

    def set_source_state(
        self,
        source: str,
        state: str,
        run_id: str | None = None,
        detail: dict | None = None,
        since: str | None = None,
    ) -> None:
        """记录某来源当前阻塞位置：waiting_quota / cooldown /
        waiting_reply / sending / idle 等。"""
        now = _utcnow()
        with self._lock:
            self._conn.execute(
                "INSERT INTO runtime(source,run_id,pid,state,since,detail,"
                "updated_at) VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(source) DO UPDATE SET run_id=excluded.run_id, "
                "pid=excluded.pid, state=excluded.state, since=excluded.since, "
                "detail=excluded.detail, updated_at=excluded.updated_at",
                (
                    source,
                    run_id,
                    os.getpid() if run_id else None,
                    state,
                    since or now,
                    json.dumps(detail or {}, ensure_ascii=False),
                    now,
                ),
            )

    def get_source_state(self, source: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT source,run_id,pid,state,since,detail,updated_at "
                "FROM runtime WHERE source=?",
                (source,),
            ).fetchone()
        if not row:
            return None
        return {
            "source": row[0],
            "run_id": row[1],
            "pid": row[2],
            "state": row[3],
            "since": row[4],
            "detail": json.loads(row[5]) if row[5] else {},
            "updated_at": row[6],
        }

    def all_source_states(self) -> dict[str, dict]:
        with self._lock:
            names = [
                r[0] for r in self._conn.execute(
                    "SELECT source FROM runtime ORDER BY source"
                ).fetchall()
            ]
        return {n: s for n in names if (s := self.get_source_state(n))}

    def begin_attempt(
        self,
        run_id: str,
        source: str,
        phase: str,
        endpoint: str,
        request_key: str,
        detail: dict | None = None,
    ) -> str:
        """在发出请求前落一条 inflight 记录；返回 attempt_id。

        强停后这条记录会一直是 inflight——即"未确认回包的在途请求"，
        由 `reconcile_attempts` 标记为结果未知。
        """
        attempt_id = uuid.uuid4().hex[:16]
        now = _utcnow()
        prior = self.get_progress(source)
        with self._lock:
            self._conn.execute(
                "INSERT INTO attempts(attempt_id,run_id,source,phase,endpoint,"
                "request_key,state,outcome,detail,reply_confirmed,"
                "progress_written,progress_before,started_at,updated_at) "
                "VALUES(?,?,?,?,?,?,'inflight','pending',?,0,0,?,?,?)",
                (
                    attempt_id,
                    run_id,
                    source,
                    phase,
                    endpoint,
                    request_key,
                    json.dumps(detail or {}, ensure_ascii=False),
                    json.dumps(prior, ensure_ascii=False) if prior else None,
                    now,
                    now,
                ),
            )
        return attempt_id

    def finish_attempt(
        self,
        attempt_id: str,
        outcome: str,
        http_status: int | None = None,
        reply_confirmed: bool = False,
        detail: dict | None = None,
    ) -> None:
        now = _utcnow()
        with self._lock:
            self._conn.execute(
                "UPDATE attempts SET state='finished',outcome=?,http_status=?,"
                "reply_confirmed=?,detail=?,updated_at=?,finished_at=? "
                "WHERE attempt_id=?",
                (
                    outcome,
                    http_status,
                    1 if reply_confirmed else 0,
                    json.dumps(detail or {}, ensure_ascii=False),
                    now,
                    now,
                    attempt_id,
                ),
            )
        self._trim_attempts()

    def mark_attempt_progress(
        self, attempt_id: str, source: str, progress: dict | None
    ) -> None:
        """记录该回包对应的进度是否已落盘（可续读进度）。"""
        now = _utcnow()
        written = progress is not None
        with self._lock:
            self._conn.execute(
                "UPDATE attempts SET progress_written=?, progress_json=?, "
                "progress_at=?, updated_at=? WHERE attempt_id=?",
                (
                    1 if written else 0,
                    json.dumps(progress, ensure_ascii=False) if written else None,
                    now if written else None,
                    now,
                    attempt_id,
                ),
            )

    def _trim_attempts(self, cap: int = ATTEMPT_HISTORY_CAP) -> int:
        """只保留最近 cap 条已结束（非 inflight）请求。

        inflight（当前等待回包）永不被上限淘汰；progress 表的各源续点独立保留。
        """
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM attempts WHERE state != 'inflight' AND attempt_id "
                "NOT IN (SELECT attempt_id FROM attempts WHERE state != 'inflight' "
                "ORDER BY COALESCE(finished_at, updated_at) DESC, rowid DESC "
                "LIMIT ?)",
                (cap,),
            )
            return cur.rowcount

    def reconcile_attempts(self) -> list[dict]:
        """把属于已消失进程的 inflight 尝试标为"结果未知"。

        返回本次被标记的尝试列表；已确认回包但进度未落盘的同样标出，
        让重跑后能查清旧尝试到底有没有落盘。
        """
        marked: list[dict] = []
        stale_runs: set[str] = set()
        with self._lock:
            rows = self._conn.execute(
                "SELECT a.attempt_id,a.run_id,a.source,a.phase,a.endpoint,"
                "a.request_key,a.progress_written,a.progress_json,"
                "r.pid,r.status FROM attempts a "
                "LEFT JOIN runs r ON r.run_id=a.run_id "
                "WHERE a.state='inflight'"
            ).fetchall()
            now = _utcnow()
            for (
                attempt_id, run_id, source, phase, endpoint, request_key,
                progress_written, progress_json, pid, run_status,
            ) in rows:
                if run_id in _ACTIVE_RUN_IDS:
                    continue
                if run_status == "running" and pid and _pid_alive(pid):
                    # 进程仍在跑：属于当前等待，保持 inflight
                    continue
                detail = {
                    "reason": "process_vanished",
                    "run_status": run_status,
                    "pid": pid,
                    "note": (
                        "进程强停，未确认回包；重跑后请以各源续点为准"
                        if not progress_written
                        else "进程强停前该回包对应进度已落盘"
                    ),
                }
                self._conn.execute(
                    "UPDATE attempts SET state='unknown',outcome='unknown',"
                    "reply_confirmed=0,detail=?,updated_at=?,finished_at=? "
                    "WHERE attempt_id=?",
                    (json.dumps(detail, ensure_ascii=False), now, now, attempt_id),
                )
                if run_id:
                    stale_runs.add(run_id)
                marked.append(
                    {
                        "attempt_id": attempt_id,
                        "run_id": run_id,
                        "source": source,
                        "phase": phase,
                        "endpoint": endpoint,
                        "request_key": request_key,
                        "progress_written": bool(progress_written),
                        "progress": json.loads(progress_json)
                        if progress_json
                        else None,
                        "pid": pid,
                        "run_status": run_status,
                        "outcome": "unknown",
                    }
                )
            for run_id in stale_runs:
                self._conn.execute(
                    "UPDATE runs SET status='interrupted' "
                    "WHERE run_id=? AND status='running'",
                    (run_id,),
                )
        return marked

    def attempts_by_state(self, state: str, limit: int = 200) -> list[dict]:
        """按状态取尝试（如所有结果未知/在途请求），不受近期条数截断。"""
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {_ATTEMPT_COLUMNS} FROM attempts WHERE state=? "
                "ORDER BY started_at DESC, rowid DESC LIMIT ?",
                (state, limit),
            ).fetchall()
        return [_attempt_row(r) for r in rows]

    def attempts_for_run(self, run_id: str, limit: int = 200) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {_ATTEMPT_COLUMNS} FROM attempts WHERE run_id=? "
                "ORDER BY started_at, rowid LIMIT ?",
                (run_id, limit),
            ).fetchall()
        return [_attempt_row(r) for r in rows]

    def recent_attempts(self, limit: int = 20) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {_ATTEMPT_COLUMNS} FROM attempts "
                "ORDER BY started_at DESC, rowid DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [_attempt_row(r) for r in rows]

    def attempts_for_source(self, source: str, limit: int = 20) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {_ATTEMPT_COLUMNS} FROM attempts WHERE source=? "
                "ORDER BY started_at DESC, rowid DESC LIMIT ?",
                (source, limit),
            ).fetchall()
        return [_attempt_row(r) for r in rows]

    def attempt(self, attempt_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                f"SELECT {_ATTEMPT_COLUMNS} FROM attempts WHERE attempt_id=?",
                (attempt_id,),
            ).fetchone()
        return _attempt_row(row) if row else None

    def attempts_since(self, since: str, limit: int = 50) -> list[dict]:
        """查看某一时刻之后（通常是某次运行开始后）的旧尝试。"""
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {_ATTEMPT_COLUMNS} FROM attempts "
                "WHERE started_at >= ? ORDER BY started_at DESC, rowid DESC LIMIT ?",
                (since, limit),
            ).fetchall()
        return [_attempt_row(r) for r in rows]

    def count_attempts(self, state: str | None = None) -> int:
        with self._lock:
            if state:
                return self._conn.execute(
                    "SELECT COUNT(*) FROM attempts WHERE state=?", (state,)
                ).fetchone()[0]
            return self._conn.execute(
                "SELECT COUNT(*) FROM attempts"
            ).fetchone()[0]

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
        with self._lock:
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
        with self._lock:
            return self._conn.execute(
                "SELECT COUNT(*) FROM records WHERE source=?", (source,)
            ).fetchone()[0]

    def clear_staging(self, source: str) -> None:
        """清空某来源暂存区（已发布的快照表不受影响）。

        游标清单整体过期、记录集合可能变化时，重读前调用，
        避免新版清单已删除的旧记录残留进新快照。
        """
        with self._lock:
            self._conn.execute("DELETE FROM records WHERE source=?", (source,))

    def _latest_revision_rows(self, source: str) -> list[dict]:
        """发布投影：每个巡检单号只保留最高修订，并保持来源清单顺序。"""
        sql = (
            "WITH ranked AS ("
            "SELECT order_id,revision,source,inspected_at,gaps_json,position,"
            "ROW_NUMBER() OVER ("
            "PARTITION BY order_id ORDER BY revision DESC) AS rn "
            "FROM records WHERE source=?) "
            "SELECT order_id,revision,source,inspected_at,gaps_json "
            "FROM ranked WHERE rn=1 ORDER BY position, order_id"
        )
        with self._lock:
            cur = self._conn.execute(sql, (source,))
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

    def publish_snapshot(self, source: str, fmt: str = "jsonl") -> dict:
        """发布某来源当前暂存区的完整快照（原子切换）。

        发布投影中每个巡检单号只保留最高修订（暂存区仍保留全量记录）。
        """
        rows = self._latest_revision_rows(source)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
        snapshot_id = f"{stamp}_{random.getrandbits(24):06x}"
        table_name = f"snap_{source}_{snapshot_id}"
        file_path = self._write_snapshot_file(source, snapshot_id, rows, fmt)
        with self._lock:
            old = self.latest_snapshot_info(source)
            self._conn.execute("BEGIN")
            try:
                # 与 _latest_revision_rows 等价的 SQL：仅保留每单号最高修订
                self._conn.execute(
                    f"CREATE TABLE {table_name} AS "
                    "WITH ranked AS ("
                    "SELECT order_id,revision,source,inspected_at,gaps_json,position,"
                    "ROW_NUMBER() OVER ("
                    "PARTITION BY order_id ORDER BY revision DESC) AS rn "
                    "FROM records WHERE source=?) "
                    "SELECT order_id,revision,source,inspected_at,gaps_json,position "
                    "FROM ranked WHERE rn=1 ORDER BY position, order_id",
                    (source,),
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
