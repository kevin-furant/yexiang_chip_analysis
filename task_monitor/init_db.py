#!/usr/bin/env python3
import sqlite3
from contextlib import suppress
from pathlib import Path
from time import sleep


def _is_locked_error(exc: sqlite3.OperationalError) -> bool:
    msg = str(exc).lower()
    return "locked" in msg or "busy" in msg


def _configure_connection(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA foreign_keys=ON;")


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS task_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sample TEXT NOT NULL UNIQUE,
            bwa2gvcf TEXT NOT NULL DEFAULT 'running',
            created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
        )
        """
    )


def init_step_tracker_db(
    project_path: Path,
    max_retries: int = 5,
    retry_wait_seconds: float = 0.2,
) -> None:
    """初始化步骤跟踪数据库（可重复执行，遇到锁自动重试）。"""
    project_path = Path(project_path)
    project_path.mkdir(parents=True, exist_ok=True)
    db_path = project_path / "step_tracker.db"
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        conn: sqlite3.Connection | None = None
        in_transaction = False
        try:
            conn = sqlite3.connect(
                db_path,
                timeout=5.0,
                isolation_level=None,
                check_same_thread=False,
            )
            _configure_connection(conn)

            conn.execute("BEGIN IMMEDIATE;")
            in_transaction = True
            _ensure_schema(conn)
            conn.execute("COMMIT;")
            return
        except sqlite3.OperationalError as exc:
            if conn is not None and in_transaction:
                with suppress(sqlite3.Error):
                    conn.execute("ROLLBACK;")
            last_error = exc
            if attempt >= max_retries or not _is_locked_error(exc):
                raise RuntimeError(f"初始化数据库失败: {db_path} ({exc})") from exc
            sleep(retry_wait_seconds * attempt)
        except sqlite3.DatabaseError as exc:
            if conn is not None and in_transaction:
                with suppress(sqlite3.Error):
                    conn.execute("ROLLBACK;")
            raise RuntimeError(f"数据库不可用或已损坏: {db_path}") from exc
        finally:
            if conn is not None:
                conn.close()

    raise RuntimeError(f"初始化数据库失败，数据库持续繁忙: {last_error}")
    