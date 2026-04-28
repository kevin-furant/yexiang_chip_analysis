#!/usr/bin/env python3
import sqlite3
from pathlib import Path
from time import sleep


class StatusUpdater:
    """更新 task_status 表中指定阶段状态。"""

    VALID_STATUS_BY_STAGE = {
        "bwa2gvcf": {"running", "done", "fail"},
        "merge": {"pending", "running", "done", "fail"},
        "report": {"pending", "running", "done", "fail"},
    }

    def __init__(self, db_file: str | Path, status_tag: str, stage: str = "bwa2gvcf"):
        self.db_file = Path(db_file)
        if stage not in self.VALID_STATUS_BY_STAGE:
            raise ValueError(
                f"stage 必须是 {set(self.VALID_STATUS_BY_STAGE.keys())} 之一，实际传入: {stage}"
            )
        if status_tag not in self.VALID_STATUS_BY_STAGE[stage]:
            raise ValueError(
                f"status_tag 对于 {stage} 必须是 {self.VALID_STATUS_BY_STAGE[stage]} 之一，实际传入: {status_tag}"
            )
        self.status_tag = status_tag
        self.stage = stage

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_file,
            timeout=5.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    def update_sample_status(
        self,
        sample: str,
        max_retries: int = 5,
        retry_wait_seconds: float = 0.2,
    ) -> int:
        """
        更新指定 sample 的步骤状态。

        返回值:
            1: 更新成功
            0: sample 不存在
        """
        column = "merge_status" if self.stage == "merge" else self.stage
        sql = f"UPDATE task_status SET {column} = ? WHERE sample = ?"
        last_error: Exception | None = None

        for _ in range(max_retries):
            conn = self._connect()
            in_transaction = False
            try:
                conn.execute("BEGIN IMMEDIATE;")
                in_transaction = True
                cur = conn.execute(sql, (self.status_tag, sample))
                conn.execute("COMMIT;")
                return cur.rowcount
            except sqlite3.OperationalError as exc:
                if in_transaction:
                    conn.execute("ROLLBACK;")
                last_error = exc
                if "locked" not in str(exc).lower():
                    raise
                sleep(retry_wait_seconds)
            except Exception:
                if in_transaction:
                    conn.execute("ROLLBACK;")
                raise
            finally:
                conn.close()

        raise RuntimeError(f"更新状态失败，数据库持续繁忙: {last_error}")

    def update_all_sample_status(
        self,
        max_retries: int = 5,
        retry_wait_seconds: float = 0.2,
    ) -> int:
        """批量更新全部样本的指定阶段状态。"""
        column = "merge_status" if self.stage == "merge" else self.stage
        sql = f"UPDATE task_status SET {column} = ?"
        last_error: Exception | None = None

        for _ in range(max_retries):
            conn = self._connect()
            in_transaction = False
            try:
                conn.execute("BEGIN IMMEDIATE;")
                in_transaction = True
                cur = conn.execute(sql, (self.status_tag,))
                conn.execute("COMMIT;")
                return cur.rowcount
            except sqlite3.OperationalError as exc:
                if in_transaction:
                    conn.execute("ROLLBACK;")
                last_error = exc
                if "locked" not in str(exc).lower():
                    raise
                sleep(retry_wait_seconds)
            except Exception:
                if in_transaction:
                    conn.execute("ROLLBACK;")
                raise
            finally:
                conn.close()

        raise RuntimeError(f"批量更新状态失败，数据库持续繁忙: {last_error}")
