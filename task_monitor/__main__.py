#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from time import sleep

from .init_db import init_step_tracker_db
from .sample_sync_check import SampleSyncChecker
from .status_update import StatusUpdater


def _db_file_from_project(project_path: Path) -> Path:
    return project_path / "step_tracker.db"


def _connect(db_file: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(
        db_file,
        timeout=5.0,
        isolation_level=None,
        check_same_thread=False,
    )
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def _insert_new_samples(
    db_file: Path,
    sample_names: list[str],
    max_retries: int = 5,
    retry_wait_seconds: float = 0.2,
) -> int:
    if not sample_names:
        return 0

    sql = "INSERT OR IGNORE INTO task_status(sample, bwa2gvcf) VALUES(?, 'running')"
    for attempt in range(1, max_retries + 1):
        conn: sqlite3.Connection | None = None
        in_transaction = False
        try:
            conn = _connect(db_file)
            conn.execute("BEGIN IMMEDIATE;")
            in_transaction = True
            cur = conn.executemany(sql, ((sample,) for sample in sample_names))
            conn.execute("COMMIT;")
            return cur.rowcount
        except sqlite3.OperationalError as exc:
            if conn is not None and in_transaction:
                conn.execute("ROLLBACK;")
            msg = str(exc).lower()
            if attempt >= max_retries or ("locked" not in msg and "busy" not in msg):
                raise RuntimeError(f"写入样本失败: {exc}") from exc
            sleep(retry_wait_seconds * attempt)
        finally:
            if conn is not None:
                conn.close()

    return 0


def _list_samples(db_file: Path, status: str) -> list[tuple[str, str, str]]:
    conn = _connect(db_file)
    try:
        if status == "all":
            sql = """
                SELECT sample, bwa2gvcf, created_at
                FROM task_status
                ORDER BY created_at ASC, sample ASC
            """
            rows = conn.execute(sql).fetchall()
        else:
            sql = """
                SELECT sample, bwa2gvcf, created_at
                FROM task_status
                WHERE bwa2gvcf = ?
                ORDER BY created_at ASC, sample ASC
            """
            rows = conn.execute(sql, (status,)).fetchall()
        return [(r[0], r[1], r[2]) for r in rows]
    finally:
        conn.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="task_monitor",
        description="任务样本状态跟踪工具",
    )
    parser.add_argument(
        "--project-path",
        type=Path,
        default=Path.cwd(),
        help="项目目录（默认当前目录，数据库为 project-path/step_tracker.db）",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init", help="初始化数据库表结构")

    sync_parser = subparsers.add_parser("sync", help="同步 fastq 新样本到待执行列表")
    sync_parser.add_argument("--data-dir", type=Path, required=True, help="fastq 根目录")
    sync_parser.add_argument(
        "--no-md5",
        action="store_true",
        help="跳过 md5 校验（默认会校验）",
    )

    update_parser = subparsers.add_parser("update", help="更新样本状态")
    update_parser.add_argument("--sample", required=True, help="样本名")
    update_parser.add_argument(
        "--status",
        required=True,
        choices=sorted(StatusUpdater.VALID_STATUS),
        help="状态值",
    )

    list_parser = subparsers.add_parser("list", help="列出待执行或全部样本")
    list_parser.add_argument(
        "--status",
        default="running",
        choices=["running", "done", "fail", "all"],
        help="筛选状态（默认 running）",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    """
    主函数
    判断库文件是否在，如果不存在就初始化 table
    比较 fastq 目录下样本与数据库样本是否一致，将新增样本存入待执行列表
    子命令:
    init: 初始化数据库
    sync: 同步样本
    update: 更新样本状态
    list: 列出待执行样本
    print: 从ananlysis_pipe.py中调取方法打印脚本，并调用subprocess 执行投递任务
    help: 显示帮助信息
    这个主脚本会一直挂在后台执行，是一个监控程序，比如2分钟同步样本一次， 
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    project_path = args.project_path.resolve()
    db_file = _db_file_from_project(project_path)

    if args.command == "init":
        init_step_tracker_db(project_path)
        print(f"[OK] 数据库已初始化: {db_file}")
        return 0

    # 其余子命令都需要数据库存在并已完成表结构校验。
    init_step_tracker_db(project_path)

    if args.command == "sync":
        checker = SampleSyncChecker(data_dir=args.data_dir, db_file=db_file)
        pending = checker.collect_pending_samples(require_md5=not args.no_md5)
        inserted = _insert_new_samples(db_file, sorted(pending.keys()))
        print(f"[OK] 检测到新增样本 {len(pending)} 个，入库 {inserted} 个。")
        return 0

    if args.command == "update":
        updater = StatusUpdater(db_file=db_file, status_tag=args.status)
        affected = updater.update_sample_status(sample=args.sample)
        if affected == 0:
            print(f"[WARN] 样本不存在: {args.sample}")
            return 1
        print(f"[OK] 样本状态已更新: {args.sample} -> {args.status}")
        return 0

    if args.command == "list":
        rows = _list_samples(db_file, status=args.status)
        if not rows:
            print("[INFO] 没有匹配样本。")
            return 0

        for sample, status, created_at in rows:
            print(f"{sample}\t{status}\t{created_at}")
        print(f"[OK] 共 {len(rows)} 条记录。")
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\n[WARN] 用户中断执行。", file=sys.stderr)
        raise SystemExit(130)