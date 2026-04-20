#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shlex
import sqlite3
import subprocess
import sys
from pathlib import Path
from time import sleep
import json

from .init_db import init_step_tracker_db
from .analysis_pipe import AnalysisPipePrinter
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


def _fetch_samples_by_status(
    db_file: Path,
    status: str,
    scoped_samples: set[str] | None = None,
) -> list[str]:
    conn = _connect(db_file)
    try:
        if scoped_samples:
            placeholders = ",".join("?" for _ in scoped_samples)
            sql = f"""
                SELECT sample
                FROM task_status
                WHERE bwa2gvcf = ? AND sample IN ({placeholders})
                ORDER BY created_at ASC, sample ASC
            """
            rows = conn.execute(sql, (status, *sorted(scoped_samples))).fetchall()
        else:
            sql = """
                SELECT sample
                FROM task_status
                WHERE bwa2gvcf = ?
                ORDER BY created_at ASC, sample ASC
            """
            rows = conn.execute(sql, (status,)).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="task_monitor",
        description="长期值守任务进程：同步样本、投递脚本并监控状态",
    )
    parser.add_argument(
        "--project_path",
        type=Path,
        default=Path.cwd(),
        help="项目目录（默认当前目录，数据库为 project-path/step_tracker.db）",
    )
    parser.add_argument(
        "--config-file",
        type=Path,
        required=True,
        help="流程配置文件（JSON）",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=10,
        help="轮询间隔（分钟）",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="仅执行一轮检查，不进入持续值守循环",
    )

    subparsers = parser.add_subparsers(dest="command")
    update_parser = subparsers.add_parser("update", help="更新数据库")  
    update_parser.add_argument(
        "--sample",
        type=str,
        required=True,
        help="需要修改的样本名"
    )
    update_parser.add_argument(
        "--status",
        type=str,
        required=True,
        help="需要修改的样本状态"
    )

    return parser


def _parse_samples(values: list[str]) -> list[str]:
    samples: list[str] = []
    seen: set[str] = set()
    for value in values:
        for item in value.split(","):
            sample = item.strip()
            if not sample or sample in seen:
                continue
            seen.add(sample)
            samples.append(sample)
    return samples


def _load_mapfile_pairs(map_file: Path) -> dict[str, tuple[str, str]]:
    if not map_file.exists():
        raise FileNotFoundError(f"mapfile 不存在: {map_file}")

    pairs: dict[str, tuple[str, str]] = {}
    for line in map_file.read_text(encoding="utf-8", errors="ignore").splitlines():
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        cols = text.split()
        if len(cols) < 2:
            continue
        sample = cols[0]
        read1 = cols[1]
        read2 = cols[2] if len(cols) >= 3 else ""
        pairs[sample] = (read1, read2)
    return pairs


def _count_status(db_file: Path, status: str, scoped_samples: set[str]) -> int:
    conn = _connect(db_file)
    try:
        if not scoped_samples:
            return 0
        placeholders = ",".join("?" for _ in scoped_samples)
        sql = f"""
            SELECT COUNT(*)
            FROM task_status
            WHERE bwa2gvcf = ? AND sample IN ({placeholders})
        """
        row = conn.execute(sql, (status, *sorted(scoped_samples))).fetchone()
        return int(row[0] if row else 0)
    finally:
        conn.close()


def _run_submit(script_file: Path, submit_cmd: str, cwd: Path) -> subprocess.Popen[str]:
    cmd_text = submit_cmd.format(script=str(script_file)) if "{script}" in submit_cmd else f"{submit_cmd} {script_file}"
    cmd = shlex.split(cmd_text)
    return subprocess.Popen(cmd, cwd=cwd)


# def _chunks(items: list[str], size: int) -> list[list[str]]:
#     if size <= 0:
#         return [items]
#     return [items[i : i + size] for i in range(0, len(items), size)]


def _write_work_shell(work_shell: Path, batch_shell: Path, report_shell: Path) -> None:
    work_shell.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                f"bash {shlex.quote(str(batch_shell))}",
                f"bash {shlex.quote(str(report_shell))}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def main(argv: list[str] | None = None) -> int:
    """
    主函数，是一个服务进程，将会一直挂在后台，直到分析项目运行结束，主要完成以下几件事
    1、判断库文件是否在，如果不存在就初始化 table
    2、设置参数N，每N分钟，比较 fastq 目录下样本与数据库样本是否一致，将新增样本存入待执行列表，并更新数据库中样本状态为running
    3、打印single_step分析步骤， 这个步骤可能有多批数据， 按_1, _2... 区分批号存放于批次目录下的00.bin目录内， 并调用subprocess 执行投递任务
    4、当mapfile中的样本个数等于数据库step_tracker.db中已经完成的样本个数时， 打印batch_step分析步骤， 存放于批次目录下的00.bin目录内
    5、当mapfile中的样本个数等于数据库step_tracker.db中已经完成的样本个数时， 打印report_step分析步骤， 存放于批次目录下的00.bin目录内
    6、将第4和第5步打印的脚本用shell 串起来，在00.bin目录下生成一个work.sh, 调用subprocess执行这个脚本的投递
    7、完成以上任务后就可以退出值守进程
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    project_path = args.project_path.resolve()
    db_file = _db_file_from_project(project_path)
    config_file = args.config_file.resolve()

    init_step_tracker_db(project_path)
    config_data = json.loads(config_file.read_text(encoding="utf-8"))
    map_pairs = _load_mapfile_pairs(Path(config_data["map_file"]))

    active_samples_map = SampleSyncChecker(db_file=db_file, data_dir=config_data["fq_xj_dir"]).collect_pending_samples()
    active_samples = list(active_samples_map.keys())
    sample_scope = set(active_samples)
    sample_info = {sample: [map_pairs[sample][0], map_pairs[sample][1]] for sample in active_samples}
    inserted = _insert_new_samples(db_file, active_samples)
    print(f"[OK] 已载入样本 {len(active_samples)} 个，新增入库 {inserted} 个。")

    printer = AnalysisPipePrinter(sample_list=sample_scope, config_file=config_file)
    out_dir = Path(config_data["out_dir"]) / config_data["batch_name"] / "00.bin"
    out_dir.mkdir(parents=True, exist_ok=True)

    submitted_samples: set[str] = set()
    final_stage_submitted = False
    round_index = 0

    while True:
        to_submit = sample_scope
        total_count = len(to_submit)
        if to_submit:
            single_script = out_dir / f"single_step_{round_index}.sh"
            AnalysisPipePrinter(sample_list=to_submit, config_file=config_file).print_single_step(single_script)
            proc = _run_submit(single_script, args.submit_cmd, cwd=project_path)
            print(
                f"[OK] 已投递 single_step 批次{round_index}: {len(to_submit)} 样本, "
                f"pid={proc.pid}, script={single_script}"
            )
            submitted_samples.update(to_submit)
        running_samples = _fetch_samples_by_status(db_file, "running", sample_scope) or submitted_samples
        done_count = _count_status(db_file, "done", sample_scope) or 0
        fail_count = _count_status(db_file, "fail", sample_scope) or 0
        print(
            f"[INFO] 轮次{round_index}: total={total_count}, "
            f"running={len(running_samples)}, done={done_count}, fail={fail_count}"
        )
        round_index += 1
        to_submit = set()
        
        if not final_stage_submitted and done_count >= total_count:
            vcf_list = [
                str(printer.bam_dir / f"{sample}.fill.vcf.gz")
                for sample in active_samples
            ]
            batch_script = out_dir / "batch_step.sh"
            report_script = out_dir / "report_step.sh"
            work_script = out_dir / "work.sh"
            printer.print_batch_step(batch_script, vcf_list=vcf_list)
            printer.print_report_step(report_script)
            _write_work_shell(work_script, batch_script, report_script)
            proc = _run_submit(work_script, args.submit_cmd, cwd=project_path)
            final_stage_submitted = True
            print(f"[OK] 已投递汇总流程: pid={proc.pid}, script={work_script}")
            print("[OK] 全部样本已完成 single_step，值守进程退出。")
            return 0

        if args.once:
            print("[INFO] --once 模式，执行一轮后退出。")
            return 0

        sleep(max(args.interval, 1) * 60)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\n[WARN] 用户中断执行。", file=sys.stderr)
        raise SystemExit(130)