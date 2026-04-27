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
from .pipe_init import generate_config
from .email_notify import load_email_config_from_env, send_notify_email


email_config = load_email_config_from_env()
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
    ) -> list[str]:
    conn = _connect(db_file)
    try:
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
        dest="config_file",
        type=Path,
        default=Path("config.json"),
        help="流程配置文件(JSON)",
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

    init_parser = subparsers.add_parser("init", help="初始化流程配置")
    init_parser.add_argument(
        "--project_name",
        type=str,
        required=True,
        help="项目中文名称"
    )
    init_parser.add_argument(
        "--contract",
        type=str,
        required=True,
        help="合同编号"
    )
    init_parser.add_argument(
        "--customer",
        type=Path,
        required=True,
        help="客户名称"
    )
    init_parser.add_argument(
        "--chip-name",
        dest="chip_name",
        type=str,
        required=True,
        help="芯片名称"
    )
    init_parser.add_argument(
        "--upload_path",
        type=Path,
        required=True,
        help="fastq文件上传的路径"
    )
    init_parser.add_argument(
        "--map-file",
        dest="map_file",
        type=Path,
        required=True,
        help="mapfile 批次所有样本fastq路径配置表"
    )
    init_parser.add_argument(
        "--batch_name",
        type=str,
        default=None,
        help="bed文件"
    )
    notify_parser = subparsers.add_parser("notify", help="发送邮件通知")
    notify_parser.add_argument(
        "--send",
        action="store_true",
        help="是否发送任务结束邮件通知"
    )
    return parser

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


def _count_status(db_file: Path, status: str) -> int:
    conn = _connect(db_file)
    try:
        sql = """
            SELECT COUNT(*)
            FROM task_status
            WHERE bwa2gvcf = ?
        """
        row = conn.execute(sql, (status,)).fetchone()
        return int(row[0] if row else 0)
    finally:
        conn.close()


def _run_submit(script_file: Path, cwd: Path) -> subprocess.Popen[str]:
    cmd_text = f"nohup bash {shlex.quote(str(script_file))} >/dev/null 2>&1 &"
    return subprocess.Popen(cmd_text, cwd=cwd, shell=True, text=True)


def _cleanup_non_terminal_samples(db_file: Path) -> None:
    deleted = SampleSyncChecker.clear_running_and_fail_samples(db_file)
    print(f"[INFO] 退出前清理 running/fail 样本记录: {deleted} 条")

# def _chunks(items: list[str], size: int) -> list[list[str]]:
#     if size <= 0:
#         return [items]
#     return [items[i : i + size] for i in range(0, len(items), size)]

def _write_work_shell(
    work_shell: Path,
    shell: Path,
    lines: int,
    mem: str,
    cpu: int,
    comment: bool = False,
    append: bool = True,
) -> None:
    """
    向 work.sh 写入单条 slurm_Duty 投递命令。
    shell 仅支持单个脚本文件，便于按脚本设置不同资源。
    """
    cmd = (
        "/usr/bin/perl /work/share/ac8t81mwbn/pipline/gatk/bin/slurm_Duty.pl "
        f"--interval 30 --maxjob 10 --convert no --lines {lines} "
        f"--partition wzhcnormal --reslurm --mem {mem} --cpu {cpu} {shell}"
    )
    if comment:
        cmd = f"#{cmd}"

    existed = work_shell.exists()
    with work_shell.open("a" if append else "w", encoding="utf-8") as fh:
        if append and existed and work_shell.stat().st_size > 0:
            fh.write("\n")
        fh.write(f"{cmd}\n")


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
    total_samples_count = len(map_pairs)
    out_dir = Path(config_data["out_dir"]) / config_data["batch_name"] / "00.bin"
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir = Path(config_data["out_dir"]) / config_data["batch_name"] / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    submitted_samples: set[str] = set()
    final_stage_submitted = False
    round_index = 0
    done_count = 0
    fail_count = 0

    if args.command == "update":
        sample = args.sample
        status = args.status
        if status not in StatusUpdater.VALID_STATUS:
            print(f"[ERROR] 无效的状态: {status}")
            return 1
        status_updater = StatusUpdater(db_file=db_file, status_tag=status)
        updated_count = status_updater.update_sample_status(sample)
        if updated_count == 1:
            print(f"[OK] 更新了样本 {sample} 的状态为 {status}")
            return 0
        else:
            print(f"[ERROR] 更新了样本 {sample} 的状态为 {status} 失败，请检查样本名是否正确")
            return 1

    if args.command == "init":
        try:
            generate_config(
                out_json_path = args.config_file,
                customer_name = args.customer,
                chip_id = args.chip_name,
                fq_xj_dir = args.upload_path,
                map_file = args.map_file,
                out_dir = args.project_path,
                project_name = args.project_name,
                contract_id = args.contract,
                batch_name = args.batch_name if args.batch_name != None else args.contract
            )
        except Exception as e:
            print(f"[INIT] 配置文件生成失败: {e}")
        print(f"[INIT] 生成配置文件成功")
        return 0

    if args.command == "notify":
        if args.send:
            send_notify_email(
                subject = f"{config_data['project_name']} 任务完成",
                body = f"{config_data['project_name']} 任务已经全部完成, 请检查结果生成。",
                recipients = ['jinpeng.bi@glbizzia.com', 'zhexin.liu@glbizzia.com'],
                smtp_host = str(email_config["MAIL_HOST"]),
                smtp_port = int(email_config["MAIL_PORT"]),
                smtp_user = str(email_config["MAIL_USER"]),
                smtp_password = str(email_config["MAIL_PASSWORD"]),
                sender = str(email_config["MAIL_SENDER"])
            )
            print("[INFO] 流程完成邮件已经发送")

    try:
        while True:
            active_samples_map = SampleSyncChecker(db_file=db_file, data_dir=config_data["fq_xj_dir"]).collect_pending_samples()
            active_samples = list(active_samples_map.keys())
            sample_scope = set(active_samples)
            if not active_samples and (done_count + fail_count) < total_samples_count:
                sleep(max(args.interval, 1) * 60)
                running_samples = _fetch_samples_by_status(db_file, "running")
                done_count = _count_status(db_file, "done")
                fail_count = _count_status(db_file, "fail")
                print(
                    f"running={len(running_samples)}, done={done_count}, fail={fail_count}"
                )
                continue
            inserted = _insert_new_samples(db_file, active_samples)
            print(f"[OK] 已载入样本 {len(active_samples)} 个，新增入库 {inserted} 个。")
            to_submit = sample_scope
            batch_sample_count = len(to_submit)
            notify = False
            if to_submit:
                if not notify:
                    send_notify_email(
                        subject = f"{config_data['project_name']} 批量任务启动",
                        body = f"{config_data['project_name']} 批量任务启动，已启动, 请等待流程运行完成。",
                        recipients = ['jinpeng.bi@glbizzia.com', 'zhexin.liu@glbizzia.com'],
                        smtp_host = str(email_config["MAIL_HOST"]),
                        smtp_port = int(email_config["MAIL_PORT"]),
                        smtp_user = str(email_config["MAIL_USER"]),
                        smtp_password = str(email_config["MAIL_PASSWORD"]),
                        sender = str(email_config["MAIL_SENDER"]),
                    )
                    print("[INFO] 流程启动邮件已经发送")
                    notify = True
                single_script = out_dir / f"single_step_{round_index}.sh"
                single_step_run_shell = out_dir / f"single_step_{round_index}_run.sh"
                AnalysisPipePrinter(sample_list=to_submit, config_file=config_file).print_single_step(single_script)
                _write_work_shell(single_step_run_shell, single_script, 14, "82G", 20)
                proc = _run_submit(single_step_run_shell, cwd=out_dir)
                print(
                    f"[OK] 已投递 single_step 批次{round_index}: {len(to_submit)} 样本, "
                    f"pid={proc.pid}, script={single_script}"
                )
                submitted_samples.update(to_submit)
                print(
                    f"[INFO] 轮次{round_index}: total={batch_sample_count}"
                )
                round_index += 1
                to_submit = set()
            running_samples = _fetch_samples_by_status(db_file, "running")
            done_count = _count_status(db_file, "done")
            fail_count = _count_status(db_file, "fail")
            if done_count == total_samples_count:
                print(f"[OK] 所有样本已处理完成, 将进行合并步骤及报告生成！")
            if done_count + fail_count == total_samples_count and fail_count > 0:
                print(f"[Fail] 所有样本已处理完成，但是有样本任务失败，退出任务")
                return 1
            print(
                f"running={len(running_samples)}, done={done_count}, fail={fail_count}"
            )

            if not final_stage_submitted and done_count == total_samples_count:
                printer = AnalysisPipePrinter(sample_list=set(map_pairs.keys()), config_file=config_file)
                vcf_list = [
                    str(printer.bam_dir / f"{sample}.fill.vcf.gz")
                    for sample in map_pairs.keys()
                ]
                batch_script = out_dir / "batch_step.sh"
                report_script = out_dir / "report_step.sh"
                work_script = out_dir / "work.sh"
                printer.print_batch_step(batch_script, vcf_list=vcf_list)
                printer.print_report_step(report_script, config_data=config_data)
                _write_work_shell(
                    work_shell=work_script,
                    shell=batch_script,
                    lines=9,
                    mem="30G",
                    cpu=4,
                    comment=False,
                    append=False,
                )
                _write_work_shell(
                    work_shell=work_script,
                    shell=report_script,
                    lines=20,
                    mem="82G",
                    cpu=20,
                    comment=False,
                    append=True,
                )
                proc = _run_submit(work_script, cwd=out_dir)
                final_stage_submitted = True
                print(f"[OK] 已投递汇总流程: pid={proc.pid}, script={work_script}")
                print("[OK] 全部样本已完成 single_step，值守进程退出。")
                return 0

            if args.once:
                print("[INFO] --once 模式，执行一轮后退出。")
                return 0

            sleep(max(args.interval, 1) * 60)
    finally:
        _cleanup_non_terminal_samples(db_file)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\n[WARN] 用户中断执行。", file=sys.stderr)
        raise SystemExit(130)