#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import re
import sqlite3
from pathlib import Path
from time import sleep

FastqPairMap = dict[str, tuple[Path, Path]]


class SampleSyncChecker:
    """
    1、快速遍历指定目录下的 fastq，找出成对样本数据
    2、校验数据的 md5 值
    3、与 step_tracker.db 中样本比较，过滤出待处理样本
    """

    FASTQ_SUFFIXES = (".clean.fq.gz", ".fastq.gz", ".fq.gz", ".fastq", ".fq")
    SAMPLE_READ_PATTERNS = (
        re.compile(r"^(?P<sample>.+)_R(?P<read>[12])(?:_\d+)?$"),
        re.compile(r"^(?P<sample>.+)_(?P<read>[12])$"),
    )

    def __init__(self, data_dir: str | Path, db_file: str | Path):
        self.data_dir = Path(data_dir)
        self.db_file = Path(db_file)
        self._md5_manifest_cache: dict[str, str] | None = None
        if not self.data_dir.exists() or not self.data_dir.is_dir():
            raise FileNotFoundError(f"数据目录不存在或不是目录: {self.data_dir}")

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

    @staticmethod
    def _is_fastq_file(file_path: Path) -> bool:
        name = file_path.name.lower()
        return any(name.endswith(suffix) for suffix in SampleSyncChecker.FASTQ_SUFFIXES)

    @staticmethod
    def _strip_fastq_suffix(file_name: str) -> str | None:
        lower = file_name.lower()
        for suffix in SampleSyncChecker.FASTQ_SUFFIXES:
            if lower.endswith(suffix):
                return file_name[: -len(suffix)]
        return None

    def _parse_sample_read(self, fq_file: Path) -> tuple[str, str] | None:
        base_name = self._strip_fastq_suffix(fq_file.name)
        if not base_name:
            return None
        for pattern in self.SAMPLE_READ_PATTERNS:
            match = pattern.match(base_name)
            if match:
                sample = match.group("sample")
                read = match.group("read")
                if read in {"1", "2"}:
                    return sample, read
        return None

    def _iter_fastq_files(self) -> list[Path]:
        fastq_files = [
            p for p in self.data_dir.rglob("*") if p.is_file() and self._is_fastq_file(p)
        ]
        return sorted(fastq_files, key=lambda p: str(p))

    def traverse_directory(self) -> FastqPairMap:
        """查找并返回样本名与 (R1, R2) 路径映射。"""
        sample_reads: dict[str, dict[str, Path]] = {}
        conflict_samples: set[str] = set()

        for fq_file in self._iter_fastq_files():
            parsed = self._parse_sample_read(fq_file)
            if not parsed:
                continue
            sample, read = parsed
            bucket = sample_reads.setdefault(sample, {})
            if read in bucket and bucket[read] != fq_file:
                # 同一样本同一 read 出现多个文件，跳过该样本避免错误配对。
                conflict_samples.add(sample)
                continue
            bucket[read] = fq_file

        pairs: FastqPairMap = {}
        for sample, reads in sample_reads.items():
            if sample in conflict_samples:
                continue
            if "1" in reads and "2" in reads:
                pairs[sample] = (reads["1"], reads["2"])
        return pairs

    @staticmethod
    def _calc_md5(file_path: Path) -> str:
        hasher = hashlib.md5()
        with file_path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    @staticmethod
    def _read_expected_md5(md5_file: Path) -> str | None:
        if not md5_file.exists() or not md5_file.is_file():
            return None
        content = md5_file.read_text(encoding="utf-8", errors="ignore")
        match = re.search(r"\b[a-fA-F0-9]{32}\b", content)
        return match.group(0).lower() if match else None

    def _load_md5_manifest(self) -> dict[str, str]:
        if self._md5_manifest_cache is not None:
            return self._md5_manifest_cache

        md5_txt = self.data_dir / "md5.txt"
        mapping: dict[str, str] = {}
        if md5_txt.exists() and md5_txt.is_file():
            for raw_line in md5_txt.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                cols = line.split(maxsplit=1)
                if len(cols) != 2:
                    continue
                md5_value = cols[0].strip().lower()
                if not re.fullmatch(r"[a-f0-9]{32}", md5_value):
                    continue
                file_name = cols[1].strip().lstrip("*")
                if not file_name:
                    continue
                mapping[file_name] = md5_value
                mapping[Path(file_name).name] = md5_value

        self._md5_manifest_cache = mapping
        return mapping

    def _get_expected_md5(self, fq_file: Path) -> str | None:
        mapping = self._load_md5_manifest()
        rel_key: str | None = None
        try:
            rel_key = str(fq_file.relative_to(self.data_dir))
        except ValueError:
            rel_key = None
        return mapping.get(fq_file.name) or (mapping.get(rel_key) if rel_key else None)

    def check_md5(self, fq_file: Path, expected_md5: str | None) -> bool:
        """校验 fastq 文件与期望 md5 是否一致。"""
        try:
            if not expected_md5:
                return False
            actual_md5 = self._calc_md5(fq_file).lower()
            return actual_md5 == expected_md5
        except OSError:
            return False

    @staticmethod
    def clear_running_and_fail_samples(
        db_file: str | Path,
        max_retries: int = 5,
        retry_wait_seconds: float = 0.2,
    ) -> int:
        db_path = Path(db_file)
        if not db_path.exists():
            return 0

        sql = "DELETE FROM task_status WHERE bwa2gvcf IN ('running', 'fail')"
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
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
                conn.execute("PRAGMA busy_timeout=5000;")
                conn.execute("BEGIN IMMEDIATE;")
                in_transaction = True
                cur = conn.execute(sql)
                conn.execute("COMMIT;")
                return cur.rowcount
            except sqlite3.OperationalError as exc:
                last_error = exc
                if conn is not None and in_transaction:
                    conn.execute("ROLLBACK;")
                msg = str(exc).lower()
                if "locked" not in msg and "busy" not in msg:
                    raise
                if attempt >= max_retries:
                    break
                sleep(retry_wait_seconds * attempt)
            finally:
                if conn is not None:
                    conn.close()

        raise RuntimeError(f"清理 running/fail 样本失败，数据库持续繁忙: {last_error}")

    def compare_with_db(
        self,
        sample_name: str,
        max_retries: int = 5,
        retry_wait_seconds: float = 0.2,
    ) -> bool:
        """查询样本是否已在数据库中，存在返回 True，否则 False。"""
        if not self.db_file.exists():
            return False

        sql = "SELECT 1 FROM task_status WHERE sample = ? LIMIT 1"
        last_error: Exception | None = None
        for attempt in range(1, max_retries + 1):
            conn: sqlite3.Connection | None = None
            try:
                conn = self._connect()
                cur = conn.execute(sql, (sample_name,))
                return cur.fetchone() is not None
            except sqlite3.OperationalError as exc:
                last_error = exc
                if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                    raise
                if attempt >= max_retries:
                    break
                sleep(retry_wait_seconds * attempt)
            finally:
                if conn is not None:
                    conn.close()

        raise RuntimeError(f"查询数据库失败，数据库持续繁忙: {last_error}")

    def _pair_md5_ok(self, pair: tuple[Path, Path]) -> bool:
        for fq_file in pair:
            expected_md5 = self._get_expected_md5(fq_file)
            if not self.check_md5(fq_file, expected_md5):
                return False
        return True

    def _fetch_existing_samples(self) -> set[str]:
        if not self.db_file.exists():
            return set()
        conn = self._connect()
        try:
            rows = conn.execute("SELECT sample FROM task_status").fetchall()
            return {row[0] for row in rows}
        finally:
            conn.close()

    def collect_pending_samples(self, require_md5: bool = True) -> FastqPairMap:
        """
        返回待处理样本映射:
        - 只保留配对完整的样本
        - `require_md5=True` 时只保留 md5 校验通过的样本
        - 过滤掉已存在于数据库的样本
        """
        sample_pairs = self.traverse_directory()
        existing_samples = self._fetch_existing_samples()

        pending: FastqPairMap = {}
        for sample, pair in sample_pairs.items():
            if sample in existing_samples:
                continue
            if require_md5 and not self._pair_md5_ok(pair):
                continue
            pending[sample] = pair
        return pending