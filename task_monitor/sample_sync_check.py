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

    FASTQ_SUFFIXES = (".fastq.gz", ".fq.gz", ".fastq", ".fq")
    SAMPLE_READ_PATTERNS = (
        re.compile(r"^(?P<sample>.+)_R(?P<read>[12])(?:_\d+)?$"),
        re.compile(r"^(?P<sample>.+)_(?P<read>[12])$"),
    )

    def __init__(self, data_dir: str | Path, db_file: str | Path):
        self.data_dir = Path(data_dir)
        self.db_file = Path(db_file)
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

    def _find_md5_file(self, fq_file: Path) -> Path | None:
        candidates = [
            Path(str(fq_file) + ".md5"),
            fq_file.with_suffix(".md5"),
        ]
        seen: set[Path] = set()
        for candidate in candidates:
            if candidate in seen:
                continue
            seen.add(candidate)
            if candidate.exists() and candidate.is_file():
                return candidate
        return None

    def check_md5(self, fq_file: Path, md5_file: Path) -> bool:
        """校验 fastq 文件与 md5 文件是否一致。"""
        try:
            expected_md5 = self._read_expected_md5(md5_file)
            if not expected_md5:
                return False
            actual_md5 = self._calc_md5(fq_file).lower()
            return actual_md5 == expected_md5
        except OSError:
            return False

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
            md5_file = self._find_md5_file(fq_file)
            if md5_file is None or not self.check_md5(fq_file, md5_file):
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