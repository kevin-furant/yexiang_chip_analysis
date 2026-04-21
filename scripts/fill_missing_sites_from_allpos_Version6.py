#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import gzip
import sys
from collections import OrderedDict

def opengz(path, mode="rt"):
    # 兼容 .gz 与普通文本
    if path.endswith(".gz"):
        return gzip.open(path, mode)
    return open(path, mode)

def load_depth(depth_path):
    """
    depth file: chr \t pos \t depth
    """
    depth = {}
    with opengz(depth_path, "rt") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) < 3:
                continue
            chrom = parts[0]
            pos = int(parts[1])
            try:
                dp = int(float(parts[2]))
            except ValueError:
                continue
            depth[(chrom, pos)] = dp
    return depth

def load_allpos(allpos_path):
    """
    allpos file (succ.pos.GT.chr1):
    chr \t pos \t ref \t alt
    """
    sites = []
    with opengz(allpos_path, "rt") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) < 4:
                continue
            chrom = parts[0]
            pos = int(parts[1])
            ref = parts[2]
            alt = parts[3]
            sites.append((chrom, pos, ref, alt))
    return sites

def read_vcf(vcf_gz_path):
    """
    读取 vcf(.gz)：
    - 返回 header_lines(list[str])
    - 返回 records(dict[(chrom,pos)] = line_str_with_newline)
    - 返回 samples(list[str]) 以及最后一个 #CHROM 行里的列名列表
    """
    header_lines = []
    records = {}
    samples = []
    last_header_cols = None

    with opengz(vcf_gz_path, "rt") as f:
        for line in f:
            if line.startswith("#"):
                header_lines.append(line)
                if line.startswith("#CHROM"):
                    last_header_cols = line.rstrip("\n").split("\t")
                    if len(last_header_cols) > 9:
                        samples = last_header_cols[9:]
                continue

            parts = line.rstrip("\n").split("\t")
            if len(parts) < 8:
                continue
            chrom = parts[0]
            pos = int(parts[1])
            records[(chrom, pos)] = line  # 保留原始行（含换行）
    return header_lines, records, samples, last_header_cols

def build_missing_record(chrom, pos, ref, alt, dp, samples, use_refcall_threshold=4):
    """
    构造缺失位点的 VCF 行：
    - dp > threshold: GT=0/0, AD=dp,0 DP=dp
    - else: GT=./., AD=., DP=.
    """
    vid = "."
    qual = "."
    flt = "PASS"
    info = "."

    # FORMAT 固定为 GT:AD:DP（简单明确）
    fmt = "GT:AD:DP"

    if dp is not None and dp > use_refcall_threshold:
        gt = "0/0"
        ad = f"{dp},0"
        dp_field = str(dp)
    else:
        gt = "./."
        ad = ".,."
        dp_field = "."

    sample_field = f"{gt}:{ad}:{dp_field}"

    # 如果 VCF 里有多个样本，按同样规则给每个样本都填一样的值（通常你只有 1 个样本）
    if samples:
        sample_fields = [sample_field] * len(samples)
        cols = [chrom, str(pos), vid, ref, alt, qual, flt, info, fmt] + sample_fields
    else:
        # 没有样本列的 VCF（很少见），那就只输出到 FORMAT 列为止
        cols = [chrom, str(pos), vid, ref, alt, qual, flt, info]

    return "\t".join(cols) + "\n"

def main():
    ap = argparse.ArgumentParser(
        description="Traverse all sites; output existing VCF lines; fill missing by depth with GT/AD/DP."
    )
    ap.add_argument("--allpos", required=True, help="All positions file: chr pos ref alt (e.g. succ.pos.GT.chr1)")
    ap.add_argument("--depth", required=True, help="Depth file: chr pos depth (e.g. 3-G248.snp.depth.xls.2.txt)")
    ap.add_argument("--vcf", required=True, help="Input VCF(.gz) (e.g. 3-G248.vcf.gz)")
    ap.add_argument("--out", required=True, help="Output VCF path (can end with .gz or not)")
    ap.add_argument("--threshold", type=int, default=4, help="Depth threshold for adding 0/0 (default: 4; >threshold => 0/0)")
    args = ap.parse_args()

    depth_map = load_depth(args.depth)
    all_sites = load_allpos(args.allpos)

    header_lines, vcf_records, samples, _ = read_vcf(args.vcf)

    # 输出
    with open(args.out, "wt") as out:
        # header 原样输出
        for h in header_lines:
            out.write(h)

        # 遍历 allpos，按规则输出记录
        for chrom, pos, ref, alt in all_sites:
            key = (chrom, pos)
            if key in vcf_records:
                out.write(vcf_records[key])
            else:
                dp = depth_map.get(key)  # None if missing
                out.write(build_missing_record(chrom, pos, ref, alt, dp, samples, use_refcall_threshold=args.threshold))

if __name__ == "__main__":
    main()
