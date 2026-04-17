#!/usr/bin/env python3
import sys
import os
import gzip

def open_vcf(filename):
    return gzip.open(filename, 'rt') if filename.endswith('.gz') else open(filename, 'r')

def load_whitelist(snp_file):
    whitelist = {}
    with open(snp_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split()
            if len(parts) >= 4:
                whitelist[(parts[0], parts[1])] = (parts[2], parts[3])
            else:
                print(f"警告: 忽略 {line}", file=sys.stderr)
    print(f"读取到 {len(whitelist)} 个目标位点", file=sys.stderr)
    return whitelist

def main():
    if len(sys.argv) != 4:
        print("用法: python fix.py input.vcf[.gz] snps.txt output.vcf")
        sys.exit(1)

    in_vcf, snp_file, out_vcf = sys.argv[1:4]
    whitelist = load_whitelist(snp_file)

    fin = open_vcf(in_vcf)
    fout = open(out_vcf, 'w')
    fixed = 0

    for line in fin:
        if line.startswith('#'):
            fout.write(line)
            continue

        cols = line.strip().split('\t')
        if len(cols) < 8:
            fout.write(line)
            continue

        chrom, pos, ref, alt = cols[0], cols[1], cols[3], cols[4]
        key = (chrom, pos)

        if key in whitelist and (',' in alt or '*' in alt):
            # 替换 REF/ALT
            new_ref, new_alt = whitelist[key]
            cols[3] = new_ref
            cols[4] = new_alt
            # 所有样本的 GT 设为 './.'
            for i in range(9, len(cols)):
                fields = cols[i].split(':')
                if fields:
                    fields[0] = './.'
                    cols[i] = ':'.join(fields)
            fout.write('\t'.join(cols) + '\n')
            fixed += 1
            print(f"修正: {chrom}:{pos}  {alt} -> {new_alt}", file=sys.stderr)
        else:
            fout.write(line)

    fin.close()
    fout.close()
    print(f"修正位点数: {fixed}", file=sys.stderr)

if __name__ == "__main__":
    main()
