import pysam
import sys
import os

def process_vcf(input_vcf, snp_file, output_vcf):
    # 1. 读取 SNP 信息到字典
    snp_info = {} 
    print(f"正在读取 SNP 信息: {snp_file} ...")
    with open(snp_file, 'r') as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 4:
                chrom, pos, ref, alt = parts[0], int(parts[1]), parts[2], parts[3]
                snp_info[(chrom, pos)] = (ref, alt)

    print(f"读取到 {len(snp_info)} 个目标位点。")
    print(f"正在处理 VCF: {input_vcf} ...")

    # 2. 打开文件 (pysam 自动识别 .gz)
    try:
        vcf_in = pysam.VariantFile(input_vcf, 'r')
    except ValueError as e:
        print(f"错误: 无法打开 VCF 文件。请确保文件存在且有索引(.tbi/.csi)。错误: {e}")
        return

    vcf_out = pysam.VariantFile(output_vcf, 'w', header=vcf_in.header)

    count_modified = 0
    count_normal = 0

    # 3. 逐行读取 VCF
    for record in vcf_in:
        key = (record.chrom, record.pos)
        
        is_target = False
        
        # 检查是否在列表中
        if key in snp_info:
            # 检查是否是非双等位
            alts_str = str(record.alts)
            # 包含逗号、< 或 * 都算非双等位
            if ',' in alts_str or '<' in alts_str or '*' in alts_str:
                is_target = True

        if is_target:
            # --- 核心操作 ---
            target_ref, target_alt = snp_info[key]
            
            # 替换 REF/ALT
            record.ref = target_ref
            record.alts = (target_alt,)
            
            # 清空 GT
            for sample in record.samples:
                record.samples[sample]['GT'] = (None, None)
                
            vcf_out.write(record)
            count_modified += 1
        else:
            # 普通位点
            vcf_out.write(record)
            count_normal += 1

    vcf_in.close()
    vcf_out.close()

    print("处理完成！")
    print(f"修改并补空的位点数: {count_modified}")
    print(f"保持原样的位点数: {count_normal}")

# ================= 主程序入口 =================
if __name__ == "__main__":
    # 检查参数数量 (脚本名 + 3个参数)
    if len(sys.argv) != 4:
        print("用法: python change_vcf.py <输入vcf/vcf.gz> <snps.txt> <输出vcf>")
        print("示例: python change_vcf.py data.vcf.gz snps.txt result.vcf")
        sys.exit(1)

    # 获取参数
    in_vcf = sys.argv[1]
    txt_file = sys.argv[2]
    out_vcf = sys.argv[3]

    # 检查文件是否存在
    if not os.path.exists(in_vcf):
        print(f"错误: 找不到输入文件 {in_vcf}")
        sys.exit(1)
    if not os.path.exists(txt_file):
        print(f"错误: 找不到位点文件 {txt_file}")
        sys.exit(1)

    # 运行
    process_vcf(in_vcf, txt_file, out_vcf)
