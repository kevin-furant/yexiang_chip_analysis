import sys
from collections import Counter

def main():
    if len(sys.argv) < 2:
        print("用法: python snp_stat.py 基因型文件 输出文件")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    # 读取基因型文件
    with open(input_file, 'r') as f:
        lines = f.readlines()
    
    # 解析表头
    header = lines[0].strip().split('\t')
    samples = header[4:]  # 前4列是基本信息
    total_samples = len(samples)
    
    # 准备输出
    results = []
    
    print(f"正在统计{len(lines)-1}个SNP位点...")
    
    # 处理每个SNP位点
    for line in lines[1:]:
        line = line.strip()
        if not line:
            continue
        
        fields = line.split('\t')
        chrom = fields[0]
        pos = fields[1]
        ref = fields[2]
        alt = fields[3]
        genotypes = fields[4:]
        
        # 创建SNP ID
        #snp_id = f"{chrom}_{pos}"
        
        # 统计基因型
        genotype_counts = Counter(genotypes)
        
        # 缺失统计
        na_count = genotype_counts.get('NA', 0)
        na_rate = (na_count / total_samples) * 100
        
        # 有效基因型统计
        rr_count = 0  # REF纯合
        rv_count = 0  # 杂合
        vv_count = 0  # ALT纯合
        
        for gt, count in genotype_counts.items():
            if gt == 'NA':
                continue
            
            # 判断基因型类型
            if len(gt) == 2:  # 确保是二倍体
                if gt[0] == gt[1]:  # 纯合
                    if gt == ref * 2:
                        rr_count += count
                    elif gt == alt * 2:
                        vv_count += count
                else:  # 杂合
                    # 检查是否包含REF和ALT
                    if (gt[0] == ref and gt[1] == alt) or (gt[0] == alt and gt[1] == ref):
                        rv_count += count
        
        # 计算比例
        valid_samples = total_samples - na_count
        if valid_samples > 0:
            rr_rate = (rr_count / valid_samples) * 100
            rv_rate = (rv_count / valid_samples) * 100
            vv_rate = (vv_count / valid_samples) * 100
            
            # 计算MAF
            ref_allele_count = rr_count * 2 + rv_count
            alt_allele_count = vv_count * 2 + rv_count
            total_alleles = valid_samples * 2
            
            ref_freq = ref_allele_count / total_alleles
            alt_freq = alt_allele_count / total_alleles
            maf = min(ref_freq, alt_freq)
        else:
            rr_rate = rv_rate = vv_rate = maf = 0
        
        # 保存结果
        #results.append([
        #    snp_id, chrom, pos, ref,
        #    f"{na_rate:.2f}",
        #    f"{rr_rate:.2f}",
        #    f"{vv_rate:.2f}",
        #    f"{rv_rate:.2f}",
        #    f"{maf:.4f}"
        #])
        results.append([
            chrom, pos, ref,
            f"{na_rate:.2f}",
            f"{rr_rate:.2f}",
            f"{rv_rate:.2f}",
            f"{vv_rate:.2f}",
            f"{maf:.4f}"
        ])
    
    # 写入输出文件
    with open(output_file, 'w') as f:
        # 写入表头
        #header = ['ID', 'Chrom', 'Position', 'Ref', 'NA_rate(%)',
        #header = ['Chrom', 'Position', 'Ref', 'NA_rate(%)',
        header = ['Chrom', 'Position', 'Ref', 'NA_freq(%)', 
                 'Ref/Ref_freq(%)', 'Ref/Alt_freq(%)', 'Alt/Alt_freq(%)', 'MAF']
        f.write('\t'.join(header) + '\n')
        
        # 写入数据
        for row in results:
            f.write('\t'.join(row) + '\n')
    
    print(f"统计完成！结果已保存到 {output_file}")
    print(f"共统计了 {len(results)} 个SNP位点")

if __name__ == "__main__":
    main()
