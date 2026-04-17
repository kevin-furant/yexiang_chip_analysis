import sys

def main():
    if len(sys.argv) < 2:
        print("用法: python sample_stats.py 基因型文件 输出文件")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    # 读取基因型文件
    with open(input_file, 'r') as f:
        lines = f.readlines()
    
    # 解析表头
    header = lines[0].strip().split('\t')
    samples = header[4:]  # 前4列是基本信息
    total_snps = len(lines) - 1
    
    # 初始化每个样本的计数器
    sample_stats = []
    for sample in samples:
        sample_stats.append({
            'sample': sample,
            'na_count': 0,
            'het_count': 0,
            'hom_alt_count': 0,
            'ref_count': 0
        })
    
    print(f"正在统计{len(samples)}个样本，{total_snps}个SNP位点...")
    
    # 处理每个SNP位点
    for line_num, line in enumerate(lines[1:], 1):
        line = line.strip()
        if not line:
            continue
        
        fields = line.split('\t')
        ref = fields[2]
        alt = fields[3]
        genotypes = fields[4:]
        
        # 更新每个样本的统计
        for i, gt in enumerate(genotypes):
            if i >= len(sample_stats):
                continue
            
            # 分类基因型
            if gt == 'NA':
                sample_stats[i]['na_count'] += 1
            else:
                # 检查基因型长度
                if len(gt) == 2:
                    # 参考纯合
                    if gt == ref * 2:
                        sample_stats[i]['ref_count'] += 1
                    # 纯合突变
                    elif gt == alt * 2:
                        sample_stats[i]['hom_alt_count'] += 1
                    # 杂合（考虑未排序情况）
                    elif (gt[0] == ref and gt[1] == alt) or (gt[0] == alt and gt[1] == ref):
                        sample_stats[i]['het_count'] += 1
                    else:
                        # 其他情况视为缺失
                        sample_stats[i]['na_count'] += 1
                else:
                    # 长度不为2，视为缺失
                    sample_stats[i]['na_count'] += 1
        
        # 显示进度
        if line_num % 1000 == 0:
            print(f"已处理 {line_num}/{total_snps} 个位点...")
    
    # 写入输出文件
    with open(output_file, 'w') as f:
        # 写入表头
        header = ['Sample', 'NA_sites', 'NA_rate(%)', 'Ref/Ref_sites', 'Ref/Ref_rate(%)', 'Ref/Alt_sites', 
                 'Ref/Alt_rate(%)', 'Alt/Alt_sites', 'Alt/Alt_rate(%)']
        f.write('\t'.join(header) + '\n')
        
        # 写入每个样本的统计
        for stats in sample_stats:
            # 计算比例
            na_rate = (stats['na_count'] / total_snps) * 100
            het_rate = (stats['het_count'] / total_snps) * 100
            hom_alt_rate = (stats['hom_alt_count'] / total_snps) * 100
            ref_rate = (stats['ref_count'] / total_snps) * 100
            
            row = [
                stats['sample'],
                str(stats['na_count']),
                f"{na_rate:.2f}",
                str(stats['ref_count']),
                f"{ref_rate:.2f}",
                str(stats['het_count']),
                f"{het_rate:.2f}",
                str(stats['hom_alt_count']),
                f"{hom_alt_rate:.2f}"
            ]
            f.write('\t'.join(row) + '\n')
    
    # 计算总体统计
    total_na = sum(s['na_count'] for s in sample_stats)
    total_het = sum(s['het_count'] for s in sample_stats)
    total_hom_alt = sum(s['hom_alt_count'] for s in sample_stats)
    total_ref = sum(s['ref_count'] for s in sample_stats)
    
    avg_na_rate = total_na / (len(samples) * total_snps) * 100
    avg_het_rate = total_het / (len(samples) * total_snps) * 100
    avg_hom_alt_rate = total_hom_alt / (len(samples) * total_snps) * 100
    avg_ref_rate = total_ref / (len(samples) * total_snps) * 100
    
    print(f"\n=== 统计完成 ===")
    print(f"总样本数: {len(samples)}")
    print(f"总SNP位点数: {total_snps}")
    print(f"平均缺失率: {avg_na_rate:.2f}%")
    print(f"平均杂合率: {avg_het_rate:.2f}%")
    print(f"平均纯合突变率: {avg_hom_alt_rate:.2f}%")
    print(f"平均参考纯合率: {avg_ref_rate:.2f}%")
    print(f"\n结果已保存到: {output_file}")

if __name__ == "__main__":
    main()
