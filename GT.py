"""
VCF基因型提取脚本 - 只处理双等位位点
根据深度过滤：当DP<阈值时，基因型改为NA
基因型转换为：0/0 -> RR, 0/1 -> RV, 1/1 -> VV (R=REF, V=ALT)
用法: python extract_genotypes.py input.vcf output.tsv --min_dp 2
"""

import sys
import gzip
import argparse

def is_biallelic(ref, alt):
    """
    检查是否为双等位位点
    
    参数:
        alt: ALT字段
    
    返回:
        True: 双等位
        False: 多等位
    """
    if len(ref) > 1:
        return False

    if len(alt) > 1:
        return False

    # 如果ALT包含逗号，则是多等位
    if ',' in alt:
        return False
    
    # 检查特殊字符
    #if alt in ['*', '.', '<*>', '<NON_REF>']:
    if alt in ['*', '<*>', '<NON_REF>']:
        return False
    
    # 如果REF和ALT长度不同，可能涉及indel，但仍然是双等位
    # 这里我们接受所有双等位变异，包括SNP和indel
    return True

def convert_genotype(gt, ref, alt):
    """
    将基因型转换为字母格式（只处理双等位）
    
    参数:
        gt: 基因型，如 "0/0", "0/1", "1/1", "./." 等
        ref: 参考等位基因
        alt: 替代等位基因（双等位，只有一个ALT）
    
    返回:
        转换后的基因型，如 "GG", "GA", "AA" 或 "NA"
    """
    if gt in ['./.', '.', 'NA']:
        return 'NA'
    
    # 分割基因型
    try:
        alleles = gt.replace('|', '/').split('/')
    except:
        return 'NA'
    
    # 确保有两个等位基因
    if len(alleles) != 2:
        return 'NA'
    
    # 处理缺失数据
    if alleles[0] == '.' or alleles[1] == '.':
        return 'NA'
    
    # 转换每个等位基因
    converted_alleles = []
    for allele in alleles:
        if allele == '0':
            converted_alleles.append(ref)
        elif allele == '1':
            converted_alleles.append(alt)
        else:
            # 对于双等位位点，不应该有其他等位基因索引
            return 'NA'
    
    # 排序确保一致性（例如A/G和G/A都返回AG）
    #converted_alleles.sort()
    
    return ''.join(converted_alleles)

def parse_vcf(vcf_file, min_dp=2, verbose=False):
    """
    解析VCF文件，过滤多等位位点，根据深度过滤基因型
    
    参数:
        vcf_file: VCF文件路径
        min_dp: 最小深度阈值
        verbose: 是否显示详细统计
    
    返回:
        samples: 样本名列表
        data: 基因型数据列表
        stats: 统计信息字典
    """
    data = []
    samples = []
    
    # 统计信息
    stats = {
        'total_variants': 0,
        'biallelic_variants': 0,
        'multiallelic_variants': 0,
        'skipped_variants': 0,
        'total_genotypes': 0,
        'na_genotypes': 0
    }
    
    # 检测是否是gzip压缩文件
    if vcf_file.endswith('.gz'):
        opener = gzip.open
    else:
        opener = open
    
    with opener(vcf_file, 'rt', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            
            # 跳过注释行，但记录样本头
            if line.startswith('##'):
                continue
            elif line.startswith('#CHROM'):
                # 提取样本名
                fields = line.split('\t')
                samples = fields[9:]
                continue
            
            # 统计总变异数
            stats['total_variants'] += 1
            
            # 处理数据行
            fields = line.split('\t')
            
            # 提取基本信息
            chrom = fields[0]
            pos = fields[1]
            ref = fields[3]
            alt = fields[4]
            
            # 检查是否为双等位
            if not is_biallelic(ref, alt):
                stats['multiallelic_variants'] += 1
                continue
            
            # 双等位变异计数
            stats['biallelic_variants'] += 1
            
            # 解析FORMAT列
            format_fields = fields[8].split(':')
            
            # 初始化样本基因型列表
            genotypes = []
            
            # 处理每个样本
            for i in range(9, len(fields)):
                sample_data = fields[i]
                
                # 分割样本的各个字段
                sample_fields = sample_data.split(':')
                
                # 创建字段字典
                field_dict = dict(zip(format_fields, sample_fields))
                
                # 检查是否有GT字段
                if 'GT' not in field_dict:
                    genotypes.append('NA')
                    stats['na_genotypes'] += 1
                    continue
                
                raw_genotype = field_dict['GT']
                
                # 检查深度
                genotype = 'NA'  # 默认值
                if 'DP' in field_dict:
                    try:
                        dp = int(field_dict['DP'])
                        # 如果深度大于等于阈值，转换基因型
                        if dp >= min_dp:
                            genotype = convert_genotype(raw_genotype, ref, alt)
                            if genotype == 'NA':
                                stats['na_genotypes'] += 1  
                        else:
                            stats['na_genotypes'] += 1
                    except (ValueError, KeyError):
                        # DP字段无效，如果是.有基因型的话就原样输出
                        dp = field_dict['DP']
                        if dp == ".":
                            genotype = convert_genotype(raw_genotype, ref, alt)
                            if genotype == 'NA':
                                stats['na_genotypes'] += 1
                        else:
                            stats['na_genotypes'] += 1
                else:
                    # 没有DP字段，尝试转换基因型
                    genotype = convert_genotype(raw_genotype, ref, alt)
                    if genotype == 'NA':
                        stats['na_genotypes'] += 1
                
                genotypes.append(genotype)
            
            # 添加到数据列表
            data.append([chrom, pos, ref, alt] + genotypes)
    
    # 计算总基因型数
    if data:
        stats['total_genotypes'] = len(data) * len(samples)
    
    return samples, data, stats

def write_output(output_file, samples, data):
    """
    将结果写入输出文件
    
    参数:
        output_file: 输出文件路径
        samples: 样本名列表
        data: 基因型数据列表
    """
    with open(output_file, 'w', encoding='utf-8') as f:
        # 写入表头
        header = ['Chrom', 'Position', 'Ref', 'Alt'] + samples
        f.write('\t'.join(header) + '\n')
        
        # 写入数据
        for row in data:
            f.write('\t'.join(row) + '\n')

def main():
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='从VCF文件中提取双等位位点的基因型信息')
    parser.add_argument('--input', help='输入VCF文件路径')
    parser.add_argument('--output', help='输出结果文件路径')
    parser.add_argument('--min_dp', type=int, default=2, 
                       help='最小深度阈值（默认: 2）')
    parser.add_argument('--verbose', action='store_true',
                       help='显示详细统计信息')
    parser.add_argument('--summary', action='store_true',
                       help='生成统计摘要文件')
    
    args = parser.parse_args()
    
    print(f"正在处理文件: {args.input}")
    print(f"深度阈值: DP < {args.min_dp} 的基因型将设为NA")
    print(f"只保留双等位位点")
    print(f"输出文件: {args.output}")
    
    try:
        samples, data, stats = parse_vcf(args.input, args.min_dp, args.verbose)
        
        if len(data) == 0:
            print("警告: 没有找到双等位位点！")
            sys.exit(0)
        
        write_output(args.output, samples, data)
        
        # 显示统计信息
        print(f"\n=== 处理统计 ===")
        print(f"总变异位点数: {stats['total_variants']}")
        print(f"双等位位点数: {stats['biallelic_variants']} ({stats['biallelic_variants']/stats['total_variants']*100:.1f}%)")
        print(f"多等位位点数: {stats['multiallelic_variants']} ({stats['multiallelic_variants']/stats['total_variants']*100:.1f}%)")
        print(f"保留的位点数: {len(data)}")
        print(f"样本数: {len(samples)}")
        
        if stats['total_genotypes'] > 0:
            na_percentage = stats['na_genotypes'] / stats['total_genotypes'] * 100
            valid_percentage = 100 - na_percentage
            print(f"总基因型数: {stats['total_genotypes']}")
            print(f"有效基因型数: {stats['total_genotypes'] - stats['na_genotypes']} ({valid_percentage:.1f}%)")
            print(f"缺失基因型数(NA): {stats['na_genotypes']} ({na_percentage:.1f}%)")
        
        # 基因型分布统计
        genotype_counts = {}
        for row in data:
            for genotype in row[4:]:  # 跳过前4列（基本信息）
                if genotype in genotype_counts:
                    genotype_counts[genotype] += 1
                else:
                    genotype_counts[genotype] = 1
        
        print(f"\n=== 基因型分布 ===")
        total_genotypes = sum(genotype_counts.values())
        for gt, count in sorted(genotype_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = count / total_genotypes * 100
            print(f"{gt}: {count} ({percentage:.2f}%)")
        
        # 生成统计摘要文件
        if args.summary:
            summary_file = args.output.replace('.tsv', '_summary.txt')
            if summary_file == args.output:
                summary_file = args.output + '_summary.txt'
            
            with open(summary_file, 'w', encoding='utf-8') as f:
                f.write(f"VCF文件: {args.input}\n")
                f.write(f"输出文件: {args.output}\n")
                f.write(f"最小深度阈值: {args.min_dp}\n")
                f.write(f"处理时间: {sys.argv[0]} {args.input} {args.output} --min_dp {args.min_dp}\n")
                f.write(f"\n=== 处理统计 ===\n")
                f.write(f"总变异位点数: {stats['total_variants']}\n")
                f.write(f"双等位位点数: {stats['biallelic_variants']} ({stats['biallelic_variants']/stats['total_variants']*100:.1f}%)\n")
                f.write(f"多等位位点数: {stats['multiallelic_variants']} ({stats['multiallelic_variants']/stats['total_variants']*100:.1f}%)\n")
                f.write(f"保留的位点数: {len(data)}\n")
                f.write(f"样本数: {len(samples)}\n")
                
                if stats['total_genotypes'] > 0:
                    f.write(f"总基因型数: {stats['total_genotypes']}\n")
                    f.write(f"有效基因型数: {stats['total_genotypes'] - stats['na_genotypes']} ({(stats['total_genotypes'] - stats['na_genotypes'])/stats['total_genotypes']*100:.1f}%)\n")
                    f.write(f"缺失基因型数(NA): {stats['na_genotypes']} ({stats['na_genotypes']/stats['total_genotypes']*100:.1f}%)\n")
                
                f.write(f"\n=== 基因型分布 ===\n")
                for gt, count in sorted(genotype_counts.items(), key=lambda x: x[1], reverse=True):
                    percentage = count / total_genotypes * 100
                    f.write(f"{gt}: {count} ({percentage:.2f}%)\n")
            
            print(f"\n统计摘要已保存到: {summary_file}")
        
    except FileNotFoundError:
        print(f"错误: 找不到文件 {args.input}")
        sys.exit(1)
    except Exception as e:
        print(f"处理过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
