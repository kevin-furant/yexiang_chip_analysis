#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
靶向测序基因型分型报告生成器 - 完整版
包括静态资源复制和DataTables翻页功能
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
import argparse
from pathlib import Path
import shutil
import warnings
warnings.filterwarnings('ignore')

# 自动检测Jinja2，如果不存在则尝试安装
try:
    from jinja2 import Template, Environment, FileSystemLoader
    HAS_JINJA2 = True
except ImportError:
    print("警告: 未找到Jinja2库，尝试安装...")
    try:
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "jinja2"])
        from jinja2 import Template, Environment, FileSystemLoader
        HAS_JINJA2 = True
        print("Jinja2安装成功!")
    except:
        HAS_JINJA2 = False
        print("无法自动安装Jinja2，请手动运行: pip install jinja2")

def get_script_dir():
    """获取脚本所在目录"""
    return Path(__file__).parent.absolute()

def safe_read_file(file_path, default_encoding='utf-8', fallback_encoding='gbk'):
    """安全读取文件，尝试多种编码"""
    try:
        with open(file_path, 'r', encoding=default_encoding) as f:
            return f.read()
    except UnicodeDecodeError:
        try:
            with open(file_path, 'r', encoding=fallback_encoding) as f:
                return f.read()
        except:
            raise Exception(f"无法读取文件 {file_path}，请检查编码格式")

def copy_static_resources(src_dir, output_dir, verbose=False):
    """
    复制静态资源到输出目录
    """
    src_path = Path(src_dir)
    dest_path = Path(output_dir) / "src"
    
    if not src_path.exists():
        print(f"❌ 静态资源目录不存在: {src_path}")
        return False
    
    try:
        if dest_path.exists():
            if verbose:
                print(f"  目标目录已存在，覆盖: {dest_path}")
            shutil.rmtree(dest_path)
        
        # 复制整个目录
        shutil.copytree(src_path, dest_path)
        
        if verbose:
            print(f"✅ 复制静态资源: {src_path} -> {dest_path}")
            # 显示复制的文件列表
            for root, dirs, files in os.walk(dest_path):
                level = root.replace(str(dest_path), '').count(os.sep)
                indent = ' ' * 2 * level
                if verbose:
                    print(f'{indent}{os.path.basename(root)}/')
                subindent = ' ' * 2 * (level + 1)
                for file in files[:10]:  # 只显示前10个文件
                    if verbose:
                        print(f'{subindent}{file}')
                if len(files) > 10 and verbose:
                    print(f'{subindent}... 和 {len(files) - 10} 个更多文件')
        
        return True
        
    except Exception as e:
        print(f"❌ 复制静态资源失败: {e}")
        return False

def copy_qc_images(qc_src_dir, output_dir, verbose=False):
    """
    复制QC图片到输出目录
    """
    qc_src_path = Path(qc_src_dir)
    qc_dest_path = Path(output_dir) / "QC"
    
    if not qc_src_path.exists():
        if verbose:
            print(f"⚠️  QC目录不存在: {qc_src_path}，跳过复制")
        return False
    
    try:
        if qc_dest_path.exists():
            shutil.rmtree(qc_dest_path)
        
        # 复制整个QC目录
        shutil.copytree(qc_src_path, qc_dest_path)
        
        if verbose:
            print(f"✅ 复制QC图片: {qc_src_path} -> {qc_dest_path}")
            # 统计图片数量
            image_count = 0
            for root, dirs, files in os.walk(qc_dest_path):
                image_count += sum(1 for f in files if f.lower().endswith(('.png', '.jpg', '.jpeg', '.gif')))
            
            print(f"   找到 {image_count} 个图片文件")
        
        return True
        
    except Exception as e:
        print(f"❌ 复制QC图片失败: {e}")
        return False

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='生成靶向测序基因型分型报告',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python generate_report.py -d /path/to/project_data -n "甜瓜5K芯片测试" -c "RD2024031301-02"
  python generate_report.py --data-dir ./data --project-name "测试项目" --contract "TEST001" --copy-static
        """
    )
    
    # 数据文件参数
    parser.add_argument('-d', '--data-dir', 
                       default='.',
                       help='数据文件所在目录，包含info.xls, bwa.xls等文件（默认当前目录）')
    parser.add_argument('-p', '--chip',
                       help='芯片名称，如果info.xls中有则自动读取，否则使用此参数')
    parser.add_argument('-n', '--project-name',
                       help='项目名称，如果info.xls中有则自动读取，否则使用此参数')
    
    parser.add_argument('-c', '--contract',
                       help='合同编号，如果info.xls中有则自动读取，否则使用此参数')
    parser.add_argument('-k', '--customer-name',
                        help='客户名称')

    parser.add_argument('-s', '--sample-number',
                        help='送检样本数量')
    # 文件指定参数（如果不想使用默认文件名）
    parser.add_argument('--info', default='info.xls', help='项目信息文件（默认info.xls）')

    # 输出参数
    parser.add_argument('-o', '--output-dir', 
                       default='reports',
                       help='报告输出目录（默认reports）')
    
    parser.add_argument('--qc-dir', 
                       default='QC',
                       help='QC图片目录（默认QC）')
    
    parser.add_argument('--template', 
                       default='template/full_report.html',
                       help='HTML模板文件路径（默认template/full_report.html）')
    
    parser.add_argument('--src-dir',
                       default='src',
                       help='静态资源目录（默认src）')
    
    # 复制选项
    parser.add_argument('--copy-static', action='store_true',
                       help='复制静态资源到输出目录')
    
    parser.add_argument('--copy-qc', action='store_true',
                       help='复制QC图片到输出目录')
    
    parser.add_argument('-f', '--force', action='store_true',
                       help='强制覆盖已存在的报告')
    
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='显示详细输出信息')
    
    parser.add_argument('--no-bootstrap', action='store_true',
                       help='不自动安装缺少的依赖')
    
    return parser.parse_args()

def read_info_file(info_path):
    """读取项目信息文件"""
    info_data = {"项目名称": "", "合同编号": "", "芯片名称": "", "客户名称": "", "送检数量": ""}
    
    try:
        content = safe_read_file(info_path)
        
        for line in content.strip().split('\n'):
            line = line.strip()
            if not line:
                continue
                
            if ':' in line:
                key, value = line.split(':', 1)
                key = key.strip()
                value = value.strip()
                info_data[key] = value
            elif '\t' in line:
                parts = line.split('\t')
                if len(parts) >= 2:
                    info_data[parts[0].strip()] = parts[1].strip()
                    
    except Exception as e:
        print(f"警告: 无法读取项目信息文件 {info_path}: {e}")
    
    return info_data

def load_data_files(args):
    """加载所有数据文件"""
    data_dir = Path(args.data_dir)
    
    # 1. 读取项目信息
    info_path = Path(args.info)
    if not info_path.exists():
        print(f"警告: 项目信息文件 {info_path} 不存在，使用命令行参数")
        project_info = {}
    else:
        project_info = read_info_file(info_path)
        if args.verbose:
            print(f"📄 读取项目信息: {info_path}")
    
    # 如果命令行参数提供了项目名称和合同编号，则覆盖文件中的值
    if args.project_name:
        project_info["项目名称"] = args.project_name
    if args.contract:
        project_info["合同编号"] = args.contract
    if args.chip:
        project_info["芯片名称"] = args.chip
    if args.customer_name:
        project_info["客户名称"] = args.customer_name
    if args.sample_number:
        project_info["送检数量"] = args.sample_number

    # 检查必需字段
    #if not project_info.get("项目名称"):
    #    project_info["项目名称"] = input("请输入项目名称: ") if not args.verbose else "未命名项目"
    #if not project_info.get("合同编号"):
    #    project_info["合同编号"] = input("请输入合同编号: ") if not args.verbose else "未知"
    
    # 2. 读取比对统计
    bwa_path = data_dir / "stat/bwa_result.xls"
    if not bwa_path.exists():
        raise FileNotFoundError(f"比对统计文件不存在: {bwa_path}")
    
    try:
        alignment_df = pd.read_csv(bwa_path, sep='\t', dtype={'Sample': str})
        if args.verbose:
            print(f"📊 读取比对数据: {bwa_path} - {len(alignment_df)} 行")
        
        # 检查必要的列
        required_cols = ['Sample', 'Clean_reads', 'Mapped_reads', 'Mapping_rate']
        for col in required_cols:
            if col not in alignment_df.columns:
                raise ValueError(f"比对统计文件缺少必要列: {col}")
        
        # 清理数据
        alignment_df['Sample'] = alignment_df['Sample'].astype(str)
        alignment_df['mapping_rate'] = alignment_df['Mapping_rate'].astype(str).str.replace('%', '').astype(float)
        #alignment_df['Clean_reads'] = pd.to_numeric(alignment_df['Clean_reads'], errors='coerce')
        #alignment_df['mapped_reads'] = pd.to_numeric(alignment_df['mapped_reads'], errors='coerce')
        alignment_df['Clean_reads'] = pd.to_numeric(alignment_df['Clean_reads'], errors='ignore')
        alignment_df['mapped_reads'] = pd.to_numeric(alignment_df['Mapped_reads'], errors='ignore')
    except Exception as e:
        print(f"错误: 无法读取比对统计文件 {bwa_path}: {e}")
        raise

    # 2.1 读取捕获率等统计
    buhuo_stat_path = data_dir / "stat/stat.xls"
    if not buhuo_stat_path.exists():
        raise FileNotFoundError(f"样本统计文件不存在: {buhuo_stat_path}")

    try:
        buhuo_stats_df = pd.read_csv(buhuo_stat_path, sep='\t', dtype={'Sample': str})
        txt_cols = ['Sample', 'Site_detection_rate(%)', 'Capture_rate(%)', 'Coverage(%)', 'Average_depth']
        for col in txt_cols:
            if col not in buhuo_stats_df.columns:
                raise ValueError(f"比对统计文件缺少必要列: {col}")
        buhuo_stats_df['Sample'] = buhuo_stats_df['Sample'].astype(str)
        buhuo_stats_df['site_detection_rate'] = buhuo_stats_df['Site_detection_rate(%)'].astype(str).str.replace('%', '').astype(float)
        buhuo_stats_df['capture_rate'] = buhuo_stats_df['Capture_rate(%)'].astype(str).str.replace('%', '').astype(float)
        buhuo_stats_df['coverage'] = buhuo_stats_df['Coverage(%)'].astype(str).str.replace('%', '').astype(float)
        buhuo_stats_df['average_depth'] = buhuo_stats_df['Average_depth'].astype(str).str.replace('%', '').astype(float)
    except Exception as e:
        print(f"错误: 无法读取样本统计文件{buhuo_stat_path}: {e}")
        raise

    # 3. 读取样本统计
    sample_stat_path = data_dir / "SNP/chip_sample_stat.xls"
    if not sample_stat_path.exists():
        raise FileNotFoundError(f"样本统计文件不存在: {sample_stat_path}")
    
    try:
        sample_stats_df = pd.read_csv(sample_stat_path, sep='\t', dtype={'Sample': str})
        if args.verbose:
            print(f"📊 读取样本统计: {sample_stat_path} - {len(sample_stats_df)} 行")
    except Exception as e:
        print(f"错误: 无法读取样本统计文件 {sample_stat_path}: {e}")
        raise
    
    # 4. 读取SNP统计
    snp_stat_path = data_dir / "SNP/chip_snp_stat.xls"
    if not snp_stat_path.exists():
        raise FileNotFoundError(f"SNP统计文件不存在: {snp_stat_path}")
    
    try:
        snp_stats_df = pd.read_csv(snp_stat_path, sep='\t')
        if args.verbose:
            print(f"📊 读取SNP统计: {snp_stat_path} - {len(snp_stats_df)} 行")
    except Exception as e:
        print(f"错误: 无法读取SNP统计文件 {snp_stat_path}: {e}")
        raise
    
    # 5. 读取基因型数据
    genotype_path = data_dir / "SNP/chip_GT.xls"
    if not genotype_path.exists():
        raise FileNotFoundError(f"基因型数据文件不存在: {genotype_path}")
    
    try:
        # 对于大文件，使用chunksize读取
        genotype_df = pd.read_csv(genotype_path, sep='\t')
        if args.verbose:
            print(f"📊 读取基因型数据: {genotype_path} - {len(genotype_df)} 行 × {len(genotype_df.columns)} 列")
    except Exception as e:
        print(f"错误: 无法读取基因型数据文件 {genotype_path}: {e}")
        raise

    # 6. 读取mSNP样本统计
    msample_stat_path = data_dir / "mSNP/mSNP_sample_stat.xls"
    if not msample_stat_path.exists():
        raise FileNotFoundError(f"样本统计文件不存在: {msample_stat_path}")

    try:
        msample_stats_df = pd.read_csv(msample_stat_path, sep='\t', dtype={'Sample': str})
        if args.verbose:
            print(f"📊 读取样本统计: {msample_stat_path} - {len(msample_stats_df)} 行")
    except Exception as e:
        print(f"错误: 无法读取样本统计文件 {msample_stat_path}: {e}")
        raise

    # 7. 读取mSNP统计
    msnp_stat_path = data_dir / "mSNP/mSNP_snp_stat.xls"
    if not msnp_stat_path.exists():
        raise FileNotFoundError(f"SNP统计文件不存在: {msnp_stat_path}")

    try:
        msnp_stats_df = pd.read_csv(msnp_stat_path, sep='\t')
        if args.verbose:
            print(f"📊 读取SNP统计: {msnp_stat_path} - {len(msnp_stats_df)} 行")
    except Exception as e:
        print(f"错误: 无法读取SNP统计文件 {msnp_stat_path}: {e}")
        raise

    # 8. 读取mSNP基因型数据
    mgenotype_path = data_dir / "mSNP/mSNP_GT.xls"
    if not mgenotype_path.exists():
        raise FileNotFoundError(f"基因型数据文件不存在: {mgenotype_path}")

    try:
        # 对于大文件，使用chunksize读取
        mgenotype_df = pd.read_csv(mgenotype_path, sep='\t')
        if args.verbose:
            print(f"📊 读取基因型数据: {mgenotype_path} - {len(mgenotype_df)} 行 × {len(mgenotype_df.columns)} 列")
    except Exception as e:
        print(f"错误: 无法读取基因型数据文件 {gmenotype_path}: {e}")
        raise
    
    return project_info, alignment_df, buhuo_stats_df, sample_stats_df, snp_stats_df, genotype_df, msample_stats_df, msnp_stats_df, mgenotype_df

def prepare_template_data(project_info, alignment_df, buhuo_stats_df, sample_stats_df, snp_stats_df, genotype_df, msample_stats_df, msnp_stats_df, mgenotype_df, args):
    """准备模板渲染所需的所有数据"""
    
    if args.verbose:
        print("\n📊 准备模板数据...")
    
    # 基础统计
    total_samples = len(alignment_df)
    total_snps = len(snp_stats_df)
    avg_alignment_rate = alignment_df['mapping_rate'].mean()
    

    # 准备比对数据
    alignment_data = []
    for _, row in alignment_df.head(100).iterrows():
        alignment_data.append({
            'Sample': str(row['Sample']),
            'Clean_reads': int(row['Clean_reads']),
            'mapped_reads': int(row['mapped_reads']),
            'mapping_rate': float(row['mapping_rate'])
        })
    # 准备捕获数据
    buhuo_data = []
    for _, row in buhuo_stats_df.head(100).iterrows():
        buhuo_data.append({
            'Sample': str(row['Sample']),
            'site_detection_rate': float(row['site_detection_rate']),
            'capture_rate': float(row['capture_rate']),
            'coverage': float(row['coverage']),
            'average_depth': float(row['average_depth'])
        })
    # 准备SNP位点统计
    site_stats = []
    for _, row in snp_stats_df.head(100).iterrows():
        site_stats.append({
            'Chrom': row['Chrom'],
            'Position': int(row['Position']),
            'Ref': row['Ref'],
            'NA_rate': float(row.get('NA_freq(%)', 0)),
            'Ref_rate': float(row.get('Ref/Ref_freq(%)', 0)),
            'Hom_alt_rate': float(row.get('Alt/Alt_freq(%)', 0)),
            'Het_alt_rate': float(row.get('Ref/Alt_freq(%)', 0)),
            'MAF': float(row.get('MAF', 0))
        })
    # 准备mSNP位点统计
    msite_stats = []
    for _, row in msnp_stats_df.head(100).iterrows():
        msite_stats.append({
            'Chrom': row['Chrom'],
            'Position': int(row['Position']),
            'Ref': row['Ref'],
            'Ref_rate': float(row.get('Ref/Ref_freq(%)', 0)),
            'Hom_alt_rate': float(row.get('Alt/Alt_freq(%)', 0)),
            'Het_alt_rate': float(row.get('Ref/Alt_freq(%)', 0)),
            'MAF': float(row.get('MAF', 0))
        })
    '''
    # 准备详细位点统计（添加计数）
    site_stats_detailed = []
    for _, row in snp_stats_df.head(100).iterrows():
        na_rate = float(row.get('NA_rate(%)', 0))
        ref_rate = float(row.get('Ref_rate(%)', 0))
        het_rate = float(row.get('Het_rate(%)', 0))
        hom_rate = 100 - na_rate - ref_rate - het_rate
        
        site_stats_detailed.append({
            'Chrom': row['CHROM'],
            'Position': int(row['POS']),
            'Ref': row['REF'],
            'NA_count': int(round(na_rate / 100 * total_samples)),
            'NA_rate': na_rate,
            'Ref_count': int(round(ref_rate / 100 * total_samples)),
            'Ref_rate': ref_rate,
            'Het_count': int(round(het_rate / 100 * total_samples)),
            'Het_rate': het_rate,
            'Hom_count': int(round(hom_rate / 100 * total_samples)),
            'Hom_rate': hom_rate
        })
    '''
    
    # 准备样本统计
    sample_stats = []
    for _, row in sample_stats_df.head(100).iterrows():
        # 计算总位点数
        total_sites = int(row.get('Ref/Ref_sites', 0) + row.get('Ref/Alt_sites', 0) +
                         row.get('Alt/Alt_sites', 0) + row.get('NA_sites', 0))
        
        sample_stats.append({
            'Sample': str(row['Sample']),
            'Total_sites': total_sites,
            'NA_number': int(row.get('NA_sites', 0)),
            'NA_rate': float(row.get('NA_rate(%)', 0)),
            'Het_alt_number': int(row.get('Ref/Alt_sites', 0)),
            'Het_alt_rate': float(row.get('Ref/Alt_rate(%)', 0)),
            'Hom_alt_number': int(row.get('Alt/Alt_sites', 0)),
            'Hom_alt_rate': float(row.get('Alt/Alt_rate(%)', 0)),
            'Ref_number': int(row.get('Ref/Ref_sites', 0)),
            'Ref_rate': float(row.get('Ref/Ref_rate(%)', 0))
        })
    # 准备mSNP样本统计
    msample_stats = []
    for _, row in msample_stats_df.head(100).iterrows():
        # 计算总位点数
        mtotal_sites = int(row.get('Ref_sites', 0) + row.get('Ref/Alt_sites', 0) +
                          row.get('Alt/Alt_sites', 0) + row.get('NA_sites', 0))

        msample_stats.append({
            'Sample': str(row['Sample']),
            'Total_sites': mtotal_sites,
            'Het_alt_number': int(row.get('Ref/Alt_sites', 0)),
            'Het_alt_rate': float(row.get('Ref/Alt_rate(%)', 0)),
            'Hom_alt_number': int(row.get('Alt/Alt_sites', 0)),
            'Hom_alt_rate': float(row.get('Alt/Alt_rate(%)', 0)),
            'Ref_number': int(row.get('Ref/Ref_sites', 0)),
            'Ref_rate': float(row.get('Ref/Ref_rate(%)', 0))
        })
    # 准备基因型数据（只取前100行显示，避免页面过大）
    genotype_data = []
    #max_rows = min(1000, len(genotype_df))  # 限制显示行数
    for i, row in genotype_df.head(100).iterrows():
        genotype_data.append(row.to_dict())
    
    genotype_columns = genotype_df.columns.tolist()

    # 准备mSNP基因型数据（只取前100行显示，避免页面过大）
    mgenotype_data = []
    #mmax_rows = min(1000, len(mgenotype_df))  # 限制显示行数
    for i, row in mgenotype_df.head(100).iterrows():
        mgenotype_data.append(row.to_dict())

    mgenotype_columns = mgenotype_df.columns.tolist()


    # 获取QC样本（从比对数据中获取）
    qc_samples = alignment_df['Sample'].head(10).tolist()
    
    # 构建完整的模板数据字典
    template_data = {
        # 项目信息
        'project_info': project_info,
        'generation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        
        # 基础统计
        'total_samples': total_samples,
        'total_snps': total_snps,
        'avg_alignment_rate': float(avg_alignment_rate),
        
        # 数据表
        'alignment_data': alignment_data,
        'buhuo_data': buhuo_data,
        'site_stats': site_stats,
        'msite_stats': msite_stats,
        'sample_stats': sample_stats,
        'msample_stats': msample_stats,
        'genotype_data': genotype_data,
        'mgenotype_data': mgenotype_data,
        'genotype_columns': genotype_columns,
        'mgenotype_columns': mgenotype_columns,
        # QC相关
        'qc_samples': qc_samples,
        'q30_threshold': 90,
        
        # 路径配置
        'static_path': 'src',  # 相对路径，因为我们会复制静态资源
        'qc_dir': 'QC',       # 相对路径
    }
    
    if args.verbose:
        print("✅ 模板数据准备完成")
    
    return template_data

def generate_html_report(template_data, args):
    """生成HTML报告文件"""
    
    if not HAS_JINJA2:
        raise ImportError("需要Jinja2库，请运行: pip install jinja2")
    
    # 获取脚本所在目录
    script_dir = get_script_dir()
    
    # 模板路径
    template_path = Path(args.template)
    if not template_path.is_absolute():
        template_path = script_dir / template_path
    
    if not template_path.exists():
        # 尝试在template目录下查找
        alt_path = script_dir / 'template' / 'full_report.html'
        if alt_path.exists():
            template_path = alt_path
        else:
            raise FileNotFoundError(f"找不到模板文件: {args.template}")
    
    if args.verbose:
        print(f"📄 使用模板: {template_path}")
    
    # 读取模板
    try:
        template_content = safe_read_file(template_path)
    except Exception as e:
        raise Exception(f"无法读取模板文件 {template_path}: {e}")
    
    # 创建输出目录
    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = script_dir / output_dir
    
    # 生成报告文件名（使用项目名称和合同编号）
    project_name = template_data['project_info'].get('项目名称', '未命名项目')
    contract_no = template_data['project_info'].get('合同编号', '未知')
    
    # 清理文件名中的非法字符
    import re
    safe_project_name = re.sub(r'[<>:"/\\|?*]', '_', project_name)
    safe_contract_no = re.sub(r'[<>:"/\\|?*]', '_', contract_no)
    
    report_filename = f"{safe_contract_no}_{safe_project_name}_结题报告.html"
    report_path = output_dir / report_filename
    
    # 如果文件已存在且不是强制模式
    #if report_path.exists() and not args.force:
    #    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    #    report_filename = f"{safe_contract_no}_{safe_project_name}_结题报告_{timestamp}.html"
    #    report_path = output_dir / report_filename
    
    # 确保输出目录存在
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 使用Jinja2渲染模板
    try:
        template = Template(template_content)
        html_content = template.render(**template_data)
    except Exception as e:
        raise Exception(f"模板渲染失败: {e}")
    
    # 保存报告
    try:
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        if args.verbose:
            print(f"💾 报告保存到: {report_path}")
    except Exception as e:
        raise Exception(f"无法保存报告文件 {report_path}: {e}")
    
    return report_path, output_dir

def check_required_files():
    """检查必要的静态资源文件"""
    script_dir = get_script_dir()
    required_files = {
        'css': ['bootstrap.min.css', 'dataTables.bootstrap.min.css'],
        'js': [
            'jquery-1.9.1-min.js',
            'jquery.dataTables.min.js',
            'dataTables.bootstrap.min.js',
            'bootstrap.min.js'
        ]
    }
    
    missing_files = []
    
    for file_type, files in required_files.items():
        for file in files:
            file_path = script_dir / 'src' / file_type / file
            if not file_path.exists():
                missing_files.append(str(file_path))
    
    return missing_files

def main():
    """主函数"""
    print("=" * 60)
    print("      🧬 靶向测序基因型分型报告生成器 🧬")
    print("=" * 60)
    
    # 解析命令行参数
    args = parse_arguments()
    
    # 检查必要的依赖
    if not HAS_JINJA2 and not args.no_bootstrap:
        print("正在安装必要的依赖...")
        try:
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip", "install", "jinja2", "pandas"])
            print("✅ 依赖安装成功，请重新运行脚本")
            return 0
        except:
            print("❌ 无法自动安装依赖，请手动运行:")
            print("   pip install jinja2 pandas")
            return 1
    
    if args.verbose:
        print(f"📁 数据目录: {args.data_dir}")
        print(f"📁 输出目录: {args.output_dir}")
        print(f"📁 模板文件: {args.template}")
        print(f"📁 资源目录: {args.src_dir}")
        print(f"📁 QC目录: {args.qc_dir}")
        print("-" * 60)
    
    try:
        # 1. 加载数据文件
        print("📂 加载数据文件...")
        project_info, alignment_df, buhuo_stats_df, sample_stats_df, snp_stats_df, genotype_df, msample_stats_df, msnp_stats_df, mgenotype_df = load_data_files(args)
        
        # 2. 准备模板数据
        template_data = prepare_template_data(
            project_info, alignment_df, buhuo_stats_df, sample_stats_df, snp_stats_df, genotype_df, msample_stats_df, msnp_stats_df, mgenotype_df, args
        )

        # 3. 生成HTML报告
        print("\n🎨 生成HTML报告...")
        report_path, output_dir = generate_html_report(template_data, args)
        
        # 4. 复制静态资源（如果指定）
        if args.copy_static:
            print("\n📁 复制静态资源...")
            copy_static_resources(args.src_dir, output_dir, args.verbose)
        else:
            print("\n⚠️  未复制静态资源，请确保报告可以访问到静态资源")
            missing_files = check_required_files()
            if missing_files:
                print("❌ 缺少以下必要的静态资源文件:")
                for file in missing_files:
                    print(f"   - {file}")
                print("\n💡 建议使用 --copy-static 参数自动复制静态资源")
        
        # 5. 复制QC图片（如果指定）
        if args.copy_qc:
            print("\n🖼️  复制QC图片...")
            copy_qc_images(args.qc_dir, output_dir, args.verbose)
        elif args.qc_dir and os.path.exists(args.qc_dir):
            print(f"\nℹ️  QC目录存在但未复制: {args.qc_dir}")
            print("   使用 --copy-qc 参数复制QC图片")
        
        # 6. 显示报告信息
        print("\n" + "=" * 60)
        print("✅ 报告生成成功!")
        print("=" * 60)
        print(f"📋 项目名称: {template_data['project_info'].get('项目名称')}")
        print(f"📋 合同编号: {template_data['project_info'].get('合同编号')}")
        print(f"🧪 样本数量: {template_data['total_samples']}")
        print(f"🧬 SNP位点数: {template_data['total_snps']}")
        print(f"📈 平均比对率: {template_data['avg_alignment_rate']:.2f}%")
        print(f"📄 报告文件: {report_path}")
        
        # 文件大小
        if os.path.exists(report_path):
            size = os.path.getsize(report_path) / 1024
            print(f"📊 报告大小: {size:.1f} KB")
        
        print("\n💡 使用提示:")
        print("   1. 用浏览器打开报告文件查看完整功能")
        print("   2. 表格支持排序、搜索和翻页")
        print("   3. 点击样本名称查看QC图片")
        
        if not args.copy_static:
            print("\n⚠️  重要提醒: 由于未复制静态资源，请确保报告目录包含:")
            print(f"      {output_dir}/src/css/  (CSS样式文件)")
            print(f"      {output_dir}/src/js/   (JavaScript文件)")
        
        print("=" * 60)
        
        return 0
        
    except FileNotFoundError as e:
        print(f"\n❌ 文件不存在: {e}")
        print("请检查文件路径是否正确")
        return 1
    except Exception as e:
        print(f"\n❌ 生成报告时出错: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
