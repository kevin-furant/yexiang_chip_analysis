#!/usr/bin/env python
#-*- coding:utf-8 -*-

import os
import argparse
import math
import pandas as pd

parser = argparse.ArgumentParser(prog='bwa mem pipline', description='bwa mem pipline Ver 3.0')
parser.add_argument('-l', '--cleanlist', dest='cleanlist', required=True, help='list of clean data files :str')
parser.add_argument('-r', '--reference', dest='reference', required=True, help='reference genome :str')
parser.add_argument('-s', '--genomelength', dest='genomelength', required=True, help='genome length without N :int. This script maybe usefull: /public/work/Pipline/bwa/statFa.py')
parser.add_argument('-c', '--clean', dest='clean', required=False, default='true', help='automatically delete original bam after remove duplication, true or false, default true')
parser.add_argument('-o', '--out', dest='out', required=False, default=os.getcwd(), help='out put path :str, default ./ ')
parser.add_argument('-t', '--thread', dest='thread', required=False, default=4, help='number of threads :int, default 4')
parser.add_argument('-p', '--partition', dest='partition', required=False, default='node01', help='default node01, node queue :str')
parser.add_argument('-b', '--bed', dest='bed', required=False, default='', help='PCR amplification of area')
parser.add_argument('-d', '--snplist', dest='snplist', required=False, default='', help='snp list')
parser.add_argument('-n', '--projectname', dest='projectname', required=True, help='projectname')
parser.add_argument('-a', '--contract', dest='contract', required=True, help='contract')
parser.add_argument('-x', '--chip', dest='chip', required=True, help='chip')
parser.add_argument('-q', '--qiye', dest='qiye', required=False, default='N', help='Determine enterprise user, default:N')





def count_lines(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        return len(file.readlines())

args = parser.parse_args()
input_samplelist = os.path.abspath(args.cleanlist)
ref_genome = args.reference
genomelen = int(args.genomelength)
out = args.out
cpu = int(args.thread)
cpu_half = math.floor(cpu / 2)
partition = args.partition
clean = args.clean.strip().lower()
bed = args.bed
snp_list=args.snplist
sup_num=count_lines(snp_list)
project = args.projectname
hetong = args.contract
xinpian = args.chip
userclass = args.qiye

opj = os.path.join
plink = '/work/share/ac8t81mwbn/soft/PLINK/plink'
java_env = 'export JAVA_HOME=/work/share/ac8t81mwbn/soft/java17/jdk-17.0.12;export PATH=$JAVA_HOME/bin:$PATH;export CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar'
bwa = '/work/share/ac8t81mwbn/soft/bwa-mem2-2.2.1_x64-linux/bin/bwa-mem2.avx2'
samtools = '/work/share/ac8t81mwbn/soft/samtools-1.15.1/samtools'
vcftools = '/work/share/ac8t81mwbn/soft/vcftools/bin/vcftools'
bcftools = '/work/share/ac8t81mwbn/soft/bcftools-1.22/bcftools'
sambamba = '/work/share/ac8t81mwbn/soft/Sambamba_1.0.1/sambamba'
perl = '/usr/bin/perl'
python = '/work/share/ac8t81mwbn/miniforge3/envs/ai/bin/python3'
parse_bwa_stat = '/work/share/ac8t81mwbn/pipline/bwa_mem_pipline/parse_bwa_stat.py'
sge = "/work/share/ac8t81mwbn/pipline/gatk/bin/slurm_Duty.pl"
pandepth="/work/share/ac8t81mwbn/soft/PanDepth-2.26-Linux-x86_64/pandepth"
bamdst="/work/share/ac8t81mwbn/miniforge3/envs/bamdst/bin/bamdst"
gatk="/work/share/ac8t81mwbn/soft/gatk-4.6.2.0/gatk"
gt_stat = "/work/share/ac8t81mwbn/pipline/yexiang_ana3/GT.py"
sample_stat = "/work/share/ac8t81mwbn/pipline/yexiang_ana3/sample.stat.py"
snp_stat = "/work/share/ac8t81mwbn/pipline/yexiang_ana3/snp_stat.py"
png = "/work/share/ac8t81mwbn/pipline/yexiang_ana3/genotype_boxplot.py"
output_dir = os.getcwd()
tmpdir = opj(output_dir, 'tmp')
bindir = opj(output_dir,'00.bin')

bwa_dir = opj(output_dir,'01.BWA')
bam_dir = opj(bwa_dir, 'bam')
stat_dir = opj(bwa_dir, 'stat_tmp')
unmap_dir = opj(bwa_dir, 'unmapped')
result_dir = opj(bwa_dir, 'result')
vcfstat_dir = opj(bwa_dir, 'vcfstat')
report_dir = opj(output_dir, 'report')
SNP_dir = opj(report_dir, 'SNP')
mSNP_dir = opj(report_dir, 'mSNP')
finalstat_dir = opj(report_dir, 'stat')

if not os.path.exists(bindir):
    os.mkdir(bindir)

if not os.path.exists(bwa_dir):
    os.mkdir(bwa_dir)

if not os.path.exists(tmpdir):
    os.mkdir(tmpdir)

if not os.path.exists(bam_dir):
    os.mkdir(bam_dir)

if not os.path.exists(stat_dir):
    os.mkdir(stat_dir)

if not os.path.exists(unmap_dir):
    os.mkdir(unmap_dir)

if not os.path.exists(result_dir):
    os.mkdir(result_dir)

if not os.path.exists(vcfstat_dir):
    os.mkdir(vcfstat_dir)

if not os.path.exists(report_dir):
    os.mkdir(report_dir)

if not os.path.exists(SNP_dir):
    os.mkdir(SNP_dir)

if userclass == "N":
    if not os.path.exists(mSNP_dir):
        os.mkdir(mSNP_dir)

if not os.path.exists(finalstat_dir):
    os.mkdir(finalstat_dir)


if os.path.exists('bwa_mem.work.sh'):
    os.remove('bwa_mem.work.sh')

bwamem_file = opj(bindir, 'bwa_mem.sh')
gatk_file = opj(bindir, 'gatk_call.sh')
work_shell_file = opj(bindir, "work.sh")
clean_shell_file = opj(bindir, 'clean.sh')
stat_file = opj(bindir, 'stat.sh')
report_file = opj(bindir, 'report.sh')
sample_dict = {}
with open(input_samplelist) as samplelist, open(bwamem_file, 'w') as bwamem_shell, open(work_shell_file, "w") as work_shell, open(gatk_file, "w") as gatk_shell, open(stat_file, "w") as stat_shell, open(report_file, "w") as report_shell:
    gvcf_list=[]
    for line in samplelist:
        line = line.strip().split('\t')
        length = len(line)
        if length == 3:
            sample_name = line[0]
            read1 = line[1]
            read2 = line[2]
        elif length == 2:
            sample_name = line[0]
            read1 = line[1]
            read2 = ""

        else:
            raise ValueError("样本必须是单端或者双端reads")

        sample_dict[sample_name] = [[read1, read2]]

    for sample_name in sample_dict.keys():
        gvcf_list.append("-V %s/%s.g.vcf.gz" %(bam_dir,sample_name))
        reads_list = sample_dict[sample_name]
        len_reads_list = len(reads_list)

        read1 = reads_list[0][0]
        read2 = reads_list[0][1]
        bwa_shell = f"""{bwa} mem -t {cpu} -R '@RG\\tID:{sample_name}\\tPL:illumina\\tSM:{sample_name}' {ref_genome} {read1} {read2} | {samtools} sort -@ {cpu_half} -m 2G --output-fmt BAM -o {bam_dir}/{sample_name}.sorted.bam && {samtools} index -@ {cpu} {bam_dir}/{sample_name}.sorted.bam && {samtools} quickcheck {bam_dir}/{sample_name}.sorted.bam && {samtools} view -b -f 4 -@ {cpu} {bam_dir}/{sample_name}.sorted.bam > {unmap_dir}/{sample_name}.unmap.bam && {samtools} stat --coverage 1,100,1 -@ {cpu} {bam_dir}/{sample_name}.sorted.bam > {stat_dir}/{sample_name}.bwa.stat && {samtools} rmdup {bam_dir}/{sample_name}.sorted.bam {bam_dir}/{sample_name}.sorted.rmdup.bam && {samtools} index -@ {cpu} {bam_dir}/{sample_name}.sorted.rmdup.bam
{java_env};{gatk} --java-options "-Xmx15G"  HaplotypeCaller -R {ref_genome} -ERC GVCF -I {bam_dir}/{sample_name}.sorted.rmdup.bam -O {bam_dir}/{sample_name}.g.vcf.gz -L {bed} --do-not-run-physical-phasing"""
        stat_cmd1 = f"""{pandepth} -i {bam_dir}/{sample_name}.sorted.rmdup.bam -b {bed} -o {bam_dir}/{sample_name}
mkdir -p {bam_dir}/{sample_name}_buhuo_stat
{bamdst} -p {bed} -o {bam_dir}/{sample_name}_buhuo_stat  {bam_dir}/{sample_name}.sorted.rmdup.bam
{samtools} depth -b {snp_list} {bam_dir}/{sample_name}.sorted.rmdup.bam >  {bam_dir}/{sample_name}.snp.depth.xls
zcat {bam_dir}/{sample_name}.bed.stat.gz > {result_dir}/{sample_name}.bed.stat.xls\n"""
        if clean == 'true':
            bwa_shell = f'{bwa_shell} && rm {bam_dir}/{sample_name}.sorted.bam {bam_dir}/{sample_name}.sorted.bam.bai\n'
        else:
            bwa_shell = f'{bwa_shell}\n'
        bwamem_shell.write(bwa_shell)
        stat_shell.write(stat_cmd1)
    tmp=' '.join(gvcf_list)
    gatk_shell.write(f"""{java_env};{gatk} --java-options "-Xmx15G" CombineGVCFs -R {ref_genome} {tmp} -O {result_dir}/final.gvcf.gz && {gatk} --java-options "-Xmx15G" GenotypeGVCFs --include-non-variant-sites -R {ref_genome} -V  {result_dir}/final.gvcf.gz -O {result_dir}/final.vcf.gz && {vcftools} --gzvcf {result_dir}/final.vcf.gz --positions {snp_list} --recode --stdout | /public/software/Biosoft/htslib-1.15.1-gcc485/bin/bgzip -c > {result_dir}/final.chip.vcf.gz\n""")
    work_shell_tmp = f'''{perl} {sge} --interval 30 --maxjob 500 --convert no  --lines 2 --partition {partition} --reslurm --mem {math.ceil(cpu*4.1)}G --cpu {cpu} {bwamem_file}\n'''
    work_shell.write(work_shell_tmp)
#    work_shell.write("""grep "Fraction of Target Reads in all reads" %s/*/coverage.report |awk '{print $1"\\t"$NF}'|sed 's/_/\\t/g' |cut -f 1,4 > %s/捕获效率统计.xls\n"""  %(bam_dir,result_dir))
    work_shell.write(f"""{perl} {sge} --interval 30 --maxjob 10  --convert no  --lines 6 --partition {partition} --reslurm --mem 30G --cpu 4 {gatk_file}\n""")
    work_shell.write(f"""{perl} {sge} --interval 30 --maxjob 10  --convert no  --lines 5 --partition {partition} --reslurm --mem 82G --cpu 20 {stat_file}\n""")
    report_shell.write("""cat %s |cut -f 1  |while read line ;do echo -ne "$line\\t" && grep "Fraction of Target Reads in all reads" %s/${line}_buhuo_stat/coverage.report |awk '{gsub(/%%/, "", $NF); print $NF}';done > %s/捕获效率统计.xls\n"""  %(input_samplelist,bam_dir,result_dir))
    #work_shell.write("""sed -i '1i样本名称\\t捕获效率' %s/捕获效率统计.xls\n""" %(result_dir))
    report_shell.write("""sed -i '1iSample\\tCapture_rate(%%)' %s/捕获效率统计.xls\n""" % (result_dir))
    report_shell.write("""cat %s  |cut -f 1  |while read line ;do echo -ne "$line\\t" && zcat %s/$line.bed.stat.gz |tail -1 ;done | awk '{print $1"\\t"$7"\\t"$9}' > %s/探针覆盖区域统计.xls\n""" %(input_samplelist,bam_dir,result_dir))
    #work_shell.write("""sed -i '1i样本名称\\t覆盖度\\t平均深度' %s/探针覆盖区域统计.xls\n""" %(result_dir))
    report_shell.write("""sed -i '1iSample\\tCoverage(%%)\\tAverage_depth' %s/探针覆盖区域统计.xls\n""" % (result_dir))
    report_shell.write(f"""cat {input_samplelist} | cut -f 1 | while read line; do  echo -ne "$line\\t" &&  awk -v lines1=$(wc -l < {snp_list}) -v lines2=$(wc -l < {bam_dir}/"$line".snp.depth.xls) 'BEGIN {{ printf "%.2f\\n", (lines2/lines1)*100 }}' ; done > {result_dir}/位点检出统计.xls\n""")
    #work_shell.write(f"""sed -i '1i样本名称\\t位点检出率' {result_dir}/位点检出统计.xls\n""")
    report_shell.write(f"""sed -i '1iSample\\tSite_detection_rate(%)' {result_dir}/位点检出统计.xls\n""")
    report_shell.write(f"""paste {result_dir}/位点检出统计.xls  {result_dir}/捕获效率统计.xls  {result_dir}/探针覆盖区域统计.xls | cut -f 1,2,4,6,7 > {result_dir}/stat.xls\n""")
    report_shell.write(f"""{python} /work/share/ac8t81mwbn/pipline/yexiang_ana3/parse_bwa_stat.py {input_samplelist} {bwa_dir} {genomelen}\n""")
    work_shell.write(f"""{perl} {sge} --interval 30 --maxjob 10  --convert no  --lines 40 --partition {partition} --reslurm --mem 30G --cpu 4 {report_file}\n""")
    if userclass == "N":
        gatk_shell.write(f"""{bcftools} +setGT {result_dir}/final.chip.vcf.gz -o {result_dir}/final.chip.filtdp.vcf -- -t q -n . -i 'FMT/DP<4'\n""")
        gatk_shell.write(f"""/public/software/Biosoft/htslib-1.15.1-gcc485/bin/bgzip {result_dir}/final.chip.filtdp.vcf\n""")
        gatk_shell.write(f"""{bcftools} view -m2 -M2 -v snps -o {result_dir}/final.filtsnp.vcf.gz -O z {result_dir}/final.vcf.gz\n""")
        gatk_shell.write(f"""{bcftools} +setGT  {result_dir}/final.filtsnp.vcf.gz -o {result_dir}/final.filtdp.vcf -- -t q -n . -i 'FMT/DP<4'\n""")
        gatk_shell.write(f"""/public/software/Biosoft/htslib-1.15.1-gcc485/bin/bgzip {result_dir}/final.filtdp.vcf\n""")
        report_shell.write(f"""{python} {gt_stat} --input {result_dir}/final.chip.filtdp.vcf.gz --output {vcfstat_dir}/chip_GT.xls --min_dp 4\n""")
        report_shell.write(f"""{python} {snp_stat} {vcfstat_dir}/chip_GT.xls {vcfstat_dir}/chip_snp_stat.xls\n""")
        report_shell.write(f"""{python} {sample_stat} {vcfstat_dir}/chip_GT.xls {vcfstat_dir}/chip_sample_stat.xls\n""")
        report_shell.write(f"""{python} {gt_stat} --input {result_dir}/final.filtdp.vcf.gz --output {vcfstat_dir}/mSNP_GT.xls --min_dp 4\n""")
        report_shell.write(f"""{python} {snp_stat} {vcfstat_dir}/mSNP_GT.xls {vcfstat_dir}/mSNP_snp_stat.xls\n""")
        report_shell.write(f"""{python} {sample_stat} {vcfstat_dir}/mSNP_GT.xls {vcfstat_dir}/mSNP_sample_stat.xls\n""")
        report_shell.write(f"""cat {result_dir}/bwa_result.xls | cut -f 1,2,4,7 > {finalstat_dir}/bwa_result.xls\n""")
        report_shell.write(f"""cp {result_dir}/stat.xls {finalstat_dir}\n""")
        report_shell.write(f"""cp {vcfstat_dir}/chip_GT.xls  {SNP_dir}\n""")
        report_shell.write(f"""cp {vcfstat_dir}/chip_snp_stat.xls  {SNP_dir}\n""")
        report_shell.write(f"""cp {vcfstat_dir}/chip_sample_stat.xls  {SNP_dir}\n""")
        report_shell.write(f"""cp {vcfstat_dir}/mSNP_GT.xls  {mSNP_dir}\n""")
        report_shell.write(f"""cp {vcfstat_dir}/mSNP_snp_stat.xls  {mSNP_dir}\n""")
        report_shell.write(f"""cp {vcfstat_dir}/mSNP_sample_stat.xls  {mSNP_dir}\n""")
        report_shell.write(f"""{python} {png} --snp_stat {SNP_dir}/chip_snp_stat.xls --spl_stat {SNP_dir}/chip_sample_stat.xls --outpath {SNP_dir}\n""")
        report_shell.write(f"""{python} {png} --snp_stat {mSNP_dir}/mSNP_snp_stat.xls --spl_stat {mSNP_dir}/mSNP_sample_stat.xls --outpath {mSNP_dir}\n""")
        report_shell.write(f"""cat {mSNP_dir}/mSNP_snp_stat.xls | cut -f 1,2,3,5,6,7,8 > {mSNP_dir}/mSNP_snp_stat.xls1\n""")
        report_shell.write(f"""mv {mSNP_dir}/mSNP_snp_stat.xls1 {mSNP_dir}/mSNP_snp_stat.xls\n""")
        report_shell.write(f"""cat {mSNP_dir}/mSNP_sample_stat.xls | cut -f 1,4,5,6,7,8,9 > {mSNP_dir}/mSNP_sample_stat.xls1\n""")
        report_shell.write(f"""mv {mSNP_dir}/mSNP_sample_stat.xls1 {mSNP_dir}/mSNP_sample_stat.xls\n""")
        report_command = f"""required_files=("{finalstat_dir}/bwa_result.xls" "{finalstat_dir}/stat.xls" "{SNP_dir}/chip_GT.xls" "{SNP_dir}/chip_snp_stat.xls" "{SNP_dir}/chip_sample_stat.xls" "{mSNP_dir}/mSNP_GT.xls" "{mSNP_dir}/mSNP_snp_stat.xls" "{mSNP_dir}/mSNP_sample_stat.xls" "{SNP_dir}/sample_boxplot.png" "{SNP_dir}/snp_boxplot.png"); all_files_exist=true; for file in "${{required_files[@]}}"; do if [ ! -f "$file" ]; then echo "错误: 必需文件不存在: $file"; all_files_exist=false; fi; done; if $all_files_exist; then echo "所有必需文件已就绪，开始生成报告..."; /work/share/ac8t81mwbn/miniforge3/envs/ai/bin/python /work/share/ac8t81mwbn/pipline/yexiang_ana3/report/yexiang_genohtml.py -d {report_dir} -p {xinpian} -n {project} -c {hetong} -o {report_dir} --template /work/share/ac8t81mwbn/pipline/yexiang_ana3/report/template/full_report.html --src-dir /work/share/ac8t81mwbn/pipline/yexiang_ana3/report/src/ --copy-static; else echo "错误: 缺少必需文件，无法生成报告"; exit 1; fi\n"""
        report_shell.write(report_command)
    else:
        gatk_shell.write(f"""{bcftools} +setGT {result_dir}/final.chip.vcf.gz -o {result_dir}/final.chip.filtdp.vcf -- -t q -n . -i 'FMT/DP<4'\n""")
        gatk_shell.write(f"""/public/software/Biosoft/htslib-1.15.1-gcc485/bin/bgzip {result_dir}/final.chip.filtdp.vcf\n""")
        gatk_shell.write(f"""sample_count=$({bcftools} query -l {result_dir}/final.chip.filtdp.vcf.gz | wc -l) && {plink} --vcf {result_dir}/final.chip.filtdp.vcf.gz --recode --out "{result_dir}/{hetong}-${{sample_count}}例样本检测结果" --allow-extra-chr --chr-set 80 --double-id && cd {result_dir} && mkdir {hetong}-${{sample_count}}例样本检测结果 && mv {hetong}-${{sample_count}}例样本检测结果.ped {hetong}-${{sample_count}}例样本检测结果 && mv {hetong}-${{sample_count}}例样本检测结果.map {hetong}-${{sample_count}}例样本检测结果 && zip -r {hetong}-${{sample_count}}例样本检测结果.zip {hetong}-${{sample_count}}例样本检测结果\n""")
        report_shell.write(f"""{python} {gt_stat} --input {result_dir}/final.chip.filtdp.vcf.gz --output {vcfstat_dir}/chip_GT.xls --min_dp 4\n""")
        report_shell.write(f"""{python} {snp_stat} {vcfstat_dir}/chip_GT.xls {vcfstat_dir}/chip_snp_stat.xls\n""")
        report_shell.write(f"""{python} {sample_stat} {vcfstat_dir}/chip_GT.xls {vcfstat_dir}/chip_sample_stat.xls\n""")
        report_shell.write(f"""cat {result_dir}/bwa_result.xls | cut -f 1,2,4,7 > {finalstat_dir}/bwa_result.xls\n""")
        report_shell.write(f"""cp {result_dir}/stat.xls {finalstat_dir}\n""")
        report_shell.write(f"""cp {vcfstat_dir}/chip_GT.xls  {SNP_dir}\n""")
        report_shell.write(f"""cp {vcfstat_dir}/chip_snp_stat.xls  {SNP_dir}\n""")
        report_shell.write(f"""cp {vcfstat_dir}/chip_sample_stat.xls  {SNP_dir}\n""")
        report_shell.write(f"""{python} {png} --snp_stat {SNP_dir}/chip_snp_stat.xls --spl_stat {SNP_dir}/chip_sample_stat.xls --outpath {SNP_dir}\n""")
        report_command = f"""required_files=("{finalstat_dir}/bwa_result.xls" "{finalstat_dir}/stat.xls" "{SNP_dir}/chip_GT.xls" "{SNP_dir}/chip_snp_stat.xls" "{SNP_dir}/chip_sample_stat.xls" "{SNP_dir}/sample_boxplot.png" "{SNP_dir}/snp_boxplot.png"); all_files_exist=true; for file in "${{required_files[@]}}"; do if [ ! -f "$file" ]; then echo "错误: 必需文件不存在: $file"; all_files_exist=false; fi; done; if $all_files_exist; then echo "所有必需文件已就绪，开始生成报告..."; /work/share/ac8t81mwbn/miniforge3/envs/ai/bin/python /work/share/ac8t81mwbn/pipline/yexiang_ana3/qiyereport/yexiang_genohtml.py -d {report_dir} -p {xinpian} -n {project} -c {hetong} -o {report_dir} --template /work/share/ac8t81mwbn/pipline/yexiang_ana3/qiyereport/template/full_report.html --src-dir /work/share/ac8t81mwbn/pipline/yexiang_ana3/qiyereport/src/ --copy-static; else echo "错误: 缺少必需文件，无法生成报告"; exit 1; fi\n"""
        report_shell.write(report_command)
