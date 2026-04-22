#！/usr/bin/env python3
from __future__ import annotations
from asyncio import StreamReaderProtocol
import os
import json
from pathlib import Path
from dotenv import load_dotenv
import math

ENV_FILE = (Path(__file__).parent / ".env").resolve()
load_dotenv(ENV_FILE)

class AnalysisPipePrinter():
    def __init__(self, sample_list:set[str], config_file: Path):
        """
        config_file: 配置文件路径
        sample_info: 样本信息，字典类型，key为样本名，value为r1, r2路径的列表
        """
        self.config = json.loads(config_file.read_text())
        self.sample_list = sample_list
        self.map_file = Path(self.config["map_file"])
        self.batch_name = self.config["batch_name"]
        self.bwa = os.getenv("BWA")
        self.cpu = self.config["cpu"]
        self.ref_genome = self.config["fa"]
        self.samtools = os.getenv("SAMTOOLS")
        self.cpu_half = math.floor(self.cpu / 2)
        self.bam_dir = Path(self.config["out_dir"]) / self.config["batch_name"] / "01.BWA/bam"
        self.stat_dir = Path(self.config["out_dir"]) / self.config["batch_name"] / "01.BWA/stat_tmp"
        self.unmap_dir = Path(self.config["out_dir"]) / self.config["batch_name"] / "01.BWA/unmapped"
        self.result_dir = Path(self.config["out_dir"]) / self.config["batch_name"] / "01.BWA/result"
        self.vcfstat_dir = Path(self.config["out_dir"]) / self.config["batch_name"] / "01.BWA/vcfstat"
        self.report_dir = Path(self.config["out_dir"]) / self.config["batch_name"] / "report"
        self.snp_list = self.config["snp_list"]
        self.gatk = os.getenv("GATK")
        self.bed = self.config["bed"]
        self.vcftools = os.getenv("VCFTOOLS")
        self.bgzip = os.getenv("BGZIP")
        self.python3 = os.getenv("PYTHON")
        self.pythonlib = os.getenv("PYTHONLIB")
        self.script_path = os.getenv("SCRIPT_PATH")
        self.pos_gt = self.config["pos_gt"]
        self.bcftools = os.getenv("BCFTOOLS")
        self.pandepth = os.getenv("PANDEPTH")
        self.bamdst = os.getenv("BAMDST")
        self.genome_length = self.config["genome_length"]
        self.plink = os.getenv("PLINK")

    def _get_sample_fq_dict(self) -> dict[str, tuple[Path, Path]]:
        "获取样本名与fq文件路径的映射"
        sample_fq_dict = {}
        with open(self.map_file, "r") as inf:
            for each in inf:
                sample, r1, r2 = each.strip().split("\t")
                sample_fq_dict[sample] = [r1, r2]
        return sample_fq_dict

    def print_single_step(self, script_file: Path):
        "单样本任务: 打印单样本的bwa分析到生成vcf及bam stat的脚本"
        sample_fq_dict = self._get_sample_fq_dict()
        with open(script_file, 'w') as outf:
            for sample_name in self.sample_list:
                read1 = sample_fq_dict[sample_name][0]
                read2 = sample_fq_dict[sample_name][1]
                if not self.bam_dir.exists():
                    self.bam_dir.mkdir(parents=True, exist_ok=True)
                if not self.stat_dir.exists():
                    self.stat_dir.mkdir(parents=True, exist_ok=True)
                if not self.unmap_dir.exists():
                    self.unmap_dir.mkdir(parents=True, exist_ok=True)
                outf.write(
                    f"""
                    export PYTHONPATH=$PYTHONPATH:{self.pythonlib}
                    {self.bwa} mem -t {self.cpu} -R '@RG\\tID:{sample_name}\\tPL:illumina\\tSM:{sample_name}' {self.ref_genome} {read1} {read2} | {self.samtools} sort -@ {self.cpu_half} -m 2G --output-fmt BAM -o {self.bam_dir}/{sample_name}.sorted.bam && {self.samtools} index -@ {self.cpu} {self.bam_dir}/{sample_name}.sorted.bam && {self.samtools} quickcheck {self.bam_dir}/{sample_name}.sorted.bam && {self.samtools} view -b -f 4 -@ {self.cpu} {self.bam_dir}/{sample_name}.sorted.bam > {self.unmap_dir}/{sample_name}.unmap.bam && {self.samtools} stat --coverage 1,100,1 -@ {self.cpu} {self.bam_dir}/{sample_name}.sorted.bam > {self.stat_dir}/{sample_name}.bwa.stat && {self.samtools} rmdup {self.bam_dir}/{sample_name}.sorted.bam {self.bam_dir}/{sample_name}.sorted.rmdup.bam && {self.samtools} index -@ {self.cpu} {self.bam_dir}/{sample_name}.sorted.rmdup.bam
                    {self.samtools} depth -b {self.snp_list} {self.bam_dir}/{sample_name}.sorted.rmdup.bam >  {self.bam_dir}/{sample_name}.snp.depth.xls
                    export JAVA_HOME={os.getenv("JAVA_HOME")};export PATH=$JAVA_HOME/bin:$PATH;export CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar;{self.gatk} --java-options "-Xmx15G"  HaplotypeCaller -R {self.ref_genome} -ERC GVCF -I {self.bam_dir}/{sample_name}.sorted.rmdup.bam -O {self.bam_dir}/{sample_name}.g.vcf.gz -L {self.bed} --do-not-run-physical-phasing --tmp-dir {self.config["out_dir"]}/tmp
                    export JAVA_HOME={os.getenv("JAVA_HOME")};export PATH=$JAVA_HOME/bin:$PATH;export CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar;{self.gatk} --java-options "-Xmx15G"  GenotypeGVCFs -R {self.ref_genome} -V {self.bam_dir}/{sample_name}.g.vcf.gz -O {self.bam_dir}/{sample_name}.vcf.gz
                    {self.vcftools} --gzvcf {self.bam_dir}/{sample_name}.vcf.gz --positions {self.snp_list} --recode --stdout | {self.bgzip} -c > {self.bam_dir}/{sample_name}.chip.vcf.gz
                    {self.python3} {self.script_path}/fill_missing_sites_from_allpos_Version6.py --allpos {self.pos_gt} --depth {self.bam_dir}/{sample_name}.snp.depth.xls --vcf {self.bam_dir}/{sample_name}.chip.vcf.gz --out {self.bam_dir}/{sample_name}.fill.vcf
                    {self.bgzip} {self.bam_dir}/{sample_name}.fill.vcf
                    {self.bcftools} index {self.bam_dir}/{sample_name}.fill.vcf.gz && rm {self.bam_dir}/{sample_name}.sorted.bam {self.bam_dir}/{sample_name}.sorted.bam.bai
                    {self.pandepth} -i {self.bam_dir}/{sample_name}.sorted.rmdup.bam -b {self.bed} -o {self.bam_dir}/{sample_name}
                    mkdir -p {self.bam_dir}/{sample_name}_buhuo_stat
                    {self.bamdst} -p {self.bed} -o {self.bam_dir}/{sample_name}_buhuo_stat  {self.bam_dir}/{sample_name}.sorted.rmdup.bam
                    {self.samtools} depth -b {self.snp_list} {self.bam_dir}/{sample_name}.sorted.rmdup.bam >  {self.bam_dir}/{sample_name}.snp.depth.xls
                    if [ $? == 0 ];then
                        {self.python3} -m task_monitor update --sample {sample_name} --status done
                    else
                        {self.python3} -m task_monitor update --sample {sample_name} --status fail
                    fi
                    """
                )

    def print_batch_step(self, script_file: Path, vcf_list: list):
        "批次任务:所有样本都跑完比对后打印gvcf合并输出的脚本"
        if not self.result_dir.exists():
            self.result_dir.mkdir(parents=True, exist_ok=True)

        with open(script_file, 'w') as outf:
            outf.write(
                f"""
                ulimit -n 10000
                {self.bcftools} merge -m all {" ".join(vcf_list)} -O z -o {self.result_dir}/final.chip.vcf.gz && {self.bcftools} index {self.result_dir}/final.chip.vcf.gz
                {self.python3} {self.script_path}/fix.py {self.result_dir}/final.chip.vcf.gz {self.pos_gt} {self.result_dir}/final.chip.2M.vcf
                {self.bgzip} {self.result_dir}/final.chip.2M.vcf
                {self.bcftools} index {self.result_dir}/final.chip.2M.vcf.gz
                {self.bcftools} +setGT {self.result_dir}/final.chip.2M.vcf.gz -o {self.result_dir}/final.chip.filtdp.vcf -- -t q -n . -i 'FMT/DP<4'
                {self.bgzip} {self.result_dir}/final.chip.filtdp.vcf
                {self.bcftools} index {self.result_dir}/final.chip.filtdp.vcf.gz
                sample_count=$({self.bcftools} query -l {self.result_dir}/final.chip.filtdp.vcf.gz | wc -l) && {self.plink} --vcf {self.result_dir}/final.chip.filtdp.vcf.gz --make-bed --out "{self.result_dir}/{self.batch_name}-${{sample_count}}例样本检测结果" --allow-extra-chr --chr-set 80 --double-id && {self.plink} --vcf {self.result_dir}/final.chip.filtdp.vcf.gz --recode --out "{self.result_dir}/{self.batch_name}-${{sample_count}}例样本检测结果" --allow-extra-chr --chr-set 80 --double-id && cd {self.result_dir} && mkdir {self.batch_name}-${{sample_count}}例样本检测结果 && mv {self.batch_name}-${{sample_count}}例样本检测结果.ped {self.batch_name}-${{sample_count}}例样本检测结果 && mv {self.batch_name}-${{sample_count}}例样本检测结果.map {self.batch_name}-${{sample_count}}例样本检测结果 && zip -r {self.batch_name}-${{sample_count}}例样本检测结果.zip {self.batch_name}-${{sample_count}}例样本检测结果
                """
            )
        self.sample_num = len(vcf_list)

    def print_report_step(self, script_file: Path):
        "打印生成报告脚本"
        if not self.vcfstat_dir.exists():
            self.vcfstat_dir.mkdir(parents=True, exist_ok=True)
        if not self.report_dir.exists():
            self.report_dir.mkdir(parents=True, exist_ok=True)
        if not (self.report_dir / "stat").exists():
            (self.report_dir / "stat").mkdir(parents=True, exist_ok=True)
        if not (self.report_dir / "SNP").exists():
            (self.report_dir / "SNP").mkdir(parents=True, exist_ok=True)

        with open(script_file, 'w') as outf:
            outf.write(
                f"""
                cat {self.map_file} | cut -f 1 | while read line; do echo -ne "$line\t" && grep "Fraction of Target Reads in all reads" {self.bam_dir}/${{line}}_buhuo_stat/coverage.report | awk '{{gsub(/%/, "", $NF); print $NF}}'; done > {self.stat_dir}/捕获效率统计.xls
                sed -i '1iSample\tCapture_rate(%)' {self.result_dir}/捕获效率统计.xls
                cat {self.map_file}  |cut -f 1  |while read line ;do echo -ne "$line\t" && zcat {self.bam_dir}/$line.bed.stat.gz |tail -1 ;done | awk '{{print $1"\t"$7"\t"$9}}' > {self.result_dir}/探针覆盖区域统计.xls
                sed -i '1iSample\tCoverage(%)\tAverage_depth' {self.result_dir}/探针覆盖区域统计.xls
                cat {self.map_file} | cut -f 1 | while read line; do  echo -ne "$line\t" &&  awk -v lines1=$(wc -l < {self.snp_list}) -v lines2=$(wc -l < {self.bam_dir}/"$line".snp.depth.xls) 'BEGIN {{ printf "%.2f\n", (lines2/lines1)*100 }}' ; done > {self.result_dir}/位点检出统计.xls
                sed -i '1iSample\tSite_detection_rate(%)' {self.result_dir}/位点检出统计.xls
                paste {self.result_dir}/位点检出统计.xls  {self.result_dir}/捕获效率统计.xls  {self.result_dir}/探针覆盖区域统计.xls | cut -f 1,2,4,6,7 > {self.result_dir}/stat.xls
                {self.python3} {self.script_path}/parse_bwa_stat.py {self.map_file} {Path(self.config["out_dir"]) / self.config["batch_name"] / "01.BWA"} {self.genome_length}
                {self.python3} {self.script_path}/GT.py --input {self.result_dir}/final.chip.filtdp.vcf.gz --output {self.vcfstat_dir}/chip_GT.xls --min_dp 4
                {self.python3} {self.script_path}/snp_stat.py {self.vcfstat_dir}/chip_GT.xls {self.vcfstat_dir}/chip_snp_stat.xls
                {self.python3} {self.script_path}/sample.stat.py {self.vcfstat_dir}/chip_GT.xls {self.vcfstat_dir}/chip_sample_stat.xls
                cat {self.result_dir}/bwa_result.xls | cut -f 1,2,4,7 > {self.report_dir}/stat/bwa_result.xls
                cp {self.result_dir}/stat.xls {self.report_dir}/stat
                cp {self.vcfstat_dir}/chip_snp_stat.xls  {self.report_dir}/SNP
                cp {self.vcfstat_dir}/chip_sample_stat.xls  {self.report_dir}/SNP
                {self.python3} {self.script_path}/genotype_boxplot.py --snp_stat {self.report_dir}/SNP/chip_snp_stat.xls --spl_stat {self.report_dir}/SNP/chip_sample_stat.xls --outpath {self.report_dir}/SNP
                required_files=("{self.report_dir}/stat/bwa_result.xls" "{self.report_dir}/stat/stat.xls" "{self.report_dir}/SNP/chip_snp_stat.xls" "{self.report_dir}/SNP/chip_sample_stat.xls" "{self.report_dir}/SNP/sample_boxplot.png" "{self.report_dir}/SNP/snp_boxplot.png"); all_files_exist=true; for file in "${{required_files[@]}}"; do if [ ! -f "$file" ]; then echo "错误: 必需文件不存在: $file"; all_files_exist=false; fi; done; if $all_files_exist; then echo "所有必需文件已就绪，开始生成报告..."; {self.python3} {self.script_path}/qiyereport/yexiang_genohtml.py -d {self.report_dir} -p 肉鸡10K育种芯片 -n AI驱动的育种检测体系及智能分析流程构建 -c GZBY20260002-BC01-01 -o {self.report_dir} --template {self.script_path}/qiyereport/template/full_report.html --src-dir {self.script_path}/qiyereport/src/ --copy-static -k AI驱动的育种检测体系及智能分析流程构建 -s {self.sample_num}; else echo "错误: 缺少必需文件，无法生成报告"; exit 1; fi
                """
            )