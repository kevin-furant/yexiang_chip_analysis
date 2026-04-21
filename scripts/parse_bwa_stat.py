"""
@author: zhangzhichao
@contact: zhichao.zhang@glbizzia.com
@time: 2023-03-06 12:07 
"""
import os
import sys

args = sys.argv
opj = os.path.join
in_sample_list_file = args[1]
bwa_work_result_path = args[2]
genome_length = int(args[3])
output_file_path_1 = opj(opj(bwa_work_result_path, 'result'), 'bwa_result.xls')
output_file_path_2 = opj(opj(bwa_work_result_path, 'result'), 'bwa_result_release.xls')

sample_list = []
with open(in_sample_list_file) as sample_list_file:
    for line in sample_list_file:
        sample_name = line.strip().split()[0]
        sample_list.append(sample_name)

with open(output_file_path_1, 'w') as output_file1:
    # , open(output_file_path_2, 'w') as output_file2
    coverage_target_list = [1, 2, 5, 10, 15, 20, 30, 40, 50, 100]
    coverage_target_str = '\t'.join(['Coverage_%sX' % dep for dep in coverage_target_list])
    output_file1_title = f'''Sample\tClean_reads\tClean_bases(bp)\tMapped_reads\tmapped_bases(bp)\tmismatch_bases(bp)\tMapping_rate\tmismatch_rate\tAverage_depth\t{coverage_target_str}\n'''
    output_file1.write(output_file1_title)
    for sample_name in sample_list:
        collect_dict = {
    'sequences': '',
    'reads paired': '',
    'reads mapped': '',
    'reads unmapped': '',
    'reads duplicated': '',
    'total length': '',
    'bases mapped': '',
    'mismatches': '',
    'average length': '',
    'pairs on different chromosomes': ''
}
        stat_file_path = opj(opj(bwa_work_result_path, 'stat_tmp'), sample_name + '.bwa.stat')
        coverage_tmp_dict = {}
        with open(stat_file_path) as stat_file:
            for line in stat_file:
                if line.startswith('SN'):
                    line_list = line.strip().split('\t')
                    term = line_list[1].replace(':', '').strip()

                    if collect_dict.get(term) == '':
                        value = int(line_list[2])
                        collect_dict[term] = value
                elif line.startswith('COV'):
                    line_list = line.strip().split('\t')
                    cover_dis = line_list[1]
                    if cover_dis == '[100<]':
                        continue
                    cov_dep = int(line_list[2])
                    cov_base = int(line_list[3])
                    coverage_tmp_dict[cov_dep] = cov_base
        Clean_reads = collect_dict['sequences']
        Clean_bases = collect_dict['total length']
        mapped_reads = collect_dict['reads mapped']
        mapped_bases = collect_dict['bases mapped']
        mismatch_bases = collect_dict['mismatches']
        mapping_rate = '%.2f' % round(mapped_bases / Clean_bases * 100, 2)
        mismatch_rate = '%.2f' % round(mismatch_bases / Clean_bases * 100, 2)
        average_depth = '%.2f' % round(Clean_bases / genome_length, 2)

        coverage_list = []
        for coverage_target in coverage_target_list:
            cov_base = 0
            for cov_dep in coverage_tmp_dict.keys():
                if cov_dep >= coverage_target:
                    cov_base += coverage_tmp_dict[cov_dep]
                else:
                    cov_base += 0
            cov_rate = '%.2f%s' % (round(cov_base/genome_length*100, 2), '%')
            coverage_list.append(cov_rate)
        coverage_str = '\t'.join(coverage_list)
        output_file1_str = f'''{sample_name}\t{Clean_reads}\t{Clean_bases}\t{mapped_reads}\t{mapped_bases}\t{mismatch_bases}\t{mapping_rate}%\t{mismatch_rate}%\t{average_depth}\t{coverage_str}\n'''
        output_file1.write(output_file1_str)



