#!/public/home/liuzhexin/.local/share/mamba/envs/jupyter/bin/python
# %%
import pandas as pd
import matplotlib.pyplot as plt
from os import sep
import argparse
import matplotlib
matplotlib.use('Agg') 

# %%

# snp_stat = pd.read_csv("/public/work/Personal/gaoying/01.work/06.pipe/06.yexiang/test/snp_stat.xls", sep = '\t')
# spl_stat = pd.read_csv("/public/work/Personal/gaoying/01.work/06.pipe/06.yexiang/test/sample_stat.xls", sep = '\t')
# outpath = 'xxx'


# %%


def draw_box(df: pd.DataFrame, type: str, out_dir: str) -> None:

    if type == 'sample': title = 'Sample level'
    if type ==  'snp':  title = 'Site level'

    labels = [ i.replace('(%)', '') for i in df.columns ]
    colors = ['lightsteelblue', 'mediumseagreen', 'gold', 'plum']

    fig, ax = plt.subplots()
    ax.set_ylabel('Percentage (%)', fontsize = 18)
    ax.set_title(title, fontsize = 20)


    # Change the dot (flier) style here
    bplot = ax.boxplot(
        df,
        patch_artist=True,  # fill with color
        flierprops=dict(
            marker='o',
            markeredgecolor='black',
            alpha=0.5,
            markersize=2,
            linestyle='none'
        ),  # make outliers light gray, dot, partially transparent
        tick_labels=labels
    )
    # fill with colors
    for patch, color in zip(bplot['boxes'], colors):
        patch.set_facecolor(color)
    plt.tight_layout()
    fig.savefig(f"{out_dir}/{type}_boxplot.png", dpi=300, bbox_inches='tight')
    fig.savefig(f"{out_dir}/{type}_boxplot.pdf", dpi=300, bbox_inches='tight')
    plt.close(fig)



def main():



    parser = argparse.ArgumentParser(
        description='Draw boxplot for genotype statistics.',
        epilog='Example usage: python genotype_boxplot.py --snp_stat snp_stat.xls --spl_stat sample_stat.xls --outpath ./output'
    )
    parser.add_argument('--snp_stat', type=str, required=True, 
                        help='Path to the SNP-level statistics file (e.g., snp_stat.xls)')
    parser.add_argument('--spl_stat', type=str, required=True, 
                        help='Path to the sample-level statistics file (e.g., sample_stat.xls)')
    parser.add_argument('--outpath', type=str, required=True, 
                        help='Directory where output boxplots will be saved')
    # Do NOT add a manual '-h'/'--help' argument; argparse adds it automatically.

    args = parser.parse_args()

    snp_stat_path = args.snp_stat
    spl_stat_path = args.spl_stat
    outpath = args.outpath


    columns = ['NA_rate(%)', 'Ref/Ref_rate(%)', 'Ref/Alt_rate(%)', 'Alt/Alt_rate(%)']
    snpcolumns = ['NA_freq(%)', 'Ref/Ref_freq(%)', 'Ref/Alt_freq(%)', 'Alt/Alt_freq(%)']
    
    snp_stat = pd.read_csv(snp_stat_path, sep = '\t')
    snp_df =  snp_stat[snpcolumns]
    spl_stat = pd.read_csv(spl_stat_path, sep = '\t')
    spl_df = spl_stat[columns]

    ## draw sample level plot
    draw_box(spl_df, type='sample', out_dir=outpath)
    print(f"sample level GT stat PNG has been saved at {outpath}/sample_boxplot.png")
    print(f"sample level GT stat PDF has been saved at {outpath}/sample_boxplot.pdf")

    ## draw SNP level plot
    draw_box(snp_df, type='snp', out_dir=outpath)
    print(f"SNP level GT stat PNG has been saved at {outpath}/_boxplot.png")
    print(f"SNP level GT stat PDF has been saved at {outpath}/snp_boxplot.pdf")






if __name__ == '__main__':
    main()
