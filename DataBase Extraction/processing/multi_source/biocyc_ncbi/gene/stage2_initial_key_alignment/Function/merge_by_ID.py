"""
Editor: INK
Create Time: 2024/6/3 9:36
File Name: merge_by_ID.py
Function: 
"""
import pandas as pd

import utils.my_df_function as until


def depends_on_LocusTag(df_biocyc_gene, df_ncbi_gene):
    biocyc_column = df_biocyc_gene.columns.to_list()

    df_ncbi_gene['lower_LocusTag'] = df_ncbi_gene['locustag'].str.lower()
    df_biocyc_gene['lower_Gene_biocyc_ID'] = df_biocyc_gene['gene_biocyc_id'].str.lower()

    df_merge = pd.merge(left=df_ncbi_gene, right=df_biocyc_gene,
                        left_on='lower_LocusTag', right_on='lower_Gene_biocyc_ID',
                        how='left', indicator=True)

    lower_columns = ['lower_LocusTag', 'lower_Gene_biocyc_ID']
    df_merge = df_merge.drop(columns=lower_columns)

    # Reference column names for merge result
    drop_indicator_columns = ['_merge']

    # Part where NCBI successfully added Biocyc data via LocusTag first time
    merge_success_1st = df_merge[df_merge['_merge'] == 'both']
    merge_success_1st = merge_success_1st.drop(columns=drop_indicator_columns)

    # Part where NCBI table failed first LocusTag match
    merge_fail_1st = df_merge[df_merge['_merge'] == 'left_only']
    merge_fail_1st = merge_fail_1st.drop(columns=drop_indicator_columns)

    df_merge_result = df_merge.drop(columns=drop_indicator_columns)

    return df_merge_result, merge_fail_1st
