"""
Editor: INK
Create Time: 2024/6/25 13:12
File Name: filter_data_by_id.py
Function: 
"""
import pandas as pd

import utils.my_df_function as until


# Delete rows that simultaneously meet following conditions
# Simultaneously satisfy NCBI_gene_id has no value and gene_biocyc_id has value
def filter_process(df):
    def conclude_process(ncbi_gene_id, gene_biocyc_id):
        if pd.isnull(ncbi_gene_id) or ncbi_gene_id == 'NA_NO':
            if pd.notna(gene_biocyc_id) and gene_biocyc_id != 'NA_NO':
                return False

        return True

    df['is_keep'] = df.apply(lambda x: conclude_process(x['NCBI_gene_id'], x['gene_biocyc_id']), axis=1)
    # print(df['is_keep'])
    df_filter = df[df['is_keep']]

    if 'is_keep' in df_filter.columns.tolist():
        drop_column = ['is_keep']
        df_filter = df_filter.drop(columns=drop_column)

    return df_filter
