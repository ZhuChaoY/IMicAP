"""
Editor: INK
Create Time: 2024/6/4 13:00
File Name: build_gene_li_shan.py
Function: 
"""
import numpy as np
import pandas as pd

import utils.my_df_function as until


# Add new column "gene_lishan_id", value as "GRm" + "NCBI_gene_id"
# When NCBI_gene_id has value: "GRm" + "NCBI_gene_id"
# When NCBI_gene_id has no value: use "GRm" + "gene_biocyc_id"
def start_build_process(df):
    def lishan_build_process(ncbi_gene_id, gene_biocyc_id):
        if pd.isnull(ncbi_gene_id) and pd.isnull(gene_biocyc_id):
            return np.nan

        if pd.isnull(ncbi_gene_id) or ncbi_gene_id == 'NA_NO':
            li_shan_id = "GRm" + gene_biocyc_id
            # print(li_shan_id)
            return li_shan_id
        else:
            li_shan_id = "GRm" + ncbi_gene_id
            # print(li_shan_id)
            return li_shan_id
    # print(df)
    df['gene_lishan_id'] = df.apply(lambda x: lishan_build_process(x['NCBI_gene_id'], x['gene_biocyc_id']), axis=1)
    return df
