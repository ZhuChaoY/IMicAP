"""
Editor: INK
Create Time: 2024/5/31 15:09
File Name: combine_table_NCBI_Biocyc.py
Function: 
"""
import pandas as pd

import utils.my_df_function as until


# Handle situation where two dfs have identical column names
def deal_same_columns(df_ncbi, df_biocyc):
    ncbi_column_list = df_ncbi.columns.to_list()
    biocyc_column_list = df_biocyc.columns.to_list()

    dup_column_list = []

    for column in ncbi_column_list:
        if column in biocyc_column_list:
            dup_column_list.append(column)

    if len(dup_column_list) != 0:
        ncbi_rename_dict = {}
        biocyc_rename_dict = {}
        for column in dup_column_list:
            ncbi_column = column + "_NCBI"
            biocyc_column = column + '_Biocyc'

            d1 = {column: ncbi_column}
            ncbi_rename_dict.update(d1)

            d2 = {column: biocyc_column}
            biocyc_rename_dict.update(d2)

        df_ncbi = df_ncbi.rename(columns=ncbi_rename_dict)
        df_biocyc = df_biocyc.rename(columns=biocyc_rename_dict)

    return df_ncbi, df_biocyc


# First match merge: Merge by matching NCBI table's LocusTag column with Biocyc table's Gene_biocyc_ID (case-insensitive)
def combine_by_LocusTag_and_Gene_biocyc_ID(df_ncbi_gene, df_biocyc_gene):
    """
    :param df_ncbi_gene: NCBI RNA table
    :param df_biocyc_gene: Biocyc RNA table
    :return: Part where NCBI successfully added Biocyc data via LocusTag first time, part where NCBI table failed first LocusTag match
    """
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
    merge_fail_1st = merge_fail_1st.drop(columns=biocyc_column)

    return merge_success_1st, merge_fail_1st


# Second match: For parts where first match failed, merge by matching NCBI table's LocusTag column with Biocyc table's ACCESSION_ID (case-insensitive)
def combine_by_locusTag_and_ACCESSION_ID(merge_success_1st, merge_fail_1st, df_biocyc_gene):
    """
    :param merge_success_1st: NCBI first successful match result
    :param merge_fail_1st: NCBI first match failure result
    :param df_biocyc_gene: Biocyc rna data
    :return: Sum of NCBI first and second successful match results, NCBI second match failure result
    """
    # Determine if ACCESSION_ID field is available for matching
    columns_list_of_biocyc = df_biocyc_gene.columns.to_list()
    if 'accession_id' not in columns_list_of_biocyc:
        merge_fail_2nd = merge_fail_1st
        merge_success_2nd = merge_success_1st
        return merge_success_2nd, merge_fail_2nd

    biocyc_column = df_biocyc_gene.columns.to_list()

    # Convert to lowercase for case-insensitive matching
    merge_fail_1st['lower_LocusTag'] = merge_fail_1st['locustag'].str.lower()
    df_biocyc_gene['lower_ACCESSION_ID'] = df_biocyc_gene['accession_id'].str.lower()

    # Convert multi-value lower_ACCESSION_ID column to list and expand
    df_biocyc_gene['lower_ACCESSION_ID'] = df_biocyc_gene.apply(lambda x: until.
                                                                transform_back_to_list(x['lower_ACCESSION_ID']), axis=1)
    df_biocyc_gene_exploded = df_biocyc_gene.explode('lower_ACCESSION_ID')

    # Merge operation
    merge_2nd_df = pd.merge(
        left=merge_fail_1st,
        right=df_biocyc_gene_exploded,
        left_on='lower_LocusTag',
        right_on='lower_ACCESSION_ID',
        how='left',
        indicator=True
    )

    # Clean temporary columns
    lower_columns = ['lower_LocusTag', 'lower_ACCESSION_ID']
    merge_2nd_df = merge_2nd_df.drop(columns=lower_columns)

    # Reference column names for merge result
    drop_indicator_columns = ['_merge']

    # Distinguish successful and failed match parts: NCBI part successful in 2nd match
    merge_success_2nd = merge_2nd_df[merge_2nd_df['_merge'] == 'both']
    merge_success_2nd = merge_success_2nd.drop(columns=drop_indicator_columns)

    # Merge successfully matched data: Total of NCBI data successfully matched via 1st and 2nd
    ncbi_total_success_matched_df = pd.concat([merge_success_2nd, merge_success_1st])

    # NCBI data that failed in 2nd match
    merge_fail_2nd = merge_2nd_df[merge_2nd_df['_merge'] == 'left_only']
    merge_fail_2nd = merge_fail_2nd.drop(columns=biocyc_column)
    merge_fail_2nd = merge_fail_2nd.drop(columns=drop_indicator_columns)

    # Find Biocyc genes not successfully supplemented into NCBI: Biocyc part not successfully added to NCBI, where Gene_biocyc_ID exists only in Biocyc table, not in merge result
    list_biocyc_id_of_merge = ncbi_total_success_matched_df['gene_biocyc_id'].to_list()
    fail_biocyc_gene_df = df_biocyc_gene[-df_biocyc_gene['gene_biocyc_id'].isin(list_biocyc_id_of_merge)]

    # Merge NCBI matched success and matched failure parts
    final_merge_result = pd.concat([ncbi_total_success_matched_df, merge_fail_2nd, fail_biocyc_gene_df])

    # Return result
    return final_merge_result, merge_fail_2nd, ncbi_total_success_matched_df, fail_biocyc_gene_df


# Store data
def save_data_process(success_merge_after_twice, ncbi_rna_fail_1st_3rd_df, biocyc_rna_fail_1st_3rd_df,
                      path_save_success, path_fail_ncbi, path_fail_biocyc):
    until.save_df(path_save_success, success_merge_after_twice)

    until.save_df(path_fail_ncbi, ncbi_rna_fail_1st_3rd_df)

    until.save_df(path_fail_biocyc, biocyc_rna_fail_1st_3rd_df)


# Start entire merge process
def start_combine_process(df_ncbi, df_biocyc,
                          save_dir,
                          path_save_success, path_fail_ncbi, path_fail_biocyc):
    df_ncbi_gene = df_ncbi
    df_biocyc_gene = df_biocyc.copy()
    # print(len(df_biocyc_gene))
    until.create_dir(save_dir)

    # Get: Part where NCBI successfully added Biocyc data via LocusTag first time, part where NCBI table failed first LocusTag match
    ncbi_1st_success_df, ncbi_1st_fail_df = combine_by_LocusTag_and_Gene_biocyc_ID(df_ncbi_gene,
                                                                                   df_biocyc_gene)

    # Get: Part where NCBI successfully supplemented data via LocusTag column, ACCESSION_ID column, part where NCBI failed both supplements, part where Biocyc not successfully supplemented into NCBI
    df_biocyc_gene = df_biocyc.copy()
    # print(len(df_biocyc_gene))
    final_merge_result, merge_fail_2nd, ncbi_total_success_matched_df, fail_biocyc_gene_df = \
        combine_by_locusTag_and_ACCESSION_ID(
            merge_success_1st=ncbi_1st_success_df, merge_fail_1st=ncbi_1st_fail_df, df_biocyc_gene=df_biocyc_gene)

    # Store
    save_data_process(final_merge_result, merge_fail_2nd, fail_biocyc_gene_df,
                      path_save_success, path_fail_ncbi, path_fail_biocyc)

    return ncbi_total_success_matched_df, merge_fail_2nd, fail_biocyc_gene_df
