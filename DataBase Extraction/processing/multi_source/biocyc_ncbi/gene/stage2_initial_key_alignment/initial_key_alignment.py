
import pandas as pd

import utils.my_df_function as utils
from .Function import deal_same_column
from .Function import change_column_to_lower
from .Function import get_combine_situation
from .Function import deal_table_out_join


def merge_by_primary_key(file_name_id,  file_name_save_dir):
    # Construct path for data to be processed
    prepare_data_dir = file_name_save_dir + '/stage1_prepare_source/'
    path_biocyc_gene = prepare_data_dir + '/Biocyc_gene/' + file_name_id + '_gene_bio.tsv'
    path_ncbi_gene = prepare_data_dir + '/NCBI_info/' + file_name_id + '_gene_ncbi.tsv'

    df_biocyc_gene = utils.load_df(path_biocyc_gene)
    df_ncbi_gene = utils.load_df(path_ncbi_gene)

    # Construct storage path
    type_dir = file_name_save_dir
    save_dir = type_dir + '/stage2_initial_key_alignment/'
    utils.create_dir(save_dir)

    # For recording overall processing situation
    path_combine_situation_path = save_dir + file_name_id + '_match_result.txt'

    # Unify column names
    df_ncbi_gene = change_column_to_lower.change_columns(df_ncbi_gene)
    df_biocyc_gene = change_column_to_lower.change_columns(df_biocyc_gene)

    # Unique primary keys for Biocyc and NCBI
    unique_column_biocyc = 'gene_biocyc_id'
    unique_column_ncbi = 'geneid'

    # Handle identical column names to avoid duplicate column issues in subsequent joins
    df_ncbi_gene, df_biocyc_gene = deal_same_column.deal_same_columns(df_ncbi_gene, df_biocyc_gene, save_dir)

    # Merge gene table data from both parts based on Biocyc's gene_biocyc_id and NCBI's locustag
    df_success_1th, df_biocyc_only_1th, df_ncbi_only_1th = deal_table_out_join.outer_join_two_table_1th(
        df_A=df_biocyc_gene,
        df_B=df_ncbi_gene,
        join_column_a='gene_biocyc_id',
        join_column_b='locustag'
    )

    # For parts where 1st match failed, merge gene table data from both parts based on Biocyc's accession_id and NCBI's LocusTag
    df_success_2th, df_biocyc_only_2th, df_ncbi_only_2th = deal_table_out_join.outer_join_two_table_2th(
        df_A=df_biocyc_only_1th,
        df_B=df_ncbi_only_1th,
        join_column_a='accession_id',
        join_column_b='locustag',
        unique_id_a=unique_column_biocyc,
        unique_id_b=unique_column_ncbi
    )

    # Determine which synonyms field in NCBI
    list_column_ncbi = df_ncbi_only_2th.columns.tolist()
    ncbi_synonyms_column = 'synonyms_ncbi'
    if ncbi_synonyms_column not in list_column_ncbi:
        ncbi_synonyms_column = 'synonyms'
    # For parts where 2nd match failed, merge gene table data from both parts based on Biocyc's gene_biocyc_id and NCBI's synonyms_ncbi
    df_success_3th, df_biocyc_only_3th, df_ncbi_only_3th = deal_table_out_join.outer_join_two_table_1th(
        df_A=df_biocyc_only_2th,
        df_B=df_ncbi_only_2th,
        join_column_a='gene_biocyc_id',
        join_column_b=ncbi_synonyms_column
    )

    # Determine which synonyms field in NCBI
    list_column_ncbi = df_ncbi_only_2th.columns.tolist()
    ncbi_synonyms_column = 'synonyms_ncbi'
    if ncbi_synonyms_column not in list_column_ncbi:
        ncbi_synonyms_column = 'synonyms'
    # For parts where 3rd match failed, merge gene table data from both parts based on Biocyc's accession_id and NCBI's synonyms_ncbi
    df_success_4th, df_biocyc_only_4th, df_ncbi_only_4th = deal_table_out_join.outer_join_two_table_4th(
        df_A=df_biocyc_only_3th,
        df_B=df_ncbi_only_3th,
        join_column_a='accession_id',
        join_column_b=ncbi_synonyms_column,
        unique_id_a=unique_column_biocyc,
        unique_id_b=unique_column_ncbi
    )

    # Total successful data count after four-step merging
    df_total_success_matched = pd.concat([df_success_1th, df_success_2th, df_success_3th, df_success_4th])

    # Add ncbi failed; biocyc failed data
    df_final = pd.concat([df_total_success_matched, df_ncbi_only_4th, df_biocyc_only_4th])
    # Store final result
    final_file_name = file_name_id + '_gene_ncbi_bio.tsv'
    save_final_combine_path = save_dir + final_file_name
    utils.save_df(save_final_combine_path, df_final)

    # Get merge details
    df_biocyc_gene = utils.load_df(path_biocyc_gene)
    df_ncbi_gene = utils.load_df(path_ncbi_gene)
    get_combine_situation.get_process(df_biocyc_gene, df_ncbi_gene, path_combine_situation_path,
                                      df_total_success_matched, df_ncbi_only_4th, df_biocyc_only_4th)

    return df_final
