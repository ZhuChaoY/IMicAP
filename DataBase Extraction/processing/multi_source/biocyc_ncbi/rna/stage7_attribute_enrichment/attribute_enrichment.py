import utils.my_df_function as utils

from .Function import combine_ncbi_eco
from .Function import compare_gene_name_and_Symbol
from .Function import compare_Name
from .Function import compare_description
from .Function import combine_dbXref_DBLINKS
from .Function import change_common_name_value
from .Function import combine_synonyms
from .Function import change_column_to_lower
from .Function import deal_multiply_data
from .Function import deal_empty_data
from .Function import change_columns_as_need
from .Function import append_columns
from .Function import check_base_and_position
from .Function import special_value_deal


def enrich_process(
        file_name_id,
        current_scientific_name,
        save_dir,
        df_merge,
        df_biocyc_fail
):
    # Construct storage path
    utils.create_dir(save_dir)

    df_ncbi = df_merge
    df_biocyc = df_biocyc_fail

    # Merge data
    df_merge = combine_ncbi_eco.combine_process(df_ncbi, df_biocyc)
    save_path = save_dir + file_name_id + '_RNA_entity_bioncbi_combine.tsv'
    utils.save_df(save_path, df_merge)

    df_merge = compare_gene_name_and_Symbol.start_combine(df_merge, save_dir)
    # print(df_merge)

    # Compare if PRODUCT-NAME and COMMON-NAME are consistent
    df_merge = compare_Name.compare_process(df_merge, save_dir)
    # ---- Manually added: Delete PRODUCT-NAME regardless of consistency ----
    drop_column = ['PRODUCT-NAME']
    if 'PRODUCT-NAME' in df_merge.columns.tolist():
        df_merge = df_merge.drop(columns=drop_column)
    # print(df_merge.columns)
    df_merge = compare_description.compare_process_of_description(df_merge)
    # ---- Manually added: Delete TYPES_gene regardless of consistency ----
    drop_column = ['TYPES_gene']
    if 'TYPES_gene' in df_merge.columns.tolist():
        df_merge = df_merge.drop(columns=drop_column)

    # Merge dbXref and dblinks
    df_merge = combine_dbXref_DBLINKS.start_combine_process(df_merge)

    # Generate a temporary table-1
    save_path = save_dir + file_name_id + '_RNA_entity_bioncbi_v0_combine_6_1.tsv'
    utils.save_df(save_path, df_merge)

    df_merge = change_common_name_value.change_special_common_name(df_merge)

    df_merge = combine_synonyms.combine_process(df_merge)

    drop_column = ['UniProt-ID']
    df_merge = df_merge.drop(columns=drop_column)

    # Column names all lowercase
    df_merge = change_column_to_lower.change_columns(df_merge)

    # Multi-value processing
    df_merge = deal_multiply_data.deal_multiply_value(df_merge)

    # Empty value replacement
    df_merge = deal_empty_data.fill_none(df_merge)

    # Modify column names as required -1
    df_merge = change_columns_as_need.change_column(df_merge)

    # Modify column names as required -2
    df_merge = change_columns_as_need.change_rna(df_merge)

    # Add new column current_scientific_name
    target_name = current_scientific_name
    df_merge = append_columns.append_process(df_merge, target_name)

    # Handle special request: modify data values for specified li_shan_id
    df_merge = special_value_deal.deal_special_need(df_merge, li_shan_id=file_name_id)

    # Generate a temporary table-2
    save_path = save_dir + file_name_id + '_RNA_entity_bioncbi_v0_combine_6_2.tsv'
    utils.save_df(save_path, df_merge)

    # Check if columns can be deleted
    df_merge = check_base_and_position.start_check(df_merge)

    save_path = save_dir + file_name_id + '_RNA_entity_bioncbi_v0_combine_v0.tsv'
    utils.save_df(save_path, df_merge)

    return df_merge, save_path
