import os
import shutil
import utils.my_df_function as utils
from .Function import append_new_columns
from .Function import build_gene_li_shan
from .Function import deal_empty_value
from .Function import deal_multiply_data
from .Function import check_base_and_position


def unify_identifiers(file_name_id, current_scientific_name, species_tax_id, save_dir, df):

    # Storage directory for data results
    if os.path.exists(save_dir):
        shutil.rmtree(save_dir)
    utils.create_dir(save_dir)

    # Comparison of "start_base" "end_base" with "left_end_position" "right_end_position"
    df = check_base_and_position.start_check(df, save_dir)

    # Handle multi-value situation
    df = deal_multiply_data.deal_multiply_value(df)

    # Handle specified column empty values
    df = deal_empty_value.deal_target_columns(df)
    df = deal_empty_value.replace_with_target_str(df)

    # Add new column "gene_lishan_id", value as "GRm" + "NCBI_gene_id"
    df = build_gene_li_shan.start_build_process(df)

    # Add new column "current_scientific_name", value as corresponding value in Reference for this bacteria
    # Add new column "species_tax_id", value as corresponding value in Reference for this bacteria
    df = append_new_columns.append_process(df, current_scientific_name, species_tax_id)

    # Store a copy of result in Data
    save_path = save_dir + file_name_id + '_gene_ncbi_bio.tsv'
    utils.save_df(save_path, df)
    return df
