import utils.my_df_function as utils
import pandas as pd


def combine_fail_success(df_fail, df_success):
    df_final = pd.concat([df_fail, df_success])
    return df_final


def consolidate_process(file_name_id, save_dir, df_ncbi_success, df_ncbi_fail):

    # Construct storage path
    utils.create_dir(save_dir)

    # Merge data
    df_final = combine_fail_success(df_ncbi_success, df_ncbi_fail)
    save_path = save_dir + file_name_id + '_RNA_entity_bioncbi_combine.tsv'
    utils.save_df(save_path, df_final)
    return df_final
