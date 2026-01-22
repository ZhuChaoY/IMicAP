import utils.my_df_function as utils
from .Function import change_columns


def fuse_cross_references(file_name_id, save_dir, df):

    utils.create_dir(save_dir)

    # Rename column names
    df = change_columns.rename_process(df)

    # Generate table
    temp_save_path = save_dir + file_name_id + '_gene_ncbi_bio.tsv'
    utils.save_df(temp_save_path, df)

    return df




