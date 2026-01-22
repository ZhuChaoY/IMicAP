import utils.my_df_function as utils
from .Function import compare_columns


def align_schemas(file_name_id, save_dir, df):
    # Construct storage path
    utils.create_dir(save_dir)

    # Handle comparison issue
    base_column = 'description'
    support_column = 'other_designations'
    df = compare_columns.compare_process(df, base_column, support_column, save_dir, file_name_id)

    # Handle comparison issue
    base_column = 'product_name'
    support_column = 'description'
    df = compare_columns.compare_process(df, base_column, support_column, save_dir, file_name_id)

    # Check if column name exists before column name comparison
    if 'other_designations' in df.columns.tolist():
        base_column = 'product_name'
        support_column = 'other_designations'
        df = compare_columns.compare_process(df, base_column, support_column, save_dir, file_name_id)

    save_path = save_dir + file_name_id + '_gene_ncbi_bio.tsv'
    utils.save_df(save_path, df)

    return df
