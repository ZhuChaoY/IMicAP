import utils.my_df_function as utils
from .Function import combine_synonyms
from .Function import compare_symbol_gene_name
from .Function import combine_dbxrefs_dblinks
from .Function import combine_synonyms_gene_name


def integrate_synonyms(file_name_id, save_dir, df):

    utils.create_dir(save_dir)

    # When gene_name column has value and symbol column is empty: supplement gene_name value to symbol
    # Filter out where symbol and gene_name are different
    compare_symbol_gene_name.start_filter(df, save_dir)

    # If data simultaneously has synonyms_ncbi column, synonyms_biocyc column and accession_id column
    # Or simultaneously has synonyms, accession
    # Perform merge
    df = combine_synonyms.start_combine_process(df)

    # When symbol and gene_name are different, deduplicate merge of gene_name and synonyms, then delete gene_name column
    df = combine_synonyms_gene_name.start_combine_process(df)

    # Merge dblinks and dbxrefs
    columns_list = df.columns.tolist()
    if 'dblinks' in columns_list and 'dbxrefs' in columns_list:
        df = combine_dbxrefs_dblinks.start_combine_process(df)

    save_path = save_dir + file_name_id + '_gene_ncbi_bio.tsv'
    utils.save_df(save_path, df)

    return df
