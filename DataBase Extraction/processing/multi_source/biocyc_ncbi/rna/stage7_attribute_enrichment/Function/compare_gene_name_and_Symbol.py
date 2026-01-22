import pandas as pd
import utils.my_df_function as utils


# Start gene_name and Symbol merge
def start_combine(df, save_dir):
    """
    :param df: df to process
    :param save_dir: Data storage path
    :return: Merge result of gene_name and Symbol
    """
    # Filter out data where gene_name, Symbol are both not empty for comparison
    df_conclude = df[df['gene_name'].notnull() & df['symbol'].notnull()]

    # Use .equals() method to determine if two columns are completely identical
    all_equal = df_conclude['gene_name'].equals(df_conclude['symbol'])
    # print(all_equal)

    # If not identical, filter out different parts
    if not all_equal:
        df_different = df_conclude[df_conclude['gene_name'] != df_conclude['symbol']]
        print(df_different)
        file_name = 'difference_of_gene_name_and_symbol.tsv'
        save_path = save_dir + file_name
        utils.save_df(save_path, df_different)
    # If identical, when gene_name not empty, Symbol empty: add gene_name content to Symbol column then delete gene_name column
    else:
        def supply_Symbol(gene_name, symbol):
            if pd.isnull(symbol):
                return gene_name
            else:
                return symbol

        df['symbol'] = df.apply(lambda x: supply_Symbol(x['gene_name'], x['symbol']), axis=1)
        drop_column = ['gene_name']
        df = df.drop(columns=drop_column)
    return df
