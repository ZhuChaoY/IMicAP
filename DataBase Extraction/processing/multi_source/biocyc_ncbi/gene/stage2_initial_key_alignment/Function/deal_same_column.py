"""
Editor: INK
Create Time: 2024/6/3 10:09
File Name: deal_same_column.py
Function: 
"""
import utils.my_df_function as until


# Handle situation where two dfs have identical column names
def deal_same_columns(df_ncbi, df_biocyc, save_dir):
    ncbi_column_list = df_ncbi.columns.to_list()
    biocyc_column_list = df_biocyc.columns.to_list()

    dup_column_list = []

    for column in ncbi_column_list:
        if column in biocyc_column_list:
            dup_column_list.append(column)

    # Handle identical column names
    if len(dup_column_list) != 0:
        ncbi_rename_dict = {}
        biocyc_rename_dict = {}

        # Record identical column names
        save_condition = save_dir + 'The same column in the two tables.txt'
        with open(save_condition, 'w+') as f:
            for column in dup_column_list:

                f.write(column)
                f.write('\n')

                ncbi_column = column + "_ncbi"
                biocyc_column = column + '_biocyc'

                f.write('\t')
                f.write(ncbi_column)
                f.write('\n')
                f.write('\t')
                f.write(biocyc_column)
                f.write('\n')

                d1 = {column: ncbi_column}
                ncbi_rename_dict.update(d1)

                d2 = {column: biocyc_column}
                biocyc_rename_dict.update(d2)

        df_ncbi = df_ncbi.rename(columns=ncbi_rename_dict)
        df_biocyc = df_biocyc.rename(columns=biocyc_rename_dict)
    else:
        save_condition = save_dir + 'There is no column with the same name in the two tables..txt'
        with open(save_condition, 'w+') as f:
            pass

    return df_ncbi, df_biocyc
