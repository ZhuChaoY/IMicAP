# Modify column names as required
def change_column(df):
    target_column_dict = {
        'type_of_gene': 'type_of_RNA',
        'synonyms': 'synonyms_of_gene',
        'product': 'synonyms_of_RNA',
        'geneid': 'NCBI_gene_id',
        'symbol': 'gene_symbol',
        'common_name': 'RNA_name',
        'tax_id': 'strains_tax_id'
    }

    # Determine if these columns really exist
    columns_list = df.columns.tolist()
    rename_dict = {}
    for column in target_column_dict:
        if column in columns_list:
            # print(column)
            value = target_column_dict.get(column)
            d = {column: value}
            rename_dict.update(d)
    # print(rename_dict)

    df = df.rename(columns=rename_dict)
    return df


# Replace all rna in column names with uppercase RNA
def change_rna(df):
    column_list = df.columns.tolist()

    rename_dict = {}
    for column in column_list:
        if 'rna' in column:
            new_column = column.replace('rna', 'RNA')
            d = {column: new_column}
            rename_dict.update(d)
    df = df.rename(columns=rename_dict)
    return df
