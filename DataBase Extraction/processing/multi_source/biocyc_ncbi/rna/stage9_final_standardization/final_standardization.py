
# When RNA_name column is NA_NO, add description value to RNA_name
def deal_name(df):
    def deal_process(rna_name, description):

        if rna_name == 'NA_NO':
            return description
        else:
            return rna_name

    if 'description' not in df.columns.tolist():
        return df

    df['RNA_name'] = df.apply(
        lambda x: deal_process(x['RNA_name'], x['description']), axis=1
    )
    return df


# Fill type_of_RNA data with type value
# When data value is "Unclassified-Genes", change to "protein-coding"
def deal_type_append(df):
    def deal_process(row):

        type_of_RNA = row['type_of_RNA']
        types_RNA = row['types_RNA']
        if type_of_RNA == 'NA_NO':
            type_of_RNA = types_RNA
        else:
            type_of_RNA = type_of_RNA
        row['type_of_RNA'] = type_of_RNA
        return row

    df = df.apply(deal_process, axis=1)
    drop_column = ['types_RNA']
    df = df.drop(columns=drop_column)

    return df

def standardize_process(df):
    df = deal_name(df)
    df = deal_type_append(df)

    return df
