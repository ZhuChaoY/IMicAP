def compare_process_of_description(df):
    drop_column = ['description']

    # Filter out data where description, type_of_gene are both not empty for comparison
    df_conclude = df[df['description'].notnull() & df['type_of_gene'].notnull()]

    # Part where description and type_of_gene are different
    df_different = df_conclude[df_conclude['description'] != df_conclude['type_of_gene']]

    # If no differences exist, directly delete description column
    if len(df_different) == 0:
        df = df.drop(columns=drop_column)
        return df

    # When description and type_of_gene are different, judge difference between description and TYPES column
    all_equal = df_different['description'].equals(df_different['TYPES_gene'])

    # If same, delete description column
    if all_equal:
        df = df.drop(columns=drop_column)
        return df
    else:
        return df
