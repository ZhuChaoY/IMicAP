def fill_none(df):
    column_list = df.columns.to_list()

    # Fill empty values
    def set_none(value):
        if value == '-':
            return 'NA_NO'
        else:
            return value

    for column in column_list:
        if column != 'transcription_direction':
            df[column] = df.apply(lambda x: set_none(x[column]), axis=1)
        else:
            # print(column)
            pass
    df = df.fillna('NA_NO')
    return df
