def deal_li_shan_83332(df):
    # When locustag is Rvnr01, change synonyms_of_gene column data to rrnS

    df.loc[df['locustag'] == 'Rvnr01', 'synonyms_of_gene'] = 'rrnS'

    return df


def deal_special_need(df, li_shan_id):
    if li_shan_id == 'lishan_83332':
        df = deal_li_shan_83332(df)

    return df
