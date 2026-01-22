def change_special_common_name(df):
    special_common_name_dict = {'small RNA IstR-2': 'small regulatory RNA IstR-1',
                                'tRNA-Ala(UGC)': 'L-alanyl-tRNA<SUP>alaT</SUP>'}

    list_common_name = df['COMMON-NAME'].tolist()

    for special_name in special_common_name_dict:
        if special_name in list_common_name:
            new_name = special_common_name_dict.get(special_name)
            df.loc[df['COMMON-NAME'] == special_name, 'COMMON-NAME'] = new_name

    return df

