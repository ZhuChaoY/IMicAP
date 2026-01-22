import os.path

import utils.my_df_function as utils


def deal_information(output_dir, df):
    utils.create_dir(output_dir)
    save_name = f'GO_terms_information.tsv'
    save_path = os.path.join(output_dir, save_name)

    change_data_list = ['EXACT', 'BROAD', 'NARROW', 'RELATED', 'CUSTOM SYNONYM']

    def modify_synonyms(data, change_list):
        if data == 'NA_NO':
            return data

        for old_str in change_list:
            new_str = f'[{old_str}]'
            data = data.replace(old_str, new_str)

        data_list = utils.transform_back_to_list(data)
        new_data_list = []
        for item in data_list:
            new_item = item.replace('"', '')
            new_data_list.append(new_item)
        data = utils.change_list_to_special_data(new_data_list)

        return data

    df['synonyms'] = df.apply(lambda x: modify_synonyms(
        x['synonyms'], change_list=change_data_list
    ), axis=1)

    data_dict = {
        'term_id': 'GO_term_id',
        'term_name': 'GO_term_name'
    }
    df = df.rename(columns=data_dict)
    utils.save_df(save_path, df)
    return df


def deal_relationship(output_dir, df):
    utils.create_dir(output_dir)
    save_name = f'GO_terms_relationship.tsv'
    save_path = os.path.join(output_dir, save_name)

    rename_dict = {
        'term_id_1': 'GO_term_id_1',
        'term_name_1': 'GO_term_name_1',
        'term_id_2': 'GO_term_id_2',
        'term_name_2': 'GO_term_name_2',
    }
    df = df.rename(columns=rename_dict)
    utils.save_df(save_path, df)
    return df


def run(output_dir, df_info, df_relation):
    utils.create_dir(output_dir)

    df_info = deal_information(output_dir, df_info)
    df_relation = deal_relationship(output_dir, df_relation)

    return df_info, df_relation
