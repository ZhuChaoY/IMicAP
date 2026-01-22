import os.path
import re
import utils.my_df_function as utils


def build_go_info(output_dir, df):
    df_copy = df.copy()
    save_name_main = f'GO_terms_information.tsv'
    save_path = os.path.join(output_dir, save_name_main)

    df_copy = df_copy.drop(columns=['synonyms'])
    utils.save_df(save_path, df_copy)


def build_go_synonyms(output_dir, df):
    target_col = ['GO_term_id', 'synonyms']
    df = df[target_col]

    df['synonyms'] = df['synonyms'].apply(lambda x: utils.transform_back_to_list(x))
    df = df.explode(column=['synonyms'])

    def exact_value(row):
        # Initialize extraction result
        synonyms = row['synonyms']
        type = 'NA_NO'
        source = 'NA_NO'

        # 1. Extract content within []
        extract_pattern = r"\[([^\]]*)\]"
        matches = re.findall(extract_pattern, synonyms)
        matches = [match if match else 'NA_NO' for match in matches]  # Empty content replace with None
        # print(matches)

        if len(matches) >= 2:

            # 2. Delete [] and its content, get cleaned text
            clean_pattern = r"\[[^\]]*\]"
            cleaned_text = re.sub(clean_pattern, "", synonyms).strip()  # Delete [] and its content, remove leading/trailing spaces

            # 3. Reassign data values
            synonyms = cleaned_text
            type = matches[0]
            source = matches[1]

            print(synonyms)
            print(type)
            print(synonyms)
            print('#######')

            if type != 'NA_NO':
                type = type.lower()

        row['synonyms'] = synonyms
        row['type'] = type
        row['source'] = source
        return row

    df = df.apply(exact_value, axis=1)

    # New column synonyms_zibiao_id, generate encoding based on row number
    df['synonyms_zibiao_id'] = ['synonyms_b' + str(i + 1) for i in range(len(df))]

    save_name_child = f'GO_terms_synonyms.tsv'
    save_path = os.path.join(output_dir, save_name_child)
    utils.save_df(save_path, df)


def build_go_relationship(output_dir, df):

    # New column synonyms_zibiao_id, generate encoding based on row number
    df['relationship_id'] = ['relationship_' + str(i + 1) for i in range(len(df))]
    save_name = f'GO_terms_relationship.tsv'
    save_path = output_dir + save_name
    save_path = os.path.join(output_dir, save_name)
    utils.save_df(save_path, df)


def run(output_dir, df_info, df_relation):
    utils.create_dir(output_dir)
    build_go_info(output_dir, df_info)
    build_go_synonyms(output_dir, df_info)
    build_go_relationship(output_dir, df_relation)
