import pandas as pd
import utils.my_df_function as utils


def compare_process(df, save_dir):
    # Filter out data where PRODUCT-NAME, COMMON-NAME are both not empty for comparison
    df_conclude = df[df['COMMON-NAME'].notnull() & df['PRODUCT-NAME'].notnull()]

    # Use .equals() method to determine if two columns are completely identical
    all_equal = df_conclude['PRODUCT-NAME'].equals(df_conclude['COMMON-NAME'])
    # print(all_equal)

    # If not identical, filter out different parts
    if not all_equal:
        df_different = df_conclude[df_conclude['PRODUCT-NAME'] != df_conclude['COMMON-NAME']]
        # print(df_different)
        file_name = 'difference_of_PRODUCT-NAME_and_COMMON-NAME.tsv'
        save_path = save_dir + file_name
        utils.save_df(save_path, df_different)
    # If identical, when PRODUCT-NAME not empty, COMMON-NAME empty: add PRODUCT-NAME content to COMMON-NAME column then delete PRODUCT-NAME column
    else:
        def supply_COMMON_NAME(PRODUCT_NAME, COMMON_NAME):
            if pd.isnull(COMMON_NAME):
                return PRODUCT_NAME
            else:
                return COMMON_NAME

        df['COMMON-NAME'] = df.apply(lambda x: supply_COMMON_NAME(x['PRODUCT-NAME'], x['COMMON-NAME']), axis=1)
        drop_column = ['PRODUCT-NAME']
        df = df.drop(columns=drop_column)
    return df
