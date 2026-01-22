import pandas as pd
import utils.my_df_function as utils


# And Synonyms_NCBI and Synonyms_Biocyc columns
def combine_process(df):
    def combine(synonyms_ncbi, synonyms_biocyc):
        if pd.isnull(synonyms_ncbi) or synonyms_ncbi == '-':
            return synonyms_biocyc
        if pd.isnull(synonyms_biocyc):
            return synonyms_ncbi

        list_synonyms_biocyc = utils.transform_back_to_list(synonyms_biocyc)
        list_synonyms_ncbi = utils.transform_back_to_list(synonyms_ncbi)

        final_list = list_synonyms_biocyc + list_synonyms_ncbi
        final_str = utils.change_list_to_special_data(final_list)
        return final_str

    df['synonyms'] = df.apply(lambda x: combine(x['Synonyms_NCBI'], x['Synonyms_Biocyc']), axis=1)

    drop_column = ['Synonyms_Biocyc', 'Synonyms_NCBI']
    df = df.drop(columns=drop_column)

    return df
