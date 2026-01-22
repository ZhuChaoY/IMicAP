import pandas as pd


def combine_process(df_ncbi, df_eco):
    print(len(df_ncbi))
    print(len(df_eco))
    df_final = pd.concat([df_ncbi, df_eco])
    return df_final
