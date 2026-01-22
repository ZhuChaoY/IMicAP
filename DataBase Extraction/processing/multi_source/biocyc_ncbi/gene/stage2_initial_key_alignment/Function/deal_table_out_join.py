"""
Editor: INK
Create Time: 2024/8/16 10:45
File Name: deal_table_out_join.py
Function: 
"""
import pandas as pd

import utils.my_df_function as until


# 1th, Outer join of Table A and Table B based on join_column_a, join_column_b (case-insensitive)
# Merge gene table data from both parts based on Biocyc's gene_biocyc_id and NCBI's LocusTag
def outer_join_two_table_1th(df_A, df_B, join_column_a, join_column_b):
    """
    :param df_A:
    :param df_B:
    :param join_column_a:
    :param join_column_b:
    :return:
    """
    # Initialize data for join, preserving original data
    df_A_join = df_A.copy()
    df_B_join = df_B.copy()

    # Field sets of Table A and B before processing
    list_columns_A = df_A.columns.tolist()
    list_columns_B = df_B.columns.tolist()

    # Case-insensitive matching, i.e., convert to all lowercase for matching
    df_A_join['lower_column_a'] = df_A_join[join_column_a].str.lower()
    df_B_join['lower_column_b'] = df_B_join[join_column_b].str.lower()

    df_outer = pd.merge(
        left=df_A_join,
        right=df_B_join,
        left_on='lower_column_a',
        right_on='lower_column_b',
        how='outer',
        indicator=True
    )

    # Fields for handling case issues
    lower_column_list = ['lower_column_a', 'lower_column_b']
    df_outer = df_outer.drop(columns=lower_column_list)

    # Fields for determining join results
    indicate_column = ['_merge']

    # Successfully joined result of both parts
    df_both = df_outer[df_outer['_merge'] == 'both']
    df_both = df_both.drop(columns=indicate_column)

    # Table A data not successfully joined
    df_A_only = df_outer[df_outer['_merge'] == 'left_only']
    df_A_only = df_A_only.drop(columns=list_columns_B)
    df_A_only = df_A_only.drop(columns=indicate_column)

    # Table B data not successfully joined
    df_B_only = df_outer[df_outer['_merge'] == 'right_only']
    df_B_only = df_B_only.drop(columns=list_columns_A)
    df_B_only = df_B_only.drop(columns=indicate_column)

    return df_both, df_A_only, df_B_only


# 2th, Outer join of Table A and Table B based on join_column_a, join_column_b (case-insensitive)
# For parts where 1st match failed, merge gene table data from both parts based on Biocyc's accession_id and NCBI's LocusTag
def outer_join_two_table_2th(df_A, df_B, join_column_a, join_column_b, unique_id_a, unique_id_b):
    """
    :param unique_id_b: Field for uniquely distinguishing Table A data
    :param unique_id_a: Field for uniquely distinguishing Table B data
    :param df_A:
    :param df_B:
    :param join_column_a:
    :param join_column_b:
    :return:
    """
    # Initialize data for join, preserving original data
    df_A_join = df_A.copy()
    df_B_join = df_B.copy()

    # Filter out Table A, Table B not null for matching, while preserving key fields as null for match failure data
    df_A_join_null = df_A_join[df_A_join[join_column_a].isnull()]
    df_B_join_null = df_B_join[df_B_join[join_column_b].isnull()]
    df_A_join = df_A_join[df_A_join[join_column_a].notna()]
    df_B_join = df_B_join[df_B_join[join_column_b].notna()]

    # Case-insensitive matching, i.e., convert to all lowercase for matching
    df_A_join['lower_column_a'] = df_A_join[join_column_a].str.lower()
    df_B_join['lower_column_b'] = df_B_join[join_column_b].str.lower()

    # Since join_column_a = accession_id is multi-value, need multi-value conversion, explode before use
    # Handle BioCyc accession_id special multi-value to assist subsequent joins
    df_A_join['lower_column_a'] = df_A_join['lower_column_a'].apply(lambda x: until.
                                                                    transform_back_to_list(x))
    df_A_join = df_A_join.explode(column='lower_column_a')

    df_outer = pd.merge(
        left=df_A_join,
        right=df_B_join,
        left_on='lower_column_a',
        right_on='lower_column_b',
        how='outer',
        indicator=True
    )

    # Fields for handling case issues
    lower_column_list = ['lower_column_a', 'lower_column_b']
    df_outer = df_outer.drop(columns=lower_column_list)

    # Fields for determining join results
    indicate_column = '_merge'

    # Successfully joined result of both parts
    df_both = df_outer[df_outer['_merge'] == 'both']
    df_both = df_both.drop(columns=indicate_column)

    # Table A data not successfully joined
    if len(df_both) > 0:
        success_id_A = df_both[unique_id_a].tolist()
        df_A_only = df_A[-df_A[unique_id_a].isin(success_id_A)]
    else:
        df_A_only = df_A

    # Table B data not successfully joined
    if len(df_both) > 0:
        success_id_B = df_both[unique_id_b].tolist()
        df_B_only = df_B[-df_B[unique_id_b].isin(success_id_B)]
    else:
        df_B_only = df_B

    return df_both, df_A_only, df_B_only


# 3th, Outer join of Table A and Table B based on join_column_a, join_column_b (case-insensitive)
# For parts where 2nd match failed, merge gene table data from both parts based on Biocyc's gene_biocyc_id and NCBI's synonyms_ncbi
def outer_join_two_table_3th(df_A, df_B, join_column_a, join_column_b):
    """
    :param df_A:
    :param df_B:
    :param join_column_a:
    :param join_column_b:
    :return:
    """
    # Field sets of Table A and B before processing
    list_columns_A = df_A.columns.tolist()
    list_columns_B = df_B.columns.tolist()

    # Initialize data for join, preserving original data
    df_A_join = df_A.copy()
    df_B_join = df_B.copy()


    # Case-insensitive matching, i.e., convert to all lowercase for matching
    df_A_join['lower_column_a'] = df_A_join[join_column_a].str.lower()
    df_B_join['lower_column_b'] = df_B_join[join_column_b].str.lower()

    df_outer = pd.merge(
        left=df_A_join,
        right=df_B_join,
        left_on='lower_column_a',
        right_on='lower_column_b',
        how='outer',
        indicator=True
    )

    # Fields for handling case issues
    lower_column_list = ['lower_column_a', 'lower_column_b']
    df_outer = df_outer.drop(columns=lower_column_list)

    # Fields for determining join results
    indicate_column = ['_merge']

    # Successfully joined result of both parts
    df_both = df_outer[df_outer['_merge'] == 'both']
    df_both = df_both.drop(columns=indicate_column)

    # Table A data not successfully joined
    df_A_only = df_outer[df_outer['_merge'] == 'left_only']
    df_A_only = df_A_only.drop(columns=list_columns_B)
    df_A_only = df_A_only.drop(columns=indicate_column)

    # Table B data not successfully joined
    df_B_only = df_outer[df_outer['_merge'] == 'right_only']
    df_B_only = df_B_only.drop(columns=list_columns_A)
    df_B_only = df_B_only.drop(columns=indicate_column)

    return df_both, df_A_only, df_B_only


# 4th, Outer join of Table A and Table B based on join_column_a, join_column_b (case-insensitive)
# For parts where 1st match failed, merge gene table data from both parts based on Biocyc's accession_id and NCBI's LocusTag
def outer_join_two_table_4th(df_A, df_B, join_column_a, join_column_b, unique_id_a, unique_id_b):
    """
    :param unique_id_b: Field for uniquely distinguishing Table A data
    :param unique_id_a: Field for uniquely distinguishing Table B data
    :param df_A:
    :param df_B:
    :param join_column_a:
    :param join_column_b:
    :return:
    """
    # Initialize data for join, preserving original data
    df_A_join = df_A.copy()
    df_B_join = df_B.copy()

    # Filter out Table A, Table B not null for matching, while preserving key fields as null for match failure data
    df_A_join_null = df_A_join[df_A_join[join_column_a].isnull()]
    df_B_join_null = df_B_join[df_B_join[join_column_b].isnull()]
    df_A_join = df_A_join[df_A_join[join_column_a].notna()]
    df_B_join = df_B_join[df_B_join[join_column_b].notna()]

    # Case-insensitive matching, i.e., convert to all lowercase for matching
    df_A_join['lower_column_a'] = df_A_join[join_column_a].str.lower()
    df_B_join['lower_column_b'] = df_B_join[join_column_b].str.lower()

    # Since join_column_a = accession is multi-value, need multi-value conversion, explode before use
    # Handle BioCyc accession_id special multi-value to assist subsequent joins
    df_A_join['lower_column_a'] = df_A_join['lower_column_a'].apply(lambda x: until.
                                                                    transform_back_to_list(x))
    df_A_join = df_A_join.explode(column='lower_column_a')

    df_outer = pd.merge(
        left=df_A_join,
        right=df_B_join,
        left_on='lower_column_a',
        right_on='lower_column_b',
        how='outer',
        indicator=True
    )

    # Fields for handling case issues
    lower_column_list = ['lower_column_a', 'lower_column_b']
    df_outer = df_outer.drop(columns=lower_column_list)

    # Fields for determining join results
    indicate_column = '_merge'

    # Successfully joined result of both parts
    df_both = df_outer[df_outer['_merge'] == 'both']
    df_both = df_both.drop(columns=indicate_column)

    # Table A data not successfully joined
    if len(df_both) > 0:
        success_id_A = df_both[unique_id_a].tolist()
        df_A_only = df_A[-df_A[unique_id_a].isin(success_id_A)]
    else:
        df_A_only = df_A

    # Table B data not successfully joined
    if len(df_both) > 0:
        success_id_B = df_both[unique_id_b].tolist()
        df_B_only = df_B[-df_B[unique_id_b].isin(success_id_B)]
    else:
        df_B_only = df_B

    return df_both, df_A_only, df_B_only


# 5th, Outer join of Table A and Table B based on join_column_a, join_column_b (case-insensitive)
# Merge by matching Biocyc table's gene_name with NCBI table's Symbol column (case-insensitive)
def outer_join_two_table_5th(df_A, df_B, join_column_a, join_column_b):
    """
    :param df_A:
    :param df_B:
    :param join_column_a:
    :param join_column_b:
    :return:
    """
    # Initialize data for join, preserving original data
    df_A_join = df_A.copy()
    df_B_join = df_B.copy()

    # Field sets of Table A and B before processing
    list_columns_A = df_A_join.columns.tolist()
    list_columns_B = df_B_join.columns.tolist()

    # Case-insensitive matching, i.e., convert to all lowercase for matching
    df_A_join['lower_column_a'] = df_A_join[join_column_a].str.lower()
    df_B_join['lower_column_b'] = df_B_join[join_column_b].str.lower()

    df_outer = pd.merge(
        left=df_A_join,
        right=df_B_join,
        left_on='lower_column_a',
        right_on='lower_column_b',
        how='outer',
        indicator=True
    )

    # Fields for handling case issues
    lower_column_list = ['lower_column_a', 'lower_column_b']
    df_outer = df_outer.drop(columns=lower_column_list)

    # Fields for determining join results
    indicate_column = ['_merge']

    # Successfully joined result of both parts
    df_both = df_outer[df_outer['_merge'] == 'both']
    df_both = df_both.drop(columns=indicate_column)

    # Table A data not successfully joined
    df_A_only = df_outer[df_outer['_merge'] == 'left_only']
    df_A_only = df_A_only.drop(columns=list_columns_B)
    df_A_only = df_A_only.drop(columns=indicate_column)

    # Table B data not successfully joined
    df_B_only = df_outer[df_outer['_merge'] == 'right_only']
    df_B_only = df_B_only.drop(columns=list_columns_A)
    df_B_only = df_B_only.drop(columns=indicate_column)

    return df_both, df_A_only, df_B_only
