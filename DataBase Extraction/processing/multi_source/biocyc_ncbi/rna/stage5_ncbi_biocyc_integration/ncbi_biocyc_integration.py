import pandas as pd

import utils.my_df_function as utils


# 1th, Outer join of Table A and Table B based on join_column_a, join_column_b (case-insensitive)
# Merge by matching Biocyc table's Gene_biocyc_ID with NCBI table's LocusTag column (case-insensitive)
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


# 2th, Outer join of Table A and Table B based on join_column_a, join_column_b (case-insensitive)
# For parts where 1st match failed, merge RNA table data from both parts based on Biocyc's ACCESSION_ID and NCBI's LocusTag
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

    # Case-insensitive matching, i.e., convert to all lowercase for matching
    df_A_join['lower_column_a'] = df_A_join[join_column_a].str.lower()
    df_B_join['lower_column_b'] = df_B_join[join_column_b].str.lower()

    # Since join_column_a = ACCESSION_ID is multi-value, need multi-value conversion, explode before use
    # Handle BioCyc accession_id special multi-value to assist subsequent joins
    df_A_join['lower_column_a'] = df_A_join['lower_column_a'].apply(lambda x: utils.
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
# For parts where 2nd match failed, merge RNA table data from both parts based on Biocyc's Gene_biocyc_ID and NCBI's Synonyms
def outer_join_two_table_3th(df_A, df_B, join_column_a, join_column_b):
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


# 4th, Outer join of Table A and Table B based on join_column_a, join_column_b (case-insensitive)
# For parts where 3th match failed, merge RNA table data from both parts based on Biocyc's ACCESSION_ID and NCBI's Synonyms
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

    # Filter out empty data in Biocyc
    df_A_join = df_A_join[df_A_join[join_column_a].notna()]

    # Case-insensitive matching, i.e., convert to all lowercase for matching
    df_A_join['lower_column_a'] = df_A_join[join_column_a].str.lower()
    df_B_join['lower_column_b'] = df_B_join[join_column_b].str.lower()

    # Since join_column_a = ACCESSION_ID is multi-value, need multi-value conversion, explode before use
    # Handle BioCyc accession_id special multi-value to assist subsequent joins
    df_A_join['lower_column_a'] = df_A_join['lower_column_a'].apply(lambda x: utils.
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


# Handle situation where two dfs have identical column names
def deal_same_columns(df_ncbi, df_biocyc):
    ncbi_column_list = df_ncbi.columns.to_list()
    biocyc_column_list = df_biocyc.columns.to_list()

    dup_column_list = []

    for column in ncbi_column_list:
        if column in biocyc_column_list:
            dup_column_list.append(column)

    if len(dup_column_list) != 0:
        ncbi_rename_dict = {}
        biocyc_rename_dict = {}
        for column in dup_column_list:
            ncbi_column = column + "_NCBI"
            biocyc_column = column + '_Biocyc'

            d1 = {column: ncbi_column}
            ncbi_rename_dict.update(d1)

            d2 = {column: biocyc_column}
            biocyc_rename_dict.update(d2)

        df_ncbi = df_ncbi.rename(columns=ncbi_rename_dict)
        df_biocyc = df_biocyc.rename(columns=biocyc_rename_dict)

    return df_ncbi, df_biocyc


# First match merge: Merge by matching NCBI table's LocusTag column with Biocyc table's Gene_biocyc_ID (case-insensitive)
def combine_by_LocusTag_and_Gene_biocyc_ID(df_ncbi_rna, df_biocyc_rna):
    """
    :param df_ncbi_rna: NCBI RNA table
    :param df_biocyc_rna: Biocyc RNA table
    :return: Part where NCBI successfully added Biocyc data via LocusTag first time, part where NCBI table failed first LocusTag match
    """
    biocyc_column = df_biocyc_rna.columns.to_list()
    df_ncbi_rna_for_match = df_ncbi_rna.copy()
    df_biocyc_rna_for_match = df_biocyc_rna.copy()

    df_ncbi_rna_for_match['lower_LocusTag'] = df_ncbi_rna_for_match['LocusTag'].str.lower()
    df_biocyc_rna_for_match['lower_Gene_biocyc_ID'] = df_biocyc_rna_for_match['Gene_biocyc_ID'].str.lower()

    df_merge = pd.merge(left=df_ncbi_rna_for_match, right=df_biocyc_rna_for_match,
                        left_on='lower_LocusTag', right_on='lower_Gene_biocyc_ID',
                        how='left', indicator=True)

    lower_columns = ['lower_LocusTag', 'lower_Gene_biocyc_ID']
    df_merge = df_merge.drop(columns=lower_columns)

    # Reference column names for merge result
    drop_indicator_columns = ['_merge']

    # Part where NCBI successfully added Biocyc data via LocusTag first time
    merge_success_1st = df_merge[df_merge['_merge'] == 'both']
    merge_success_1st = merge_success_1st.drop(columns=drop_indicator_columns)

    # Part where NCBI table failed first LocusTag match
    merge_fail_1st = df_merge[df_merge['_merge'] == 'left_only']
    merge_fail_1st = merge_fail_1st.drop(columns=biocyc_column)
    merge_fail_1st = merge_fail_1st.drop(columns=drop_indicator_columns)

    return merge_success_1st, merge_fail_1st


# Second match: For parts where first match failed, merge by matching NCBI table's LocusTag column with Biocyc table's ACCESSION_ID (case-insensitive)
def combine_by_locusTag_and_ACCESSION_ID(merge_success_1st, merge_fail_1st, df_biocyc_rna):
    """
    :param merge_success_1st: NCBI first successful match result
    :param merge_fail_1st: NCBI first match failure result
    :param df_biocyc_rna: Biocyc rna data
    :return: Sum of NCBI first and second successful match results, NCBI second match failure result
    """

    df_biocyc_rna_for_match = df_biocyc_rna.copy()

    # Determine if ACCESSION_ID field is available for matching
    columns_list_of_biocyc = df_biocyc_rna_for_match.columns.to_list()
    if 'ACCESSION_ID' not in columns_list_of_biocyc:
        merge_fail_2nd = merge_fail_1st
        merge_success_2nd = merge_success_1st
        return merge_success_2nd, merge_fail_2nd

    biocyc_column = df_biocyc_rna_for_match.columns.to_list()

    # Case-insensitive, convert to all lowercase for matching
    merge_fail_1st['lower_LocusTag'] = merge_fail_1st['LocusTag'].str.lower()
    df_biocyc_rna_for_match['lower_ACCESSION_ID'] = df_biocyc_rna_for_match['ACCESSION_ID'].str.lower()

    # lower_ACCESSION_ID first convert from special multi-value storage format to list, then explode, finally match
    df_biocyc_rna_for_match['lower_ACCESSION_ID'] = df_biocyc_rna_for_match. \
        apply(lambda x: utils.transform_back_to_list(x['lower_ACCESSION_ID']), axis=1)
    df_biocyc_rna_for_match = df_biocyc_rna_for_match.explode('lower_ACCESSION_ID')

    merge_2nd_df = pd.merge(left=merge_fail_1st, right=df_biocyc_rna_for_match,
                            left_on='lower_LocusTag', right_on='lower_ACCESSION_ID',
                            how='left', indicator=True)

    lower_columns = ['lower_LocusTag', 'lower_ACCESSION_ID']
    merge_2nd_df = merge_2nd_df.drop(columns=lower_columns)

    # Reference column names for merge result
    drop_indicator_columns = ['_merge']

    # NCBI part successful in 2nd match
    merge_success_2nd = merge_2nd_df[merge_2nd_df['_merge'] == 'both']
    merge_success_2nd = merge_success_2nd.drop(columns=drop_indicator_columns)

    # Total NCBI data successfully matched via 1st and 2nd
    ncbi_total_success_matched_df = pd.concat([merge_success_2nd, merge_success_1st])

    # NCBI data that failed in 2nd match
    merge_fail_2nd = merge_2nd_df[merge_2nd_df['_merge'] == 'left_only']
    merge_fail_2nd = merge_fail_2nd.drop(columns=biocyc_column)
    merge_fail_2nd = merge_fail_2nd.drop(columns=drop_indicator_columns)

    return ncbi_total_success_matched_df, merge_fail_2nd


# Third match merge: For NCBI second match failure part, match again via Symbol column with Biocyc table's gene_name.
def combine_by_Symbol_and_gene_name(ncbi_total_success_matched_df, fail_2nd_ncbi_df, df_biocyc_rna):
    """
    :param ncbi_total_success_matched_df: First and second successful match part
    :param fail_2nd_ncbi_df: Part that failed both matches
    :param df_biocyc_rna: Biocyc RNA table
    :return: Sum of NCBI first, second, third successful match results, NCBI third match failure result, Biocyc three times match failure result
    """

    df_biocyc_rna_for_match = df_biocyc_rna.copy()

    df_merge = pd.merge(left=fail_2nd_ncbi_df, right=df_biocyc_rna_for_match,
                        left_on='Symbol', right_on='gene_name',
                        how='outer', indicator=True)

    columns_of_indicator = ['_merge']

    # NCBI part successfully matched via Symbol with gene_name
    success_3rd_ncbi_merge_df = df_merge[df_merge['_merge'] == 'both']
    success_3rd_ncbi_merge_df = success_3rd_ncbi_merge_df.drop(columns=columns_of_indicator)

    # NCBI part still matching failed via Symbol with gene_name
    merge_fail_3rd = df_merge[df_merge['_merge'] == 'left_only']
    merge_fail_3rd = merge_fail_3rd.drop(columns=columns_of_indicator)

    # Sum of parts successfully matched via 1st, 2nd, 3rd
    ncbi_total_success_matched_df = pd.concat([ncbi_total_success_matched_df, success_3rd_ncbi_merge_df])

    # Biocyc part not successfully supplemented into NCBI, where Gene_biocyc_ID exists only in Biocyc table, not in merge result
    list_biocyc_id_of_merge = ncbi_total_success_matched_df['Gene_biocyc_ID'].to_list()
    fail_biocyc_rna_df = df_biocyc_rna[
        -df_biocyc_rna['Gene_biocyc_ID'].isin(list_biocyc_id_of_merge)]

    # Merge NCBI matched success and matched failure parts
    final_merge_result = pd.concat([ncbi_total_success_matched_df, merge_fail_3rd])

    return final_merge_result, merge_fail_3rd, fail_biocyc_rna_df, ncbi_total_success_matched_df


# Store data
def save_data_process(success_merge_after_twice, ncbi_rna_fail_1st_3rd_df, biocyc_rna_fail_1st_3rd_df,
                      path_save_success, path_fail_ncbi, path_fail_biocyc):
    utils.save_df(path_save_success, success_merge_after_twice)

    utils.save_df(path_fail_ncbi, ncbi_rna_fail_1st_3rd_df)

    utils.save_df(path_fail_biocyc, biocyc_rna_fail_1st_3rd_df)


# Start entire merge process
def combine_ncbi_biocyc(df_ncbi, df_biocyc,
                        save_dir,
                        path_save_success, path_fail_ncbi, path_fail_biocyc,
                        now_time, li_shan_id):
    df_ncbi_rna = df_ncbi.copy()
    df_biocyc_rna = df_biocyc.copy()

    utils.create_dir(save_dir)

    # Get: Part where NCBI successfully added Biocyc data via LocusTag first time, part where NCBI table failed first LocusTag match
    ncbi_1st_success_df, ncbi_1st_fail_df = combine_by_LocusTag_and_Gene_biocyc_ID(df_ncbi_rna,
                                                                                   df_biocyc_rna)
    df_biocyc_rna = df_biocyc.copy()
    # Get: Part where NCBI successfully supplemented data via LocusTag column, ACCESSION_ID column, part where NCBI failed both supplements, part where Biocyc not successfully supplemented into NCBI
    ncbi_total_success_matched_df, merge_fail_2nd = combine_by_locusTag_and_ACCESSION_ID(
        merge_success_1st=ncbi_1st_success_df, merge_fail_1st=ncbi_1st_fail_df, df_biocyc_rna=df_biocyc_rna)

    df_biocyc_rna = df_biocyc.copy()
    # Get: Part where NCBI successfully supplemented data via Symbol column, gene_name column, part where NCBI failed both supplements, part where Biocyc not successfully supplemented into NCBI
    success_merge_after_twice, ncbi_rna_fail_1st_3rd_df, biocyc_rna_fail_1st_3rd_df, ncbi_total_success_matched_df = \
        combine_by_Symbol_and_gene_name(
            ncbi_total_success_matched_df=ncbi_total_success_matched_df, fail_2nd_ncbi_df=merge_fail_2nd,
            df_biocyc_rna=df_biocyc_rna)

    # Store
    save_data_process(success_merge_after_twice, ncbi_rna_fail_1st_3rd_df, biocyc_rna_fail_1st_3rd_df,
                      path_save_success, path_fail_ncbi, path_fail_biocyc)


# Handle situation where df_biocyc or df_ncbi data has one side as empty table; supplement fields to non-empty part, store as match result
def conclude_none_table(
        df_final,
        df_ncbi,
        df_biocyc_fail,
        df_biocyc,
        save_dir,
        final_data_path,
        save_biocyc_fail_path
):
    # One potential issue: if df_biocyc is empty table, which column names should be added?
    # NCBI data is filtered from NCBI's All info, even if no data, column names are retained
    # Biocyc data is downloaded and parsed, if no data downloaded, no column names, which column names should be supplemented? Directly add column names from a Biocyc with data?
    print(len(df_biocyc))
    print(len(df_ncbi))
    if len(df_ncbi) == 0:
        check_path = save_dir + 'lack NCBI data.txt'
        with open(check_path, 'w+') as f:
            pass
        # Add NCBI fields to Biocyc data, update data as merge result
        merge_df = pd.concat([df_biocyc, df_ncbi])
        utils.save_df(final_data_path, merge_df)

        # Because Biocyc failure needed in 6th merge, to prevent errors, update Biocyc match failure as empty table with only column headers
        df_biocyc_fail = pd.DataFrame(columns=df_biocyc.columns.tolist())
        utils.save_df(save_biocyc_fail_path, df_biocyc_fail)

    if len(df_biocyc) == 0:
        check_path = save_dir + 'lack BioCyc data.txt'
        with open(check_path, 'w+') as f:
            pass

        merge_df = pd.concat([df_biocyc, df_ncbi])
        utils.save_df(final_data_path, merge_df)

        df_biocyc_fail = pd.DataFrame(columns=df_biocyc.columns.tolist())
        utils.save_df(save_biocyc_fail_path, df_biocyc_fail)

    return df_final, df_biocyc_fail


def get_process(df_biocyc, df_ncbi, save_path, df_ncbi_fail, df_biocyc_fail):
    # Count matched and unmatched records
    total_biocyc = len(df_biocyc)
    biocyc_fail_count = len(df_biocyc_fail)
    biocyc_success_count = total_biocyc - biocyc_fail_count

    total_ncbi = len(df_ncbi)
    ncbi_fail_count = len(df_ncbi_fail)
    ncbi_success_count = total_ncbi - ncbi_fail_count

    # Output result
    print(f"BioCyc total {total_biocyc} records, matched {biocyc_success_count}, unmatched {biocyc_fail_count}")
    print(f"NCBI total {total_ncbi} records, matched {ncbi_success_count}, unmatched {ncbi_fail_count}")

    with open(save_path, 'w+') as f:
        f.write(
            'biocyc total ' + str(total_biocyc) + ' records, matched ' + str(biocyc_success_count) + ', unmatched ' + str(
                biocyc_fail_count) + '\n')
        f.write('NCBI total ' + str(total_ncbi) + ' records, matched ' + str(ncbi_success_count) + ', unmatched ' + str(
            ncbi_fail_count) + '\n')


def integrate_process(file_name_id, save_dir, df_ncbi, df_biocyc):
    # To solve issue of subsequent program unable to execute due to NCBI modification adjustments, rename synonyms to Synonyms here
    if len(df_ncbi) > 0:
        if 'synonyms' in df_ncbi.columns:
            rename_dict = {
                'synonyms': 'Synonyms'
            }
            df_ncbi = df_ncbi.rename(columns=rename_dict)

    # Construct storage path
    utils.create_dir(save_dir)
    save_success_path = save_dir + file_name_id + '_RNA_entity_bioncbi_combine.tsv'
    save_ncbi_fail_path = save_dir + file_name_id + '_RNA_ncbi_fail.tsv'
    save_biocyc_fail_path = save_dir + file_name_id + '_RNA_entity_bio_fail.tsv'

    # Handle duplicate column names
    df_ncbi, df_biocyc = deal_same_columns(df_ncbi, df_biocyc)

    # Merge NCBI and Biocyc RNA tables
    # 1th join: Merge by matching Biocyc table's Gene_biocyc_ID with NCBI table's LocusTag column (case-insensitive)
    df_both_1th, df_biocyc_only_1th, df_ncbi_only_1th = outer_join_two_table_1th(
        df_A=df_biocyc,
        df_B=df_ncbi,
        join_column_a='Gene_biocyc_ID',
        join_column_b='locustag'
    )

    # 2th join: For parts where 1th match failed, merge RNA table data from both parts based on Biocyc's ACCESSION_ID and NCBI's LocusTag
    df_both_2th, df_biocyc_only_2th, df_ncbi_only_2th = outer_join_two_table_2th(
        df_A=df_biocyc_only_1th,
        df_B=df_ncbi_only_1th,
        join_column_a='ACCESSION_ID',
        join_column_b='locustag',
        unique_id_a='UNIQUE-ID',
        unique_id_b='geneid'
    )

    # Determine which Synonyms field in NCBI
    list_column_ncbi = df_ncbi_only_1th.columns.tolist()
    ncbi_synonyms_column = 'Synonyms_NCBI'
    if ncbi_synonyms_column not in list_column_ncbi:
        ncbi_synonyms_column = 'Synonyms'
    # 3th join: For parts where 2th match failed, merge RNA table data from both parts based on Biocyc's Gene_biocyc_ID and NCBI's synonyms
    df_both_3th, df_biocyc_only_3th, df_ncbi_only_3th = outer_join_two_table_3th(
        df_A=df_biocyc_only_2th,
        df_B=df_ncbi_only_2th,
        join_column_a='Gene_biocyc_ID',
        join_column_b=ncbi_synonyms_column,
    )

    # 4th join: For parts where 3th match failed, merge RNA table data from both parts based on Biocyc's ACCESSION_ID and NCBI's Synonyms
    df_both_4th, df_biocyc_only_4th, df_ncbi_only_4th = outer_join_two_table_4th(
        df_A=df_biocyc_only_3th,
        df_B=df_ncbi_only_3th,
        join_column_a='ACCESSION_ID',
        join_column_b=ncbi_synonyms_column,
        unique_id_a='UNIQUE-ID',
        unique_id_b='geneid'
    )

    # # 5th join: For parts where 4th match failed, merge RNA table data from both parts based on Biocyc's ACCESSION_ID and NCBI's Symbol
    # df_both_5th, df_biocyc_only_5th, df_ncbi_only_5th = combine_process_20240820.outer_join_two_table_5th(
    #     df_A=df_biocyc_only_4th,
    #     df_B=df_ncbi_only_4th,
    #     join_column_a='gene_name',
    #     join_column_b='Symbol',
    # )

    # print(df_ncbi_only_1th.columns)
    # print(df_ncbi_only_2th.columns)
    # print(df_ncbi_only_3th.columns)
    # print(df_ncbi_only_4th.columns)
    # print(df_biocyc_only_5th.columns)

    # Store data
    df_all_success = pd.concat([df_both_1th, df_both_2th, df_both_3th, df_both_4th])
    utils.save_df(path=save_success_path, df=df_all_success)

    utils.save_df(save_biocyc_fail_path, df=df_biocyc_only_4th)
    utils.save_df(save_ncbi_fail_path, df=df_ncbi_only_4th)

    # Store match situation
    path_combine_situation = save_dir + file_name_id + '_match_result.txt'
    get_process(
        df_biocyc=df_biocyc,
        df_ncbi=df_ncbi,
        save_path=path_combine_situation,
        df_ncbi_fail=df_ncbi_only_4th,
        df_biocyc_fail=df_biocyc_only_4th
    )

    # Handle situation where one of two tables to merge is empty table
    df_final, df_biocyc_fail = conclude_none_table(
        df_ncbi=df_ncbi,
        df_biocyc_fail=df_biocyc_only_4th,
        df_biocyc=df_biocyc,
        df_final=df_all_success,
        save_dir=save_dir,
        final_data_path=save_success_path,
        save_biocyc_fail_path=save_biocyc_fail_path
    )

    df_ncbi_fail = df_ncbi_only_4th

    return df_final, df_ncbi_fail, df_biocyc_fail
