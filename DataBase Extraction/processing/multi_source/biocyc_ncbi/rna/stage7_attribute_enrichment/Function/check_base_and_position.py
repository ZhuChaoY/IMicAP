import pandas as pd

# Under condition transcription_direction is '+' or '-', corresponding combination of start_base, end_base, right_end_position, left_end_position: when one side has value, other side empty, fill empty with value side
def supply_none_transcription_data(row):
    transcription_direction = row['transcription_direction']
    start_base = row['start_base']
    end_base = row['end_base']
    left_end_position = row['left_end_position']
    right_end_position = row['right_end_position']

    # Empty value filling process when transcription_direction is '-'
    if transcription_direction == '-':
        if pd.isnull(start_base) or start_base == 'NA_NO':
            row['start_base'] = right_end_position
        if pd.isnull(right_end_position):
            row['right_end_position'] = start_base

        if pd.isnull(end_base) or end_base == 'NA_NO':
            row['end_base'] = left_end_position
        if pd.isnull(left_end_position):
            row['left_end_position'] = end_base

    # Empty value filling process when transcription_direction is '+'
    elif transcription_direction == '+':
        if pd.isnull(start_base) or start_base == 'NA_NO':
            row['start_base'] = left_end_position
        if pd.isnull(left_end_position) or left_end_position == 'NA_NO':
            row['left_end_position'] = start_base

        if pd.isnull(end_base) or end_base == 'NA_NO':
            row['end_base'] = right_end_position
        if pd.isnull(right_end_position) or right_end_position == 'NA_NO':
            row['right_end_position'] = end_base


    else:
        if transcription_direction != 'NA_NO' and pd.notnull(transcription_direction):
            print('Ambiguous value of transcription_direction: ' + str(transcription_direction))

    return row



# Verify under condition transcription_direction is + or -, whether corresponding combination of start_base, end_base, right_end_position, left_end_position are completely identical
def start_check(df):
    # Empty value filling
    df = df.apply(supply_none_transcription_data, axis=1)

    # Data comparison
    df_positive = df[df['transcription_direction'] == '+']
    df_positive_diff_start = df_positive[df_positive['start_base'] != df_positive['left_end_position']]
    df_positive_diff_end = df_positive[df_positive['end_base'] != df_positive['right_end_position']]
    print('positive')
    # print(df_positive_diff_start)
    # print(df_positive_diff_end)

    df_negative = df[df['transcription_direction'] == '-']
    df_negative_diff_start = df_negative[df_negative['start_base'] != df_negative['right_end_position']]
    df_negative_diff_end = df_negative[df_negative['end_base'] != df_negative['left_end_position']]
    print('negative')
    # print(df_negative_diff_start)
    # print(df_negative_diff_end)

    if (len(df_positive_diff_start) == 0 and len(df_positive_diff_end) == 0) and (
            len(df_negative_diff_start) == 0 and len(df_negative_diff_end) == 0):
        drop_column = ['left_end_position', 'right_end_position']
        df = df.drop(columns=drop_column)

    return df
