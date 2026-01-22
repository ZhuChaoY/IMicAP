import ast
import json
import os
import random
import time
import numpy as np
import pandas as pd


# Create a directory
def create_dir(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)


# Load a DataFrame file in a specific way
def load_df(path, encode='utf-8'):
    df = pd.read_csv(path, sep='\t', index_col=False, dtype=str, encoding=encode)
    return df


# Save data
def save_df(path, df, encoding='UTF-8'):
    df.to_csv(path, sep='\t', index=False, encoding=encoding)


# Get a collection of files and their corresponding paths
def get_file_list(path, file_dict):
    if os.path.isdir(path):
        file_list = os.listdir(path)
        for file in file_list:
            new_path = path + '/' + file
            file_dict = get_file_list(new_path, file_dict)
    else:
        file_name = path.split('/')[-1]
        d = {file_name: path}
        file_dict.update(d)

    return file_dict


# Get all file paths under a folder
def get_file_path_list(path, path_list):
    if os.path.isdir(path):
        file_list = os.listdir(path)
        for file in file_list:
            new_path = path + '/' + file
            path_list = get_file_path_list(new_path, path_list)
    else:
        path_list.append(path)

    return path_list


# Convert data to a specified format, old version
def change_to_special_type_old(data):
    if type(data) == list:
        if np.nan in data:
            data.remove(np.nan)
        if len(data) == 1:
            new_data = data[0]
        else:
            new_data_list = []
            for item in data:
                new_item = '"' + str(item) + '"'
                new_data_list.append(new_item)
            new_data = '; '.join(new_data_list)
    else:
        try:
            new_data = ast.literal_eval(data)
            if type(new_data) != list:
                # print('Special case')
                # print(type(new_data))
                # print(new_data)
                # print('#########')
                new_data = str(new_data)
            else:
                if np.nan in new_data:
                    new_data.remove(np.nan)

                if len(new_data) == 1:
                    new_data = new_data[0]
                else:
                    new_data_list = []
                    for item in new_data:
                        new_item = '"' + str(item) + '"'
                        new_data_list.append(new_item)
                    new_data = '; '.join(new_data_list)
        except:
            new_data = data
    return new_data


# Convert the specified format back to list
def transform_back_to_list(data):
    if pd.isnull(data):
        return data

    data = data.strip('"')
    data_list = data.split('"; "')
    data_list = list(set(data_list))
    data_list.sort()
    return data_list
    # print(data_list)


# Load data from the given path and extract its column names
def get_column_list(data_path):
    df = load_df(data_path)
    column_list = df.columns.tolist()
    return column_list


# Convert list data to the format '""; ""'
def change_list_to_special_data(data_list):
    if type(data_list) != list:
        return data_list

    if len(data_list) == 0:
        return np.nan
    if len(data_list) == 1:
        return data_list[0]

    new_data_list = []
    for item in data_list:
        new_item = '"' + item + '"'
        new_data_list.append(new_item)

    final_data = '; '.join(new_data_list)
    return final_data


# Random sleep
def random_sleep(mu=5, sigma=0.4):
    mu = random.randint(1, 3)
    '''
    Normal distribution random sleep
    :param mu: mean
    :param sigma: standard deviation, determines the range of fluctuation
    '''
    secs = random.normalvariate(mu, sigma)
    if secs <= 0:
        secs = mu  # Reset to mean if too small
    time.sleep(secs)


# Get current date (YYYYMMDD)
def get_now_time():
    from datetime import datetime

    # Get current time
    current_time = datetime.now()

    # Format as 'YYYYMMDD'
    formatted_date = current_time.strftime('%Y%m%d')
    return formatted_date


# Check if a column contains list-type data
def is_list_in_column(df, column_name):
    """
    Check if the specified column contains list-type data
    """
    if column_name not in df.columns:
        return False

    for value in df[column_name]:
        try:
            new_value = ast.literal_eval(value)
            if type(new_value) == list:
                return True
        except:
            pass

    return False


# Handle multiple values in data
def deal_multiply_data(save_path):
    # Load data
    df = pd.read_csv(save_path, sep='\t', index_col=False, encoding='utf-8')

    # Check each column for list type
    column_contains_list = []
    column_list = df.columns.to_list()
    for column in column_list:
        is_contains_list = is_list_in_column(df, column)
        if column == 'COMPONENT-OF':
            print(is_contains_list)
        if is_contains_list:
            column_contains_list.append(column)

    print(column_contains_list)

    # Reload data
    df = load_df(save_path)

    # Convert columns containing list-type data
    def change_to_special_data(data):
        if pd.isnull(data):
            return data

        try:
            list_data = ast.literal_eval(data)
            if type(list_data) == list:
                final_data = change_list_to_special_data(list_data)
                return final_data
            else:
                return data
        except:
            return data

    for column in column_contains_list:
        df[column] = df[column].apply(lambda x: change_to_special_data(x))

    save_df(save_path, df)
    return df


# Handle multiple values in DataFrame
def deal_multiply_df_value(df):
    # Check each column for list type
    column_contains_list = []
    column_list = df.columns.to_list()
    for column in column_list:
        is_contains_list = is_list_in_column(df, column)
        if column == 'COMPONENT-OF':
            print(is_contains_list)
        if is_contains_list:
            column_contains_list.append(column)

    # Convert columns containing list-type data
    def change_to_special_data(data):
        if pd.isnull(data):
            return data

        try:
            list_data = ast.literal_eval(data)
            if type(list_data) == list:
                final_data = change_list_to_special_data(list_data)
                return final_data
            else:
                return data
        except:
            return data

    for column in column_contains_list:
        df[column] = df[column].apply(lambda x: change_to_special_data(x))

    return df


# If data is a list-type string, convert to list
def get_truth_data_type(data):
    if pd.isnull(data):
        return data
    else:
        try:
            new_data = ast.literal_eval(data)
            if type(new_data) == list:
                return new_data
            else:
                return data
        except:
            return data


# Determine which columns from a prepared list actually exist in the DataFrame
def conclude_prepare_columns(prepare_column_list, df):
    """
    :param prepare_column_list: List of prepared column names
    :param df: DataFrame to check
    :return: List of prepared columns that actually exist in the DataFrame
    """

    indeed_in_df = []

    columns_list = df.columns.to_list()

    for column in prepare_column_list:
        if column in columns_list:
            indeed_in_df.append(column)

    return indeed_in_df


# Get Record data
def record_json_get(record_path):
    if os.path.exists(record_path):
        with open(record_path, 'r') as f:
            record_dict = json.load(f)
    else:
        record_dict = {}

    return record_dict
