import json
import os

import utils.my_df_function as utils


class GbffQualityAssessment:

    def __init__(self, input_dir, output_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir

    def run(self):
        data_dir = self.input_dir
        dir_list = os.listdir(data_dir)

        save_dir = self.output_dir
        utils.create_dir(save_dir)

        save_path = os.path.join(save_dir, 'all_empty_column.txt')
        with open(save_path, 'w+'):
            pass

        # Determine sequentially which columns' data are all empty
        for dir in dir_list:
            print(dir)
            target_data_dir = os.path.join(data_dir, dir)
            path_list = utils.get_file_path_list(target_data_dir, [])

            not_all_empty_column = {}  # Initialize not all empty field collection
            all_empty_column = []  # Initialize all empty field collection

            for path in path_list:

                df = utils.load_df(path)
                column_list = df.columns.tolist()
                for column in column_list:

                    # Skip already judged as non-empty fields
                    if column in not_all_empty_column:
                        continue
                    else:
                        # Determine if this column all empty
                        if all(df[column] == 'NA_NO'):
                            if column not in all_empty_column:
                                all_empty_column.append(column)
                        else:
                            d = {
                                column: path
                            }
                            not_all_empty_column.update(d)

                            if column in all_empty_column:
                                all_empty_column.remove(column)

            print(all_empty_column)

            with open(save_path, 'a+') as f1:
                f1.write(f'#######{dir}#######\n')
                for column in all_empty_column:
                    f1.write(column)
                    f1.write('\n')

            save_path_not = os.path.join(save_dir, f'{dir}_not_all_empty_column.json')
            with open(save_path_not, 'w+') as f:
                json.dump(not_all_empty_column, f, indent=4)


