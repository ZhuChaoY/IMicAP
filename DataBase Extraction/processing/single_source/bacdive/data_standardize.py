"""
Editor: INK
Create Time: 2025/3/31 9:58
File Name: data_standardize.py
Function: 
"""
import ast
import os.path

import pandas as pd
import utils.my_df_function as utils


class BacDiveStandardize:
    # Unified regular processing
    @staticmethod
    def _regular_process(df):
        rename_dict = {}
        for col in df.columns:
            new_col = col.lower()
            new_col = new_col.replace('-', '_')
            new_col = new_col.replace(' ', '_')
            d = {
                col: new_col
            }
            rename_dict.update(d)
        df = df.rename(columns=rename_dict)

        df = df.fillna('NA_NO')
        return df

    def __init__(self, input_dir, output_dir, ref_path, CSN_path):
        self.input_dir = input_dir
        self.path_dict = {
            'culture_pH': f'{input_dir}/Culture and growth conditions/culture_pH.tsv',
            'culture_temp': f'{input_dir}/Culture and growth conditions/culture_temp.tsv',
            'Culture collection no': f'{input_dir}/External links/Culture collection no.tsv',
            'Straininfo link': f'{input_dir}/External links/Straininfo link.tsv',
            'General': f'{input_dir}/General/General.tsv',
            'antibiogram': f'{input_dir}/Physiology and metabolism/antibiogram.tsv',
            'halophily': f'{input_dir}/Physiology and metabolism/halophily.tsv',
        }
        self.output_dir = output_dir
        self.ref_path = ref_path
        self.CSN_path = CSN_path

    def culture_pH(self, save_dir_main, now_time):
        file_type = 'culture_pH'
        data_path = self.path_dict.get(file_type)
        if not os.path.exists(data_path):
            return []
        df = utils.load_df(data_path)

        save_dir_this = os.path.join(save_dir_main, 'Culture and growth conditions')
        utils.create_dir(save_dir_this)

        # Build new column ph_ref
        def build_ph_ref(ph, ref):
            if pd.isnull(ph):
                return ref
            if pd.isnull(ref):
                return ph

            ph_ref = f'{ph}(ref:{ref})'
            return ph_ref

        df['ph_ref'] = df.apply(lambda x: build_ph_ref(ph=x['ph'], ref=x['@ref']), axis=1)
        # Initialize processing record
        record_dict_list = [
            {
                'file_name': data_path.split('/')[-1],
                'file_count': len(df)
            }
        ]
        # Split data by type column
        according_dict = {
            'optimum': 'optimum_ph',
            'minimum': 'minimum_ph',
            'maximum': 'maximum_ph',
            'growth': 'growth_ph'
        }
        append_columns = [
            'optimum_ph',
            'minimum_ph',
            'maximum_ph',
            'growth_ph'
        ]
        for type_data in according_dict:
            new_column = according_dict.get(type_data)
            df_part = df[df['type'] == type_data].copy()

            rename_dict = {'ph_ref': new_column}
            df_part = df_part.rename(columns=rename_dict)

            drop_cols = [
                'ph', '@ref'
            ]
            df_part = df_part.drop(columns=drop_cols)

            df_part = self._regular_process(df_part)

            for col in append_columns:
                if col not in df_part.columns:
                    df_part[col] = 'NA_NO'

            save_name = f'culture_pH_{type_data}_{now_time}.tsv'
            save_path = os.path.join(save_dir_this, save_name)

            utils.save_df(save_path, df_part)

            d = {
                'file_name': save_name,
                'file_count': len(df_part)
            }
            record_dict_list.append(d)

        return record_dict_list

    def culture_temp(self, save_dir_main, now_time):
        file_type = 'culture_temp'
        data_path = self.path_dict.get(file_type)
        df = utils.load_df(data_path)

        save_dir_this = os.path.join(save_dir_main, 'Culture and growth conditions')
        utils.create_dir(save_dir_this)

        # Build new column ph_ref
        def build_ph_ref(ph, ref):

            if pd.isnull(ph):
                return ref
            if pd.isnull(ref):
                return ph

            ph_ref = f'{ph}(ref:{ref})'
            return ph_ref

        df['temperature_ref'] = df.apply(lambda x: build_ph_ref(ph=x['temperature'], ref=x['@ref']), axis=1)
        # Initialize processing record
        record_dict_list = [
            {
                'file_name': data_path.split('/')[-1],
                'file_count': len(df)
            }
        ]
        # Split data by type column
        according_dict = {
            'optimum': 'optimum_temperature',
            'minimum': 'minimum_temperature',
            'maximum': 'maximum_temperature',
            'growth': 'growth_temperature'
        }
        append_columns = [
            'optimum_temperature',
            'minimum_temperature',
            'maximum_temperature',
            'growth_temperature'
        ]
        for type_data in according_dict:
            new_column = according_dict.get(type_data)
            df_part = df[df['type'] == type_data].copy()

            rename_dict = {'temperature_ref': new_column}
            df_part = df_part.rename(columns=rename_dict)

            drop_cols = [
                'temperature', '@ref'
            ]
            df_part = df_part.drop(columns=drop_cols)

            df_part = self._regular_process(df_part)

            for col in append_columns:
                if col not in df_part.columns:
                    df_part[col] = 'NA_NO'

            save_name = f'culture_temp_{type_data}_{now_time}.tsv'
            save_path = os.path.join(save_dir_this, save_name)

            utils.save_df(save_path, df_part)

            d = {
                'file_name': save_name,
                'file_count': len(df_part)
            }
            record_dict_list.append(d)

        return record_dict_list

    def Culture_collection_no(self, save_dir_main, now_time):
        file_type = 'Culture collection no'
        data_path = self.path_dict.get(file_type)
        df = utils.load_df(data_path)

        record_list = [
            {
                'file_name': data_path.split('/')[-1],
                'file_count': len(df)
            }
        ]

        save_dir_this = os.path.join(save_dir_main, 'External links')
        utils.create_dir(save_dir_this)

        def deal_culture_collection_no(data):
            if pd.isnull(data):
                return data

            data_list = data.split(', ')
            data = utils.change_list_to_special_data(data_list)
            return data

        df['culture_collection_no.'] = df['culture_collection_no.'].apply(lambda x: deal_culture_collection_no(x))
        df = self._regular_process(df)

        save_name = f'Culture collection no_{now_time}.tsv'
        save_path = os.path.join(save_dir_this, save_name)
        utils.save_df(save_path, df)

        d = {
            'file_name': save_name,
            'file_count': len(df)
        }
        record_list.append(d)
        return record_list

    def Strain_info_link(self, save_dir_main, now_time):
        file_type = 'Straininfo link'
        data_path = self.path_dict.get(file_type)
        df = utils.load_df(data_path)

        record_list = [
            {
                'file_name': data_path.split('/')[-1],
                'file_count': len(df)
            }
        ]

        save_dir_this = os.path.join(save_dir_main, 'External links')
        utils.create_dir(save_dir_this)

        def deal_passport(data):
            if pd.isnull(data):
                return data

            data = f'http://www.straininfo.net/strains/{data}'
            return data

        df['passport'] = df['passport'].apply(lambda x: deal_passport(x))
        df = self._regular_process(df)
        save_name = f'Straininfo link_{now_time}.tsv'
        save_path = os.path.join(save_dir_this, save_name)
        utils.save_df(save_path, df)

        d = {
            'file_count': len(df),
            'file_name': save_name
        }
        record_list.append(d)
        return record_list

    def General(self, save_dir_main, save_dir_record):
        file_type = 'General'
        data_path = self.path_dict.get(file_type)
        df = utils.load_df(data_path)

        record_list = [
            {
                'file_name': data_path.split('/')[-1],
                'file_count': len(df)
            }
        ]

        # Initialize auxiliary table data path
        ref_path = self.ref_path
        df_ref = utils.load_df(ref_path)

        csn_path = self.CSN_path
        df_csn = utils.load_df(csn_path)

        # Initialize storage directory
        save_dir_this = os.path.join(save_dir_main, 'General')
        utils.create_dir(save_dir_this)

        # 1. Remove duplicates across all fields
        df = df.drop_duplicates()

        # 2. Filter delete invalid data
        #   2.1 Get species_tax_id set of auxiliary tables
        ref_species_tax_id_list = df_ref['species_tax_id'].unique().tolist()
        if 'NA_NO' in ref_species_tax_id_list:
            ref_species_tax_id_list.remove('NA_NO')
        #   2.2 When mapping_level is species, filter out data where ncbi_tax_id not in ref
        df_species = df[df['matching_level'] == 'species'].copy()
        df_species_not_fit = df_species[-df_species['ncbi_tax_id'].isin(ref_species_tax_id_list)]
        delete_source_file_list = df_species_not_fit['source_file'].unique().tolist()

        #   2.3 Delete all data of this class based on source_file
        save_name = f'general_ncbi_tax_id_not_contain.tsv'
        df_not_fit = df[df['source_file'].isin(delete_source_file_list)]
        d = {
            'file_name': save_name,
            'file_count': len(df_not_fit)
        }
        record_list.append(d)
        df = df[-df['source_file'].isin(delete_source_file_list)]

        # 3. First split df, process separately according to different matching_level values
        #   3.1 Split data by mapping_level sequentially
        df_temp_1 = pd.DataFrame()
        according_dict_matching_level = {
            'species': 'bacdive_species_tax_id',
            'subspecies': 'bacdive_subspecies_tax_id',
            'strain': 'bacdive_strains_tax_id'
        }
        for matching_level in according_dict_matching_level:
            new_column = according_dict_matching_level.get(matching_level)
            rename_dict = {'ncbi_tax_id': new_column}

            df_apart = df[df['matching_level'] == matching_level].copy()
            df_apart = df_apart.rename(columns=rename_dict)
            df_temp_1 = pd.concat([df_temp_1, df_apart])

        #   3.2 When matching_level is empty, no processing
        df_na = df[df['matching_level'].isnull()].copy()
        df_temp_1 = pd.concat([df_temp_1, df_na])

        # 4. Group merge and deduplicate by source_file
        #   4.1 First convert key_words data
        def convert_kw(x):
            try:
                data = ast.literal_eval(x)
                if isinstance(data, list):
                    return data
                else:
                    return x
            except:
                return x

        df_temp_1['keywords'] = df_temp_1['keywords'].apply(lambda x: convert_kw(x))

        #   4.2 Then perform group merge
        def get_unique_items(items):
            """
            Get unique item list maintaining original order
            (Not using concise but difficult-to-understand code writing)

            Parameters:
                items: Original list

            Return:
                Deduplicated list, maintaining original order
            """
            seen = set()  # For recording seen elements
            unique_items = []  # Store result

            for item in items:
                if item == 'NA_NO':
                    continue
                if pd.isnull(item):
                    continue

                if item not in seen:  # If new element
                    seen.add(item)  # Add to seen set
                    unique_items.append(item)  # Add to result list

            return unique_items

        def flatten_to_unique_element(series):
            """Flatten elements in Series into unique list"""
            # Step 1: Flatten all elements
            all_items = []
            for item in series:
                if isinstance(item, list):
                    all_items.extend(item)  # If iterable object (non-string), expand
                else:
                    if pd.isnull(item):
                        continue
                    all_items.append(item)  # Otherwise directly add

            # Step 2: Deduplicate and maintain order
            unique_items = get_unique_items(all_items)

            # Step 3: If only one element, return element itself not list
            if len(unique_items) == 1:
                return unique_items[0]
            if len(unique_items) == 0:
                return None

            unique_items_str = utils.change_list_to_special_data(unique_items)
            return unique_items_str

        def merge_dataframe_clearly(df, group_by_column):
            """
            Clear and readable DataFrame merge function
            All columns use unique_list mode merge

            Parameters:
                df: Original DataFrame
                group_by_column: Column name for grouping

            Return:
                Merged DataFrame
            """
            # Define aggregation method
            aggregation_rules = {
                col: flatten_to_unique_element
                for col in df.columns
                if col != group_by_column
            }

            # Execute group aggregation
            merged_df = df.groupby(group_by_column).agg(aggregation_rules).reset_index()

            return merged_df

        df_temp_1 = merge_dataframe_clearly(df=df_temp_1, group_by_column='source_file')

        # 5. Handle situation where bacdive_species_tax_id is empty in data
        def deal_na_matching_level(source_file, bacdive_species_tax_id):

            if pd.isnull(bacdive_species_tax_id):
                temp_list = source_file.split('_')
                species_tax_id = temp_list[0]
                return species_tax_id
            else:
                return bacdive_species_tax_id

        df_temp_1['bacdive_species_tax_id'] = df_temp_1.apply(
            lambda x: deal_na_matching_level(x['source_file'], x['bacdive_species_tax_id']), axis=1
        )

        # 6. Remove specified columns
        drop_columns = [
            'matching_level',
            'ncbi_tax_id'
        ]
        df_temp_1 = df_temp_1.drop(columns=drop_columns)

        # 7. Extract bacdive_name from description
        def exact_bacdive_name(description):
            if pd.isnull(description):
                return description

            temp_list = description.split(' is ')
            bacdive_name = temp_list[0]
            return bacdive_name

        df_temp_1['bacdive_name'] = df_temp_1['description'].apply(lambda x: exact_bacdive_name(x))

        # 8. Add data from ref
        #   8.1 Create 3 new empty columns
        append_column = [
            'substrains_name',
            'lishan_substrains_tax_id',
            'substrains_tax_id',
        ]
        for col in append_column:
            df_temp_1[col] = 'NA_NO'

        #   8.2 Add data from ref by biological classification
        according_dict_ref = {
            'bacdive_strains_tax_id': [
                'species',
                'lishan_species_tax_id',
                'species_tax_id',
                'subspecies_name',
                'lishan_subspecies_tax_id',
                'subspecies_tax_id',
                'serotype_name',
                'lishan_serotype_tax_id',
                'serotype_tax_id',
                'strains_name',
                'lishan_strains_tax_id',
                'strains_tax_id',
            ],
            'bacdive_subspecies_tax_id': [
                'species',
                'lishan_species_tax_id',
                'species_tax_id',
                'subspecies_name',
                'lishan_subspecies_tax_id',
                'subspecies_tax_id',
            ],
            'bacdive_species_tax_id': [
                'species',
                'lishan_species_tax_id',
                'species_tax_id',
            ]
        }
        df_temp_2 = pd.DataFrame()
        original_columns = df_temp_1.columns.tolist()
        for bio_type in according_dict_ref:

            # If previous round already completed all data associations, skip subsequent associations
            if len(df_temp_1) == 0:
                continue

            # Filter out specified column data from ref
            append_columns = according_dict_ref.get(bio_type)
            df_ref_part = df_ref[append_columns].copy()
            df_ref_part = df_ref_part.drop_duplicates()

            # Associate two parts of data
            df_merge = pd.merge(
                left=df_temp_1,
                right=df_ref_part,
                left_on=bio_type,
                right_on=bio_type.replace('bacdive_', ''),
                how='left',
                indicator=True
            )

            # Get successfully associated part, merge and store
            df_merge_success = df_merge[df_merge['_merge'] == 'both']
            df_temp_2 = pd.concat([df_temp_2, df_merge_success])

            # Get failed part, remove extra columns, update remaining part, proceed to next round association
            df_merge_fail = df_merge[df_merge['_merge'] == 'left_only']
            df_merge_fail = df_merge_fail[original_columns]
            df_temp_1 = df_merge_fail
        # If still remaining data, still need to supplement
        if len(df_temp_1) > 0:
            df_temp_2 = pd.concat([df_temp_2, df_temp_1])
        # Remove columns for judging match success/failure
        if '_merge' in df_temp_2.columns:
            df_temp_2 = df_temp_2.drop(
                columns=['_merge']
            )

        # 9. Merge General table and Ref table Bacteria information
        def bacteria_combine_process(general_data, ref_data):
            # With General table data as baseline (when both sides have values, prioritize retaining General table data)
            if pd.isnull(general_data):
                return ref_data
            else:
                return general_data

        #   9.1 Merge bacdive_strains_tax_id and strains_tax_id into strains_tax_id
        df_temp_2['strains_tax_id'] = df_temp_2.apply(lambda x:
                                                      bacteria_combine_process(
                                                          x['bacdive_strains_tax_id'],
                                                          x['strains_tax_id']
                                                      ), axis=1
                                                      )

        #   9.2 Merge bacdive_subspecies_tax_id and subspecies_tax_id into subspecies_tax_id
        df_temp_2['subspecies_tax_id'] = df_temp_2.apply(lambda x:
                                                         bacteria_combine_process(
                                                             x['bacdive_subspecies_tax_id'],
                                                             x['subspecies_tax_id']
                                                         ), axis=1
                                                         )

        #   9.3 Merge bacdive_species_tax_id and species_tax_id into species_tax_id
        df_temp_2['species_tax_id'] = df_temp_2.apply(lambda x:
                                                      bacteria_combine_process(
                                                          x['bacdive_species_tax_id'],
                                                          x['species_tax_id']
                                                      ), axis=1
                                                      )
        #   9.4 Remove invalid columns
        drop_columns = [
            'bacdive_strains_tax_id',
            'bacdive_subspecies_tax_id',
            'bacdive_species_tax_id'
        ]
        df_temp_2 = df_temp_2.drop(columns=drop_columns)

        # 10. Change species to species_name
        rename_dict = {
            'species': 'species_name'
        }
        df_temp_2 = df_temp_2.rename(columns=rename_dict)

        # 11. Unified processing
        df_temp_3 = self._regular_process(df_temp_2)

        # 12. Store
        save_name = f'General.tsv'
        save_path = os.path.join(save_dir_this, save_name)
        utils.save_df(save_path, df_temp_3)
        d = {
            'file_name': save_name,
            'file_count': len(df_temp_3)
        }
        record_list.append(d)

        return record_list

    def antibiogram(self, save_dir_main, now_time):
        file_type = 'antibiogram'
        data_path = self.path_dict.get(file_type)
        if not os.path.exists(data_path):
            return []

        df = utils.load_df(data_path)
        record_list = [
            {
                'file_name': data_path.split('/')[-1],
                'file_count': len(df)
            }
        ]

        # For each unique value group, add sequential numbering to rows within each group
        df['i_count'] = df.groupby('bacdive_id').cumcount() + 1

        # Build test
        def build_test_id(row):
            bacdive_id = row['bacdive_id']
            i_count = row['i_count']
            row['test'] = 'test' + '_' + bacdive_id + "_" + str(i_count)
            return row

        df = df.apply(build_test_id, axis=1)

        # Delete auxiliary columns
        drop_column = ['i_count']
        df = df.drop(columns=drop_column)

        df = self._regular_process(df)

        save_dir = os.path.join(save_dir_main, 'Physiology and metabolism')
        utils.create_dir(save_dir)

        save_name = f'antibiogram_{now_time}.tsv'
        utils.save_df(os.path.join(save_dir, save_name), df)

        d = {
            'file_name': save_name,
            'file_count': len(df)
        }
        record_list.append(d)

        return record_list

    def halophily(self, save_dir_main, now_time):
        file_type = 'halophily'
        data_path = self.path_dict.get(file_type)
        df = utils.load_df(data_path)

        record_list = [
            {
                'file_name': data_path.split('/')[-1],
                'file_count': len(df)
            }
        ]

        def change_tested_relation(growth, tested_relation):
            if growth == 'no':
                return 'no growth'
            else:
                return tested_relation

        df['tested_relation'] = df.apply(lambda x: change_tested_relation(x['growth'], x['tested_relation']), axis=1)

        drop_columns = [
            'growth'
        ]
        df = df.drop(columns=drop_columns)
        df = self._regular_process(df)

        save_dir = os.path.join(save_dir_main, 'Physiology and metabolism')
        utils.create_dir(save_dir)

        save_name = f'halophily_{now_time}.tsv'
        utils.save_df(os.path.join(save_dir, save_name), df)

        d = {
            'file_name': save_name,
            'file_count': len(df)
        }
        record_list.append(d)

        return record_list

    def deal_else(self, save_dir_data):
        record_list = []

        data_dir = self.input_dir
        already_finish_file_list = [
            f'culture_pH.tsv',
            f'culture_temp.tsv',
            f'Culture collection no.tsv',
            f'Straininfo link.tsv',
            f'General.tsv',
            f'antibiogram.tsv',
            f'halophily.tsv',
        ]

        path_dict = utils.get_file_list(path=data_dir, file_dict={})
        for file in path_dict:
            if file not in already_finish_file_list:
                data_path = path_dict.get(file)
                df = utils.load_df(data_path)
                d = {
                    'file_name': file,
                    'file_count': len(df)
                }
                record_list.append(d)

                df = self._regular_process(df)

                save_dir = data_path.replace(file, '')
                save_dir = save_dir.replace(self.input_dir, save_dir_data)
                utils.create_dir(save_dir)

                save_name = file
                save_path = os.path.join(save_dir, save_name)
                utils.save_df(save_path, df)
                d = {
                    'file_name': save_name,
                    'file_count': len(df)
                }
                record_list.append(d)

        return record_list

    def run(self):
        now_time = utils.get_now_time()
        save_dir_main = self.output_dir
        save_dir_data = os.path.join(save_dir_main, 'Data')
        save_dir_record = os.path.join(save_dir_main, 'Record')
        utils.create_dir(save_dir_record)

        all_record_list = []

        record_list = self.culture_pH(save_dir_data, now_time)
        all_record_list += record_list

        record_list = self.culture_temp(save_dir_data, now_time)
        all_record_list += record_list

        record_list = self.Culture_collection_no(save_dir_data, now_time)
        all_record_list += record_list

        record_list = self.Strain_info_link(save_dir_data, now_time)
        all_record_list += record_list

        record_list = self.General(save_dir_data, save_dir_record)
        all_record_list += record_list

        record_list = self.antibiogram(save_dir_data, now_time)
        all_record_list += record_list

        record_list = self.halophily(save_dir_data, now_time)
        all_record_list += record_list

        record_list = self.deal_else(save_dir_data)
        all_record_list += record_list

        df_record = pd.DataFrame(all_record_list)
        save_name = 'record.tsv'
        utils.save_df(os.path.join(save_dir_record, save_name), df_record)
