import json
import os
import bacdive
import pandas as pd
from datetime import datetime
from utils import my_df_function as utils
import os.path
import numpy as np
import re
import requests
from bs4 import BeautifulSoup

"""
This website requires an account. You need to fill in the username and password in the main function to collect data.
In bacdive, strains can be retrieved using a tax_id, but the results may not always correspond to the desired organism. Specifically, there are three possible retrieval scenarios:
    1) The ncbi taxonomy ID resolves to the species level. The following strains belong to the species Bacteroides salyersiae.
    2) Sorry, nothing was found for taxid: 2764329.
    3) We could not find the ncbi taxonomy ID 1054460, but we did find other strains belonging to the species Streptococcus pseudopneumoniae.
Therefore, manual curation is required to confirm the correct strains before proceeding with subsequent Archive collection.
"""


class BacDiveTaxonomyCollector:
    def __init__(self, ref_path, now_time, root_dir, save_taxonomy, save_redirect_type, save_search_type,
                 save_final_combine):
        self.ref_path = ref_path
        self.now_time = now_time
        self.out_dir = root_dir
        self.save_taxonomy = save_taxonomy
        self.save_redirect_type = save_redirect_type
        self.save_search_type = save_search_type
        self.save_final_combine = save_final_combine

    def load_ref(self):
        ref_col = [
            'species',
            'species_tax_id',
            'strains_name',
            'strains_tax_id',
            'subspecies_tax_id',
            'subspecies_name',
            'substrains_name',
            'substrains_tax_id',
            'serotype_tax_id',
            'serotype_name'
        ]
        df_ref = utils.load_df(self.ref_path)
        df_ref = df_ref[ref_col]
        return df_ref

    def get_species_name(self, data):
        # Extract species name
        pattern = r'species\s+"([^""]+)"'
        species_name = 'lack'
        match = re.search(pattern, data)
        if match:
            species_name = match.group(1)
        return species_name

    def start_bacdive_name_spider_process(self):
        save_dir = os.path.join(self.out_dir, 'Data')
        utils.create_dir(save_dir)
        save_path = os.path.join(save_dir, self.save_taxonomy)

        result_list = []

        df_ref = self.load_ref()
        ref_tax_id = {
            'species_tax_id': 'species',
            'strains_tax_id': 'strains_name',
            'subspecies_tax_id': 'subspecies_name',
            'substrains_tax_id': 'substrains_name',
            'serotype_tax_id': 'serotype_name'
        }
        for tax_id_col in ref_tax_id:
            tax_name_col = ref_tax_id.get(tax_id_col)
            id_list = df_ref[tax_id_col].unique().tolist()
            for tax_id in id_list:
                # if tax_id != '2792859':
                #     continue

                print('######')
                print(tax_id_col)
                print(tax_id)
                if tax_id == 'NA_NO':
                    continue

                tax_name = df_ref[df_ref[tax_id_col] == tax_id][tax_name_col].head(1).values[0]
                print(tax_name)

                url = f'https://bacdive.dsmz.de/search?search=taxid:{tax_id}'
                print(url)

                d = {
                    'tax_id': tax_id,
                    'name_type': tax_name_col,
                    'ref_name': tax_name,
                    'bacdive_name': 'lack',
                    'search_message': None,
                    'request_status': 'fail',
                    'URL': url,
                    'is_redirect': 'NO',
                    'URL_redirect': None
                }
                # print(d)

                response = requests.get(url)
                if response.status_code == 200:

                    if response.history:
                        print("redirect：")
                        print(f"Final URL: {response.url}")
                        redirect_d = {
                            'is_redirect': 'Yes',
                            'URL_redirect': response.url
                        }
                        d.update(redirect_d)

                    soup = BeautifulSoup(response.text, 'html.parser')
                    search_message_li = soup.find('li', class_='search-message')
                    if search_message_li:
                        search_message = search_message_li.find('span').text
                        print(search_message)
                        website_message_d = {
                            'request_status': 'success',
                            'search_message': search_message,
                            'bacdive_name': self.get_species_name(search_message)
                        }
                        d.update(website_message_d)

                print(d)
                result_list.append(d)
                # print(result_list)

                if len(result_list) % 20 == 0:
                    df_res = pd.DataFrame(result_list)
                    utils.save_df(save_path, df_res)
        df_res = pd.DataFrame(result_list)
        utils.save_df(save_path, df_res)

    def filter_data(self):
        data_path = os.path.join(self.out_dir, 'Data', self.save_taxonomy)
        df = utils.load_df(data_path)
        df = df[df['name_type'] == 'species']
        return df

    def deal_redirect_condition(self):
        df = self.filter_data()
        df = df[df['is_redirect'] == 'Yes']

        redirect_url_list = df['URL_redirect'].tolist()
        redirect_url_list = list(set(redirect_url_list))
        if np.nan in redirect_url_list:
            redirect_url_list.remove(np.nan)
        result_list = []

        for url in redirect_url_list:
            if pd.isnull(url):
                continue

            tax_id = df[df['URL_redirect'] == url]['tax_id'].head(1).values[0]
            response = requests.get(url)
            # if tax_id != '2792859':
            #     continue
            print(f'#########\n{url}\n{tax_id}')

            d = {
                'url': url,
                'tax_id': tax_id,
                'bacdive_name': 'lack'
            }

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')

                p_tags = soup.find_all('p', class_='infobox_key')
                target_p_tag = None
                for p_tag in p_tags:
                    if p_tag.get_text().strip().startswith('Species: '):
                        target_p_tag = p_tag
                        break

                if target_p_tag:
                    span_text = target_p_tag.find_next('span').get_text()
                    print(span_text)
                    d_name = {
                        'tax_id': tax_id,
                        'bacdive_name': span_text
                    }
                    d.update(d_name)

            print(d)
            result_list.append(d)

        if len(result_list) > 0:
            result_df = pd.DataFrame(result_list)
            save_path = os.path.join(self.out_dir, 'Data', self.save_redirect_type)
            utils.save_df(save_path, result_df)

    def deal_search_message_condition(self):
        df = self.filter_data()
        df = df[df['search_message'].notna()]

        def build_bacdive_name(row):
            search_message = row['search_message']
            row['message_type'] = 'lack'
            if 'We could not find the ncbi taxonomy ID' in search_message:
                row['message_type'] = 'We could not find the ncbi taxonomy ID'
                pattern = r'"([^"]*)"'
                match = re.search(pattern, search_message)
                result = match.group(1)
                row['bacdive_name'] = result
            if 'The ncbi taxonomy ID  does resolve to species level.' in search_message:
                row['message_type'] = 'The ncbi taxonomy ID  does resolve to species level.'
                pattern = r'"([^"]*)"'
                match = re.search(pattern, search_message)
                result = match.group(1)
                row['bacdive_name'] = result
            if 'Sorry, nothing found' in search_message:
                row['message_type'] = 'Sorry, nothing found'

            return row

        df = df.apply(build_bacdive_name, axis=1)

        target_col = [
            'tax_id',
            'search_message',
            'message_type',
            'bacdive_name'
        ]
        df = df[target_col]

        save_dir = os.path.join(self.out_dir, 'Data')
        utils.create_dir(save_dir)
        save_path = os.path.join(save_dir, self.save_search_type)
        utils.save_df(save_path, df)

    def combine_final_result(self):
        df_original = self.filter_data()
        drop_col = ['bacdive_name']
        df_original = df_original.drop(columns=drop_col)

        path_redirect = os.path.join(self.out_dir, 'Data', self.save_redirect_type)
        df_redirect = pd.DataFrame()
        if os.path.exists(path_redirect):
            df_redirect = utils.load_df(path_redirect)
            drop_col = ['url']
            df_redirect = df_redirect.drop(columns=drop_col)
            df_redirect['message_type'] = 'URL has been redirect'

        path_search = os.path.join(self.out_dir, 'Data', self.save_search_type)
        df_search = pd.DataFrame()
        if os.path.exists(path_search):
            df_search = utils.load_df(path_search)
            drop_col = ['search_message']
            df_search = df_search.drop(columns=drop_col)

        df_bacdive_name = pd.concat([df_redirect, df_search])
        # save_path = os.path.join(self.out_dir, "Data", "test.tsv")
        # utils.save_df(save_path, df_bacdive_name)

        df_merge = pd.merge(
            left=df_original,
            right=df_bacdive_name,
            on='tax_id'
        )

        df_merge['is_same_of_ref'] = df_merge['bacdive_name'] == df_merge['ref_name']

        columns_order = [
            'tax_id',
            'ref_name',
            'bacdive_name',
            'is_same_of_ref',
            'name_type',
            'search_message',
            'message_type',
            'URL',
            'request_status',
            'is_redirect',
            'URL_redirect',
        ]
        df_merge = df_merge[columns_order]

        save_path = os.path.join(self.out_dir, 'Data', self.save_final_combine)
        utils.save_df(save_path, df_merge)

    def process_all(self):
        self.start_bacdive_name_spider_process()
        self.deal_redirect_condition()
        self.deal_search_message_condition()
        self.combine_final_result()


class BacDiveArchiveDownloader:
    @staticmethod
    def now_time():
        now = datetime.now()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        date_string = year + month + day
        return date_string

    @staticmethod
    def now_time_with_seconds():
        now = datetime.now()
        time_string = now.strftime("%Y-%m-%d-%H-%M-%S")
        return time_string

    @staticmethod
    def build_basement_tree(website_name):
        file_path = os.path.join(os.sep, "Result", website_name)
        explain_path = os.path.join(file_path, "explain")
        log_path = os.path.join(file_path, "log")
        organism_data_path = os.path.join(file_path, "organism_data")

        if not os.path.exists(explain_path):
            os.makedirs(explain_path)
        if not os.path.exists(log_path):
            os.makedirs(log_path)
        if not os.path.exists(organism_data_path):
            os.makedirs(organism_data_path)

    @staticmethod
    def create_dir(path):
        if not os.path.exists(path):
            os.makedirs(path)

    @staticmethod
    def bacdive_spider(user_name, password, scan_path, save_dir, spider_time):
        save_dir_data = os.path.join(save_dir, 'Data')
        utils.create_dir(save_dir_data)
        save_dir_record = os.path.join(save_dir, 'Record')
        utils.create_dir(save_dir_record)
        record_path = os.path.join(save_dir_record, f'Bacdive_{spider_time}_record.tsv')
        record_list = []

        df_ref = utils.load_df(scan_path)
        df_ref = df_ref[['bacdive_name', 'tax_id']]
        df_ref = df_ref.drop_duplicates()
        species_name_dict = df_ref.set_index('bacdive_name')['tax_id'].to_dict()

        client = bacdive.BacdiveClient(user_name, password)

        for bacdive_species_name in species_name_dict:
            print('############')
            if bacdive_species_name == 'lack':
                continue
            print(bacdive_species_name)
            species_tax_id = species_name_dict.get(bacdive_species_name)
            record_d = {
                'species': bacdive_species_name,
                'species_tax_id': species_tax_id,
                'search_count': 0,
            }

            try:
                count = client.search(taxonomy=bacdive_species_name)

                print(count, 'strains found.')
                count_d = {
                    'search_count': count
                }
                record_d.update(count_d)

                i_count = 1
                for strain in client.retrieve():
                    save_path = os.path.join(save_dir_data, f'{species_tax_id}_res{i_count}.json')
                    with open(save_path, 'w+') as f:
                        json.dump(strain, f, indent=4)
                    i_count += 1
            except Exception as e:
                print(f'collect error!：{e}')
                error_message_d = {
                    'error_message': str(e)
                }
                record_d.update(error_message_d)
            record_list.append(record_d)
            df_record = pd.DataFrame(record_list)
            utils.save_df(record_path, df_record)


def run(ref_path, output_dir, username, password, now_time):
    save_taxonomy = f'Bacdive_taxonomy_scan_{now_time}.tsv'
    save_redirect_type = f'Bacdive_redirect_result_{now_time}.tsv'
    save_search_type = f'Bacdive_search_message_result_{now_time}.tsv'
    save_final_combine = f'Bacdive_final_summary_result_{now_time}.tsv'
    taxonomy_save_dir = os.path.join(output_dir, 'Taxonomy')
    TaxonomyCollector = BacDiveTaxonomyCollector(
        ref_path, now_time, taxonomy_save_dir,
        save_taxonomy, save_redirect_type, save_search_type,
        save_final_combine
    )
    TaxonomyCollector.process_all()

    spider_time = now_time
    scan_path = os.path.join(taxonomy_save_dir, 'Data', save_final_combine)
    archive_save_dir = os.path.join(output_dir, 'Retrieve')
    ArchiveDownloader = BacDiveArchiveDownloader()
    ArchiveDownloader.bacdive_spider(
        username,
        password,
        scan_path,
        archive_save_dir,
        spider_time
    )
