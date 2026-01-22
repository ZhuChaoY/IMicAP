import json
import os
import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
from utils import my_df_function as utils

"""
    biocyc, MetaCyc, and EcoliCyc share the same data source.
    Since biocyc requires a subscription to access its data, the code provided here is for illustrative purposes only.
"""


class BioCycCollector:

    def __init__(self, outer_save_dir, spider_time, headers):
        self.outer_save_dir = outer_save_dir
        self.spider_time = spider_time

        self.headers = headers

        self.summary_dir = None
        self.data_dir = None
        self.record_dir = None

    def _get_html_page(self, save_path, url):

        if os.path.exists(save_path):
            print('exist offline page!')
            with open(save_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
                return html_content

        html_content = ''
        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                html_content = response.text
                with open(save_path, "w", encoding="utf-8") as f:
                    f.write(response.text)
                print("Summary success")
            else:
                print(f"Summary fail, status code: {response.status_code}")
                print(response.text)
        except Exception as e:
            print(f'Summary fail: {e}')

        return html_content

    @staticmethod
    def _extract_summary_info(html_content):

        soup = BeautifulSoup(html_content, 'html.parser')

        tables = soup.find_all('table', {'cellspacing': '0'})

        if len(tables) >= 3:
            third_table = tables[2]
        else:
            third_table = None
            print("do not exist enough needed <table>")
            return pd.DataFrame()

        list_tr = third_table.find_all('tr')

        final_result_list = []

        for tr in list_tr:
            td_list = tr.find_all('td')

            organism_name = td_list[0].get_text()
            a_tag = td_list[1].find('a')
            if a_tag:
                download_link = td_list[1].find('a')['href']
            else:
                download_link = None
            data_version = td_list[2].get_text(strip=True)

            d = {
                'organism_name': organism_name,
                'download_link': download_link,
                'data_version': data_version
            }
            final_result_list.append(d)

        df = pd.DataFrame(final_result_list)

        df = df[df['organism_name'] != 'Database']

        def get_file_name(download_url):
            if pd.isnull(download_url):
                return download_url
            temp_list = download_url.split('/')
            file_name = temp_list[-1]
            return file_name

        df['file_name'] = df['download_link'].apply(lambda x: get_file_name(x))

        return df

    def collect_summary(self, summary_url):
        self.summary_dir = os.path.join(
            self.outer_save_dir,
            f'Summary_data_result/Biocyc_summary_result_{self.spider_time}/'
        )
        utils.create_dir(self.summary_dir)

        html_save_path = os.path.join(self.summary_dir, 'Biocyc_summary_offline_page.html')
        html_content = self._get_html_page(html_save_path, summary_url)

        df = self._extract_summary_info(html_content)

        save_path = os.path.join(self.summary_dir, f'Biocyc_summary_exact_result_{self.spider_time}.tsv')
        utils.save_df(save_path, df)

        print(f"Summary collect end. {len(df)} records")
        return df

    def _download_single_file(self, url, local_filename, save_dir, record_json, download_record_path):
        url = url.replace('http:', 'https:')

        d = {local_filename: 'success'}
        try:
            response = requests.get(url, headers=self.headers)

            if response.status_code == 200:
                save_path = os.path.join(save_dir, local_filename)
                print(save_path)
                with open(save_path, "wb") as f:
                    f.write(response.content)
                print("download success！")
            else:
                print(f"download fail：{response.status_code}")
                print(response.text)
                d = {local_filename: 'fail'}
        except Exception as e:
            print(f'download error: {e}')
            d = {local_filename: 'fail'}

        record_json.update(d)

        with open(download_record_path, 'w+') as f:
            json.dump(record_json, f)

        return record_json

    def download_data_files(self, path_summary):
        self.data_dir = os.path.join(self.outer_save_dir,
                                     f'Data_collect_result/Biocyc_data_collect_{self.spider_time}/')
        self.record_dir = os.path.join(self.data_dir, 'Record/')

        utils.create_dir(self.data_dir)
        utils.create_dir(self.record_dir)

        df_summary = utils.load_df(path_summary)
        download_dict = df_summary.set_index('download_link')['file_name'].to_dict()

        save_dir = os.path.join(self.data_dir, 'Data/')
        utils.create_dir(save_dir)

        download_record_path = os.path.join(self.record_dir, 'Biocyc_download_record.json')

        if os.path.exists(download_record_path):
            with open(download_record_path, 'r') as f:
                record_json = json.load(f)
        else:
            record_json = {}

        for url in download_dict:
            file_name = download_dict.get(url)

            conclude_download = record_json.get(file_name)
            if conclude_download == 'success':
                print(f'{file_name} already collect, next.')
                continue
            else:
                print(f'start collect {file_name}')
                print(url)
                print(file_name)
                print(datetime.datetime.now())
                record_json = self._download_single_file(url, file_name, save_dir, record_json, download_record_path)
                utils.random_sleep(mu=1)
            print('##########')

        print("data file download end")

    def run(self, summary_url):
        df_summary = self.collect_summary(summary_url)
        summary_file_path = os.path.join(self.summary_dir, f'Biocyc_summary_exact_result_{self.spider_time}.tsv')
        self.download_data_files(summary_file_path)