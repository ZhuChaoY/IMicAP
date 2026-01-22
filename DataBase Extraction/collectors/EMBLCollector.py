import os.path
import concurrent.futures
import json
import os.path
import requests
import utils.my_df_function as utils
import concurrent.futures


class DatabaseQuickGOCollector:
    def get_tax_id(self):
        ref_path = self.ref_path
        df_ref = utils.load_df(ref_path)

        tax_id_list = []
        target_column_list = [
            'strains_tax_id',
            'substrains_tax_id',
            'serotype_tax_id',
            'species_tax_id',
            'subspecies_tax_id',
        ]
        for column in target_column_list:
            id_list = df_ref[column].unique().tolist()
            tax_id_list = id_list + tax_id_list
        tax_id_list = list(set(tax_id_list))
        while 'NA_NO' in tax_id_list:
            tax_id_list.remove('NA_NO')

        return tax_id_list

    def download_page(self, tax_id, page_number, batch_size, spider_time, tax_save_dir):
        save_name = f'{tax_id}_QuickGO_annotation_page{page_number}_{spider_time}.json'
        save_path = tax_save_dir + save_name
        url = f'https://www.ebi.ac.uk/QuickGO/services/annotation/search?geneProductType=RNA%2Cprotein&taxonId={tax_id}&taxonUsage=exact&limit={batch_size}&page={page_number}'
        print(url)

        if os.path.exists(save_path):
            return None

        try:
            response = requests.get(url)
            if response.status_code == 200:
                data_json = response.json()
                with open(save_path, 'w+') as f:
                    json.dump(data_json, f, indent=4)
                pageInfo = data_json.get('pageInfo')
                if pageInfo:
                    total_page = pageInfo.get('total')
                    return total_page
                utils.random_sleep(mu=0.9936)
            else:
                print(f"Request failed with status code: {response.status_code}")
        except Exception as e:
            print(f"Request failed with Error: {e}")

        utils.random_sleep(mu=0.9936)
        return None

    def __init__(self, spider_time, batch_nums, root_dir, ref_path, limit_size):
        self.spider_time = spider_time
        self.Batch_nums = batch_nums
        self.root_dir = root_dir
        self.ref_path = ref_path
        self.limit_size = limit_size

    def quickgo_collect(self):
        spider_time = self.spider_time
        main_dir = self.root_dir

        save_dir = main_dir + 'Data/'
        utils.create_dir(save_dir)

        tax_id_list = self.get_tax_id()
        if 'NA_NO' in tax_id_list:
            tax_id_list.remove('NA_NO')

        for tax_id in tax_id_list:
            # if tax_id != '294':
            #     continue

            tax_save_dir = save_dir + f'{tax_id}/'
            utils.create_dir(tax_save_dir)

            page_number = 1
            max_page_number = 10000
            batch_size = 200

            print('###################')
            print(tax_id)

            url = f'https://www.ebi.ac.uk/QuickGO/services/annotation/search?geneProductType=RNA%2Cprotein&taxonId={tax_id}&taxonUsage=exact&limit={batch_size}&page={page_number}'
            print(url)
            save_name = f'{tax_id}_QuickGO_annotation_page{page_number}_{spider_time}.json'
            save_path = tax_save_dir + save_name
            try:
                response = requests.get(url)
                if response.status_code == 200:

                    data_json = response.json()
                    with open(save_path, 'w+') as f:
                        json.dump(data_json, f, indent=4)
                    pageInfo = data_json.get('pageInfo')
                    if pageInfo:
                        total_page = pageInfo.get('total')
                        max_page_number = total_page
                else:
                    print(f"Request failed with status code: {response.status_code}")
            except Exception as e:
                print(f"Request failed with Error: {e}")

            print(f"max_pages: {max_page_number}")

            while page_number <= max_page_number:

                if self.limit_size:
                    if page_number > self.limit_size:
                        break

                pages_to_download = min(5, max_page_number - page_number + 1)
                print(f'pages_to_download:{pages_to_download}')
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future_to_total_page = {
                        executor.submit(self.download_page, tax_id, page_number + i, batch_size, spider_time,
                                        tax_save_dir): i
                        for i in range(pages_to_download)
                    }
                    for future in concurrent.futures.as_completed(future_to_total_page):
                        total_page = future.result()
                        if total_page is not None and total_page < max_page_number:
                            max_page_number = total_page
                page_number += pages_to_download


class DatabaseRfamCollector:
    @staticmethod
    def RFAM_data_collect_process(url, save_path):
        response = requests.get(url)

        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                f.write(response.content)
            print(f"save: {save_path}")
        else:
            print(f"download fail，status: {response.status_code}")

    def init_summary_collect(self, save_dir):
        utils.create_dir(save_dir)

        url = "https://ftp.ebi.ac.uk/pub/databases/Rfam/CURRENT/database_files/literature_reference.txt.gz"
        save_name = 'literature_reference.txt.gz'
        save_path = save_dir + save_name
        self.RFAM_data_collect_process(url, save_path)

        url = "https://ftp.ebi.ac.uk/pub/databases/Rfam/CURRENT/database_files/family_literature_reference.txt.gz"
        save_name = 'family_literature_reference.txt.gz'
        save_path = save_dir + save_name
        self.RFAM_data_collect_process(url, save_path)

        url = 'https://ftp.ebi.ac.uk/pub/databases/Rfam/CURRENT/database_files/family.txt.gz'
        save_name = 'family.txt.gz'
        save_path = save_dir + save_name
        self.RFAM_data_collect_process(url, save_path)

    def init_clan_collect(self, save_dir):
        utils.create_dir(save_dir)

        url = "https://ftp.ebi.ac.uk/pub/databases/Rfam/CURRENT/database_files/clan_literature_reference.txt.gz"
        save_name = 'clan_literature_reference.txt.gz'
        save_path = save_dir + save_name
        self.RFAM_data_collect_process(url, save_path)

        url = "https://ftp.ebi.ac.uk/pub/databases/Rfam/CURRENT/database_files/clan_membership.txt.gz"
        save_name = 'clan_membership.txt.gz'
        save_path = save_dir + save_name
        self.RFAM_data_collect_process(url, save_path)

        url = 'https://ftp.ebi.ac.uk/pub/databases/Rfam/CURRENT/database_files/clan.txt.gz'
        save_name = 'clan.txt.gz'
        save_path = save_dir + save_name
        self.RFAM_data_collect_process(url, save_path)