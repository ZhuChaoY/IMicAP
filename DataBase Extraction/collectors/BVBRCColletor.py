import json
import os
import requests

import utils.my_df_function as utils


class BVBRCSpider:
    def __init__(self, base_save_dir, spider_time):

        self.base_save_dir = base_save_dir
        self.spider_time = spider_time
        self.save_dir = f"{base_save_dir}/BvRRC_tsv_collect_result_{spider_time}/Data/"
        self.record_dir = f"{self.save_dir}/Record/"
        self.page_size = 25000

        utils.create_dir(self.save_dir)
        utils.create_dir(self.record_dir)

    def spider_process(self, data_type, max_data_lines):
        record_path = f"{self.record_dir}/{data_type}_record_{self.spider_time}.json"

        if os.path.exists(record_path):
            with open(record_path, 'r', encoding='utf-8') as f:
                record_dict = json.load(f)
        else:
            record_dict = {}

        data_save_dir = os.path.join(self.save_dir, data_type)
        utils.create_dir(data_save_dir)

        start_page = 0
        page_size = self.page_size

        while start_page < max_data_lines:
            end_page = start_page + page_size
            if end_page > max_data_lines:
                end_page = max_data_lines
            items_data = f'items={start_page}-{end_page}'

            headers = {
                'Accept': 'text/tsv',
                'Range': items_data
            }
            url = f'https://www.bv-brc.org/api/{data_type}/'

            print(f"downloading {data_type}: {start_page} to {end_page}")

            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()

                filename = f"{data_type}_{start_page}_to_{end_page}.tsv"
                filepath = os.path.join(data_save_dir, filename)

                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(response.text)

                status = 'Success'
                print(f"download success: {filename}")

            except requests.exceptions.RequestException as e:
                status = 'Fail'
                print(f"download fail: {e}")
            except Exception as e:
                status = 'Fail'
                print(f"error: {e}")

            record_dict[start_page] = status
            print(f"status: {status}")
            print('#' * 50)

            with open(record_path, 'w', encoding='utf-8') as f:
                json.dump(record_dict, f, indent=2)

            start_page = end_page

            utils.random_sleep()

    def run_all_collections(self, collection_configs):

        print(f"start BV-BRC collect: {self.spider_time}")

        for data_type, max_data_lines in collection_configs:
            print(f"\nStart collect data_type:{data_type}")
            self.spider_process(data_type, max_data_lines)
            print(f"Finish '{data_type}' collect")

        print("\nFinish all data collect！")
