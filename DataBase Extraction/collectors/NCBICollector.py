import json
import os
import shutil
import time
from ftplib import FTP
import urllib.request

import requests
import utils.my_df_function as utils

"""
This script provides the collection of three types of NCBI data.
RefSeq
Assembly
GENE_INFO
"""


class NCBIRefSeqCollector:
    def __init__(self, root_dir, spider_time, not_time, ref_path=None):
        self.spider_time = spider_time
        self.now_time = not_time
        self.root_dir = root_dir

        self.ref_path = ref_path

        self.ftp_path = 'ftp.ncbi.nlm.nih.gov'
        self.https_path = 'https://ftp.ncbi.nlm.nih.gov'

    def _initialize_ftp_connection(self):
        ftp = FTP(self.ftp_path)
        ftp.login()
        return ftp

    def _load_reference_data(self):
        return utils.load_df(self.ref_path)

    def _setup_directories(self):
        utils.create_dir(self.root_dir)
        save_dir = f'{self.root_dir}/Data/'

        utils.create_dir(save_dir)
        return save_dir

    def _setup_record_files(self):
        record_dir = f'{self.root_dir}Record/'
        utils.create_dir(record_dir)

        record_path = f'{record_dir}record_file_level_{self.now_time}.json'
        record_dict = utils.record_json_get(record_path)

        error_path = f'{record_dir}GCF_error_{self.now_time}.txt'

        return record_path, record_dict, error_path

    def _handle_directory(self, ftp, path, item, local_folder, ftp_path, record_dict, GCF_id):
        print(f'#`#{item}` is a directory, preparing for recursive download.')
        utils.create_dir(local_folder + item + '/')
        self.download_process(
            ftp,
            path + '/' + item,
            local_folder + item + '/',
            ftp_path,
            record_dict,
            GCF_id
        )

    @staticmethod
    def _is_directory(ftp, item):
        try:
            size = ftp.size(item)
            return size is None
        except:
            try:
                original_dir = ftp.pwd()
                ftp.cwd(item)
                ftp.cwd(original_dir)
                return True
            except:
                return False

    def _download_file(self, ftp_dir, item, save_path, file_key, record_dict):
        try:
            print(f"##attempt collect {item}##")

            if record_dict.get(file_key) == 'success':
                print(f"##{item} already collect，next!##")
                return True

            os.makedirs(os.path.dirname(save_path), exist_ok=True)

            url = self.https_path + ftp_dir + item
            response = requests.get(url, stream=True, timeout=60)
            if response.status_code != 200:
                record_dict[file_key] = 'fail'
                print(f'##{item} collect fail, status code:{response.status_code}##')
                return False

            with open(save_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)

            record_dict[file_key] = 'success'
            utils.random_sleep()
            print(f'##{item} collect success##')
            return True

        except Exception as e:
            print(f'##{item} collect fail##')
            print(f'fail message: {str(e)}')
            record_dict[file_key] = 'fail'

            if os.path.exists(save_path):
                try:
                    os.remove(save_path)
                except:
                    pass
            return False

    def download_process(self, ftp, path, local_folder, ftp_path, record_dict, GCF_id):
        print(f'#######################{path}########################')

        try:
            ftp.cwd(path)
        except Exception as e:
            print(e)
            print('####directory change fail####')
            print('####login again####')
            ftp = FTP(ftp_path)
            ftp.login()
            try:
                ftp.cwd(path)
            except:
                print(f"can not enter directory: {path}")
                return

        items = []
        try:
            items = ftp.nlst()
        except Exception as e:
            print(f"can not show file list: {path}, error: {e}")
            return

        for item in items:
            file_key = f"{GCF_id}/{path}/{item}"

            if file_key in record_dict and record_dict[file_key] == 'success':
                print(f"##{item} already collect，next!##")
                continue

            save_path = os.path.join(local_folder, item)

            if self._is_directory(ftp, item):
                print(f'`#{item}` is a directory, preparing for recursive download')
                utils.create_dir(save_path)
                self.download_process(
                    ftp,
                    path + '/' + item,
                    save_path + '/',
                    ftp_path,
                    record_dict,
                    GCF_id
                )
            else:
                self._download_file(path, item, save_path, file_key, record_dict)

        ftp.cwd('..')

    @staticmethod
    def _get_gcf_list(df_reference):
        df_reference = df_reference[df_reference['NCBI_RefSeq_assembly'] != 'NA_NO']
        return df_reference['NCBI_RefSeq_assembly'].unique().tolist()

    @staticmethod
    def _cleanup_failed_downloads(gcf_dir, GCF_id, record_dict):
        has_failures = any(k.startswith(GCF_id) and v == 'fail' for k, v in record_dict.items())

        if has_failures and os.path.exists(gcf_dir):
            for file_key in [k for k in record_dict if k.startswith(GCF_id) and record_dict[k] == 'fail']:
                relative_path = file_key.replace(GCF_id, '').lstrip('/')
                file_path = os.path.join(gcf_dir, relative_path)
                if os.path.exists(file_path):
                    if os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                    else:
                        os.remove(file_path)
        elif not os.path.exists(gcf_dir):
            utils.create_dir(gcf_dir)

    @staticmethod
    def _build_ftp_path(GCF_id):
        cleaned_data = GCF_id[4:].split('.')[0]
        return f"/genomes/all/GCF/{cleaned_data[:3]}/{cleaned_data[3:6]}/{cleaned_data[6:]}/"

    @staticmethod
    def _process_gcf_directory(ftp, GCF_ftp_dir, ftp_path):
        try:
            ftp.cwd(GCF_ftp_dir)
            return True
        except:
            print('###############################################')
            print('Failed to switch directory!!!!!!!!')
            print('Attempting to log in again!!!!!!!!')
            ftp = FTP(ftp_path)
            ftp.login()
            try:
                ftp.cwd(GCF_ftp_dir)
                return True
            except:
                return False

    def _download_gcf_content(self, ftp, GCF_ftp_dir, save_dir, GCF_id, ftp_path, record_dict):
        detail_GCF_list = ftp.nlst()

        for detail_GCF in detail_GCF_list:
            local_dir_gca = save_dir + GCF_id + '/' + detail_GCF + '/'
            local_dir_gca = local_dir_gca.replace('//', '/')
            utils.create_dir(local_dir_gca)

            detail_GCF_dir = GCF_ftp_dir + detail_GCF + '/'
            print(detail_GCF_dir)

            self.download_process(ftp, detail_GCF_dir, local_dir_gca, ftp_path, record_dict, GCF_id)

    def start_download(self):
        df_reference = self._load_reference_data()
        save_dir = self._setup_directories()
        record_path, record_dict, error_path = self._setup_record_files()
        GCF_id_list = self._get_gcf_list(df_reference)

        while True:
            is_all_finish = True

            for GCF_id in GCF_id_list:
                print(GCF_id)

                gcf_dir = save_dir + GCF_id + '/'
                self._cleanup_failed_downloads(gcf_dir, GCF_id, record_dict)

                try:
                    GCF_ftp_dir = self._build_ftp_path(GCF_id)
                    print(GCF_ftp_dir)

                    ftp = self._initialize_ftp_connection()
                    if not self._process_gcf_directory(ftp, GCF_ftp_dir, self.ftp_path):
                        raise Exception(f"can not enter directory: {GCF_ftp_dir}")

                    self._download_gcf_content(ftp, GCF_ftp_dir, save_dir, GCF_id, self.ftp_path, record_dict)
                    print(f'{GCF_id} finish download fail!')

                except Exception as e:
                    print(f'An error occurred during the download of {GCF_id}')
                    print(e)
                    is_all_finish = False

                    with open(error_path, 'a+') as error_f:
                        error_f.write(GCF_id + '\t' + self.now_time + '\t' + str(e) + '\n')
                    time.sleep(6)
                finally:
                    with open(record_path, 'w+') as f:
                        json.dump(record_dict, f, indent=4)

                print('#############')
                utils.random_sleep()

            if is_all_finish:
                break


def common_download(url, save_dir, save_name):
    utils.create_dir(save_dir)
    save_path = save_dir + save_name
    try:
        urllib.request.urlretrieve(url, save_path)
        print(f'finish {save_name} download')
    except Exception as e:
        print(e)
