import utils.my_df_function as utils

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
import time


def create_session_with_retry(retries=3, backoff_factor=0.5, status_forcelist=None):
    session = requests.Session()

    if status_forcelist is None:
        status_forcelist = [500, 502, 503, 504]

    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    return session


def ChEBI_data_collect_process(url, save_path, max_retries=3):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    }

    print(f"start: {url}")
    print(f"save: {save_path}")

    temp_path = save_path + '.tmp'
    downloaded_size = 0

    if os.path.exists(temp_path):
        downloaded_size = os.path.getsize(temp_path)
        print(f"Incomplete download detected, downloaded: {downloaded_size} bytes")
        headers['Range'] = f'bytes={downloaded_size}-'

    session = create_session_with_retry(retries=max_retries)

    for attempt in range(max_retries):
        try:
            print(f"try {attempt + 1}/{max_retries}")

            if downloaded_size > 0:
                response = session.get(url, headers=headers, timeout=(30, 60), stream=True)
                if response.status_code == 206:  # Partial Content
                    print(f"Continue download, status code: 206 Partial Content")
                elif response.status_code == 200:
                    print(f"server does not support resumable downloads. retry")
                    downloaded_size = 0
            else:
                response = session.get(url, headers=headers, timeout=(30, 60), stream=True)

            print(f"HTTP status code: {response.status_code}")

            if response.status_code in [200, 206]:
                total_size = int(response.headers.get('content-length', 0))
                if downloaded_size > 0 and 'content-range' in response.headers:
                    content_range = response.headers.get('content-range')
                    if content_range and '/' in content_range:
                        total_size = int(content_range.split('/')[-1])

                print(f"file size: {total_size} Byte ({total_size / 1024 / 1024:.2f} MB)")
                print(f"already download: {downloaded_size} Byte")

                mode = 'ab' if downloaded_size > 0 else 'wb'
                with open(temp_path, mode) as f:
                    downloaded = downloaded_size
                    chunk_size = 8192

                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            f.flush()

                            if downloaded % (5 * 1024 * 1024) == 0:
                                percent = (downloaded / total_size * 100) if total_size > 0 else 0
                                print(f"  processing: {downloaded}/{total_size} Byte ({percent:.1f}%)")

                if 0 < total_size == os.path.getsize(temp_path):
                    if os.path.exists(save_path):
                        os.remove(save_path)
                    os.rename(temp_path, save_path)
                    print(f"✓ Finish Download！file size: {os.path.getsize(save_path)} Byte")
                    return True
                else:
                    downloaded_size = os.path.getsize(temp_path)
                    print(f"Incomplete download, downloaded: {downloaded_size} bytes")
                    if attempt < max_retries - 1:
                        print(f"Waiting for 5 seconds before retrying....")
                        time.sleep(5)
                    continue

            else:
                print(f"Request failed, status code: {response.status_code}")
                if attempt < max_retries - 1:
                    print(f"Waiting for 3 seconds before retrying...")
                    time.sleep(3)

        except requests.exceptions.Timeout:
            print(f"Request timed out, attempting to retry. {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                print(f"Waiting for 10 seconds before retrying....")
                time.sleep(10)

        except requests.exceptions.ChunkedEncodingError as e:
            print(f"connect lost: {e}")
            if attempt < max_retries - 1:
                print(f"Waiting for 5 seconds before retrying...")
                time.sleep(5)

        except Exception as e:
            print(f"download error: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Waiting for 5 seconds before retrying...")
                time.sleep(5)

    print(f"✗ all file，retry {max_retries} times")

    if os.path.exists(temp_path):
        print(f"save temporary file: {temp_path}")

    return False


def init_ontology_collect(save_dir):
    utils.create_dir(save_dir)

    url = "https://ftp.ebi.ac.uk/pub/databases/chebi/ontology/chebi.json.gz"
    save_name = 'chebi.json.gz'
    save_path = save_dir + save_name
    ChEBI_data_collect_process(url, save_path)

    url = "https://ftp.ebi.ac.uk/pub/databases/chebi/ontology/chebi.obo.gz"
    save_name = 'chebi.obo.gz'
    save_path = save_dir + save_name
    ChEBI_data_collect_process(url, save_path)

    url = 'https://ftp.ebi.ac.uk/pub/databases/chebi/ontology/chebi.owl.gz'
    save_name = 'chebi.owl.gz'
    save_path = save_dir + save_name
    ChEBI_data_collect_process(url, save_path)


def init_SDF_collect(save_dir):
    utils.create_dir(save_dir)

    url = "https://ftp.ebi.ac.uk/pub/databases/chebi/SDF/chebi.sdf.gz"
    save_name = 'chebi.sdf.gz'
    save_path = save_dir + save_name
    ChEBI_data_collect_process(url, save_path)