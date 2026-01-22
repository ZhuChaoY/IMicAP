import json

import pandas as pd
import requests as re
from utils import my_df_function as utils


def collect_process(page_size, save_dir, now_time, save_name, encode):
    """Collect and save the original file"""
    url = f'https://hgmb.nmdc.cn/sapi/api/hgmb?pageNo=1&pageSize={page_size}&keyword=&speciesName=undefined&phylum=undefined&typeStrains=undefined&cgmccAccession=undefined&kctcAccession=undefined'

    utils.create_dir(save_dir)

    response = re.get(url)
    content = response.content

    data = json.loads(content.decode(encode))

    with open(save_dir + save_name, 'w+', encoding=encode) as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def parse_json(path_collect, encode, save_dir, save_name):
    """Extract tsv data"""
    with open(path_collect, 'r', encoding=encode) as f:
        data_json = json.load(f)

    data_list = data_json.get('data').get('content')
    df = pd.DataFrame(data_list)

    utils.create_dir(save_dir)
    utils.save_df(save_dir + save_name, df)