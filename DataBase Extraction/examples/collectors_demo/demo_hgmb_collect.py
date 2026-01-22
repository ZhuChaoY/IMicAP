from collectors import hGMBCollector
from utils import my_df_function as utils



def demo_hgmb_collect():
    encode = 'utf-8'
    now_time = utils.get_now_time()
    save_dir = f'../../result/collect_result/DB_hGMB/'
    utils.create_dir(save_dir)
    page_size = 500

    print("Start demo hGMB scan...")

    save_name = f'hGMB_original_{now_time}.json'
    hGMBCollector.collect_process(page_size, save_dir, now_time, save_name, encode)
    path_collect = save_dir + save_name

    save_name = f'hGMB_parsed_{now_time}.tsv'
    hGMBCollector.parse_json(path_collect, encode, save_dir, save_name)

    print("Demo finished!")


if __name__ == '__main__':
    demo_hgmb_collect()
