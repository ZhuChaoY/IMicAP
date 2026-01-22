import os.path

from collectors import EMBLCollector
from utils import my_df_function as utils



def demo_embl_collect():
    spider_time = utils.get_now_time()
    batch_nums = 'batch3'
    root_dir = f'../../result/collect_result/DB_EMBL/QuickGO/'
    ref_path = '../../sample_data/00_reference/reference.tsv'

    print("Start demo EMBL scan...")

    # QuickGO
    Collector = EMBLCollector.DatabaseQuickGOCollector(spider_time, batch_nums, root_dir, ref_path, limit_size=False)
    Collector.quickgo_collect()

    # Rfam
    save_dir = f'../../result/collect_result/DB_EMBL/Rfam/'
    Collector = EMBLCollector.DatabaseRfamCollector()
    Collector.init_summary_collect(save_dir+'summary/')
    Collector.init_clan_collect(save_dir+'clan/')
    print("Demo finished!")


if __name__ == '__main__':
    demo_embl_collect()
