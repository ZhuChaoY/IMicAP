from collectors import BVBRCColletor
from utils import my_df_function as utils


def demo_bvbrc_collect():
    spider_time = utils.get_now_time()
    base_save_dir = '../../result/collect_result/DB_BV-BRC/'

    spider = BVBRCColletor.BVBRCSpider(base_save_dir, spider_time)

    # sample_data
    collection_configs = [
        ('genome_amr', 2500),
        ('epitope', 2500),
        ('experiment', 700),
        ('bioset', 5000)
    ]

    print("Start demo BVBRC scan...")
    spider.run_all_collections(collection_configs)
    print("Demo finished!")


if __name__ == '__main__':
    demo_bvbrc_collect()
