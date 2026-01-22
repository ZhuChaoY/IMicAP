from collectors import MicrobeWikiCollector
from utils import my_df_function as utils



def demo_microbewiki_collect():
    ref_path = '../../sample_data/00_reference/reference.tsv'
    now_time = utils.get_now_time()
    root_dir = f'../../result/collect_result/DB_MicrobeWiki/'

    print("Start demo MicrobeWiki scan...")
    Collector = MicrobeWikiCollector.MicrobeWikiCollector(
        ref_path=ref_path,
        root_dir=root_dir,
        now_time=now_time
    )
    Collector.spider_process()
    print("Demo finished!")


if __name__ == '__main__':
    demo_microbewiki_collect()
