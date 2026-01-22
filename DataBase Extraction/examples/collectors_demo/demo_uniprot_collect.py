from collectors import UniProtCollector
from utils import my_df_function as utils



def demo_uniprot_collect():
    now_time = utils.get_now_time()
    ref_path = '../../sample_data/00_reference/reference.tsv'
    root_dir = f'../../result/collect_result/DB_UniProt/'
    Spider = UniProtCollector.UniProtDataSpider(
        now_time,
        ref_path,
        root_dir
    )

    print("Start demo UniProt scan...")
    Spider.start_all()
    print("Demo finished!")


if __name__ == '__main__':
    demo_uniprot_collect()
