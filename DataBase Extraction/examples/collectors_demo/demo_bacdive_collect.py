from collectors import BacDiveCollector
from utils import my_df_function as utils



def demo_bacdive_collect():
    ref_path = '../../sample_data/00_reference/reference.tsv'
    now_time = utils.get_now_time()
    output_dir = '../../result/collect_result-/DB_BacDive'
    username = '1059393866@qq.com'
    password = 'lssw123456'

    collector = BacDiveCollector

    print("Start demo taxonomy scan...")
    collector.run(
        ref_path=ref_path,
        output_dir=output_dir,
        username=username,
        password=password,
        now_time=now_time
    )
    print("Demo finished!")


if __name__ == '__main__':
    demo_bacdive_collect()
