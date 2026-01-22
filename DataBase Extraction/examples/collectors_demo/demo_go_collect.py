from collectors import GeneOntologyCollector
from utils import my_df_function as utils



def demo_go_collect():
    save_dir = '../../result/collect_result/DB_GeneOntology/'
    utils.create_dir(save_dir)
    save_path = save_dir + 'go_raw.obo'

    print("Start demo GeneOntology scan...")
    collector = GeneOntologyCollector.GeneOntologyCollector()
    collector.collect_raw_data(save_path=save_path)
    print("Demo finished!")


if __name__ == '__main__':
    demo_go_collect()
