from collectors import NCBICollector
from utils import my_df_function as utils



def demo_ncbi_collect():
    spider_time = utils.get_now_time()
    now_time = utils.get_now_time()

    print("Start demo NCBI scan...")
    # assembly_summary
    url = 'https://ftp.ncbi.nlm.nih.gov/genomes/genbank/assembly_summary_genbank.txt'
    save_dir = '../../result/collect_result/DB_NCBI/Assembly/'
    utils.create_dir(save_dir)
    save_name = f"assembly_summary_genbank_{spider_time}.txt"
    NCBICollector.common_download(
        url,
        save_dir,
        save_name
    )

    # gene_info
    url = 'https://ftp.ncbi.nlm.nih.gov/gene/DATA/GENE_INFO/All_Data.gene_info.gz'
    save_dir = '../../result/collect_result/DB_NCBI/GENE_INFO/'
    utils.create_dir(save_dir)
    save_name = f"All_Data.gene.info_{spider_time}.gz"
    NCBICollector.common_download(
        url,
        save_dir,
        save_name
    )

    # RefSeq
    outer_save_dir = '../../result/collect_result/DB_NCBI/GCF/'
    ref_path = '../../sample_data/00_reference/reference.tsv'
    collector = NCBICollector.NCBIRefSeqCollector(
        root_dir=outer_save_dir,
        spider_time=spider_time,
        not_time=now_time,
        ref_path=ref_path
    )
    collector.start_download()
    print("Demo finished!")


if __name__ == '__main__':
    demo_ncbi_collect()
