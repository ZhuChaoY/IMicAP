from collectors import BacDiveCollector
from collectors import BioCycCollector
from collectors import BVBRCColletor
from collectors import ChEBICollector
from collectors import EMBLCollector
from collectors import GeneOntologyCollector
from collectors import hGMBCollector
from collectors import MicrobeWikiCollector
from collectors import NCBICollector
from collectors import UniProtCollector
from utils import my_df_function as utils

# BacDiveCollector
"""
BacDive Collector Usage Notes:
- This website requires an account. You need to fill in the username and password in the main function to collect data.
- In BacDive, strains can be retrieved using a tax_id, but the results may not always correspond to the desired organism.
    There are three possible retrieval scenarios:
        1) The ncbi taxonomy ID resolves to the species level. The following strains belong to the species.
        2) Sorry, nothing was found for taxid: ...
        3) The ncbi taxonomy ID was not found, but other strains belonging to the species were found.
- Therefore, manual curation is required to confirm the correct strains before proceeding with subsequent Archive collection.
"""


def demo_bacdive_collect():
    """
    BacDive Collector Example:
    Demonstrates how to collect data from BacDive.

    Parameters:
        ref_path (str): Path to the reference file containing taxonomic information.
        now_time (str): Current timestamp for output naming.
        output_dir (str): Directory to save the BacDive results.
        username (str): BacDive account username (required).
        password (str): BacDive account password (required).
    """
    ref_path = '../sample_data/00_reference/reference.tsv'
    now_time = utils.get_now_time()
    output_dir = '../result/collect_result/DB_BacDive'
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


"""
BioCyc, MetaCyc, and EcoliCyc share the same data source.
Since biocyc requires a subscription to access its data, the code provided here is for illustrative purposes only.
"""


# BioCycCollector Example
def demo_biocyc_collect():
    """
    BioCyc Collector Example:
    Demonstrates how to collect data from BioCyc/MetaCyc/EcoliCyc.

    Parameters:
        outer_save_dir (str): Directory to save the BioCyc results.
        spider_time (str): Current timestamp for output naming.
        summary_url (str): URL to the BioCyc summary/index page.
        headers (dict): HTTP headers for the request (User-Agent, Cookie, etc.).
    Note:
        BioCyc requires a subscription to access its data. This code is for illustrative purposes only.
    """
    outer_save_dir = '../result/collect_result/DB_BioCyc/'
    spider_time = utils.get_now_time()
    summary_url = 'https://brg-files.ai.sri.com/subscription/dist/flatfiles-52983746/index.html'

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        "Cookie": "...",
    }

    collector = BioCycCollector.BioCycCollector(
        outer_save_dir=outer_save_dir,
        spider_time=spider_time,
        headers=headers
    )
    collector.run(summary_url)


# Example of BVBRCCollector
"""
BVBRC Collector Example:
Demonstrates how to collect data from the Bacterial and Viral Bioinformatics Resource Center (BV-BRC).
"""


def demo_bvbrc_collect():
    """
    BVBRC Collector Example:
    Demonstrates how to collect data from the Bacterial and Viral Bioinformatics Resource Center (BV-BRC).

    Parameters:
        spider_time (str): Current timestamp for output naming.
        base_save_dir (str): Directory to save the BV-BRC results.
        collection_configs (list): List of tuples specifying collection types and their limits.
    """
    spider_time = utils.get_now_time()
    base_save_dir = '../result/collect_result/DB_BV-BRC/'

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


# Example of ChEBICollector
"""
ChEBI Collector Example:
Demonstrates how to collect ontology and SDF data from the Chemical Entities of Biological Interest (ChEBI) database.
"""


def demo_chebi_collect():
    """
    ChEBI Collector Example:
    Demonstrates how to collect ontology and SDF data from the Chemical Entities of Biological Interest (ChEBI) database.

    Parameters:
        root_dir (str): Root directory to save ChEBI results.
    """
    print("Start demo ChEBI scan...")
    root_dir = "../result/collect_result/DB_ChEBI/"

    save_dir = f'{root_dir}DB_ChEBI/Ontology/'
    ChEBICollector.init_ontology_collect(save_dir)

    save_dir = f'{root_dir}DB_ChEBI/SDF/'
    ChEBICollector.init_SDF_collect(save_dir)
    print("Demo finished!")


# Example of EMBLCollector
"""
EMBL Collector Example:
Demonstrates how to collect data from the European Molecular Biology Laboratory (EMBL) databases, including QuickGO and Rfam.
"""


def demo_embl_collect():
    """
    EMBL Collector Example:
    Demonstrates how to collect data from the European Molecular Biology Laboratory (EMBL) databases, including QuickGO and Rfam.

    Parameters:
        spider_time (str): Current timestamp for output naming.
        batch_nums (str): Batch identifier for the collection.
        root_dir (str): Directory to save QuickGO results.
        ref_path (str): Path to the reference file.
    """
    spider_time = utils.get_now_time()
    batch_nums = 'batch3'
    root_dir = f'../result/collect_result/DB_EMBL/QuickGO/'
    ref_path = '../sample_data/00_reference/reference.tsv'

    print("Start demo EMBL scan...")

    # QuickGO
    limit_size = 15
    Collector = EMBLCollector.DatabaseQuickGOCollector(spider_time, batch_nums, root_dir, ref_path,
                                                       limit_size=limit_size)
    Collector.quickgo_collect()

    # Rfam
    save_dir = f'../result/collect_result/DB_EMBL/Rfam/'
    Collector = EMBLCollector.DatabaseRfamCollector()
    Collector.init_summary_collect(save_dir + 'summary/')
    Collector.init_clan_collect(save_dir + 'clan/')
    print("Demo finished!")


# Example of GeneOntologyCollector
"""
Gene Ontology Collector Example:
Demonstrates how to collect raw Gene Ontology data (go_raw.obo).
"""


def demo_go_collect():
    """
    Gene Ontology Collector Example:
    Demonstrates how to collect raw Gene Ontology data (go_raw.obo).

    Parameters:
        save_dir (str): Directory to save Gene Ontology results.
        save_path (str): Path to save the raw OBO file.
    """
    save_dir = '../result/collect_result/DB_GeneOntology/'
    utils.create_dir(save_dir)
    save_path = save_dir + 'go_raw.obo'

    print("Start demo GeneOntology scan...")
    collector = GeneOntologyCollector.GeneOntologyCollector()
    collector.collect_raw_data(save_path=save_path)
    print("Demo finished!")


# Example of hGMBCollector
"""
hGMB Collector Example:
Demonstrates how to collect and parse data from the human Gut Microbiome (hGMB) database.
"""


def demo_hgmb_collect():
    """
    hGMB Collector Example:
    Demonstrates how to collect and parse data from the human Gut Microbiome (hGMB) database.

    Parameters:
        encode (str): Encoding format for files.
        now_time (str): Current timestamp for output naming.
        save_dir (str): Directory to save hGMB results.
        page_size (int): Number of records per page for collection.
    """
    encode = 'utf-8'
    now_time = utils.get_now_time()
    save_dir = f'../result/collect_result/DB_hGMB/'
    utils.create_dir(save_dir)
    page_size = 500

    print("Start demo hGMB scan...")

    save_name = f'hGMB_original_{now_time}.json'
    hGMBCollector.collect_process(page_size, save_dir, now_time, save_name, encode)
    path_collect = save_dir + save_name

    save_name = f'hGMB_parsed_{now_time}.tsv'
    hGMBCollector.parse_json(path_collect, encode, save_dir, save_name)

    print("Demo finished!")


# Example of MicrobeWikiCollector
"""
MicrobeWiki Collector Example:
Demonstrates how to collect data from the MicrobeWiki database using a reference file.
"""


def demo_microbewiki_collect():
    """
    MicrobeWiki Collector Example:
    Demonstrates how to collect data from the MicrobeWiki database using a reference file.

    Parameters:
        ref_path (str): Path to the reference file.
        now_time (str): Current timestamp for output naming.
        root_dir (str): Directory to save MicrobeWiki results.
    """
    ref_path = '../sample_data/00_reference/reference.tsv'
    now_time = utils.get_now_time()
    root_dir = f'../result/collect_result/DB_MicrobeWiki/'

    print("Start demo MicrobeWiki scan...")
    Collector = MicrobeWikiCollector.MicrobeWikiCollector(
        ref_path=ref_path,
        root_dir=root_dir,
        now_time=now_time
    )
    Collector.spider_process()
    print("Demo finished!")


# Example of NCBICollector
"""
NCBI Collector Example:
Demonstrates how to download and process data from NCBI, including assembly summary, gene info, and RefSeq data.
"""


def demo_ncbi_collect():
    """
    NCBI Collector Example:
    Demonstrates how to download and process data from NCBI, including assembly summary, gene info, and RefSeq data.

    Parameters:
        spider_time (str): Current timestamp for output naming.
        now_time (str): Current timestamp for file naming.
        url (str): Download URL for each NCBI resource.
        save_dir (str): Directory to save each type of NCBI data.
        save_name (str): Output file name for each resource.
        outer_save_dir (str): Directory to save RefSeq data.
        ref_path (str): Path to the reference file.
    """
    spider_time = utils.get_now_time()
    now_time = utils.get_now_time()

    print("Start demo NCBI scan...")
    #########assembly_summary#########
    url = 'https://ftp.ncbi.nlm.nih.gov/genomes/genbank/assembly_summary_genbank.txt'
    save_dir = '../result/collect_result/DB_NCBI/Assembly/'
    utils.create_dir(save_dir)
    save_name = f"assembly_summary_genbank_{spider_time}.txt"
    NCBICollector.common_download(
        url,
        save_dir,
        save_name
    )

    #########gene_info#########
    url = 'https://ftp.ncbi.nlm.nih.gov/gene/DATA/GENE_INFO/All_Data.gene_info.gz'
    save_dir = '../result/collect_result/DB_NCBI/GENE_INFO/'
    utils.create_dir(save_dir)
    save_name = f"All_Data.gene_info_{spider_time}.gz"
    NCBICollector.common_download(
        url,
        save_dir,
        save_name
    )

    #########RefSeq#########
    outer_save_dir = '../result/collect_result/DB_NCBI/GCF/'
    ref_path = '../sample_data/00_reference/reference.tsv'
    collector = NCBICollector.NCBIRefSeqCollector(
        root_dir=outer_save_dir,
        spider_time=spider_time,
        not_time=now_time,
        ref_path=ref_path
    )
    collector.start_download()
    print("Demo finished!")


# Example of UniProtCollector
"""
UniProt Collector Example:
Demonstrates how to collect data from the UniProt database using a reference file.
"""


def demo_uniprot_collect():
    """
    UniProt Collector Example:
    Demonstrates how to collect data from the UniProt database using a reference file.

    Parameters:
        now_time (str): Current timestamp for output naming.
        ref_path (str): Path to the reference file.
        root_dir (str): Directory to save UniProt results.
    """
    now_time = utils.get_now_time()
    ref_path = '../sample_data/00_reference/reference.tsv'
    root_dir = f'../result/collect_result/DB_UniProt/'
    Spider = UniProtCollector.UniProtDataSpider(
        now_time,
        ref_path,
        root_dir,
        limit_cycle=3
    )

    print("Start demo UniProt scan...")
    Spider.start_all()
    print("Demo finished!")


if __name__ == '__main__':
    demo_bacdive_collect()
    demo_bvbrc_collect()
    demo_chebi_collect()
    demo_embl_collect()
    demo_go_collect()
    demo_hgmb_collect()
    demo_microbewiki_collect()
    demo_ncbi_collect()
    demo_uniprot_collect()
