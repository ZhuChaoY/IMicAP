from collectors import BioCycCollector
from utils import my_df_function as utils

"""
    biocyc, MetaCyc, and EcoliCyc share the same data source.
    Since biocyc requires a subscription to access its data, the code provided here is for illustrative purposes only.
"""


def demo_biocyc_collect():
    outer_save_dir = '../result/collect_result/DB_BioCyc/'
    spider_time = utils.get_now_time()
    summary_url = 'https://brg-files.ai.sri.com/subscription/dist/flatfiles-52983746/index.html'

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        "Cookie": "_hjSessionUser_2449904=eyJpZCI6IjUwZmE3MmQ4LTdmOTItNWYxYi04MmQ5LThmZWI5Y2Y2ZDM3NyIsImNyZWF0ZWQiOjE3MTc3NTE2MDQwNzIsImV4aXN0aW5nIjp0cnVlfQ==; __stripe_mid=29a9d2f1-c0e1-469e-9022-e13afb31b0be2c20cb; _ga_V4KYG66FG7=GS1.2.1718262743.4.0.1718262743.0.0.0; PTools-session=biocyc18-3926740398%7CNIL%20NIL%20NIL%2080156%200%20(%3AWEB%20NIL%203927061601%20((%3ABASICS%206)%20(%3AQUERIES%20-1)%20(%3AADVANCED%20-1)))%20NIL%20NIL%20NIL%20ECOBASE%20NIL%20NIL%20%7Cyzis14dmm63l2kigekxar4psvcxmuv; _ga_NVK8K9ZK0V=GS1.1.1718262743.6.0.1718262758.0.0.0; _ga=GA1.1.1446503372.1716182379; _ga_7CXVNGJ2YW=GS1.1.1718262828.7.0.1718262830.0.0.0",
    }

    collector = BioCycCollector.BioCycCollector(
        outer_save_dir=outer_save_dir,
        spider_time=spider_time,
        headers=headers
    )
    collector.run(summary_url)


if __name__ == '__main__':
    demo_biocyc_collect()
