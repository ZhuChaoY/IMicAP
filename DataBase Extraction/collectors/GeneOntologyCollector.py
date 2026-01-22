from utils import my_df_function as utils
import requests


class GeneOntologyCollector:
    def __init__(self):
        self.go_terms_url = 'https://current.geneontology.org/ontology/go.obo'

    def collect_raw_data(self, save_path):

        try:
            response = requests.get(self.go_terms_url, timeout=60)
            response.raise_for_status()

            raw_text = response.text

            if save_path:
                with open(save_path, 'w', encoding='utf-8') as f:
                    f.write(raw_text)

        except Exception as e:
            print(f"Collection error: {e}")

