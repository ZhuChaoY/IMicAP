import json
import os.path
import re

import chardet
import requests
from bs4 import BeautifulSoup

import utils.my_df_function as utils


class MicrobeWikiCollector:

    def __init__(self, ref_path, root_dir,now_time):
        self.ref_path = ref_path
        self.root_dir = root_dir
        self.now_time = now_time

    def print_data_dict(self, data_dict, indent='4'):
        for key, value in data_dict.items():
            if isinstance(value, dict):
                # print(f'{indent}{key}:')
                self.print_data_dict(value, indent + '  ')
            elif isinstance(value, list):
                # print(f'{indent}{key}:')
                for item in value:
                    # print(f'{indent}  - {item}')
                    print()
            else:
                print(f'{indent}{key}: {value}')

    def match_dict_values(self, data_dict, soup):
        for tag in data_dict:

            next_tag_message = data_dict.get(tag)
            # print(next_tag_message)
            if isinstance(next_tag_message, dict) and len(next_tag_message) != 0:
                self.match_dict_values(next_tag_message, soup)
            else:
                span_tag = soup.find('span', {'id': tag})
                p_tag = span_tag.parent.find_next_sibling('p')
                tag_content = [p_tag.text.strip()] if p_tag and len(p_tag.text.strip()) > 0 else []

                span_tag_text = span_tag.text
                # if span_tag_text != 'Genome structure':
                #     continue

                # print('#########')
                print(p_tag)
                print(f"span_tag: {span_tag}")
                print(f'tag_content: {tag_content}')

                if span_tag.find_next().name == "h3":
                    print('Yes')
                    print(span_tag.find_next())
                    continue

                next_p_tag = p_tag.find_next_sibling('p')
                while next_p_tag and next_p_tag.find_previous_sibling().name != "h2" and next_p_tag.find_previous_sibling().name != "h3":
                    data = next_p_tag.text.strip()
                    if len(data) > 0:
                        tag_content.append(data)
                    next_p_tag = next_p_tag.find_next_sibling('p')
                current_value = data_dict[tag]
                if isinstance(current_value, list):
                    current_value.extend(tag_content)
                elif current_value is None:
                    data_dict[tag] = tag_content
        return data_dict

    def match_head_tag(self, soup):
        # Find all the headings
        headings = soup.find("div", {"aria-labelledby": "mw-toc-heading"})

        class_regex = re.compile(r"toclevel-1.*")

        if headings is not None:
            li_tags = headings.find_all("li", {"class": class_regex})

            temp_tag_dict = {}

            for li in li_tags:
                a = li.find("a")
                href = a["href"]
                title = href.replace("#", "")
                # title = a.text.strip()
                temp_tag_dict[title] = self.process_tag(li, 2, title)

                # print(li)
                # print("---------------------------")

                # print(temp_tag_dict)
            return temp_tag_dict
        else:
            return False

    def process_tag(self, tag, cycle_count, father_title):
        sub_dict = []
        ul = tag.find("ul")
        if ul:
            sub_dict = {}
            sub_dict.update({father_title: []})
            class_regex = re.compile(r"toclevel-" + str(cycle_count) + ".*")
            li_tags = ul.find_all("li", class_regex)

            for li in li_tags:
                a = li.find("a")
                href = a["href"]
                title = href.replace("#", "")
                # title = a.text.strip()
                sub_dict[title] = self.process_tag(li, cycle_count + 1, title)
        return sub_dict

    def spider_process(self):
        ref_path = self.ref_path
        spider_time = self.now_time
        df_ref = utils.load_df(ref_path)

        save_dir = self.root_dir + 'Data/'
        utils.create_dir(save_dir)

        record_dir = self.root_dir + 'Record/'
        utils.create_dir(record_dir)
        record_path = record_dir + f'MicrobeWiki_spider_record_{spider_time}.json'
        if os.path.exists(record_path):
            with open(record_path, 'r') as f:
                record_dict = json.load(f)
        else:
            record_dict = {}

        species_name_list = df_ref['species'].unique().tolist()
        for species_name in species_name_list:

            print('##############')
            print(species_name)

            temp_list = species_name.split(' ')
            spider_species_name = '_'.join(temp_list)
            url = f'https://microbewiki.kenyon.edu/index.php/{spider_species_name}'
            print(url)

            try:
                response = requests.get(url)

                deal_status = 'fail'
                print(response.status_code)

                if response.status_code == 200:
                    content = response.content

                    encoding = chardet.detect(content)['encoding']
                    html_string = content.decode(encoding)
                    print(encoding)

                    soup = BeautifulSoup(html_string, "html.parser")

                    tag_dict = self.match_head_tag(soup)
                    # print(tag_dict)

                    d = {
                        'encoding': encoding
                    }
                    data_dict = self.match_dict_values(tag_dict, soup)
                    data_dict.update(d)
                    print(data_dict)

                    save_name = f'MicrobeWiki_{species_name}.json'
                    save_path = save_dir + save_name
                    with open(save_path, 'w+', encoding='utf-8') as f:
                        json.dump(data_dict, f, indent=4)

                    deal_status = 'success'
            except Exception as e:
                deal_status = 'error'
                print(e)

            d = {
                species_name: deal_status
            }
            print(d)
            record_dict.update(d)
            with open(record_path, 'w+') as f:
                json.dump(record_dict, f, indent=4)

            utils.random_sleep(mu=1.24)
