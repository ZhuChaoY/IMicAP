import os
import re
import json
import pickle
import requests
import pandas as pd
from fuzzywuzzy import fuzz
from bs4 import BeautifulSoup as BS



def check_dir(_dir):
    """If directory not exists, create it."""
    
    if not os.path.exists(_dir):
        os.makedirs(_dir)
    return _dir


def myjson(p, data = None):
    """Read (data is None) or Dump (data is not None) a json file."""

    if '.json' not in p:
        p += '.json'
    if data is None:
        with open(p) as file:
            return json.load(file)
    else:
        with open(p, 'w') as file:
            json.dump(data, file, indent = 4)
            
            
def get_elements(obj, key, defaut = None):
    """Get xml elements by tag name."""
    
    content = obj.getElementsByTagName(key)
    if not defaut:
        return content
    else:
        if content == []:
            return defaut
        else:
            return content[0].firstChild.data


def get_attribute(obj, key):
    """Get xml attribute by key from infon object."""
    
    for infon in get_elements(obj, 'infon'):
        if infon.getAttribute('key') == key:
            if infon.firstChild:
                return infon.firstChild.data
            else:
                return '-'
    return '-'


def check_min_distance(loc1, loc2, max_distance): 
    """Compare the shortest distance between two entities with max_distance."""

    for s1, e1 in loc1:
        for s2, e2 in loc2:
            dis = min(abs(s1 - e2), abs(e1 - s2))
            if dis <= max_distance:
                return True
    return False


def process_paragraph(text, loc1, typ1, loc2, typ2):
    """
    Add special tokens into text for two entities.
    Cut the sentences without entities at the start and end of the text.
    """
    
    locs = [(loc, typ1) for loc in set(loc1)] + \
           [(loc, typ2) for loc in set(loc2)]
    locs.sort(key = lambda x: x[0][0], reverse = True)
    for (start, end), typ in locs:
        text = '{}@{}$ {} @/{}${}'.format(text[: start], typ, 
                                          text[start: end], typ, text[end: ])
    
    splits = text.split('. ')
    n_split = len(splits)
    for i in range(n_split):
        if re.findall('@[a-z]{2}\$', splits[i]) != []:
            left = i
            break
    for j in range(n_split - 1, -1, -1):
        if re.findall('@[a-z]{2}\$', splits[j]) != [] or \
            re.findall('@/[a-z]{2}\$', splits[j]) != []:
            right = j
            break
    text = '. '.join(splits[left: right + 1]).strip()
    if text[-1] != '.':
        text += '.'
    
    return text


def count2df(count_dict, key):
    """Transfer count dict to dataframe."""
    
    df = pd.DataFrame([[x, c] for x, c in count_dict.items()])
    df.columns = [key, 'count']
    df = df.sort_values(by = ['count', key], ascending = [False, True])
    
    return df


def doi2journal(doi):
    """Get journal name by doi number."""

    base_url = 'https://pubmed.ncbi.nlm.nih.gov/?term='    
    url = base_url + doi
    obj = BS(requests.get(url).text, features = 'html.parser')
    try:
        journal = obj.findAll('meta', {'name': 'citation_publisher'}) \
            [0]['content'].lower()
    except:
        journal = '-'
    
    return journal
        

def link_jcr_journals(pt3_js, jcr_js):
    """Link PubTator3 journal names to JCR standard journal names."""
    
    jcr_js.remove('frontiers of earth science')
    jcr_js.remove('frontiers of medicine')
    jcr_js.remove('frontiers of physics')
        
    ban_tokens = ['of', 'and', 'in', '&', 'the', 'on', 'de', 'for', 'et', 'fur',
                  'to', 'at', 'da', 'der', 'del', 'di', 'do', 'du', 'la', 'und']
    trans = lambda j: ' '.join([x for x in j.replace('-', ' ').split() 
                                if x not in ban_tokens])
    
    pt3_dict, jcr_dict = {}, {}
    for j in pt3_js:
        y = ' '.join([x[0] for x in trans(j).split()])
        pt3_dict[y] = pt3_dict.get(y, []) + [j]
    for j in jcr_js:
        y = ' '.join([x[0] for x in trans(j).split()])
        jcr_dict[y] = jcr_dict.get(y, []) + [j]
    
    j2jcr = {}       
    for key, pt3s in pt3_dict.items():
        if key in jcr_dict:
            jcrs = jcr_dict[key]
            for pt3 in pt3s:
                for jcr in jcrs:
                    flag = True
                    for t1, t2 in zip(trans(pt3).split(),
                                      trans(jcr).split()):
                        if not t2.startswith(t1):
                            flag = False
                            break
                    if flag and len(pt3) > 1:
                        j2jcr[pt3] = j2jcr.get(pt3, []) + [jcr]
    
    for pt3, jcrs in j2jcr.items():
        if len(jcrs) != 1:
            jcrs.sort(key = lambda x: -fuzz.ratio(x, pt3))
        j2jcr[pt3] = jcrs[0]
    
    j2jcr['acs es t water'] = 'acs es&t water'
    j2jcr['aging'] = 'aging-us'
    j2jcr['amyloid'] = 'amyloid-journal of protein folding disorders'
    j2jcr['angew chem int ed engl'] = 'angewandte chemie-international edition'
    j2jcr['antonie van leeuwenhoek'] = 'antonie van leeuwenhoek international'\
                              ' journal of general and molecular microbiology'
    j2jcr['altex'] = 'altex-alternatives to animal experimentation'
    j2jcr['archaea'] = 'archaea-an international microbiological journal'
    j2jcr['arthroscopy'] = 'arthroscopy-the journal of arthroscopic and ' \
                           'related surgery'
    j2jcr['birth'] = 'birth-issues in perinatal care'
    j2jcr['bjog'] = 'bjog-an international journal of obstetrics and ' \
                    'gynaecology'
    j2jcr['bmj'] = 'bmj-british medical journal'
    j2jcr['dtsch med wochenschr'] = 'deutsche medizinische wochenschrift'
    j2jcr['ca cancer j clin'] = 'ca-a cancer journal for clinicians'
    j2jcr['chem asian j'] = 'chemistry-an asian journal'
    j2jcr['cir cir'] = 'cirugia y cirujanos'
    j2jcr['cranio'] = 'cranio-the journal of craniomandibular & sleep practice'
    j2jcr['copd'] = 'copd-journal of chronic obstructive pulmonary disease'
    j2jcr['daru'] = 'daru-journal of pharmaceutical sciences'
    j2jcr['drugs r d'] = 'drugs in r&d'
    j2jcr['echocardiography'] = 'echocardiography-a journal of ' \
                              'cardiovascular ultrasound and allied techniques'
    j2jcr['encephale'] = 'encephale-revue de psychiatrie clinique ' \
                         'biologique et therapeutique'
    j2jcr['enferm infecc microbiol clin'] = 'enfermedades infecciosas y ' \
                                            'microbiologia clinica'
    j2jcr['explore'] = 'explore-the journal of science and healing'
    j2jcr['g3'] = 'g3-genes genomes genetics'    
    j2jcr['hormones'] = 'hormones-international journal of ' \
                        'endocrinology and metabolism'
    j2jcr['injury'] = 'injury-international journal of the care of the injured'
    j2jcr['j diabetes complications'] = 'journal of diabetes and its ' \
                                        'complications'
    j2jcr['j photochem photobiol b'] = 'journal of photochemistry and ' \
                                       'photobiology b-biology'
    j2jcr['j physiol'] = 'journal of physiology-london'
    j2jcr['jaapa'] = 'jaapa-journal of the american academy of ' \
                     'physician assistants'
    j2jcr['jama'] = 'jama-journal of the american medical association'
    j2jcr['jsls'] = 'jsls-journal of the society of laparoendoscopic surgeons'
    j2jcr['lab chip'] = 'lab on a chip'
    j2jcr['menopause'] = 'menopause-the journal of the north american ' \
                         'menopause society'
    j2jcr['metabolism'] = 'metabolism-clinical and experimental'
    j2jcr['nitric oxide'] = 'nitric oxide-biology and chemistry'
    j2jcr['nuklearmedizin'] = 'nuklearmedizin-nuclear medicine'
    j2jcr['omics'] = 'omics-a journal of integrative biology'
    j2jcr['patient'] = 'patient-patient centered outcomes research'
    j2jcr['perfusion'] = 'perfusion-uk'
    j2jcr['pm r'] = 'pm&r'
    j2jcr['proteins'] = 'proteins-structure function and bioinformatics'
    j2jcr['qjm'] = 'qjm-an international journal of medicine'
    j2jcr['retina'] = 'retina-the journal of retinal and vitreous diseases'
    j2jcr['rofo'] = 'rofo-fortschritte auf dem gebiet der rontgenstrahlen ' \
                    'und der bildgebenden verfahren'
    j2jcr['seizure'] = 'seizure-european journal of epilepsy'
    j2jcr['sports health'] = 'sports health-a multidisciplinary approach'
    j2jcr['stress'] = 'stress-the international journal on the biology ' \
                      'of stress'
    j2jcr['surgeon'] = 'surgeon-journal of the royal colleges of ' \
                       'surgeons of edinburgh and ireland' 
    j2jcr['vasa'] = 'vasa-european journal of vascular medicine'
    j2jcr['wounds'] = 'wounds-a compendium of clinical research and practice'
    j2jcr['yakugaku zasshi'] = 'yakugaku zasshi-journal of the ' \
                               'pharmaceutical society of japan'
    j2jcr['j pediatr'] = 'journal of pediatrics'
    j2jcr['j dermatol'] = 'journal of dermatology'
    for j in ['antibiotics', 'biosensors', 'plants', 'life']:
        j2jcr[j] = j + '-basel'
        
    return j2jcr


def mypickle(p, data = None):
    """Read (data is None) or Dump (data is not None) a pickle file."""

    if '.data' not in p:
        p += '.data'
    if data is None:
        with open(p, 'rb') as file:
            return pickle.load(file)
    else:
        with open(p, 'wb') as file:
            pickle.dump(data, file)  