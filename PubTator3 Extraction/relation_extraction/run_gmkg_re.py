import argparse
from train_gmkg_re import GMKG_RE


parser = argparse.ArgumentParser(description = 'RUN Relation Extraction')

parser.add_argument('--relation_type', type = str, default = 'GM2DI',
                    help = 'relation type: GM2DI, GM2SM')
parser.add_argument('--plm_name', type = str, default = 'biobert',
                    help = 'plm: biobert, scibert, pubmedbert, biolinkbert')
parser.add_argument('--sampling_type', type = str, default = 'none',
                    help = 'sampling type: none, sub, sub-up, up')
parser.add_argument('--cache_dir', type = str, default = '../../hf_cache/',
                     help = 'huggingface cache dir')
parser.add_argument('--learning_rate', type = float, default = 1e-5, 
                    help = 'learning rate')
parser.add_argument('--batch_size', type = int, default = 16,
                    help = 'batch size')
parser.add_argument('--max_epoch', type = int, default = 10,
                    help = 'max training epoch')
parser.add_argument('--earlystop_epochs', type = int, default = 1,
                    help = 'earlystop epochs')
parser.add_argument('--do_train', type = int, default = 1,
                    help = 'whether to train')
parser.add_argument('--do_evaluate', type = int, default = 0,
                    help = 'whether to evaluate')
parser.add_argument('--do_predict', type = int, default = 0,
                    help = 'whether to predict')
parser.add_argument('--gpu_number', type = str, default = '0',
                    help = 'gpu number')

args = parser.parse_args()
obj = GMKG_RE(args)
obj.run()