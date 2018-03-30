import os

curr_dir = os.path.dirname(os.path.abspath(__file__))
conf_path = os.path.join(curr_dir, 'conf')
os.environ['DLVM_CONF'] = conf_path
