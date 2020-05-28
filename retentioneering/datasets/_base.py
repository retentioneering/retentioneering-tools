import pandas as pd
from os.path import dirname

module_path = dirname(__file__)

def load_simple_shop():
    return pd.read_csv(module_path+'/data/simple-onlineshop.csv')
