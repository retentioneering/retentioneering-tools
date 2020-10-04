import pandas as pd
from os.path import dirname

module_path = dirname(__file__)


def load_simple_shop():
    return pd.read_csv(module_path+'/data/simple-onlineshop.csv')


def load_simple_ab_test():
    data = pd.read_csv(module_path+'/data/ab_test_demo.csv')
    data['transaction_ID'] = data['transaction_ID'].astype(str)
    data.loc[data['transaction_ID'] == 'nan', 'transaction_ID'] = None
    data.loc[data['transaction_ID'].notna(), 'transaction_ID'] = \
        data.loc[data['transaction_ID'].notna(), 'transaction_ID'].apply(lambda x: x.replace('.0', ''))
    return data
