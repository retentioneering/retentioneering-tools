from collections import namedtuple
from retentioneering import init_config
from retentioneering import datasets
from retentioneering.datasets import gen_corner_cases
import operator


# global constants
REL_TOL = 0.0001
ABS_TOL = 0.0001

# declare config format
DatasetConfig = namedtuple('DatasetConfig', ['event',
                                             'bigram',
                                             'index_col',
                                             'next_event',
                                             'timestamp'])

# ******************************
# **** INITIALIZE dataset 1 ****
# ******************************

simple_shop_data = datasets.load_simple_shop()
simple_shop_config = DatasetConfig(event='event',
                                   bigram='bi_gram',
                                   index_col='client_id',
                                   next_event='next_event',
                                   timestamp='timestamp')

# setup init_config and apply retention.prepare()
init_config(
    experiments_folder='experiments',
    index_col=simple_shop_config.index_col,
    event_col=simple_shop_config.event,
    event_time_col=simple_shop_config.timestamp,
    positive_target_event='passed',
    negative_target_event='lost',
    pos_target_definition={},
    neg_target_definition={}
)
simple_shop_data = simple_shop_data.retention.prepare()


# **********************************
# **** ADD corner cases to list ****
# **********************************

# list of parameters to test:
# add non-corner case:
test_datasets =[dict(test_dataset=simple_shop_data,
                     dataset_config=simple_shop_config)]


# add corner cases to list of cases
for case in gen_corner_cases.__all__:
    # extract function from module by name
    func = operator.attrgetter(case)(gen_corner_cases)
    test_datasets.append(dict(test_dataset=func(simple_shop_data, simple_shop_config),
                              dataset_config=simple_shop_config))