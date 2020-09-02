import retentioneering
from retentioneering import datasets
from retentioneering.datasets import gen_corner_cases
import operator


# global constants
REL_TOL = 0.0001
ABS_TOL = 0.0001


# ******************************
# **** INITIALIZE dataset 1 ****
# ******************************

simple_shop_data = datasets.load_simple_shop()


retentioneering.config.update({
    'event_col':'event',
    'event_time_col':'timestamp',
    'index_col': 'client_id'
})


# **********************************
# **** ADD corner cases to list ****
# **********************************

# list of parameters to test:
# add non-corner case:
test_datasets = [dict(test_dataset=simple_shop_data)]


# add corner cases to list of cases
for case in gen_corner_cases.__all__:
    # extract function from module by name
    func = operator.attrgetter(case)(gen_corner_cases)
    test_datasets.append(dict(test_dataset=func(simple_shop_data)))
