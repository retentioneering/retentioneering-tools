# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md


import retentioneering
from retentioneering import datasets
from retentioneering.datasets import gen_corner_cases
import operator

# ******************************
# **** INITIALIZE dataset 1 ****
# ******************************

simple_shop_data = datasets.load_simple_shop()


retentioneering.config.update({
    'event_col': 'event',
    'event_time_col': 'timestamp',
    'user_col': 'user_id'
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
