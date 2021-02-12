# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md


from math import isclose

import retentioneering
from retentioneering import datasets

# import data
retentioneering.config.update({
    'event_col': 'event',
    'event_time_col': 'timestamp',
    'user_col': 'user_id'
})
data = datasets.load_simple_shop()
test_datasets = [dict(test_dataset=data)]


# util function to cycle through parameters for each test
def pytest_generate_tests(metafunc):
    # called once per each test function
    funcarglist = metafunc.cls.params[metafunc.function.__name__]
    argnames = sorted(funcarglist[0])
    metafunc.parametrize(
        argnames, [[funcargs[name] for name in argnames] for funcargs in funcarglist]
    )


class TestSequencesFuncs:
    # for each test we would like to run all cases
    params = {}.fromkeys(["test_wrong_ngram_ranges",
                          "test_wrong_target_list",
                          "test_wrong_fraction",
                          "test_wrong_coeffs",
                          "test_correct_behaviour"], test_datasets)
    def test_wrong_ngram_ranges(self, test_dataset):
        assert wrong_ngram_ranges(test_dataset)

    def test_wrong_target_list(self, test_dataset):
        assert wrong_target_list(test_dataset)

    def test_wrong_fraction(self, test_dataset):
        assert wrong_fraction(test_dataset)

    def test_wrong_coeffs(self, test_dataset):
        assert wrong_coeffs(test_dataset)

    def test_correct_behaviour(self, test_dataset):
        assert correct_behaviour(test_dataset)


# *****************************
# *** FUNCTIONS DEFINITIONS ***
# *****************************
def wrong_ngram_ranges(test_dataset):
    for wrong_ngrams in [(0,0),(0,1,1),('s',1),(False,True),[1,3]]:
        try:
            test_dataset.rete.find_sequences(target_list=['cart'],ngram_range=wrong_ngrams)
        except ValueError:
            pass
        else:
            return False
        return True

def wrong_target_list(test_dataset):
    for wrong_target_list in [(0,0),(0,1,1),('s',1),(False,True),[1,3],test_dataset.event.unique()]:
        try:
            test_dataset.rete.find_sequences(target_list=wrong_target_list,ngram_range=(1,1))
        except ValueError:
            pass
        else:
            return False
        return True

def wrong_fraction(test_dataset):
    for wrong_fraction in [2,-1,True,'sda']:
        try:
            test_dataset.rete.find_sequences(target_list=['cart'],ngram_range=(1,1),fraction=wrong_fraction)
        except ValueError:
            pass
        else:
            return False
        return True

def wrong_coeffs(test_dataset):
    for wrong_coeffs in [[1,1],[1.,1],[1.,1.]]:
        try:
            test_dataset.rete.find_sequences(target_list=['cart'],ngram_range=(1,1),\
                                             threshold = wrong_coeffs[0],
                                             coefficient = wrong_coeffs[1],
            )
        except ValueError:
            pass
        else:
            return False
        return True

def correct_behaviour(test_dataset):
    try:
        res = test_dataset.rete.find_sequences(target_list=['cart'],ngram_range=(1,1))
    except ValueError:
        return True
    else:
#             return res,test_dataset
        return res.shape[0] == test_dataset[test_dataset.event != 'lost'].event.nunique()
