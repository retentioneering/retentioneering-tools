# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md



from math import isclose
from .prepare_test_datasets import test_datasets

# global constants
REL_TOL = 0.0001
ABS_TOL = 0.0001


# util function to cycle through parameters for each test
def pytest_generate_tests(metafunc):
    # called once per each test function
    funcarglist = metafunc.cls.params[metafunc.function.__name__]
    argnames = sorted(funcarglist[0])
    metafunc.parametrize(
        argnames, [[funcargs[name] for name in argnames] for funcargs in funcarglist]
    )


event_col = test_datasets[0]['test_dataset'].rete.retention_config['event_col']
next_event_col = 'next_' + event_col
time_col = test_datasets[0]['test_dataset'].rete.retention_config['event_time_col']
index_col = test_datasets[0]['test_dataset'].rete.retention_config['user_col']


# **********************
# **** define tests ****
# **********************

class TestNormalizationFuncs:
    # for each test we would like to run all cases
    params = {}.fromkeys(["test_no_norm_by_events",
                          "test_full_norm_by_events",
                          "test_node_norm_by_events",
                          "test_no_norm_by_users",
                          "test_full_norm_by_users",
                          "test_node_norm_by_users"], test_datasets)

    def test_no_norm_by_events(self, test_dataset):
        assert no_norm_by_events(test_dataset)

    def test_full_norm_by_events(self, test_dataset):
        assert full_norm_by_events(test_dataset)

    def test_node_norm_by_events(self, test_dataset):
        assert node_norm_by_events(test_dataset)

    def test_no_norm_by_users(self, test_dataset):
        assert no_norm_by_users(test_dataset)

    def test_full_norm_by_users(self, test_dataset):
        assert full_norm_by_users(test_dataset)

    def test_node_norm_by_users(self, test_dataset):
        assert node_norm_by_users(test_dataset)


# *****************************
# *** FUNCTIONS DEFINITIONS ***
# *****************************


def dict_compare(d1, d2):
    """
    Compare equality of two dictionaries with numeric values

    Parameters
    ----------
    d1, d2 - dictionaries with numeric values

    Returns
    -------
    bool

    """

    # 1. make sure keys are the same
    if d1.keys() != d2.keys():
        return False

    # 2. for each key check if values are close
    return all(isclose(d1[key], d2[key],
                       rel_tol=REL_TOL,
                       abs_tol=ABS_TOL) for key in d1.keys())


# represent result as dictionary for comparison with control dict
def result_to_dict(result_dataframe):
    result = result_dataframe.copy()
    result['bi-gram'] = result[event_col] + '~~~' + \
                        result[next_event_col]
    return dict(zip(result['bi-gram'], result['edge_weight']))


def no_norm_by_events(test_dataset):
    """
    norm_type=None,
    weight_col=None
    """

    edgelist = test_dataset.rete.get_edgelist(norm_type=None,
                                              weight_col=None)
    result_rete = result_to_dict(edgelist)

    # obtain expected result using control dataset
    control_dataset = test_dataset.rete._get_shift()
    control_dataset['bi-gram'] = control_dataset[event_col] + '~~~' + \
                                 control_dataset[next_event_col]

    result_test = dict(control_dataset['bi-gram'].value_counts())

    return dict_compare(result_rete, result_test)


def full_norm_by_events(test_dataset):
    """
    norm_type='full',
    weight_col=None
    """
    # obtain expected result using rete lib
    edgelist = test_dataset.rete.get_edgelist(norm_type='full',
                                              weight_col=None)
    result_rete = result_to_dict(edgelist)

    # obtain expected result using control dataset
    control_dataset = test_dataset.rete._get_shift()
    control_dataset['bi-gram'] = control_dataset[event_col] + '~~~' + \
                                 control_dataset[next_event_col]

    result_test = dict(control_dataset['bi-gram'].value_counts(normalize=True))

    check_dictionaries = dict_compare(result_rete, result_test)

    # make sure all weights sum to 1:
    check_control_sum = isclose(edgelist['edge_weight'].sum(), 1,
                                rel_tol=REL_TOL, abs_tol=ABS_TOL)

    return all([check_dictionaries, check_control_sum])


def node_norm_by_events(test_dataset):
    """
    norm_type='node',
    weight_col=None
    """
    # obtain expected result using rete lib
    edgelist = test_dataset.rete.get_edgelist(norm_type='node',
                                              weight_col=None)
    result_rete = result_to_dict(edgelist)

    # obtain expected result using control dataset
    control_dataset = test_dataset.rete._get_shift()
    control_dataset['bi-gram'] = control_dataset[event_col] + '~~~' + \
                                 control_dataset[next_event_col]

    node_transitions_counts = control_dataset.groupby(event_col)['bi-gram'].count()
    grouped_control = control_dataset.groupby('bi-gram').agg({event_col: 'first',
                                                              time_col: 'count'}).reset_index()

    grouped_control['node_norm'] = grouped_control[time_col] \
                                   / node_transitions_counts.loc[grouped_control[event_col]].values

    result_test = dict(zip(grouped_control['bi-gram'], grouped_control['node_norm']))

    # are the control and test dicts same?
    check_dictionaries = dict_compare(result_rete, result_test)

    # make sure sum of normalized weights for transitions
    # from each event is equal to 1 (total prob is 1)
    control_sum = edgelist.groupby(event_col)['edge_weight'].sum()
    check_control_sum = all(isclose(x, 1, rel_tol=REL_TOL, abs_tol=ABS_TOL)
                            for x in control_sum)

    return all([check_dictionaries, check_control_sum])


def no_norm_by_users(test_dataset):
    """
    norm_type=None,
    weight_col=INDEX_COL
    """
    edgelist = test_dataset.rete.get_edgelist(norm_type=None,
                                              weight_col=index_col)
    result_rete = result_to_dict(edgelist)

    # obtain expected result using control dataset
    control_dataset = test_dataset.rete._get_shift()
    control_dataset['bi-gram'] = control_dataset[event_col] + '~~~' + \
                                 control_dataset[next_event_col]

    result_test = dict(control_dataset.groupby(['bi-gram'])[index_col].nunique())

    return dict_compare(result_rete, result_test)


def full_norm_by_users(test_dataset):
    """
    norm_type='full',
    weight_col=INDEX_COL
    """
    edgelist = test_dataset.rete.get_edgelist(norm_type='full',
                                              weight_col=index_col)
    result_rete = result_to_dict(edgelist)

    # obtain expected result using control dataset
    control_dataset = test_dataset.rete._get_shift()
    control_dataset['bi-gram'] = control_dataset[event_col] + '~~~' + \
                                 control_dataset[next_event_col]

    result_test = dict(control_dataset.groupby(['bi-gram'])[index_col].nunique()
                       / control_dataset[index_col].nunique())

    return dict_compare(result_rete, result_test)


def node_norm_by_users(test_dataset):
    """
    norm_type='node',
    weight_col=INDEX_COL
    """
    edgelist = test_dataset.rete.get_edgelist(norm_type='node',
                                              weight_col=index_col)
    result_rete = result_to_dict(edgelist)

    # obtain expected result using control dataset
    control_dataset = test_dataset.rete._get_shift()
    control_dataset['bi-gram'] = control_dataset[event_col] + '~~~' + \
                                 control_dataset[next_event_col]

    node_counts = control_dataset.groupby(event_col)[index_col].nunique().to_dict()
    g_test = control_dataset.groupby('bi-gram').agg({event_col: 'first',
                                                     index_col: lambda x: x.nunique()}).reset_index()
    g_test['node_norm'] = g_test[index_col] / g_test[event_col].map(node_counts)
    result_test = dict(zip(g_test['bi-gram'], g_test['node_norm']))

    return dict_compare(result_rete, result_test)
