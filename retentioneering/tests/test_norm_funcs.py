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

    def test_no_norm_by_events(self, test_dataset, dataset_config):
        assert no_norm_by_events(test_dataset, dataset_config)

    def test_full_norm_by_events(self, test_dataset, dataset_config):
        assert full_norm_by_events(test_dataset, dataset_config)

    def test_node_norm_by_events(self, test_dataset, dataset_config):
        assert node_norm_by_events(test_dataset, dataset_config)

    def test_no_norm_by_users(self, test_dataset, dataset_config):
        assert no_norm_by_users(test_dataset, dataset_config)

    def test_full_norm_by_users(self, test_dataset, dataset_config):
        assert full_norm_by_users(test_dataset, dataset_config)

    def test_node_norm_by_users(self, test_dataset, dataset_config):
        assert node_norm_by_users(test_dataset, dataset_config)


#*****************************
#*** FUNCTIONS DEFINITIONS ***
#*****************************

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
def result_to_dict(result_dataframe, dataset_config):
    result = result_dataframe.copy()
    result[dataset_config.bigram] = result[dataset_config.event] + '~~~' + \
                                     result[dataset_config.next_event]
    return dict(zip(result[dataset_config.bigram], result['edge_weight']))


def no_norm_by_events(test_dataset, dataset_config):
    """
    norm_type=None,
    weight_col=None
    """
    edgelist = test_dataset.retention.get_edgelist(norm_type=None,
                                                   weight_col=None)
    result_rete = result_to_dict(edgelist, dataset_config)

    # obtain expected result using control dataset
    control_dataset = test_dataset.retention.get_shift()
    control_dataset[dataset_config.bigram] = control_dataset[dataset_config.event] + '~~~' + \
                                             control_dataset[dataset_config.next_event]

    result_test = dict(control_dataset[dataset_config.bigram].value_counts())

    return dict_compare(result_rete, result_test)


def full_norm_by_events(test_dataset, dataset_config):
    """
    norm_type='full',
    weight_col=None
    """
    # obtain expected result using rete lib
    edgelist = test_dataset.retention.get_edgelist(norm_type='full',
                                                   weight_col=None)
    result_rete = result_to_dict(edgelist, dataset_config)

    # obtain expected result using control dataset
    control_dataset = test_dataset.retention.get_shift()
    control_dataset[dataset_config.bigram] = control_dataset[dataset_config.event] + '~~~' + \
                                             control_dataset[dataset_config.next_event]

    result_test = dict(control_dataset[dataset_config.bigram].value_counts(normalize=True))

    check_dictionaries = dict_compare(result_rete, result_test)

    # make sure all weights sum to 1:
    check_control_sum = isclose(edgelist['edge_weight'].sum(), 1,
                                rel_tol=REL_TOL, abs_tol=ABS_TOL)

    return all([check_dictionaries, check_control_sum])


def node_norm_by_events(test_dataset, dataset_config):
    """
    norm_type='node',
    weight_col=None
    """
    # obtain expected result using rete lib
    edgelist = test_dataset.retention.get_edgelist(norm_type='node',
                                                   weight_col=None)
    result_rete = result_to_dict(edgelist, dataset_config)

    # obtain expected result using control dataset
    control_dataset = test_dataset.retention.get_shift()
    control_dataset[dataset_config.bigram] = control_dataset[dataset_config.event] + '~~~' + \
                                             control_dataset[dataset_config.next_event]

    node_transitions_counts = control_dataset.groupby(dataset_config.event)[dataset_config.bigram].count()
    grouped_control = control_dataset.groupby(dataset_config.bigram).agg({dataset_config.event: 'first',
                                                    dataset_config.timestamp: 'count'}).reset_index()

    grouped_control['node_norm'] = grouped_control[dataset_config.timestamp]\
                                   / node_transitions_counts.loc[grouped_control[dataset_config.event]].values

    result_test = dict(zip(grouped_control[dataset_config.bigram],grouped_control['node_norm']))

    # are the control and test dicts same?
    check_dictionaries = dict_compare(result_rete, result_test)

    # make sure sum of normalized weights for transitions
    # from each event is equal to 1 (total prob is 1)
    control_sum = edgelist.groupby(dataset_config.event)['edge_weight'].sum()
    check_control_sum = all(isclose(x, 1, rel_tol=REL_TOL, abs_tol=ABS_TOL)
                            for x in control_sum)

    return all([check_dictionaries, check_control_sum])


def no_norm_by_users(test_dataset, dataset_config):
    """
    norm_type=None,
    weight_col=INDEX_COL
    """
    edgelist = test_dataset.retention.get_edgelist(norm_type=None,
                                           weight_col=dataset_config.index_col)
    result_rete = result_to_dict(edgelist, dataset_config)

    # obtain expected result using control dataset
    control_dataset = test_dataset.retention.get_shift()
    control_dataset[dataset_config.bigram] = control_dataset[dataset_config.event] + '~~~' + \
                                             control_dataset[dataset_config.next_event]

    result_test = dict(control_dataset.groupby([dataset_config.bigram])[dataset_config.index_col].nunique())

    return dict_compare(result_rete, result_test)


def full_norm_by_users(test_dataset, dataset_config):
    """
    norm_type='full',
    weight_col=INDEX_COL
    """
    edgelist = test_dataset.retention.get_edgelist(norm_type='full',
                                                   weight_col=dataset_config.index_col)
    result_rete = result_to_dict(edgelist, dataset_config)

    # obtain expected result using control dataset
    control_dataset = test_dataset.retention.get_shift()
    control_dataset[dataset_config.bigram] = control_dataset[dataset_config.event] + '~~~' + \
                                             control_dataset[dataset_config.next_event]

    result_test = dict(control_dataset.groupby([dataset_config.bigram])[dataset_config.index_col].nunique()\
                       /control_dataset[dataset_config.index_col].nunique())

    return dict_compare(result_rete, result_test)


def node_norm_by_users(test_dataset, dataset_config):
    """
    norm_type='node',
    weight_col=INDEX_COL
    """
    edgelist = test_dataset.retention.get_edgelist(norm_type='node',
                                           weight_col=dataset_config.index_col)
    result_rete = result_to_dict(edgelist, dataset_config)

    # obtain expected result using control dataset
    control_dataset = test_dataset.retention.get_shift()
    control_dataset[dataset_config.bigram] = control_dataset[dataset_config.event] + '~~~' + \
                                             control_dataset[dataset_config.next_event]

    node_counts = control_dataset.groupby(dataset_config.event)[dataset_config.index_col].nunique().to_dict()
    g_test = control_dataset.groupby(dataset_config.bigram).agg({dataset_config.event: 'first',
                                             dataset_config.index_col: lambda x: x.nunique()}).reset_index()
    g_test['node_norm'] = g_test[dataset_config.index_col] / g_test[dataset_config.event].map(node_counts)
    result_test = dict(zip(g_test[dataset_config.bigram], g_test['node_norm']))

    return dict_compare(result_rete, result_test)