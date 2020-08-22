from retentioneering import init_config
from retentioneering import datasets
from math import isclose

# global constants
REL_TOL = 0.0001
ABS_TOL = 0.0001

BI_GRAM = 'bi_gram'
EVENT_COL = 'event'
INDEX_COL = 'client_id'
NEXT_EVENT_COL = 'next_event'
TIMESTAMP = 'timestamp'

data = datasets.load_simple_shop()

# setup init_config and apply retention.prepare()
init_config(
    experiments_folder='experiments',  # folder for saving experiment results: graph visualization, heatmaps and etc.
    index_col=INDEX_COL,  # column by which we split users / sessions / whatever
    event_col=EVENT_COL,  # column that describes event
    event_time_col=TIMESTAMP,  # column that describes timestamp of event
    positive_target_event='passed',  # name of positive target event
    negative_target_event='lost',  # name of negative target event
    pos_target_definition={  # how to define positive event, e.g. empty means that add passed for whom was not 'lost'
        'event_list': ['payment_done']
    },
    neg_target_definition={}
)
data = data.retention.prepare()

# make control array for manual normalizations
control = data.retention.get_shift()
control[BI_GRAM] = control[EVENT_COL] + '~~~' + control[NEXT_EVENT_COL]


# compare
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


def test_no_norm_by_events():
    """
    norm_type=None,
    weight_col=None
    """
    edgelist = data.retention.get_edgelist(norm_type=None,
                                           weight_col=None)
    edgelist[BI_GRAM] = edgelist[EVENT_COL] + '~~~' + edgelist[NEXT_EVENT_COL]
    result_rete = dict(zip(edgelist[BI_GRAM], edgelist['edge_weight']))

    # obtain expected result using control dataset
    result_test = dict(control[BI_GRAM].value_counts())

    assert dict_compare(result_rete, result_test)


def test_full_norm_by_events():
    """
    norm_type='full',
    weight_col=None
    """
    # obtain expected result using rete lib
    edgelist = data.retention.get_edgelist(norm_type='full',
                                           weight_col=None)
    edgelist[BI_GRAM] = edgelist[EVENT_COL] + '~~~' + edgelist[NEXT_EVENT_COL]
    result_rete = dict(zip(edgelist[BI_GRAM], edgelist['edge_weight']))

    # obtain expected result using control dataset
    result_test = dict(control[BI_GRAM].value_counts(normalize=True))

    assert dict_compare(result_rete, result_test)

    # make sure all weights sum to 1:
    assert isclose(edgelist['edge_weight'].sum(), 1,
                   rel_tol=REL_TOL, abs_tol=ABS_TOL)


def test_node_norm_by_events():
    """
    norm_type='node',
    weight_col=None
    """
    # obtain expected result using rete lib
    edgelist = data.retention.get_edgelist(norm_type='node',
                                           weight_col=None)
    edgelist[BI_GRAM] = edgelist[EVENT_COL] + '~~~' + edgelist[NEXT_EVENT_COL]
    result_rete = dict(zip(edgelist[BI_GRAM], edgelist['edge_weight']))

    # obtain expected result using control dataset
    node_transitions_counts = control.groupby(EVENT_COL)[BI_GRAM].count()
    grouped_control = control.groupby(BI_GRAM).agg({EVENT_COL: 'first',
                                                    TIMESTAMP: 'count'}).reset_index()

    grouped_control['node_norm'] = grouped_control[TIMESTAMP]\
                                   / node_transitions_counts.loc[grouped_control[EVENT_COL]].values

    result_test = dict(zip(grouped_control[BI_GRAM],grouped_control['node_norm']))

    assert dict_compare(result_rete, result_test)

    # make sure sum of normalized weights for transitions
    # from each event is equal to 1 (total prob is 1)
    control_sum = edgelist.groupby(EVENT_COL)['edge_weight'].sum()
    assert all(isclose(x, 1, rel_tol=REL_TOL, abs_tol=ABS_TOL)
               for x in control_sum)


def test_no_norm_by_users():
    """
    norm_type=None,
    weight_col=INDEX_COL
    """
    edgelist = data.retention.get_edgelist(norm_type=None,
                                           weight_col=INDEX_COL)
    edgelist[BI_GRAM] = edgelist[EVENT_COL] + '~~~' + edgelist[NEXT_EVENT_COL]
    result_rete = dict(zip(edgelist[BI_GRAM], edgelist['edge_weight']))

    # obtain expected result using control dataset
    result_test = dict(control.groupby([BI_GRAM])[INDEX_COL].nunique())

    assert dict_compare(result_rete, result_test)


def test_full_norm_by_users():
    """
    norm_type='full',
    weight_col=INDEX_COL
    """
    edgelist = data.retention.get_edgelist(norm_type='full',
                                           weight_col=INDEX_COL)
    edgelist[BI_GRAM] = edgelist[EVENT_COL] + '~~~' + edgelist[NEXT_EVENT_COL]
    result_rete = dict(zip(edgelist[BI_GRAM], edgelist['edge_weight']))

    # obtain expected result using control dataset
    result_test = dict(control.groupby([BI_GRAM])[INDEX_COL].nunique()\
                       /control[INDEX_COL].nunique())

    assert dict_compare(result_rete, result_test)


def test_node_norm_by_users():
    """
    norm_type='node',
    weight_col=INDEX_COL
    """
    edgelist = data.retention.get_edgelist(norm_type='node',
                                           weight_col=INDEX_COL)
    edgelist[BI_GRAM] = edgelist[EVENT_COL] + '~~~' + edgelist[NEXT_EVENT_COL]
    result_rete = dict(zip(edgelist[BI_GRAM], edgelist['edge_weight']))

    # obtain expected result using control dataset
    node_counts = control.groupby(EVENT_COL)[INDEX_COL].nunique().to_dict()
    g_test = control.groupby('bi_gram').agg({EVENT_COL: 'first',
                                             INDEX_COL: lambda x: x.nunique()}).reset_index()
    g_test['node_norm'] = g_test[INDEX_COL] / g_test[EVENT_COL].map(node_counts)
    result_test = dict(zip(g_test['bi_gram'], g_test['node_norm']))

    assert dict_compare(result_rete, result_test)
