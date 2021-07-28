# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md

import retentioneering
from retentioneering import datasets

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
    
class TestStepMatrix:
    params = {}.fromkeys(["test_cluster_segments",
                          "test_step_matrix_segments",
                          "test_custom_func_segments"], test_datasets)
    
    def test_cluster_segments(self, test_dataset):
        assert cluster_segment_test(test_dataset, n_clusters=2, export_segments="test_case")
        assert cluster_segment_test(datasets.keep_one_event(test_dataset), 
                                    n_clusters=2, export_segments="test_case")
        assert cluster_override_test(test_dataset)
    #     assert cluster_segment_test(datasets.keep_one_user(test_dataset), n_clusters=2, export_segments="test_case")  

    def test_step_matrix_segments(self, test_dataset):
        assert step_matrix_segment_test(test_dataset, centered={
                                    'left_gap':10,
                                    'occurrence':1}, show_plot=False, export_segments="test_case")
        assert step_matrix_segment_test(datasets.keep_one_event(test_dataset),centered={
                                    'left_gap':10,
                                    'occurrence':1}, show_plot=False, export_segments="test_case")    
        assert step_matrix_segment_test(datasets.keep_one_user(test_dataset),centered={
                                    'left_gap':10,
                                    'occurrence':1}, show_plot=False, export_segments="test_case")
        assert step_matrix_override_test(test_dataset)

    def test_custom_func_segments(self, test_dataset):
        assert custom_func_segment_test(test_dataset, export_segments="test_case")
        assert custom_func_segment_test(datasets.keep_one_event(test_dataset), export_segments="test_case")    
        assert custom_func_segment_test(datasets.keep_one_user(test_dataset), export_segments="test_case")
        assert custom_func_override_test(test_dataset)
        
        
def cluster_segment_test(test_dataset, **kwargs):
    test_dataset.rete.set_config({
        'user_col': 'user_id',
        'event_col':'event',
        'event_time_col':'timestamp'
    })
    segment_name = kwargs['export_segments']
    test_dataset.rete.get_clusters(**kwargs)
    res = test_dataset.rete.segments.show_segments().set_index('user_col')
    mapping = test_dataset.rete.cluster_mapping
    res = res[[t for t in res.columns if segment_name in t]]
    for col in res.columns:
        ind = int(col.split('.')[1])
        if set(mapping[ind]) != set(res[res[col]==1].index):
            return False
    return True
    
def cluster_override_test(test_dataset):
    test_dataset.rete.set_config({
        'user_col': 'user_id',
        'event_col':'event',
        'event_time_col':'timestamp'
    })
    test_dataset.rete.get_clusters(n_clusters=2, export_segments='test_case')
    res = test_dataset.rete.segments.show_segments()['test_case.0']
    test_dataset.rete.get_clusters(n_clusters=3, export_segments='test_case')
    res2 = test_dataset.rete.segments.show_segments()['test_case.0']
    if res.equals(res2):
        return True
    else:
        return False

def step_matrix_segment_test(test_dataset, **kwargs):
    test_dataset.rete.set_config({
        'user_col': 'user_id',
        'event_col':'event',
        'event_time_col':'timestamp'
    })
    kwargs['centered']['event'] = test_dataset.event.unique()[0]
    segment_name = kwargs['export_segments']
    test_dataset.rete.step_matrix(**kwargs)
    res = test_dataset.rete.segments.show_segments().set_index('user_col')
    users = test_dataset[test_dataset.event == kwargs['centered']['event']]['user_id']
    if set(res[res[segment_name + '.0']==1].index) == set(users):
        return True
    else:
        return False

def step_matrix_override_test(test_dataset):
    test_dataset.rete.set_config({
        'user_col': 'user_id',
        'event_col':'event',
        'event_time_col':'timestamp'
    })
    test_dataset.rete.step_matrix(centered={'event':'cart','left_gap':2,'occurrence':1}, 
                                  export_segments='test_case')
    res = test_dataset.rete.segments.show_segments()['test_case.0']
    test_dataset.rete.step_matrix(centered={'event':'catalog','left_gap':2,'occurrence':1}, 
                                  export_segments='test_case')
    res2 = test_dataset.rete.segments.show_segments()['test_case.0']
    if res.equals(res2):
        return True
    else:
        return False

def custom_func_segment_test(test_dataset, **kwargs):
    test_dataset.rete.set_config({
        'user_col': 'user_id',
        'event_col':'event',
        'event_time_col':'timestamp'
    })
    segm_name = kwargs['export_segments']
    mask = test_dataset.user_id.isin(test_dataset.user_id.unique()[:1])
    test_dataset.rete.segments.add_segment(segm_name,test_dataset.loc[mask]['user_id'].unique())
    res = test_dataset.rete.segments.show_segments().set_index('user_col')
    if set(res[res[segm_name+'.0']==1].index) == set(test_dataset.loc[mask].user_id):
        return True
    else:
        return False

def custom_func_override_test(test_dataset):
    test_dataset.rete.set_config({
        'user_col': 'user_id',
        'event_col':'event',
        'event_time_col':'timestamp'
    })
    mask = test_dataset.user_id.isin(test_dataset.user_id.unique()[:1])
    test_dataset.rete.segments.add_segment('test_case',test_dataset.loc[mask]['user_id'].unique())
    res = test_dataset.rete.segments.show_segments()['test_case.0']
    mask = test_dataset.user_id.isin(test_dataset.user_id.unique()[2:5])
    test_dataset.rete.segments.add_segment('test_case',test_dataset.loc[mask]['user_id'].unique())
    res2 = test_dataset.rete.segments.show_segments()['test_case.0']
    if res.equals(res2):
        return True
    else:
        return False