# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md


from math import isclose
import pandas as pd
import numpy as np

import retentioneering
from retentioneering import datasets

#import pytest

# import data
retentioneering.config.update({
    'event_col': 'event',
    'event_time_col': 'timestamp',
    'user_col': 'user_id'
})
data = datasets.load_simple_shop()

####NEW###############NEW###############NEW###########
 #4 bad and 4 good users
users = [122915, 1475907, 2724645, 3744822, 463458, 2316787, 2653683, 3522518]
 #2 good and 1 bad users
users_good2_bad1 = [122915, 1475907, 2316787]
exclude_events=['end']

#leave in dataset only 4 bad and 4 good users
data_8_users = data[data['user_id'].isin(users)]

#create new columns with next_event to check bigramms, 3gramms, repetitions and cycles
data_8_users = data_8_users.sort_values(['user_id', 'timestamp'])
data_8_users['next_event'] = \
                data_8_users.groupby('user_id').event.shift(-1).fillna('end')
data_8_users['next_event_2'] = \
                data_8_users.groupby('user_id').next_event.shift(-1).fillna('end')

#data_good2_bad1 = data_8_users[data_8_users['user_id'].isin(users_good2_bad1)]
test_datasets = [dict(test_dataset=data_8_users)]
#test_dataset=data_8_users,test_dataset_good_more=data_good2_bad1)

####NEW_END###############NEW_END###############NEW_END###########

# util function to cycle through parameters for each test
def pytest_generate_tests(metafunc):
    # called once per each test function
    funcarglist = metafunc.cls.params[metafunc.function.__name__]
    argnames = sorted(funcarglist[0])
    metafunc.parametrize(
        argnames, [[funcargs[name] for name in argnames] for funcargs in funcarglist]
    )



class TestSequencesFuncs:
    # for each test (18) we would like to run all cases
    params = {}.fromkeys(["test_wrong_ngram_ranges",
                          "test_wrong_target_list",
                          "test_wrong_fraction",
                          "test_wrong_coeffs",
                          "test_correct_behaviour",
                          "test_bigramm_seq",
                          "test_loops_seq",
                          "test_cycles_seq",
                          "test_if_good_users_more_than_bad",
                          "test_exclude_list",
                          "test_exclude_list_loops",
                          "test_exclude_list_cycles",
                          "test_good_count",
                          "test_fraction",
                          "test_sequence_type",
                          "test_collapse_loops_check1",
                          "test_coef_max_min",
                          "test_threshold",
                          "test_wrong_ngram_ranges_cycles",
                          "test_wrong_ngram_ranges_loops",
                          "test_collapse_loops_check2"

                          ],test_datasets)


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

    def test_bigramm_seq(self, test_dataset):
        assert bigramm_seq(test_dataset)

    def test_loops_seq(self, test_dataset):
        assert loops_seq(test_dataset)

    def test_cycles_seq(self, test_dataset):
        assert cycles_seq(test_dataset)

    def test_if_good_users_more_than_bad(self, test_dataset):
        assert if_good_users_more_than_bad(test_dataset)

    def test_exclude_list(self, test_dataset):
        assert exclude_list(test_dataset)

    def test_exclude_list_loops(self, test_dataset):
        assert exclude_list_loops(test_dataset)

    def test_exclude_list_cycles(self, test_dataset):
        assert exclude_list_cycles(test_dataset)

    def test_good_count(self, test_dataset):
        assert good_count(test_dataset)

    def test_fraction(self, test_dataset):
        assert fraction(test_dataset)

    def test_sequence_type(self, test_dataset):
        assert sequence_type(test_dataset)

    def test_collapse_loops_check1(self, test_dataset):
        assert collapse_loops_check1(test_dataset)

    def test_coef_max_min(self, test_dataset):
        assert coef_max_min(test_dataset)

    def test_threshold(self, test_dataset):
        assert threshold(test_dataset)

    def test_wrong_ngram_ranges_cycles(self, test_dataset):
        assert wrong_ngram_ranges_cycles(test_dataset)

    def test_wrong_ngram_ranges_loops(self, test_dataset):
        assert wrong_ngram_ranges_loops(test_dataset)

    def test_collapse_loops_check2(self, test_dataset):
        assert collapse_loops_check2(test_dataset)

# *****************************
# *** FUNCTIONS DEFINITIONS ***
# *****************************
def wrong_ngram_ranges(test_dataset):
    for wrong_ngrams in [(0,0), (0,1,1), ('s',1), (False,True), [1,3], (3,1)]:
        try:
            test_dataset.rete.find_sequences(target_list=['cart'],ngram_range=wrong_ngrams)
        except ValueError:
            pass
        else:
            return False
        return True

def wrong_target_list(test_dataset):
    for wrong_target_list in [(0,0), (0,1,1), ('s',1), (False,True), [1,3], test_dataset.event.unique()]:
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
    for wrong_coeffs in [[1,100,0], [1.,0,100], ['o',0.1,2], [1,'o',2], [1,1,'l']]:
        try:
            test_dataset.rete.find_sequences(target_list=['cart'],
                                             ngram_range=(1,1),
                                             threshold = wrong_coeffs[0],
                                             coef_min = wrong_coeffs[1],
                                             coef_max = wrong_coeffs[2]
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
        pass
    else:
#             return res,test_dataset
        return res.shape[0] == test_dataset[test_dataset.event != 'end'].event.nunique()


#########NEW####################NEW####################NEW###########

def bigramm_seq(test_dataset):

    #create check df
    check_df_seq = test_dataset[(test_dataset['event']!='end')&(test_dataset['next_event']!='end')]\
            .groupby(['event','next_event'])\
            .agg({'user_id':'nunique', 'timestamp':'count'})\
            .reset_index()\
            .sort_values('timestamp', ascending=False)

    try:
        res = test_dataset.rete.find_sequences(target_list=['cart'],ngram_range=(2,2))
    except ValueError:
        pass
    else:
        return ((len(check_df_seq)==len(res)) &\
             (check_df_seq['timestamp'].sum()==(res['Good'].sum()+res['Lost'].sum()))&\
             (check_df_seq['user_id'].sum()==(res['GoodUnique'].sum()+res['LostUnique'].sum())))

def loops_seq(test_dataset):
    try:
        res = test_dataset.rete.find_sequences(target_list=['cart'],ngram_range=(2,2))
        res_only_loops = res[res['SequenceType']=='loop']
        loops = test_dataset.rete.find_loops(target_list=['cart'])

    except ValueError:
        pass
    else:

        return (((len(res_only_loops)==len(loops)) &\
                ((res_only_loops['Good'].sum()+res_only_loops['Lost'].sum())==
                (loops['Good'].sum()+loops['Lost'].sum()))))



def cycles_seq(test_dataset):
    try:
        res = test_dataset.rete.find_sequences(target_list=['cart'],ngram_range=(3,3))
        res_only_cycles = res[res['SequenceType']=='cycle']
        cycles = test_dataset.rete.find_cycles(target_list=['cart'])

    except ValueError:
        pass
    else:
    #return res,test_dataset
        return (((len(res_only_cycles)==len(cycles)) & ((res_only_cycles['Good'].sum()+res_only_cycles['Lost'].sum())==
(cycles['Good'].sum()+cycles['Lost'].sum()))))



def if_good_users_more_than_bad(test_dataset):
    exclude_events=['end']
    users_good2_bad1 = [122915, 1475907, 2316787]
    test_dataset_good_more = test_dataset[test_dataset['user_id'].isin(users_good2_bad1)]
    #create check df
    users_df =  [1475907, 2316787]
    check_df_seq = test_dataset_good_more[(~test_dataset_good_more['event'].isin(exclude_events))\
                                          &(~test_dataset_good_more['next_event'].isin(exclude_events))\
                                          &(test_dataset_good_more['user_id'].isin(users_df))]\
                                        .groupby(['event','next_event'])\
                                        .agg({'user_id':'nunique', 'timestamp':'count'})\
                                        .reset_index()\
                                        .sort_values('timestamp', ascending=False)

    try:
        res = test_dataset_good_more.rete.find_sequences(target_list=['cart'], ngram_range=(2,2))
    except ValueError: print('ValueError')
    else:
        return ((len(check_df_seq)==len(res))&
                     (check_df_seq['timestamp'].sum()==(res['Good'].sum()+res['Lost'].sum()))
                    )

def exclude_list(test_dataset):

    #create df without 'main' event
    test_dataset_exclude_main = test_dataset[test_dataset['event']!='main']
    test_dataset_exclude_main['next_event'] = \
                test_dataset_exclude_main.groupby('user_id').event.shift(-1).fillna('end')
    test_dataset_exclude_main['next_event_2'] = \
                test_dataset_exclude_main.groupby('user_id').next_event.shift(-1).fillna('end')
    #create check df
    check_df_seq = test_dataset_exclude_main[(test_dataset_exclude_main['event']!='end')&(test_dataset_exclude_main['next_event']!='end')]\
            .groupby(['event','next_event'])\
            .agg({'user_id':'nunique', 'timestamp':'count'})\
            .reset_index()\
            .sort_values('timestamp', ascending=False)

    try:
        res = test_dataset.rete.find_sequences(target_list=['cart'], exclude_list=['main'], ngram_range=(2,2))

    except ValueError:
        pass
    else:
        return ((len(check_df_seq)==len(res)) &\
                (check_df_seq['timestamp'].sum()==(res['Good'].sum()+res['Lost'].sum()))&\
                (check_df_seq['user_id'].sum()==(res['GoodUnique'].sum()+res['LostUnique'].sum())))

def exclude_list_loops(test_dataset):


    try:
        res = test_dataset.rete.find_sequences(target_list=['cart'], exclude_list=['main'], ngram_range=(2,5))
        res = res[res['SequenceType']=='loop']
        res_loops = test_dataset.rete.find_loops(target_list=['cart'], exclude_list=['main'], ngram_range=(2,5))

    except ValueError:
        pass
    else:
        return ((len(res)==len(res_loops)))

def exclude_list_cycles(test_dataset):

    try:
        res = test_dataset.rete.find_sequences(target_list=['cart'], exclude_list=['main'], ngram_range=(3,3))
        res = res[res['SequenceType']=='cycle']
        res_cycles = test_dataset.rete.find_cycles(target_list=['cart'], exclude_list=['main'], ngram_range=(3,3))
    except ValueError:
        pass
    else:
        return ((len(res)==len(res_cycles)))



def good_count(test_dataset):

    users_good = [122915, 1475907, 2724645, 3744822]
    #create check df
    check_df_seq = test_dataset[(~test_dataset['event'].isin(exclude_events))\
                                          &(~test_dataset['next_event'].isin(exclude_events))\
                                          &(test_dataset['user_id'].isin(users_good))]\
                                        .groupby(['event','next_event'])\
                                        .agg({'user_id':'nunique', 'timestamp':'count'})\
                                        .reset_index()\
                                        .sort_values('timestamp', ascending=False)

    try:
        res = test_dataset.rete.find_sequences(target_list=['cart'], ngram_range=(2,2))
    except ValueError:
        pass
    else:
        return ((check_df_seq['timestamp'].sum()==(res['Good'].sum()))&\
                (check_df_seq['user_id'].sum()==(res['GoodUnique'].sum())))


def fraction(test_dataset):
    #create_df
    good = ['catalog', 'tool','main','catalog', 'tool','main','catalog', 'tool','main', 'cart']
    bad = ['catalog', 'tool','main','catalog', 'tool','catalog','catalog', 'catalog','main', 'lost']

    users = []
    for j in range(20):
        l = ([j for i in range(10)])
        users+=l
    events = good*10 +bad*10
    dict_with_lists = {
    'user_id': users,
    'event': events
    }
    df = pd.DataFrame(dict_with_lists)

    try:
        res1 = df.rete.find_sequences(target_list=['cart'],ngram_range=(1,1), fraction=1, random_state=42)
        res = df.rete.find_sequences(target_list=['cart'],ngram_range=(1,1), fraction=0.8, random_state=42)
    except ValueError:
        pass
    else:
        return ((res1['GoodUnique'].max()*0.8) == (res['LostUnique'].max()))

def sequence_type(test_dataset):

    def _sequence_type(data):

        temp = data.replace('_multiloop','')
        temp = temp.split('~~')
        n = len(temp)
        n_unique = len(set(temp))
        if (n_unique > 1) and (temp[0] == temp[-1]):
            return 'cycle'
        if (n_unique == 1) and (n > 1):
            return 'loop'
        return '-'

    try:
        res = test_dataset.rete.find_sequences(target_list=['cart'], ngram_range=(3,3), fraction=1, random_state=42)
    except ValueError:
        pass
    else:
        return ((len(res)) == (res.apply(lambda x: _sequence_type(x.Sequence)==x.SequenceType, axis=1).sum()))

def collapse_loops_check1(test_dataset):

    try:
        res = test_dataset.rete.find_sequences(target_list=['cart'], ngram_range=(3,3), preprocessing='collapse_loops', fraction=1, random_state=42)
    except ValueError:
        pass
    else:
        return (len(res[res['SequenceType']=='loop'])==0)

def coef_max_min(test_dataset):
    coef_min = 0.1
    coef_max = 0.7
    try:
        res = test_dataset.rete.find_sequences(target_list=['cart'], ngram_range=(3,3), coef_min=coef_min, coef_max=coef_max, fraction=1, random_state=42)
    except ValueError:
        pass
    else:
        return (res['Lost2Good'].min()>=coef_min) & (res['Lost2Good'].max()<=coef_max)


def threshold(test_dataset):

    threshold=3
    try:
        res = test_dataset.rete.find_sequences(target_list=['cart'], ngram_range=(3,3), threshold=threshold, random_state=42)
    except ValueError:
        pass
    else:
        return (len(res) == ((res['Lost'] + res['Good']) >= threshold).sum())


def wrong_ngram_ranges_cycles(test_dataset):
    for wrong_ngrams in [(0,0), (0,1,1), ('s',1), (False,True), [1,3], (2,3), (3,2)]:
        try:
            test_dataset.rete.find_cycles(target_list=['cart'],ngram_range=wrong_ngrams)
        except ValueError:
            pass
        else:
            return False
        return True

def wrong_ngram_ranges_loops(test_dataset):
    for wrong_ngrams in [(0,0), (0,1,1), ('s',1), (False,True), [1,3], (3,1), (1,2), (3,2)]:
        try:
            test_dataset.rete.find_loops(target_list=['cart'], ngram_range=wrong_ngrams)
        except ValueError:
            pass
        else:
            return False
        return True

def collapse_loops_check2(test_dataset):
    #create check df
    events_cat = [['lost','main'],
      ['main','main'],
      ['main','main','main'],
      ['main','main','main','lost'],
      ['main','main','main','main','cart'],
      ['main','catalog','main','lost'],
      ['main','main','catalog','main','main','cart'],
      ['main','main','main','main','catalog', 'catalog','catalog'],
      ['main','main', 'catalog', 'main','main','main','catalog','catalog','catalog','catalog','cart'],
      ['main','main','catalog','main','main','cart']]

    users = []
    events = []
    for num, j in enumerate(events_cat):
        for event in j:
            users.append(num)
            events.append(event)

    dict_with_lists = {
     'user_id': users,
     'event': events
    }
    df = pd.DataFrame(dict_with_lists)
    check = ['main~~main_multiloop~~catalog~~catalog_multiloop',
             'catalog~~main~~main~~cart',
             'catalog~~main~~main_multiloop~~catalog',
             'main~~catalog~~main~~main',
              'main~~catalog~~main~~main_multiloop',
              'main~~main~~catalog~~main',
               'main_multiloop~~catalog~~catalog_multiloop~~cart']
    try:
        res = df.rete.find_sequences(target_list=['cart'],preprocessing='collapse_loops', ngram_range=(4,4), fraction=1, random_state=42)
        res['check'] = check==res['Sequence']

    except ValueError:
        pass
    else:
        return ((len(res)) == (res['check']).sum())
