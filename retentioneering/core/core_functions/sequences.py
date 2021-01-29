# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md


import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
import pandas as pd
#
def _is_cycle(data):
    """
        Utilite for cycle search
    """
    temp = data.split('~~')
    return True if temp[0] == temp[-1] and len(set(temp)) > 1 else False


def _is_loop(data):
    """
        Utilite for loop search
    """
    temp = data.split('~~')
    return True if len(set(temp)) == 1 else False

def get_positive_users(self,target_list):
    """
        Utilite for getting set of positive users id's.
        Positive user is the user who has any event from target_list in it's trajectory
    """
    tmp = self._obj
    index_col = self.retention_config['user_col']
    event_col = self.retention_config['event_col']
    return tmp[tmp[event_col].isin(target_list)][index_col].unique()

def get_negative_users(self,target_list):
    """
        Utilite for getting set of negative users id's.
        Positive user is the user who has any event from target_list in it's trajectory, negative - all other users
    """
    tmp = self._obj
    index_col = self.retention_config['user_col']
    event_col = self.retention_config['event_col']
    good_users = tmp[tmp[event_col].isin(target_list)][index_col].unique()
    return tmp[~tmp[index_col].isin(good_users)][index_col].unique()



def get_equal_fraction(self, target_list,fraction=1, random_state=42):
    """
        Selects fraction of good users and the same number of bad users

        Parameters
        --------
        target_list: list of target_events
        fraction: float, optional
            Fraction of users. Should be in interval of (0,1]
        random_state: int, optional
            random state for numpy choice function

        Returns
        --------
        Two dataframes: with good and bad users

        Return type
        --------
        tuple of pd.DataFrame
    """
    if fraction <= 0 or fraction > 1:
        raise ValueError('The fraction is <= 0 or > 1')
    #Getting all the columns needed
    index_col = self.retention_config['user_col']
    event_col = self.retention_config['event_col']
    np.random.seed(random_state)
    #Get two DF with positive and negative users
    good_users = get_positive_users(self,target_list)
    bad_users = get_negative_users(self,target_list)
    #Slicing them to obtain equal number of positive and negative users
    sample_size = min(int(len(good_users) * fraction), len(bad_users))
    good_users_sample = set(np.random.choice(good_users, sample_size, replace=False))
    bad_users_sample = set(np.random.choice(bad_users, sample_size, replace=False))

    return (self._obj[self._obj[index_col].isin(good_users_sample)].copy(),
            self._obj[self._obj[index_col].isin(bad_users_sample)].copy())


def _remove_duplicates(self, data):
    """
    Removing same events, that are going one after another
    ('ev1 -> ev1 -> ev2 -> ev1 -> ev3 -> ev3   --------> ev1 -> ev2 -> ev1 -> ev3').
    This utilite is used in a find_sequences function

    """
    t = data.split('~~')
    t = '~~'.join([t[0]] + ['~~'.join(word for ind, word in enumerate(t[1:]) if t[ind] != t[ind + 1])])
    return t[:-2] if t[-1] == '~' else t


def find_sequences(self, target_list, ngram_range=(1, 1), fraction=1, random_state=42, exclude_cycles=False, exclude_loops=False,
                   exclude_repetitions=False, threshold=0, coefficient=0):
    """
        Finds all subsequences of length lying in interval

        Parameters
        --------
        target_list: list of target_events
        fraction: float, optional
            Fraction of users. Should be in interval of (0,1]
        random_state: int, optional
            random state for numpy choice function
        exclude_cycles: boolean, optional
            flag for exluding cycles from output DF
        exclude_loops: boolean, optional
            flag for exluding loops from output DF
        exclude_repetitions: boolean, optional
            flag for exluding repetitions from output DF
        threshold: int, optional
            Filter for output: Good+Lost should be bigger than threshold
        coefficient: int, optional
            Filter for output: abs(res['Lost2Good'] - 1) should be bigger than coefficient
        Returns
        --------
        Two dataframes: with good and bad users

        Return type
        --------
        tuple of pd.DataFrame
    """
    #Creating dict for sequences
    sequences = dict()
    #Get equal num of good and bad users and all of their history
    good, bad = get_equal_fraction(self,target_list,fraction, random_state)
    #Replace ' ' for '_' in event_names
    good[self.retention_config['event_col']] = good[self.retention_config['event_col']].apply(lambda x: x.replace(' ','_'))
    bad[self.retention_config['event_col']] = bad[self.retention_config['event_col']].apply(lambda x: x.replace(' ','_'))
    #Creating CountVectorizer instance and fit it to the data to extract ngrams
    countvect = CountVectorizer(ngram_range=ngram_range, token_pattern='[^~]+',lowercase=False)
    good_corpus = good.groupby(self.retention_config['user_col'])[self.retention_config['event_col']].apply(
        lambda x: '~~'.join([l for l in x if l != 'pass' and l != 'lost']))
    good_count = countvect.fit_transform(good_corpus.values)
    good_frame = pd.DataFrame(columns=['~~'.join(x.split(' ')) for x in countvect.get_feature_names()],
                              data=good_count.todense())
    bad_corpus = bad.groupby(self.retention_config['user_col'])[self.retention_config['event_col']].apply(
        lambda x: '~~'.join([l for l in x if l != 'pass' and l != 'lost']))
    bad_count = countvect.fit_transform(bad_corpus.values)
    bad_frame = pd.DataFrame(columns=['~~'.join(x.split(' ')) for x in countvect.get_feature_names()],
                             data=bad_count.todense())
    #Concat bad and good ngram counts
    res = pd.concat([good_frame.sum(), bad_frame.sum()], axis=1).fillna(0).reset_index()
    res.columns = ['Sequence', 'Good', 'Lost']
    #Filter output based on boolean flags
    if exclude_cycles:
        res = res[~res.Sequence.apply(lambda x: _is_cycle(x))]
    if exclude_loops:
        temp = res[~res.Sequence.apply(lambda x: _is_loop(x))]
    if exclude_repetitions:
        res.Sequence = res.Sequence.apply(lambda x: _remove_duplicates(x))
        res = res.groupby(res.Sequence)[['Good', 'Lost']].sum().reset_index()
        res = res[res.Sequence.apply(lambda x: len(x.split('~~')) in range(ngram_range[0], ngram_range[1] + 1))]
    #Calculate Lost2Good metric
    res['Lost2Good'] = res['Lost'] / res['Good']
    #Filter output by threshold and coefficient if needed
    return res[(abs(res['Lost2Good'] - 1) > coefficient) & (res.Good + res.Lost > threshold)] \
        .sort_values('Lost', ascending=False).reset_index(drop=True)


def find_cycles(self, ngram_range, target_list, fraction=1, random_state=42, exclude_loops=False, \
    exclude_repetitions=False,threshold=0, coefficient=0):
    """

    Parameters
    ----------
    ngram_range - interval of lengths for search. Any () TO DO
    target_list: list of target_events
    fraction: float, optional
        Fraction of users. Should be in interval of (0,1]
    random_state: int, optional
        random state for numpy choice function
    exclude_cycles: boolean, optional
        flag for exluding cycles from output DF
    exclude_loops: boolean, optional
        flag for exluding loops from output DF
    exclude_repetitions: boolean, optional
        flag for exluding repetitions from output DF
    threshold: int, optional
        Filter for output: Good+Lost should be bigger than threshold
    coefficient: int, optional
        Filter for output: abs(res['Lost2Good'] - 1) should be bigger than coefficient

    Returns pd.DataFrame with cycles
    -------

    """
    if ngram_range[1] < ngram_range[0] or ngram_range[0] < 3:
        raise ValueError('The ngram_range is set incorrectly: either first number is bigger than second or first number is smaller than 3!')
    #Get all ngrams
    temp = self.find_sequences(target_list, ngram_range, fraction, random_state, exclude_loops=exclude_loops,
                               exclude_repetitions=exclude_repetitions,threshold=threshold, coefficient=coefficient).reset_index(drop=True)
    #Leave only cycles
    return temp[temp['Sequence'].apply(lambda x: _is_cycle(x))].reset_index(drop=True)


def find_loops(self, target_list, fraction=1, random_state=42):
    """
    Function for loop searching
    Parameters
    ----------
    fraction - fraction of good users. Any float in (0,1]
    random_state - random_state for numpy random seed

    Returns pd.DataFrame with loops. Good, Lost columns are for all occurences,
    (Good/Lost)_no_duplicates are for counting each cycle only once for user in which they occur
    -------

    """
    def loop_search(data, self_loops, event_list, is_bad):
        event_list = {k: 0 for k in event_list}
        for ind, url in enumerate(data[1:]):
            if data[ind] == data[ind + 1]:
                if url in self_loops.keys():
                    self_loops[url][is_bad] += 1
                    if event_list[url] == 0:
                        self_loops[url][is_bad + 3] += 1
                        event_list[url] = 1
                else:
                    self_loops[url] = [0, 0, 0, 0, 0, 0]
                    self_loops[url][is_bad] = 1
                    if event_list[url] == 0:
                        self_loops[url][is_bad + 3] += 1
                        event_list[url] = 1
    #Preparing variables
    self_loops = dict()
    event_list = self._obj[self.retention_config['event_col']].unique()
    #Get equal num of good and bad users
    good, bad = get_equal_fraction(self,target_list,fraction, random_state)
    #Perform loop search for every good user separately
    for el in good.groupby(self.retention_config['user_col']):
        loop_search(el[1][self.retention_config['event_col']].values, self_loops, event_list, 0)
    #Perform loop search for every bad user separately
    for el in bad.groupby(self.retention_config['user_col']):
        loop_search(el[1][self.retention_config['event_col']].values, self_loops, event_list, 1)

    for key, val in self_loops.items():
        if val[0] != 0:
            self_loops[key][2] = val[1] / val[0]
        if val[3] != 0:
            self_loops[key][5] = val[4] / val[3]

    return pd.DataFrame(data=[[a[0]] + a[1] for a in self_loops.items()],
                        columns=['Sequence', 'Good', 'Lost', 'Lost2Good', 'GoodUnique',
                                 'LostUnique', 'UniqueLost2Good']) \
        .sort_values('Lost', ascending=False).reset_index(drop=True)
