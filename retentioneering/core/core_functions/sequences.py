# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md


import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
import pandas as pd


def _is_cycle(data):
    """
        Utilite for cycle search
    """
    temp = data.split('~~')
    n_unique = len(set(temp))
    return (n_unique > 1) and (temp[0] == temp[-1])

def _is_loop(data):
    """
        Utilite for loop search
    """
    temp = data.split('~~')
    n_unique = len(set(temp))
    n = len(temp)
    return (n_unique == 1) and (n > 1)


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

def get_equal_fraction(self, target_list, exclude_list, sample, random_state=42):
    """
        Selects fraction of good users and the same number of bad users

        Parameters
        --------
        target_list: list of target_list
        random_state: int, optional
            random state for numpy choice function

        Returns
        --------
        Two dataframes: with good and bad users

        Return type
        --------
        tuple of pd.DataFrame
    """
    #Getting all the columns needed
    index_col = self.retention_config['user_col']
    event_col = self.retention_config['event_col']
    np.random.seed(random_state)
    #Get two DF with positive and negative users
    good_users = get_positive_users(self,target_list)
    bad_users = get_negative_users(self,target_list)

    if sample=="all":
        good_users_sample = good_users
        bad_users_sample = bad_users

    else:
    #Slicing them to obtain equal number of positive and negative users

        sample_size = min(len(good_users), len(bad_users))

        good_users_sample = set(np.random.choice(good_users, sample_size, replace=False))
        bad_users_sample = set(np.random.choice(bad_users, sample_size, replace=False))



    if len(exclude_list)>0:
        data_wo_exclude_list = exclude_events(self, exclude_list)
    else: data_wo_exclude_list = self._obj

    return (data_wo_exclude_list[data_wo_exclude_list[index_col].isin(good_users_sample)].copy(),
            data_wo_exclude_list[data_wo_exclude_list[index_col].isin(bad_users_sample)].copy(),
            len(good_users), len(bad_users))


def exclude_events(self, exclude_list):
    tmp = self._obj
    index_col = self.retention_config['user_col']
    event_col = self.retention_config['event_col']

    return tmp[~tmp[event_col].isin(exclude_list)]


def collapse_loops(corpus):
    def add_prev_event_to_result(result, prev_event, prev_event_count):
        result_add = ''
        if prev_event_count > 2:
            result_add = prev_event + '~~' + prev_event + '_multiloop'
        if prev_event_count == 2:
            result_add = prev_event + '~~' + prev_event
        if prev_event_count == 1:
            result_add = prev_event
        if len(result) > 0:
            return result + '~~' + result_add
        else:
            return result_add
    result = ''
    prev_event = None
    prev_event_count = 0
    for event in corpus.split('~~'):
        if event == prev_event:
            prev_event_count += 1
        else:
            result = add_prev_event_to_result(result, prev_event, prev_event_count)
            prev_event = event
            prev_event_count = 1
    return add_prev_event_to_result(result, prev_event, prev_event_count)


def find_sequences(self,
                   target_list,
                   exclude_list=[],
                   show_list=[],
                   ngram_range=(1, 1),
                   preprocessing=None,
                   sample=None,
                   threshold=0,
                   coef_min=0,
                   coef_max=float('inf'),
                   random_state=42):
    """
        Finds all subsequences of length lying in interval

        Parameters
        --------
        target_list: list of target events
        exclude_list: list of events to exlude from Dataframe
        show_list: list of str
            list of events/part of event which should be in a sequence
        ngram_range: interval of lengths for search.
            Should be more or equal to (1, 1)
        preprocessing: None or 'collapse_loops'
            if 'collapse_loops' all sequences with event which repeats 3 and
            more times become to event~~event_multiloop
        sample: None or 'all'
            if None - the users the bigger group of users (bad or good) is random sampled to make these groups equal
        threshold: int, optional
            Filter for output: Good+Lost should be greater than threshold
        coef_min: float or int, optional
            Filter for output: should be min value of Lost2Good value
            Should be less than coef_max
        coef_max: float or int, optional
            Filter for output: should be max value of Lost2Good value
            Should be greater than coef_min
        random_state: int, optional
            random state for numpy choice function

        Returns
        --------
        Dataframe with columns:
        'Sequence', 'Good', 'Lost', 'Lost2Good', 'GoodUnique', 'LostUnique', 'UniqueLost2Good'
        filtered by coef_min, coef_max, show_list and threshold

        Return type
        --------
        pd.DataFrame with all sequences
    """
    if not np.all([(isinstance(target_list, list)),(isinstance(exclude_list, list)), (isinstance(show_list, list))]):
        raise ValueError('target_list, exclude_list and show_list must be list!')
    if not np.all([isinstance(i, str) for i in show_list]):
        raise ValueError('elements of show_list must be string!')
    if min(ngram_range) <= 0 or ngram_range[0] > ngram_range[1] or len(ngram_range) != 2 or \
        not all(isinstance(item, int) for item in ngram_range) or not isinstance(ngram_range,tuple):
        raise ValueError('Wrong ngram range! It should be a tuple like (1,2) with int numbers greater than zero and first number is not greater than second!')
    if not np.any(np.isin(target_list,self._obj[self.retention_config['event_col']].unique())):
        raise ValueError('There is no events from target list in data or it is empty!')
    if not (isinstance(threshold, int)):
        raise ValueError('Threshold must be int!')
    if coef_max!=float('inf'):
        if not isinstance(coef_max, (int, float)):
            raise ValueError('coef_max and coef_min must be int or float!')
        if coef_max<coef_min:
            raise ValueError('coef_max must be greater than coef_min')

    if not isinstance(coef_min, (int, float)):
        raise ValueError('coef_min must be int or float!')

    #Creating dict for sequences
    sequences = dict()
    #Get equal num of good and bad users and all of their history
    good, bad, good_num, bad_num = get_equal_fraction(self, target_list, exclude_list, sample, random_state)
    print(good_num)
    print(bad_num)
    #good_num = good[self.retention_config['user_col']].nunique()
    #bad_num = bad[self.retention_config['user_col']].nunique()

    if good.shape[0] == 0 or bad.shape[0] == 0:
        raise ValueError('There are only good/bad users in your data!')


    #Replace ' ' for '_' in event_names
    good[self.retention_config['event_col']] = good[self.retention_config['event_col']].apply(lambda x: x.replace(' ','_'))
    bad[self.retention_config['event_col']] = bad[self.retention_config['event_col']].apply(lambda x: x.replace(' ','_'))
    #Creating CountVectorizer instance and fit it to the data to extract ngrams
    countvect = CountVectorizer(ngram_range=ngram_range, token_pattern='[^~]+',lowercase=False)
    good_corpus = good.groupby(self.retention_config['user_col'])[self.retention_config['event_col']].apply(
        lambda x: '~~'.join([l for l in x]))
    bad_corpus = bad.groupby(self.retention_config['user_col'])[self.retention_config['event_col']].apply(
        lambda x: '~~'.join([l for l in x]))

    if preprocessing=='collapse_loops':
        good_corpus = good_corpus.apply(lambda x: collapse_loops(x))
        bad_corpus = bad_corpus.apply(lambda x: collapse_loops(x))


    good_count = countvect.fit_transform(good_corpus.values)
    good_frame = pd.DataFrame(columns=['~~'.join(x.split(' ')) for x in countvect.get_feature_names_out()],
                              data=good_count.todense())
    bad_count = countvect.fit_transform(bad_corpus.values)
    bad_frame = pd.DataFrame(columns=['~~'.join(x.split(' ')) for x in countvect.get_feature_names_out()],
                             data=bad_count.todense())

    #Concat bad and good ngram counts
    res = pd.concat([good_frame.sum(),
                     bad_frame.sum().astype(int),
                     good_frame.astype(bool).sum(axis=0),
                     bad_frame.astype(bool).sum(axis=0)],
                     axis=1
                     ).fillna(0).astype(int).reset_index()

    res.columns = ['Sequence', 'Good', 'Lost', 'GoodUnique', 'LostUnique']

    res['Lost2Good'] = (res['Lost'] / res['Good']).round(2)

    res['UniqueLost2Good'] = (res['LostUnique'] / res['GoodUnique']).round(2)
    cols_reorder = ['Sequence', 'Good', 'Lost', 'Lost2Good', 'GoodUnique', 'LostUnique', 'UniqueLost2Good']
    res = res[cols_reorder]
    res['ratio'] = (res['GoodUnique']/good_num)/(res['LostUnique']/bad_num)
    res['SequenceType'] = '-'
    if max(ngram_range)>=2:
        res['SequenceType'] = res['Sequence'].apply(lambda x: _sequence_type(x))

    #Filter output by threshold and coefficient if needed
    #pd.set_option('max_colwidth', 120)
    #pd.set_option('display.width', 500)

    if coef_max != float('inf'):
        coef_max=coef_max

    str_from_list = '|'.join(str(i) for i in show_list)

    return res[(res['Lost2Good']<=coef_max)\
            &(res['Lost2Good']>=coef_min) \
            &(res['Sequence'].str.contains(str_from_list))\
            & (res.Good + res.Lost > threshold)] \
        .sort_values('Lost', ascending=False).reset_index(drop=True)




def find_cycles(self,
                    target_list,
                    exclude_list=[],
                    show_list=[],
                    ngram_range=(3,3),
                    sample=None,
                    threshold=0,
                    coef_min=0,
                    coef_max=float('inf'),
                    random_state=42):
    """

    Parameters
    ----------
    target_list: list of target events
    exclude_list: list of event to exlude from Dataframe
    show_list: list of str
        list of events/part of event which should be in a sequence
    ngram_range: interval of lengths for search.
        Should be more or equal to (3, 3)
    sample: None or 'all'
        if None - the users the bigger group of users (bad or good) is random sampled to make these groups equal
    random_state: int, optional
        random state for numpy choice function
    threshold: int, optional
        Filter for output: Good+Lost should be greater than threshold
    coef_min: float or int, optional
        Filter for output: should be min value of Lost2Good value
        Should be less than coef_max
    coef_max: float or int, optional
        Filter for output: should be max value of Lost2Good value
        Should be greater than coef_min

    Returns pd.DataFrame with cycles
    -------

    """
    if ngram_range[1] < ngram_range[0] or ngram_range[0] < 3:
        raise ValueError('The ngram_range is set incorrectly: either first number is greater than second or first number is smaller than 3!')

    #Get all ngrams
    temp = self.find_sequences(target_list=target_list,
                                exclude_list=exclude_list,
                                show_list=show_list,
                                ngram_range=ngram_range,
                                sample=sample,
                                random_state=random_state,
                                threshold=threshold,
                                coef_min=coef_min,
                                coef_max=coef_max
                                ).reset_index(drop=True)
    #Leave only cycles
    return temp[temp['Sequence'].apply(lambda x: _is_cycle(x))].reset_index(drop=True)

def find_loops(self,
                    target_list,
                    exclude_list=[],
                    show_list=[],
                    ngram_range=(2, 2),
                    sample=None,
                    threshold=0,
                    coef_min=0,
                    coef_max=float('inf'),
                    random_state=42):

    """
    Parameters
    ----------
    target_list: list of target events
    exclude_list: list of event to exlude from Dataframe
    show_list: list of str
        list of events/part of event which should be in a sequence
    ngram_range: interval of lengths for search.
        Should be more or equal to (2, 2)
    sample: None or 'all'
        if None - the users the bigger group of users (bad or good) is random sampled to make these groups equal
    random_state: int, optional
        random state for numpy choice function
    threshold: int, optional
        Filter for output: Good+Lost should be greater than threshold
    coef_min: float or int, optional
        Filter for output: should be min value of Lost2Good value
        Should be less than coef_max
    coef_max: float or int, optional
        Filter for output: should be max value of Lost2Good value
        Should be greater than coef_min

    Returns pd.DataFrame with cycles
    -------

    """
    if ngram_range[1] < ngram_range[0] or ngram_range[0] < 2:
        raise ValueError('The ngram_range is set incorrectly: either first number is bigger than second or first number is smaller than 2!')


    #Get all ngrams
    temp = self.find_sequences(target_list,
                                exclude_list=exclude_list,
                                show_list=show_list,
                                ngram_range=ngram_range,
                                sample=sample,
                                random_state=random_state,
                                threshold=threshold,
                                coef_min=coef_min,
                                coef_max=coef_max
                                ).reset_index(drop=True)

    #Leave only loops
    return temp[temp['Sequence'].apply(lambda x: _is_loop(x))].reset_index(drop=True)
