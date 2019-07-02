# Copyright (C) 2019 Maxim Godzi, Anatoly Zaytsev, Dmitrii Kiselev
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.


import pandas as pd
import numpy as np
from collections import Counter
from sklearn.manifold import TSNE


def _uni_counts_embedder(data, **kwargs):
    if 'index_col' not in kwargs:
        index_col = data.trajectory.retention_config['index_col']
    else:
        index_col = kwargs['index_col']
    if 'event_col' not in kwargs:
        event_col = data.trajectory.retention_config['event_col']
    else:
        event_col = kwargs['event_col']
    last_k = kwargs.get('last_k')
    if last_k is not None:
        data = data.groupby(index_col).tail(last_k)
    cv = data.groupby([index_col, event_col]).size().rename('event_count').reset_index()
    cv = cv.pivot(index=index_col, columns=event_col).fillna(0)
    cv.columns = cv.columns.levels[1]
    cv.columns.name = None
    cv.index.name = None
    setattr(cv.retention, 'datatype', 'features')
    return cv


def _ngram_agg(x, ngram_range):
    res = []
    shifts = []
    for i in range(ngram_range[0] - 1, ngram_range[1]):
        shifts.append(x.shift(i))
        res.extend(zip(*shifts))
    return Counter(res)


def counts_embedder(data, ngram_range=(1, 1), **kwargs):
    """
    Calculate session embedding (continuous vector form) by counting of events appearance for user

    :param data: clickstream dataset
    :param ngram_range: range of ngrams to use in feature extraction
    :param kwargs: index_col, event_col params
    :return: pd.DataFrame with sessions vectorized by counts of events
    """
    if max(ngram_range) == 1:
        return _uni_counts_embedder(data, **kwargs)
    if 'index_col' not in kwargs:
        index_col = data.trajectory.retention_config['index_col']
    else:
        index_col = kwargs['index_col']
    if 'event_col' not in kwargs:
        event_col = data.trajectory.retention_config['event_col']
    else:
        event_col = kwargs['event_col']
    last_k = kwargs.get('last_k')
    if last_k is not None:
        data = data.groupby(index_col).tail(last_k)
    wo_last = kwargs.get('wo_last_k')
    if wo_last is not None:
        bad_ids = data.groupby(index_col).tail(wo_last).index.values
        data = data[~data.index.isin(bad_ids)]
    cv = data.groupby(index_col)[event_col].apply(_ngram_agg, ngram_range=ngram_range).reset_index()
    cv = cv.pivot(index=index_col, columns='level_1', values=event_col).fillna(0)
    cv = cv.loc[:, [i for i in cv.columns if i[-1] == i[-1]]]
    cv.columns.name = None
    cv.index.name = None
    return cv


def frequency_embedder(data, ngram_range=(1, 1), **kwargs):
    """
    Similar to `count_embedder`, but normalize events count over index_col story

    :param data: clickstream dataset
    :param ngram_range: range of ngrams to use in feature extraction
    :param kwargs: index_col, event_col params
    :return: pd.DataFrame with sessions vectorized by frequencies of events
    """
    cv = counts_embedder(data, ngram_range, **kwargs)
    freq = pd.DataFrame(
        cv.values / cv.values.sum(1).reshape(-1, 1),
        index=cv.index.values,
        columns=cv.columns.values,
    )
    setattr(freq.retention, 'datatype', 'features')
    return freq


def tfidf_embedder(data, ngram_range=(1, 1), **kwargs):
    """
    Similar to `frequency_embedder`, but normalize events frequencies with inversed document frequency

    :param data: clickstream dataset
    :param ngram_range: range of ngrams to use in feature extraction
    :param kwargs: index_col, event_col params
    :return: pd.DataFrame with sessions vectorized by Tf-Idf of events
    """
    tf = frequency_embedder(data, ngram_range, **kwargs)
    idf = np.log((tf.shape[0]) / ((tf > 0).sum(0) + 1e-20)).values
    tfidf = tf * idf
    setattr(tfidf.retention, 'datatype', 'features')
    return tfidf


def learn_tsne(data, **kwargs):
    """
    Calculates TSNE transform for given matrix features

    :param data: array of features
    :param kwargs: arguments for sklearn.manifold.TSNE
    :return: np.ndarray with calculated TSNE transform
    """
    _tsne_filter = TSNE.get_params(TSNE)
    kwargs = {i: j for i, j in kwargs.items() if i in _tsne_filter}
    res = TSNE(random_state=0, **kwargs).fit_transform(data.values)
    return pd.DataFrame(res, index=data.index.values)
