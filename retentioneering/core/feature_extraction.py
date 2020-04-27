# Copyright (C) 2019 Maxim Godzi, Anatoly Zaytsev, Dmitrii Kiselev
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.


import pandas as pd
import numpy as np
import umap.umap_ as umap
from collections import Counter
from sklearn.manifold import TSNE
from sklearn import decomposition
from sklearn import manifold
from sklearn.feature_extraction.text import TfidfVectorizer


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
    cv.columns.set_names(None, inplace=True)
    cv.index.set_names(None, inplace=True)
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
    Calculate ``index_col`` embedding (continuous vector form) by counting ``event_col`` appearance for each ``index_col``.

    Parameters
    --------
    data: pd.DataFrame
        Clickstream dataset.
    ngram_range: tuple, optional
        Range of ngrams to use in feature extraction. Default: ``(1, 1)``
    index_col: str, optional
        Name of custom index column, for more information refer to ``init_config``. For instance, if in config you have defined ``index_col`` as ``user_id``, but want to use function over sessions. By default the column defined in ``init_config`` will be used as ``index_col``.
    event_col: str, optional
        Name of custom event column, for more information refer to ``init_config``. For instance, you may want to aggregate some events or rename and use it as new event column. By default the column defined in ``init_config`` will be used as ``event_col``.
    last_k: int, optional
        Include only the last ``last_k`` events for each ``index_col``.
    wo_last: int, optional
        Exclude ``last_k`` events for each ``index_col``.

    Returns
    --------
    Vectorized dataframe with ``index_col`` as index and counts of ``event_col`` values as dataframe values.

    Return type
    -------
    pd.DataFrame
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
    cv.columns.set_names(None, inplace=True)
    cv.index.set_names(None, inplace=True)
    return cv


def frequency_embedder(data, ngram_range=(1, 1), **kwargs):
    """
    Similar to ``count_embedder()``, but normalizes events count over ``index_col``.

    Parameters
    -------
    data: pd.DataFrame
        Clickstream dataset.
    ngram_range: tuple, optional
        Range of ngrams to use in feature extraction. Default: ``(1, 1)``
    index_col: str, optional
        Name of custom index column, for more information refer to ``init_config``. For instance, if in config you have defined ``index_col`` as ``user_id``, but want to use function over sessions. By default the column defined in ``init_config`` will be used as ``index_col``.
    event_col: str, optional
        Name of custom event column, for more information refer to ``init_config``. For instance, you may want to aggregate some events or rename and use it as new event column. By default the column defined in ``init_config`` will be used as ``event_col``.

    Returns
    -------
    Dataframe with sessions vectorized by frequencies of events.

    Return type
    -------
    pd.DataFrame
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
    Similar to ``frequency_embedder()``, but normalizes event frequencies with inversed document frequency.

    Parameters
    --------
    data: pd.DataFrame
        Clickstream dataset.
    ngram_range: tuple, optional
        Range of ngrams to use in feature extraction. Default: ``(1, 1)``
    index_col: str, optional
        Name of custom index column, for more information refer to ``init_config``. For instance, if in config you have defined ``index_col`` as ``user_id``, but want to use function over sessions. By default the column defined in ``init_config`` will be used as ``index_col``.
    event_col: str, optional
        Name of custom event column, for more information refer to ``init_config``. For instance, you may want to aggregate some events or rename and use it as new event column. By default the column defined in ``init_config`` will be used as ``event_col``.

    Returns
    --------
    Dataframe with ``index_col`` vectorized by TF-IDF of events.

    Return type
    -------
    pd.DataFrame
    """
#     print('range',ngram_range)
    if 'index_col' not in kwargs:
        index_col = data.trajectory.retention_config['index_col']
    else:
        index_col = kwargs['index_col']
    if 'event_col' not in kwargs:
        event_col = data.trajectory.retention_config['event_col']
    else:
        event_col = kwargs['event_col']

    corpus = data.groupby(index_col)[event_col].apply(lambda x: '~~'.join([el.lower() for el in x]))
    if 'vocab' in kwargs and kwargs['vocab'] is not None:
        vectorizer = TfidfVectorizer(vocabulary=kwargs['vocab'],token_pattern = '[^~]+',ngram_range = ngram_range)

        tfidf = pd.DataFrame(index=data[index_col].unique(), columns=kwargs['vocab'].keys(),
                             data=vectorizer.fit_transform(corpus).todense())
    else:
        vectorizer = TfidfVectorizer(ngram_range=ngram_range,token_pattern = '[^~]+').fit(corpus)
        cols = [dict_key[0] for dict_key in sorted(vectorizer.vocabulary_.items(), key=lambda x: x[1])]
        tfidf = pd.DataFrame(index=data[index_col].unique(), columns=cols, data=vectorizer.transform(corpus).todense())

    setattr(tfidf.retention, 'datatype', 'features')
    return tfidf


def learn_tsne(data, **kwargs):
    """
    Calculates TSNE transformation for given matrix features.

    Parameters
    --------
    data: np.array
        Array of features.
    kwargs: optional
        Parameters for ``sklearn.manifold.TSNE()``

    Returns
    -------
    Calculated TSNE transform

    Return type
    -------
    np.ndarray
    """
    _tsne_filter = TSNE.get_params(TSNE)
    kwargs = {i: j for i, j in kwargs.items() if i in _tsne_filter}
    res = TSNE(random_state=0, **kwargs).fit_transform(data.values)
    return pd.DataFrame(res, index=data.index.values)

def learn_umap(data, **kwargs):
    """
    Calculates UMAP transformation for given matrix features.

    Parameters
    --------
    data: np.array
        Array of features.
    kwargs: optional
        Parameters for ``umap.UMAP()``

    Returns
    -------
    Calculated UMAP transform

    Return type
    -------
    np.ndarray
    """
    #_tsne_filter = TSNE.get_params(TSNE)
    #kwargs = {i: j for i, j in kwargs.items() if i in _tsne_filter}
    #res = TSNE(random_state=0, **kwargs).fit_transform(data.values)
    reducer = umap.UMAP()
    _umap_filter = reducer.get_params()
    kwargs = {i: j for i, j in kwargs.items() if i in _umap_filter}
    embedding = umap.UMAP(random_state=0, min_dist = 1, **kwargs).fit_transform(data.values)
    return pd.DataFrame(embedding, index=data.index.values)


def get_manifold(data, manifold_type, **kwargs):
    """
    Reduces number of dimensions.

    Parameters
    ---------
    data: pd.DataFrame
        Dataframe with features for clustering indexed as in ``retention_config.index_col``
    manifold_type: str
        Name dimensionality reduction method from ``sklearn.decomposition`` and ``sklearn.manifold``
    kwargs: optional
        Parameters for ``sklearn.decomposition`` and ``sklearn.manifold`` methods.

    Returns
    --------
    pd.DataFrame with reduced dimensions.

    Return type
    --------
    pd.DataFrame
    """
    if hasattr(decomposition, manifold_type):
        man = getattr(decomposition, manifold_type)
    elif hasattr(manifold, manifold_type):
        man = getattr(manifold, manifold_type)
    else:
        raise ValueError(f'There is not such manifold {manifold_type}')
    tsvd = man(**{i: j for i, j in kwargs.items() if i in man.get_params(man)})
    res = tsvd.fit_transform(data)
    return pd.DataFrame(res, index=data.index)


def merge_features(features, metadata, meta_index_col=None, manifold_type=None, fillna=None, drop=False, **kwargs):
    """
    Adds metadata to TFIDF of trajectories. Eeduced if ``manifold_type`` is not ``None``.

    Parameters
    --------
    features: pd.DataFrame
        Dataframe with users` metadata.
    metadata: pd.DataFrame
        Dataframe with user or session properties or any other information you would like to extract as features (e.g. user properties, LTV values, etc.). Default: ``None``
    meta_index_col: str, optional
        Used when metadata is not ``None``. Name of column in ``metadata`` dataframe that contains the same ID as in ``index_col``, or if not defined, same as in retention_config (e.g ID of users or sessions). If ``None``, then index of metadata dataframe is used instead. Default: ``None``
    manifold_type: str, optional
        Name dimensionality reduction method from ``sklearn.decomposition`` and ``sklearn.manifold``. Default: ``None``
    fillna: optional
        Value for filling missing metadata for any ``index_col`` value. Default: ``None``
    drop: bool, optional
        If ``True``, then drops users which do not exist in ``metadata`` dataframe. Default: ``False``
    kwargs: optional
        Keyword arguments for ``sklearn.decomposition`` and ``sklearn.manifold`` methods.

    Returns
    -------
    Dataframe with trajectory features (possibly reduced) and users metadata.

    Return type
    -------
    pd.DataFrame
    """
    if manifold_type is not None:
        features = get_manifold(features, manifold_type, **kwargs)
    if meta_index_col is not None:
        metadata.index = metadata[meta_index_col].values
        metadata = metadata.drop(meta_index_col, 1)
    res = features.join(metadata, rsuffix='_meta',)
    if drop and (fillna is None):
        res = res[res.isnull().sum(1) == 0].copy()
    if fillna is not None:
        res = res.fillna(fillna)
    return res


def drop_equal_features(features, users, thres=0.1, **kwargs):
    """
    Drop nonzero features with equal counts of negative and positive user's

    :param features: array of features
    :param users: list of positive users (e.g. data.retention.get_positive_users())
    :param thres: threshold of dropping (e.g 0.1 means area of drop equals [0.9,1.1]
    :return: dropped features: array of features
    """
    feat_group = features.groupby(features.index.isin(users)).agg(lambda x: (x > 0).sum())
    feat_neg = feat_group.iloc[0]
    feat_pos = feat_group.iloc[1]
    feat_neg += 10**-6
    feat_div = feat_pos / feat_neg - 1.

    features_drop = features.drop(features.columns[feat_div.abs() < thres], axis=1)
    return features_drop
