# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md

import pandas as pd
import numpy as np
import umap.umap_ as umap
from sklearn.manifold import TSNE
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer


_embedding_types = {'tfidf', 'count', 'binary', 'frequency'}


def extract_features(self, *,
                     feature_type='tfidf',
                     ngram_range=(1, 1)):
    """
    User trajectories vectorizer.

    Parameters
    ----------
    feature_type: str, (optional, default 'tfidf')
        Type of vectorizer. Available vectorization methods:
        {'tfidf', 'count', 'binary', 'frequency'}

    ngram_range: tuple, (optional, default (1,1))
        The lower and upper boundary of the range of n-values for different
        word n-grams or char n-grams to be extracted. For example
        ngram_range=(1, 1) means only single events, (1, 2) means single events
        and bigrams.

    Returns
    -------
    Encoded user trajectories

    Return type
    -----------
    pd.DataFrame of (number of users, number of unique events | event n-grams)
    """
    if feature_type not in _embedding_types:
        raise ValueError("Unknown feature type: {}.\nPlease choose one from {}".format(
            feature_type,
            ' '.join(_embedding_types)
        ))

    tmp = self._obj.copy()

    res = _embedder(tmp,
                    feature_type=feature_type,
                    ngram_range=ngram_range)

    return res


def _embedder(data, *,
              feature_type,
              ngram_range=(1, 1)):
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
    index_col = data.rete.retention_config['user_col']
    event_col = data.rete.retention_config['event_col']

    corpus = data.groupby(index_col)[event_col].apply(
        lambda x: '~~'.join([el.lower() for el in x])
    )

    if feature_type == 'tfidf':
        vectorizer = TfidfVectorizer(ngram_range=ngram_range, token_pattern = '[^~]+').fit(corpus)
    elif feature_type in {'count', 'frequency'}:
        vectorizer = CountVectorizer(ngram_range=ngram_range, token_pattern='[^~]+').fit(corpus)
    elif feature_type == 'binary':
        vectorizer = CountVectorizer(ngram_range=ngram_range, token_pattern='[^~]+', binary=True).fit(corpus)

    cols = [dict_key[0] for dict_key in sorted(vectorizer.vocabulary_.items(), key=lambda x: x[1])]
    vec_data = pd.DataFrame(index=sorted(data[index_col].unique()),
                            columns=cols,
                            data=vectorizer.transform(corpus).todense())

    # normalize if frequency:
    if feature_type == 'frequency':
        vec_data = vec_data.div(vec_data.sum(axis=1), axis=0).fillna(0)

    setattr(vec_data.rete, 'datatype', 'features')
    return vec_data


def _learn_tsne(data, **kwargs):
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

    TSNE_PARAMS = ['angle', 'early_exaggeration', 'init', 'learning_rate', 'method', 'metric',
            'min_grad_norm', 'n_components', 'n_iter', 'n_iter_without_progress', 'n_jobs',
            'perplexity', 'verbose']

    kwargs = {k: v for k, v in kwargs.items() if k in TSNE_PARAMS}
    res = TSNE(random_state=0, **kwargs).fit_transform(data.values)
    return pd.DataFrame(res, index=data.index.values)


def _learn_umap(data, **kwargs):
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
    reducer = umap.UMAP()
    _umap_filter = reducer.get_params()
    kwargs = {k: v for k, v in kwargs.items() if k in _umap_filter}
    embedding = umap.UMAP(random_state=0, **kwargs).fit_transform(data.values)
    return pd.DataFrame(embedding, index=data.index.values)

