import pandas as pd
import numpy as np
import umap.umap_ as umap
from sklearn.manifold import TSNE
from sklearn import decomposition
from sklearn import manifold
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer


def extract_features(self, *,
                     feature_type='tfidf',
                     ngram_range=(1, 1)):
    """
    User trajectories vectorizer.

    Parameters
    --------
    feature_type: {'ftidf'}, optional
        Type of vectorizer. Available vectorization methods:
        - TFIDF (``feature_type='tfidf'``). For more information refer to
        ``retentioneering.core.feature_extraction.tfidf_embedder``.
        - Event frequencies (``feature_type='frequency'``). For more information
        refer to ``retentioneering.core.feature_extraction.frequency_embedder``.
        - Event counts (``feature_type='counts'``). For more information refer to ``counts_embedder``.
        Default: ``tfidf``
    ngram_range: tuple, optional
        Range of ngrams to use in feature extraction. Default: ``(1, 1)``

    Returns
    -------
    Encoded user trajectories

    Return type
    -------
    pd.DataFrame of (number of users, number of unique events | event n-grams)
    """
    if feature_type not in {'tfidf', 'count'}:
        raise ValueError("Unknown feature type: {}.\nPlease choose one from {}".format(
            feature_type,
            ' '.join(self._embedding_types)
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
    index_col = data.rete.retention_config['index_col']
    event_col = data.rete.retention_config['event_col']

    corpus = data.groupby(index_col)[event_col].apply(
        lambda x: '~~'.join([el.lower() for el in x])
    )

    if feature_type == 'tfidf':
        vectorizer = TfidfVectorizer(ngram_range=ngram_range, token_pattern = '[^~]+').fit(corpus)
    elif feature_type == 'count':
        vectorizer = CountVectorizer(ngram_range=ngram_range, token_pattern='[^~]+').fit(corpus)

    cols = [dict_key[0] for dict_key in sorted(vectorizer.vocabulary_.items(), key=lambda x: x[1])]
    vec_data = pd.DataFrame(index=sorted(data[index_col].unique()),
                            columns=cols,
                            data=vectorizer.transform(corpus).todense())

    setattr(vec_data.rete, 'datatype', 'features')
    return vec_data


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


def drop_equal_features(features, users, thres=0.1, **kwargs):
    """
    Drop nonzero features with equal counts of negative and positive user's

    :param features: array of features
    :param users: list of positive users (e.g. data.rete.get_positive_users())
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
