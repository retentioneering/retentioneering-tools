import pandas as pd
import numpy as np
import umap.umap_ as umap
from collections import Counter
from sklearn.manifold import TSNE
from sklearn import decomposition
from sklearn import manifold
from sklearn.feature_extraction.text import TfidfVectorizer


def extract_features(self, *,
                     feature_type='tfidf',
                     ngram_range=(1, 1),
                     **kwargs):
    """
    User trajectories vectorizer.

    Parameters
    --------
    feature_type: str, optional
        Type of vectorizer. Available vectorization methods:
        - TFIDF (``feature_type='tfidf'``). For more information refer to ``retentioneering.core.feature_extraction.tfidf_embedder``.
        - Event frequencies (``feature_type='frequency'``). For more information refer to ``retentioneering.core.feature_extraction.frequency_embedder``.
        - Event counts (``feature_type='counts'``). For more information refer to ``counts_embedder``.
        Default: ``tfidf``
    drop_targets: bool, optional
        If ``True``, then targets will be removed from feature generation. Default: ``True``
    metadata: pd.DataFrame, optional
        Dataframe with user or session properties or any other information you would like to extract as features (e.g. user properties, LTV values, etc.). Default: ``None``
    meta_index_col: str, optional
        Used when metadata is not ``None``. Name of column in ``metadata`` dataframe that contains the same ID as in ``index_col``, or if not defined, same as in retention_config (e.g ID of users or sessions). If ``None``, then index of metadata dataframe is used instead. Default: ``None``
    manifold_type: str, optional
        Name dimensionality reduction method from ``sklearn.decomposition`` and ``sklearn.manifold``. Default: ``None``
    fillna: optional
        Value for filling missing metadata for any ``index_col`` value. Default: ``None``
    drop: bool, optional
        If ``True``, then drops users which do not exist in ``metadata`` dataframe. Default: ``False``
    ngram_range: tuple, optional
        Range of ngrams to use in feature extraction. Default: ``(1, 1)``
    index_col: str, optional
        Name of custom index column, for more information refer to ``init_config``. For instance, if in config you have defined ``index_col`` as ``user_id``, but want to use function over sessions. By default the column defined in ``init_config`` will be used as ``index_col``.
    event_col: str, optional
        Name of custom event column, for more information refer to ``init_config``. For instance, you may want to aggregate some events or rename and use it as new event column. By default the column defined in ``init_config`` will be used as ``event_col``.
    vocab_pars: dict, optional
        dictionary of parameters for creating vocabulary of ngrams with prepare_vocab() function. This vocab will be used as a feature space for TF-EDF encoding

    kwargs: optional
        Keyword arguments for ``sklearn.decomposition`` and ``sklearn.manifold`` methods.

    Returns
    -------
    Encoded user trajectories

    Return type
    -------
    pd.DataFrame of (number of users, number of unique events | event n-grams)
    """
    if feature_type not in self._embedding_types:
        raise ValueError("Unknown feature type: {}.\nPlease choose one from {}".format(
            feature_type,
            ' '.join(self._embedding_types)
        ))
    # func = getattr(feature_extraction, feature_type + '_embedder')
    func = globals()[feature_type + '_embedder']

    tmp = self._obj.copy()

    res = func(tmp, ngram_range=ngram_range, **kwargs)

    return res


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
    index_col = data.rete.retention_config['index_col']
    event_col = data.rete.retention_config['event_col']

    corpus = data.groupby(index_col)[event_col].apply(
        lambda x: '~~'.join([el.lower() for el in x])
    )

    vectorizer = TfidfVectorizer(ngram_range=ngram_range, token_pattern = '[^~]+').fit(corpus)

    cols = [dict_key[0] for dict_key in sorted(vectorizer.vocabulary_.items(), key=lambda x: x[1])]
    tfidf = pd.DataFrame(index=sorted(data[index_col].unique()),
                         columns=cols,
                         data=vectorizer.transform(corpus).todense())

    setattr(tfidf.rete, 'datatype', 'features')
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
