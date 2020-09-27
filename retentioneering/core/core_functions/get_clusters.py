# Copyright (C) 2019 Maxim Godzi, Anatoly Zaytsev, Dmitrii Kiselev
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.


import numpy as np

from retentioneering.visualization import plot
from sklearn.cluster import KMeans
from sklearn.mixture import GaussianMixture


def get_clusters(self, *,
                 plot_type=None,
                 refit_cluster=False,
                 method='kmeans',
                 feature_type='tfidf',
                 ngram_range=(1, 1),
                 n_clusters=8,
                 targets=None,
                 **kwargs):
    """
    Finds clusters of users in data.

    Parameters
    --------
    plot_type: str, optional
        Type of clustering visualization. Available methods are
        ``cluster_heatmap``,
         ``cluster_tsne``,
          ``cluster_pie``,
           ``cluster_bar``.
    Please, see examples to understand different visualization methods. Default: ``None``
    refit_cluster: bool, optional
        If ``False``, then cached results of clustering are used. Default: ``False``
    method: str, optional
        Method of clustering. Available methods:
            - ``simple_cluster``;
            - ``dbscan``;
            - ``GMM``.
        Default: ``simple_cluster``
    use_csi: bool, optional
        If ``True``, then cluster stability index will be calculated. IMPORTANT: it may take a lot of time. Default: ``True``
    epsq: float, optional
        Quantile of nearest neighbor positive distance between dots, its value will be an eps. If ``None``, then eps from keywords will be used. Default: ``None``
    max_cl_number: int, optional
        Maximal number of clusters for aggregation of small clusters. Default: ``None``
    max_n_clusters: int, optional
        Maximal number of clusters for automatic selection for number of clusters. If ``None``, then uses n_clusters from arguments. Default: `None```
    random_state: int, optional
        Random state for KMeans and GMM clusterers.
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
    index_col:
        Name of custom index column, for more information refer to ``init_config``. For instance, if in config you have defined ``index_col`` as ``user_id``, but want to use function over sessions. By default the column defined in ``init_config`` will be used as ``index_col``.
    event_col:
        Name of custom event column, for more information refer to ``init_config``. For instance, you may want to aggregate some events or rename and use it as new event column. By default the column defined in ``init_config`` will be used as ``event_col``.
    kwargs:
        Parameters for ``sklearn.decomposition()`` and ``sklearn.manifold()`` methods. Keyword arguments for clusterers. For more information, please, see ``sklearn.cluster.KMeans()``, ``sklearn.cluster.DBSCAN()``, ``sklearn.mixture.GaussianMixture()`` docs.

    Returns
    -------
    Array of clusters

    Return type
    -------
    np.array
    """

    # obtain vectorized features
    if hasattr(self, 'datatype') and self.datatype == 'features':
        features = self._obj.copy()
    else:
        features = self.extract_features(feature_type=feature_type,
                                         ngram_range=ngram_range,
                                         **kwargs)

    # obtain clusters
    if self.clusters is None or refit_cluster:
        clusterer = globals()[method]
        self.clusters = clusterer(features,
                                  n_clusters=n_clusters,
                                  **kwargs)
        _create_cluster_mapping(self, features.index.values)

    # init and obtain bool vector for targets:
    targets_bool = [np.array([False] * len(self.clusters))]
    target_names = [' ']

    if targets is not None:
        targets_bool = []
        target_names = []

        # format targets to list of lists:
        for n, i in enumerate(targets):
            if type(i) != list:
                targets[n] = [i]

        for t in targets:
            # get name
            target_names.append('CR: ' + ' '.join(t))
            # get bool vector
            targets_bool.append((self._obj
                                 .groupby('client_id')['event']
                                 .apply(lambda x: bool(set(t) & set(x)))
                                 .to_frame()
                                 .sort_index()['event']
                                 .values))

    if plot_type:
        func = getattr(plot, plot_type)
        func(
            features,
            clusters=self.clusters,
            target=targets_bool,
            target_names=target_names,
            refit=refit_cluster,
            **kwargs
        )

    return self.clusters


def filter_cluster(self, cluster_name, index_col=None):
    """
    Filters dataset against one or several clusters.

    Parameters
    --------
    cluster_name: int or list
        Cluster ID or list of cluster IDs for filtering.
    index_col: str, optional
        Name of custom index column, for more information refer
        to ``init_config``. For instance, if in config you have defined
         ``index_col`` as ``user_id``, but want to use function over
         sessions. If ``None``, the column defined in ``init_config``
         will be used as ``index_col``. Default: ``None``

    Returns
    --------
    Filtered dataset

    Return type
    --------
    pd.Dataframe
    """
    index_col = index_col or self.retention_config['index_col']
    ids = []
    if type(cluster_name) is list:
        for i in cluster_name:
            ids.extend(self.cluster_mapping[i])
    else:
        ids = self.cluster_mapping[cluster_name]
    return self._obj[self._obj[index_col].isin(ids)].copy().reset_index(drop=True)


def _create_cluster_mapping(self, ids):
    self.cluster_mapping = {}
    for cluster in set(self.clusters):
        self.cluster_mapping[cluster] = ids[self.clusters == cluster].tolist()


def kmeans(data, *,
           n_clusters=8,
           random_state=0,
           **kwargs):
    """
    Finds cluster of users in data.

    Parameters
    -------
    n_clusters
    data: pd.DataFrame
        Dataframe with features for clustering indexed as in ``retention_config.index_col``
    max_n_clusters: int, optional
        Maximal number of clusters for automatic selection for number of clusters. If ``None``, then uses n_clusters from arguments. Default: `None```
    use_csi: bool, optional
        If ``True``, then cluster stability index will be calculated. IMPORTANT: it may take a lot of time. Default: ``True``
    random_state: int, optional
        Random state for KMeans clusterer. Default: ``0``
    kwargs: optional
        Parameters for ``sklearn.cluster.KMeans``

    Returns
    -------
    Array of clusters

    Return type
    -------
    np.array
    """
    km = KMeans(random_state=random_state,
                n_clusters=n_clusters)

    cl = km.fit_predict(data.values)

    return cl


def gmm(data, *,
        n_clusters=8,
        random_state=0,
        **kwargs):
    """
    Finds cluster of users in data using Gaussian Mixture Models.

    Parameters
    --------
    data: pd.DataFrame
        Dataframe with features for clustering indexed as in ``retention_config.index_col``
    max_n_clusters: int, optional
        Maximal number of clusters for automatic selection for number of clusters. If ``None``, then uses ```n_clusters`` from arguments. Default: `None```
    use_csi: bool, optional
        If ``True``, then cluster stability index will be calculated. IMPORTANT: it may take a lot of time. Default: ``True``
    random_state: int, optional
        Random state for GaussianMixture clusterer.
    kwargs: optional
        Parameters for ``sklearn.mixture.GaussianMixture``

    Returns
    --------
    Array of clusters

    Return type
    --------
    np.array
    """

    km = GaussianMixture(random_state=random_state,
                         n_components=n_clusters)

    cl = km.fit_predict(data.values)

    return cl

