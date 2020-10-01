import numpy as np

from retentioneering.visualization import plot_clusters
from sklearn.cluster import KMeans
from sklearn.mixture import GaussianMixture


def get_clusters(self, *,
                 feature_type='tfidf',
                 ngram_range=(1, 1),
                 n_clusters=8,
                 method='kmeans',
                 plot_type=None,
                 refit_cluster=True,
                 targets=None,
                 **kwargs):
    """
    Cluster users in the dataset according to their behavior.

    Parameters
    --------
    feature_type: str (optional, default 'tfidf')
        Type of vectorizer to user to convert sequences of events to numerical vectors.
        Currently supports: 'tfidf'.

    ngram_range: tuple (optional, default (1, 1))
        The lower and upper boundary of the range of n-values for different
        n-grams to be extracted. All values of n such that min_n <= n <= max_n will
        be used. For example an ngram_range of (1, 1) means only unigrams, (1, 2)
        means unigrams and bigrams, and (2, 2) means only bigrams.

    n_clusters: int (optional, default 8)
        Number of clusters to be identified.

    method: str (optional, default 'kmeans')
        Clustering method to use. Currently supports: 'kmeans' and 'gmm'.

    plot_type: str (optional, default None)
        Type of cluster statistics overview graph to plot after clustering.
        Currently supports: 'cluster_bar'

    targets: list (optional, default None)
        List of target events to be
        Only applies if plot_type = 'cluster_bar'

    refit_cluster: bool (optional, default True)
        If False, then cached results of previous clustering are used.
        (from .cluster_mapping attribute). If True recalculates clustering.

    Returns
    -------
    Array of clusters as .cluster_mapping attribute

    Return type
    -------
    np.array
    """
    index_col = self.retention_config['index_col']
    event_col = self.retention_config['event_col']

    # obtain vectorized features
    if hasattr(self, 'datatype') and self.datatype == 'features':
        features = self._obj.copy()
    else:
        features = self.extract_features(feature_type=feature_type,
                                         ngram_range=ngram_range,
                                         **kwargs)

    # obtain clusters
    if self.clusters is None or refit_cluster:
        clusterer = globals()['_'+method]
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
                                 .groupby(index_col)[event_col]
                                 .apply(lambda x: bool(set(t) & set(x)))
                                 .to_frame()
                                 .sort_index()[event_col]
                                 .values))

    if plot_type:
        func = getattr(plot_clusters, plot_type)
        func(
            features,
            clusters=self.clusters,
            target=targets_bool,
            target_names=target_names,
            refit=refit_cluster,
            **kwargs
        )

    return self.clusters


def filter_cluster(self, cluster_name):
    """
    Filters dataset against one or several clusters.

    Parameters
    --------
    cluster_name: int or list
        Cluster ID or list of cluster IDs for filtering.

    Returns
    --------
    Filtered dataset as pandas dataframe

    Return type
    --------
    pd.Dataframe
    """
    index_col = self.retention_config['index_col']
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


def _kmeans(data, *,
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


def _gmm(data, *,
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

