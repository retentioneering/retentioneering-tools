# Copyright (C) 2019 Maxim Godzi, Anatoly Zaytsev, Dmitrii Kiselev
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.


from sklearn.cluster import KMeans, DBSCAN
from sklearn.mixture import GaussianMixture
from sklearn.metrics import pairwise_distances, silhouette_score, homogeneity_score
from sklearn.neighbors import NearestNeighbors
import numpy as np
import pandas as pd


__KMEANS_FILTER__ = [
    'n_clusters',
    'init',
    'n_init',
    'max_iter',
    'tol',
    'precompute_distances',
    'verbose',
    'random_state',
    'copy_x',
    'n_jobs',
    'algorithm'
]


def find_best_n_clusters(data, clusterer, max_n_clusters, random_state, **kwargs):
    args = {i: j for i, j in kwargs.items() if i in clusterer.get_params(clusterer)}
    if 'n_clusters' in clusterer.get_params(clusterer):
        kms = True
    else:
        kms = False
    args.pop('n_clusters' if kms else 'n_components', None)
    args.update({'random_state': random_state})
    score = {}
    for i in range(2, max_n_clusters + 1):
        args.update({'n_clusters' if kms else 'n_components': i})
        km = clusterer(**args)
        score[i] = silhouette_score(data, km.fit_predict(data), metric='cosine')
    best = pd.Series(score).idxmax()
    args.update({'n_clusters' if kms else 'n_components': best})
    print(f'Best number of clusters is {best}')
    return args


def find_best_eps(data, q=0.05):
    nn = NearestNeighbors()
    nn.fit(data)
    dist = nn.kneighbors()[0]
    dist = dist.flatten()
    dist = dist[dist > 0]
    return np.quantile(dist, q)


def simple_cluster(data, max_n_clusters=None, use_csi=True, random_state=0, **kwargs):
    """
    Finds cluster of users in data.

    :param data: feature matrix for clustering
    :param kwargs: keyword arguments for sklearn.cluster.KMeans
    :return: np.array of clusters
    """
    if max_n_clusters is not None:
        kmargs = find_best_n_clusters(data, KMeans, max_n_clusters, random_state, **kwargs)
    else:
        kmargs = {i: j for i, j in kwargs.items() if i in __KMEANS_FILTER__}
    kmargs.update({'random_state': random_state})
    km = KMeans(**kmargs)
    cl = km.fit_predict(data.values)
    bs = pd.get_dummies(cl)
    bs.index = data.index
    metrics = calc_all_metrics(data, km)
    if use_csi:
        metrics['csi'] = cluster_stability_index(data, km, bs, **kwargs)
    return cl, metrics


def aggregate_cl(cl, max_cl_number):
    res = {}
    for i in set(cl) - {-1}:
        res[i] = (cl == i).sum()
    topcl = pd.Series(res).sort_values().iloc[-max_cl_number:]
    return np.where(np.isin(cl, topcl.index.values), cl, -1)


def dbscan(data, use_csi=True, epsq=None, max_cl_number=None, **kwargs):
    """
    Finds cluster of users in data.

    :param data: feature matrix for clustering
    :param kwargs: keyword arguments for sklearn.cluster.KMeans
    :return: np.array of clusters
    """
    kmargs = {i: j for i, j in kwargs.items() if i in DBSCAN.get_params(DBSCAN)}
    if epsq is not None:
        kmargs.update({'eps': find_best_eps(data, epsq)})
    km = DBSCAN(**kmargs)
    cl = km.fit_predict(data.values)
    bs = pd.get_dummies(cl)
    bs.index = data.index
    metrics = calc_all_metrics(data, km)
    if use_csi:
        metrics['csi'] = cluster_stability_index(data, km, bs, **kwargs)
    if max_cl_number is not None:
        cl = aggregate_cl(cl, max_cl_number)
    return cl, metrics


def GMM(data, max_n_clusters=None, use_csi=True, random_state=0, **kwargs):
    """
    Finds cluster of users in data.

    :param data: feature matrix for clustering
    :param kwargs: keyword arguments for sklearn.cluster.KMeans
    :return: np.array of clusters
    """
    if max_n_clusters is not None:
        kmargs = find_best_n_clusters(data, GaussianMixture, max_n_clusters, random_state, **kwargs)
    else:
        kmargs = {i: j for i, j in kwargs.items() if i in GaussianMixture.get_params(GaussianMixture)}
    kmargs.update({'random_state': random_state})
    km = GaussianMixture(**kmargs)
    cl = km.fit_predict(data.values)
    km.labels_ = cl
    bs = pd.get_dummies(cl)
    bs.index = data.index
    metrics = calc_all_metrics(data, km)
    if use_csi:
        metrics['csi'] = cluster_stability_index(data, km, bs, **kwargs)
    return cl, metrics


def calc_mean_dist_from_center(data, km):
    res = {}
    cl = km.labels_
    cs = km.cluster_centers_
    for i in set(cl):
        res[i] = _cosine_dist(data[cl == i], cs[i]).mean()
    return res


def calc_mean_pd(data, cl):
    res = {}
    for i in set(cl):
        res[i] = _calc_mean_pd(data, cl == i)
    return res


def _cosine_dist(x, y):
    x.dot(y) / (x ** 2).sum(1)
    return (1 - x.dot(y) / np.sqrt((x ** 2).sum(1) * (y ** 2).sum(1 if len(y.shape) > 1 else 0))) / 2


def _calc_mean_pd(data, f):
    return (pairwise_distances(data[f], metric='cosine').sum() / 2) / (f.sum() ** 2 - f.sum())


def calc_all_metrics(data, km):
    res = {}
    cl = km.labels_
    res['mean_pd'] = calc_mean_pd(data, cl)
    if hasattr(km, 'cluster_centers_'):
        res['mean_fc'] = calc_mean_dist_from_center(data, km)
    if len(set(cl)) > 1:
        res['silhouette'] = silhouette_score(data, cl, metric='cosine')
    return res


def _ohe_stability(clusterers, base_clusterer):
    metrics = []
    for clusterer in clusterers:
        equals = []
        n = clusterer.shape[0]
        for i in range(base_clusterer.shape[1]):
            if clusterer.shape[1] == 0:
                break
            res = (clusterer & base_clusterer.loc[clusterer.index].iloc[:, i].values.reshape(-1, 1)).sum(0)
            equals.append(res.max())
            clusterer.pop(res.idxmax())
        metrics.append(sum(equals) / n)
    return np.mean(metrics)


def _cluster_ohe(clusterer, data):
    tmp = pd.get_dummies(clusterer.fit_predict(data))
    tmp.index = data.index
    return tmp


def cluster_stability_index(data, clusterer, base_clusterer=None, n_samples=10, frac=1, sample_size=None, replace=True, weights=None,
                            random_state=0, **kwargs):
    if base_clusterer is None:
        base_clusterer = _cluster_ohe(clusterer, data)
    clusterers = []
    for i in range(n_samples):
        tmp = data.sample(frac=frac, n=sample_size, replace=replace, weights=weights, random_state=i + random_state)

        clusterers.append(_cluster_ohe(clusterer, tmp))

    return _ohe_stability(clusterers, base_clusterer)
