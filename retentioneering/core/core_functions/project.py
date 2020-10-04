# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive, Non-Commercial Use License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md

from retentioneering.visualization import plot

def project(self, *,
            method='umap',
            targets=None,
            plot_type=None,
            refit=False,
            regression_targets=None,
            sample_size=None,
            sample_frac=None,
            proj_type=None,
            **kwargs):
    """
    Learns manifold projection using selected method for selected feature space (`feature_type` in kwargs) and visualizes it with chosen visualization type.

    Parameters
    --------
    targets: np.array, optional
        Vector of targets for users. if None, then calculates automatically based on ``positive_target_event`` and ``negative_target_event``.
    method: 'umap' or 'tsne'
    plot_type: str, optional
        Type of projection visualization:
            - ``clusters``: colors trajectories with different colors depending on cluster number.
            - ``targets``: color trajectories based on target reach.
        If ``None``, then only calculates TSNE without visualization. Default: ``None``
    refit: bool, optional
        If ``True``, then TSNE will be refitted, e.g. it is needed if you perform hyperparameters selection.
    regression_targets: dict, optional
        Mapping of ``index_col`` to regression target for custom coloring. For example, if you want to visually evaluate average LTV of user with trajectories clusterization. For more information refer to ``BaseDataset.rete.make_regression_targets()``.
    cmethod: str, optional
        Method of clustering if plot_type = 'clusters'. Refer to ``BaseDataset.rete.get_clusters()`` for more information.
    kwargs: optional
        Parameters for ``BaseDataset.rete.extract_features()``, ``sklearn.manifold.TSNE`` and ``BaseDataset.rete.get_clusters()``

    Returns
    --------
    Dataframe with data in the low-dimensional space for user trajectories indexed by user IDs.

    Return type
    --------
    pd.DataFrame
    """
    old_targs = None
    if hasattr(self, 'datatype') and self.datatype == 'features':
        features = self._obj.copy()
    else:
        features = self.extract_features(**kwargs)
        if targets is None:
            if regression_targets is not None:
                targets = self.make_regression_targets(features, regression_targets)
            else:
                targets = features.index.isin(self.get_positive_users(**kwargs))
                targets = np.where(targets, self.retention_config['positive_target_event'],
                                   self.retention_config['negative_target_event'])
        self._tsne_targets = targets

    if sample_frac is not None:
        features = features.sample(frac=sample_frac, random_state=0)
    elif sample_size is not None:
        features = features.sample(n=sample_size, random_state=0)

    if not hasattr(self, '_tsne') or refit:
        if method == 'tsne':
            self._tsne = feature_extraction.learn_tsne(features, **kwargs)
        if method == 'umap':
            self._tsne = feature_extraction.learn_umap(features, **kwargs)

    if plot_type == 'clusters':
        if kwargs.get('cmethod') is not None:
            kwargs['method'] = kwargs.pop('cmethod')
        old_targs = targets.copy()
        targets = self.get_clusters(plot_type=None, **kwargs)
    elif plot_type == 'targets':
        targets = self._tsne_targets
    else:
        return self._tsne

    plot.cluster_tsne(
        self._obj,
        clustering.aggregate_cl(targets, 7) if kwargs.get('method') == 'dbscan' else targets,
        targets,
        **kwargs
    )

    return self._tsne
