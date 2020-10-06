# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md

import pandas as pd

from retentioneering.visualization import plot_project
from .extract_features import _learn_tsne, _learn_umap


def project(self, *,
            method='tsne',
            targets=(),
            ngram_range=(1,1),
            feature_type='tfidf',
            plot_type=None,
            **kwargs):
    """
    Does dimention reduction of user trajectories and draws projection plane.

    Parameters
    ----------
    method: {'umap', 'tsne'} (optional, default 'tsne')
        Type of manifold transformation.
    plot_type: {'targets', 'clusters', None} (optional, default None)
        Type of color-coding used for projection visualization:
            - 'clusters': colors trajectories with different colors depending on cluster number.
            IMPORTANT: must do .rete.get_clusters() before to obtain cluster mapping.
            - 'targets': color trajectories based on reach to any event provided in 'targets' parameter.
            Must provide 'targets' parameter in this case.
        If None, then only calculates TSNE without visualization.

    targets: list or tuple of str (optional, default  ())
        Vector of event_names as str. If user reach any of the specified events, the dot corresponding
        to this user will be highlighted as converted on the resulting projection plot

    feature_type: str, (optional, default 'tfidf')
        Type of vectorizer to use before dimension-reduction. Available vectorization methods:
        {'tfidf', 'count', 'binary', 'frequency'}

    ngram_range: tuple, (optional, default (1,1))
        The lower and upper boundary of the range of n-values for different
        word n-grams or char n-grams to be extracted before dimension-reduction.
        For example ngram_range=(1, 1) means only single events, (1, 2) means single events
        and bigrams.

    Returns
    --------
    Dataframe with data in the low-dimensional space for user trajectories indexed by user IDs.

    Return type
    --------
    pd.DataFrame
    """
    event_col = self.retention_config['event_col']
    index_col = self.retention_config['user_col']

    if plot_type == 'clusters':
        if hasattr(self, 'clusters'):
            targets_mapping = self.clusters
            legend_title = 'cluster number:'
        else:
            raise AttributeError("Run .rete.get_clusters() before using plot_type='clusters' to obtain clusters mapping")

    elif plot_type == 'targets':
        if targets is None:
            raise ValueError("When plot_type ='targets' must provide parameter targets as list of target event names")
        else:
            targets = [list(pd.core.common.flatten(targets))]
            legend_title = 'conversion to (' + ' | '.join(targets[0]).strip(' | ') + '):'
            targets_mapping = (self._obj
                                     .groupby(index_col)[event_col]
                                     .apply(lambda x: bool(set(*targets) & set(x)))
                                     .to_frame()
                                     .sort_index()[event_col]
                                     .values)

    features = self.extract_features(feature_type=feature_type,
                                     ngram_range=ngram_range)

    if method == 'tsne':
        self._projection = _learn_tsne(features, **kwargs)
    elif method == 'umap':
        self._projection = _learn_umap(features, **kwargs)

    # return only embeddings is no plot_type:
    if plot_type is None:
        return self._projection

    plot_project.plot_projection(
        projection=self._projection.values,
        targets=targets_mapping,
        legend_title=legend_title,
    )

    return self._projection
