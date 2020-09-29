# Copyright (C) 2019 Maxim Godzi, Anatoly Zaytsev, Dmitrii Kiselev
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.


from datetime import datetime


import matplotlib.pyplot as plt
from matplotlib import rcParams
import numpy as np
import pandas as pd
import seaborn as sns

from .plot_utils import __save_plot__, ___FigureWrapper__


@__save_plot__
def cluster_tsne(data, *,
                 clusters,
                 target,
                 plot_name=None,
                 **kwargs):
    """
    Plots TSNE projection of user stories colored by clusters. Each point represents a user session or whole user trajectory.
    Parameters
    --------
    data: pd.DataFrame
        Feature matrix.
    clusters: np.array
        Array of cluster IDs.
    target: np.array
        Boolean vector, if ``True``, then user has `positive_target_event` in trajectory.
    plot_name: str, optional
        Name of plot to save. Default: ``'clusters_tsne_{timestamp}.svg'``
    Returns
    -------
    Saves plot to ``retention_config.experiments_folder``
    Return type
    -------
    PNG
    """

    if hasattr(data.rete, '_tsne') and not kwargs.get('refit'):
        tsne2 = data.rete._tsne.copy()
    else:
        tsne2 = data.rete.learn_tsne(clusters, **kwargs)
    tsne = tsne2.values
    if np.unique(clusters).shape[0] > 10:
        f, ax = sns.mpl.pyplot.subplots()
        points = ax.scatter(tsne[:, 0], tsne[:, 1], c=clusters, cmap="BrBG")
        f.colorbar(points)
        scatter = ___FigureWrapper__(f)
    else:
        scatter = sns.scatterplot(tsne[:, 0], tsne[:, 1], hue=clusters, legend='full',
                                  palette=sns.color_palette("bright")[0:np.unique(clusters).shape[0]])
    plot_name = plot_name or 'cluster_tsne_{}'.format(datetime.now()).replace(':', '_').replace('.', '_') + '.svg'
    plot_name = data.rete.retention_config['experiments_folder'] + '/' + plot_name
    return scatter, plot_name, tsne2, data.rete.retention_config


@__save_plot__
def cluster_bar(data, *,
                clusters,
                target,
                target_names,
                plot_name=None,
                **kwargs):
    """
    Plots bar charts with cluster sizes and average target conversion rate.
    Parameters
    ----------
    data : pd.DataFrame
        Feature matrix.
    clusters : np.array
        Array of cluster IDs.
    target: np.array
        Boolean vector, if ``True``, then user has `positive_target_event` in trajectory.

    target: list of np.arrays
        Boolean vector, if ``True``, then user has `positive_target_event` in trajectory.


    plot_name : str, optional
        Name of plot to save. Default: ``'clusters_bar_{timestamp}.svg'``
    kwargs: optional
        Width and height of plot.
    Returns
    -------
    Saves plot to ``retention_config.experiments_folder``
    Return type
    -------
    PNG
    """
    cl = pd.DataFrame([clusters, *target], index=['clusters', *target_names]).T
    cl['cluster size'] = 1
    for t_n in target_names:
        cl[t_n] = cl[t_n].astype(int)

    bars = cl.groupby('clusters').agg({
        'cluster size': 'sum',
        **{t_n: 'mean' for t_n in target_names}
    }).reset_index()
    bars['cluster size'] /= bars['cluster size'].sum()

    bars = bars.melt('clusters', var_name='type', value_name='value')
    bars = bars[bars['type'] != ' '].copy()

    fig_x_size = round((1 + bars['clusters'].nunique()**0.7 * bars['type'].nunique()**0.7))
    rcParams['figure.figsize'] = fig_x_size, 6

    bar = sns.barplot(x='clusters', y='value', hue='type', data=bars)

    # move legend outside the box
    bar.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

    y_value = ['{:,.2f}'.format(x * 100) + '%' for x in bar.get_yticks()]

    bar.set_yticks(bar.get_yticks().tolist())
    bar.set_yticklabels(y_value)
    bar.set(ylabel=None)

    # adjust the limits
    ymin, ymax = bar.get_ylim()
    if ymax > 1:
        bar.set_ylim(ymin, 1)

    plot_name = plot_name or 'cluster_bar_{}'.format(datetime.now()).replace(':', '_').replace('.', '_') + '.svg'
    plot_name = data.rete.retention_config['experiments_folder'] + '/' + plot_name
    return bar, plot_name, None, data.rete.retention_config


@__save_plot__
def cluster_event_dist(bars, event_col, cl1, sizes, crs, cl2=None, plot_name=None):
    bar = sns.barplot(x=event_col, y='freq', hue='hue',
                      hue_order=[f'cluster {cl1}', 'all' if cl2 is None else f'cluster {cl2}'], data=bars)
    y_value = ['{:,.2f}'.format(x * 100) + '%' for x in bar.get_yticks()]
    bar.set_yticklabels(y_value)
    bar.set_xticklabels(bar.get_xticklabels(), rotation=30)
    bar.set(ylabel=None)
    tit = f'Distribution of top {bars.shape[0] // 2} events in cluster {cl1} (size: {round(sizes[0] * 100, 2)}%, CR: {round(crs[0] * 100, 2)}% ) '
    tit += f'vs. all data (CR: {round(crs[1] * 100, 2)}%)' if cl2 is None else f'vs. cluster {cl2} (size: {round(sizes[1] * 100, 2)}%, CR: {round(crs[1] * 100, 2)}%)'
    bar.set_title(tit)

    plot_name = plot_name or 'cluster_event_dist_{}'.format(datetime.now()).replace(':', '_').replace('.', '_') + '.svg'
    plot_name = bars.rete.retention_config['experiments_folder'] + '/' + plot_name
    return bar, plot_name, None, bars.rete.retention_config


@__save_plot__
def core_event_dist(rates, thresh, plot_name=None, **kwargs):
    hist = sns.distplot(rates.values, hist=True, bins=kwargs.get('bins'), kde=kwargs.get('kde'))
    if thresh is not None:
        sns.mpl.pyplot.axvline(thresh, c='C1')

    plot_name = plot_name or 'core_event_dist_{}'.format(datetime.now()).replace(':', '_').replace('.', '_') + '.svg'
    rates = rates.reset_index()
    plot_name = rates.rete.retention_config['experiments_folder'] + '/' + plot_name
    return hist, plot_name, None, rates.rete.retention_config


@__save_plot__
def permutation_importance(x, plot_name=None, **kwargs):
    fig = sns.mpl.pyplot.figure(figsize=[20, 7])
    sns.mpl.pyplot.xticks(rotation="vertical")
    sns.mpl.pyplot.title("Permutation importances")
    sns.mpl.pyplot.bar(x.feature.map(lambda x: " ".join(x)), x.importances_mean, yerr=x.importances_std)
    plot_name = plot_name or 'permutation_importance_{}'.format(datetime.now()).replace(':', '_').replace('.',
                                                                                                          '_') + '.svg'
    return ___FigureWrapper__(fig), plot_name, None, x.rete.retention_config







