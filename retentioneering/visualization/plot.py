# Copyright (C) 2019 Maxim Godzi, Anatoly Zaytsev, Dmitrii Kiselev
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.


import itertools
import networkx as nx
import seaborn as sns
from IPython.display import IFrame, display  # TODO understand how to use visualization without it
import numpy as np
from datetime import datetime
import pandas as pd
import json
from functools import wraps
from retentioneering.visualization.cloud_logger import MongoLoader
import altair as alt
import os
from retentioneering.visualization import templates
import matplotlib.pyplot as plt


def _calc_layout(data, node_params, width=500, height=500, **kwargs):
    G = nx.DiGraph()

    G.add_weighted_edges_from(data.loc[:, ['source', 'target', 'weight']].values)

    # if nx.algorithms.check_planarity(G)[0]:
    #     pos_new = nx.layout.planar_layout(G)
    # else:
    #     pos_new = nx.layout.spring_layout(G, k=kwargs.get('k', .1),
    #                                       iterations=kwargs.get('iterations', 300),
    #                                       threshold=kwargs.get('nx_threshold', 1e-4),
    #                                       seed=0)
    pos_new = nx.layout.spring_layout(G, k=kwargs.get('k', .1),
                                      iterations=kwargs.get('iterations', 300),
                                      threshold=kwargs.get('nx_threshold', 1e-4),
                                      seed=0)

    min_x = min([j[0] for i, j in pos_new.items()])
    min_y = min([j[1] for i, j in pos_new.items()])
    max_x = max([j[0] for i, j in pos_new.items()])
    max_y = max([j[1] for i, j in pos_new.items()])
    pos_new = {
        i: [(j[0] - min_x) / (max_x - min_x) * (width - 150) + 75,
            (j[1] - min_y) / (max_y - min_y) * (height - 100) + 50]
        for i, j in pos_new.items()
    }
    return pos_new, dict(G.degree)


def _prepare_nodes(data, pos, node_params, degrees):
    node_set = set(data['source']) | set(data['target'])
    max_degree = max(degrees.values())
    nodes = {}
    for idx, node in enumerate(node_set):
        node_pos = pos.get(node)
        nodes.update({node: {
            "index": idx,
            "name": node,
            "x": node_pos[0],
            "y": node_pos[1],
            "type": (node_params.get(node) or "suit").split('_')[0] + '_node',
            "degree": (abs(degrees.get(node, 0))) / abs(max_degree) * 30 + 4
        }})
    return nodes


def _prepare_edges(data, nodes):
    edges = []
    #print(data)
    #data['weight_norm'] = data.weight
    data['weight_norm'] = data.weight / data.weight.abs().max()
    for idx, row in data.iterrows():
        edges.append({
            "source": nodes.get(row.source),
            "target": nodes.get(row.target),
            #"weight": np.log1p(abs(row.weight_norm + 1e-100)) * 1.5,
            "weight": row.weight_norm,
            "weight_text": row.weight,
            "type": row['type']
        })
    return edges, list(nodes.values())


def _filter_edgelist(data, thresh, node_params, targets=None, **kwargs):
    if targets is None:
        x = pd.Series(node_params).str.contains('target')
        targets = set(x[x].index)
    f = data.weight.abs() >= thresh
    nodes = set(data[f].source) | set(data[f].target)
    f |= (data.source.isin(targets) & data.target.isin(nodes))
    f |= (data.target.isin(targets) & data.source.isin(nodes))
    return data[f].copy()


def _make_json_data(data, node_params, layout_dump, thresh=.05, width=500, height=500, **kwargs):
    res = {}
    data.columns = ['source', 'target', 'weight']
    data = _filter_edgelist(data, thresh, node_params, **kwargs)
    if kwargs.get('mode') == 'importance':
        data['type'] = np.where(data.weight >= 0, 'positive', 'negative')
    else:
        data["type"] = data.apply(
            lambda x: node_params.get(x.source) if node_params.get(x.source) == 'source' else node_params.get(
                x.target) or 'suit',
            1
        )
    pos, degrees = _calc_layout(data, node_params, width=width, height=height, **kwargs)
    if kwargs.get('node_weights') is not None:
        degrees = kwargs.get('node_weights')
    if layout_dump is not None:
        nodes = _prepare_given_layout(layout_dump, node_params, degrees)
    else:
        nodes = _prepare_nodes(data, pos, node_params, degrees)
    res['links'], res['nodes'] = _prepare_edges(data, nodes)
    return res


def _prepare_node_params(node_params, data):
    if node_params is None:
        _node_params = {
            'positive_target_event': 'nice_target',
            'negative_target_event': 'bad_target',
            'source_event': 'source',
        }
        node_params = {}
        for key, val in _node_params.items():
            name = data.rete.retention_config.get(key)
            if name is None:
                continue
            node_params.update({name: val})
    return node_params


def _prepare_layout(layout):
    nodes = {}
    for i in layout:
        nodes.update({i['name']: i})
    return nodes


def _prepare_given_layout(nodes_path, node_params, degrees):
    if type(nodes_path) is str:
        with open(nodes_path, encoding='utf-8') as f:
            nodes = json.load(f)
    else:
        nodes = nodes_path
    if type(nodes) is list:
        nodes = _prepare_layout(nodes)
    max_degree = max(degrees.values() or [1e-20])
    for node, val in nodes.items():
        val.update({
            "type": (node_params.get(node) or "suit").split('_')[0] + '_node',
            "degree": degrees.get(node, 0) / max_degree * 30
        })
    return nodes


def __save_plot__(func):
    @wraps(func)
    def save_plot_wrapper(*args, **kwargs):
        sns.mpl.pyplot.show()
        sns.mpl.pyplot.close()
        res = func(*args, **kwargs)
        if len(res) == 2:
            (vis_object, name), res, cfg = res, None, None
        elif len(res) == 3:
            (vis_object, name, res), cfg = res, None
        else:
            vis_object, name, res, cfg = res
        idx = 'id: ' + str(int(datetime.now().timestamp()))
        coords = vis_object.axis()

        if '_3d_' not in name:
            pass
            # watermarks:
            # vis_object.text((coords[0] - (coords[1] - coords[0]) / 10),
            #                 (coords[3] + (coords[3] - coords[2]) / 10), idx, fontsize=8)
            # vis_object.text(0.25, 0.05, 'Retentioneering', fontsize=50, color='gray', va='bottom', alpha=0.1)
        vis_object.get_figure().savefig(name, bbox_inches="tight", dpi=cfg.get('save_dpi') or 200)
        if cfg.get('mongo_client') is not None:
            print(f'DB {idx}')
            ml = MongoLoader(cfg.get('mongo_client'), collection=cfg.get('mongo_user'))
            ml.put(name if '.' in name else name + '.png', idx.split(' ')[1])
            if '.html' in name:
                ml.put(vis_object.get_raw(name), idx.split(' ')[1] + '_config')
        return res

    return save_plot_wrapper


def __altair_save_plot__(func):
    @wraps(func)
    def altair_save_plot_wrapper(*args, **kwargs):
        res = func(*args, **kwargs)
        vis_object, plot_name, res, cfg = res
        idx = 'id: ' + str(int(datetime.now().timestamp())) + ", Retentioneering Copyright"
        plot_name_preffix = func.__name__

        plot_name = '{}_{}'.format(plot_name_preffix, plot_name or datetime.now()).replace(':', '_').replace('.',
                                                                                                             '_').replace(
            ' ', '_') + '.html'
        if hasattr(pd.DataFrame, 'retention'):
            plot_name = pd.DataFrame().rete.retention_config['experiments_folder'] + '/' + plot_name
        else:
            path = 'experiments'
            if not os.path.exists(path):
                os.mkdir(path)
            plot_name = path + '/' + plot_name
        vis_object.title = idx

        print("You can save plot as SVG or PNG by open three-dotted button at right =>")
        watermark = alt.Chart().mark_text(
            align='center', baseline='top', dy=vis_object.height // 2 + 30, fontSize=32, fontWeight=200,
            color='#d3d3d3', text='Retentioneering'
        )
        vis_object.save(plot_name)

        render_in_template = True

        if render_in_template and kwargs.get('interactive', True):
            html_object = templates.__VEGA_TEMPLATE__.format(
                visual_object=json.dumps(vis_object.to_dict()),
                func_name=func.__name__
            )
            fig = ___DynamicFigureWrapper__(html_object, True, vis_object.width, vis_object.height, res)
            fig.get_figure().savefig(plot_name, bbox_inches="tight", dpi=cfg.get('save_dpi') or 200)

        elif kwargs.get('interactive', True):
            print("You can save plot as SVG or PNG by open three-dotted button at right =>")
            alt.renderers.enable('notebook')
            display(vis_object)

        if cfg.get('mongo_client') is not None:
            print(f'DB {idx}')
            ml = MongoLoader(cfg.get('mongo_client'), collection=cfg.get('mongo_user'))
            ml.put(plot_name if '.' in plot_name else plot_name + '.svg', idx.split(' ')[1])
        return res

    return altair_save_plot_wrapper


@__save_plot__
def graph(data, node_params=None, thresh=0.0, width=800, height=500, interactive=True,
          layout_dump=None, show_percent=True, plot_name=None, node_weights=None, **kwargs):
    """
    Create interactive graph visualization. Each node is a unique ``event_col`` value, edges are transitions between events and edge weights are calculated metrics. By default, it is a percentage of unique users that have passed though a particular edge visualized with the edge thickness. Node sizes are  Graph loop is a transition to the same node, which may happen if users encountered multiple errors or made any action at least twice.
    Graph nodes are movable on canvas which helps to visualize user trajectories but is also a cumbersome process to place all the nodes so it forms a story.
    That is why IFrame object also has a download button. By pressing it, a JSON configuration file with all the node parameters is downloaded. It contains node names, their positions, relative sizes and types. It it used as ``layout_dump`` parameter for layout configuration. Finally, show weights toggle shows and hides edge weights.

    Parameters
    ---------
    data: pd.DataFrame
        Graph in edgelist form.
    node_params: dict, optional
        Event mapping describing which nodes or edges should be highlighted by different colors for better visualisation. Dictionary keys are ``event_col`` values, while keys have the following possible values:
            - ``bad_target``: highlights node and all incoming edges with red color;
            - ``nice_target``: highlights node and all incoming edges with green color;
            - ``bad_node``: highlights node with red color;
            - ``nice_node``: highlights node with green color;
            - ``source``: highlights node and all outgoing edges with yellow color.
        Example ``node_params`` is shown below:
        ```
        {
            'lost': 'bad_target',
            'purchased': 'nice_target',
            'onboarding_welcome_screen': 'source',
            'choose_login_type': 'nice_node',
            'accept_privacy_policy': 'bad_node',
        }
        ```
        If ``node_params=None``, it will be constructed from ``retention_config`` variable, so that:
        ```
        {
            'positive_target_event': 'nice_target',
            'negative_target_event': 'bad_target',
            'source_event': 'source',
        }
        ```
    thresh: float, optional
        Minimal edge weight value to be rendered on a graph. If a node has no edges of the weight >= ``thresh``, then it is not shown on a graph. It is used to filter out rare event and not to clutter visualization. Default: ``0.05``
    width: float, optional
        Width of plot in pixels. Default: ``500``
    height: float, optional
        Height of plot in pixels. Default: ``500``
    interactive: bool, optional
        If ``True``, then plots graph visualization in interactive session (Jupyter notebook). Default: ``True``
    layout_dump: str, optional
        Path to layout configuration file relative to current directory. If defined, uses configuration file as a graph layout. Default: ``None``
    show_percent: bool, optional
        If ``True``, then all edge weights are converted to percents by multiplying by 100 and adding percentage sign. Default: ``True``

    Returns
    -------
    Saves webpage with JS graph visualization to ``retention_config.experiments_folder``.

    Return type
    -------
    HTML
    """
    dump = 1
    if layout_dump is None:
        dump = 0
    if node_params is None:
        node_params = _prepare_node_params(node_params, data)
    res = _make_json_data(data, node_params, layout_dump, thresh=thresh,
                          width=width - width / 3, height=height - height / 3, node_weights=node_weights, **kwargs)

    res['node_params'] = node_params
    show = 0
    if show_percent:
        show = 1
    dump = 1 if (layout_dump is not None) or (kwargs.get('is_model', False)) else 0
    __TEMPLATE__ = templates.__OLD_TEMPLATE__ if kwargs.get('use_old', False) else templates.__TEMPLATE__
    x = __TEMPLATE__.format(
        width=width,
        height=height,
        links=json.dumps(res.get('links')).encode('latin1').decode('utf-8'),
        node_params=json.dumps(node_params).encode('latin1').decode('utf-8'),
        nodes=json.dumps(res.get('nodes')).encode('latin1').decode('utf-8'),
        show_percent=show,
        layout_dump=dump,
        thresh=thresh,
    )
    if hasattr(data, 'trajectory'):
        if plot_name is None:
            plot_name = f'index_{datetime.now()}'
    else:
        if plot_name is None:
            plot_name = 'index'
    plot_name = f"{data.trajectory.retention_config['experiments_folder']}/{plot_name.replace(':', '_').replace('.', '_')}" + '.html'
    return (
        ___DynamicFigureWrapper__(x, interactive, width, height, res),
        plot_name,
        plot_name,
        data.rete.retention_config
    )


@__altair_save_plot__
def altair_step_matrix(diff, plot_name=None, title='', vmin=None, vmax=None, font_size=12, **kwargs):
    heatmap_data = diff.reset_index().melt('index')
    heatmap_data.columns = ['y', 'x', 'z']
    table = alt.Chart(heatmap_data).encode(
        x=alt.X('x:O', sort=None),
        y=alt.Y('y:O', sort=None)
    )
    heatmap = table.mark_rect().encode(
        color=alt.Color(
            'z:Q',
            scale=alt.Scale(scheme='blues'),
        )
    )
    text = table.mark_text(
        align='center', fontSize=font_size
    ).encode(
        text='z',
        color=alt.condition(
            abs(alt.datum.z) < 0.8,
            alt.value('black'),
            alt.value('white'))
    )
    heatmap_object = (heatmap + text).properties(
        width=3 * font_size * len(diff.columns),
        height=2 * font_size * diff.shape[0]
    )
    return heatmap_object, plot_name, None, diff.rete.retention_config

@__save_plot__
def step_matrix(data, targets=None,*,targets_list=None, plot_name=None, title='', vmin=None, vmax=None, **kwargs):

    target_cmaps = itertools.cycle(['PuOr', 'seismic', 'PRGn', 'RdBu',  'RdGy',
            'RdYlBu', 'RdYlGn', 'Spectral', 'bwr' ])

    if targets is None:

        sns.mpl.pyplot.figure(figsize=(round(data.shape[1] * 0.6),
                                       round(data.shape[0] * 0.5)
                                       ))
        heatmap = sns.heatmap(data, annot=True, cmap="BrBG", fmt='.2f',
                              center=0, vmin=vmin, vmax=vmax, cbar=False)
        heatmap.set_title(title)

        # fix for mpl bug that cuts off top/bottom of seaborn viz
        # b, t = plt.ylim()
        # b += 0.5
        # t -= 0.5
        # plt.ylim(b, t)

    else:
        n_rows = 1 + len(targets_list)
        n_cols = 1

        f, axs = sns.mpl.pyplot.subplots(n_rows, n_cols, sharex=True,
                                         figsize=(round(data.shape[1] * 0.6),
                                                  round((len(data) + len(targets)) * 0.6)),
                                         gridspec_kw={'wspace': 0.08, 'hspace': 0.03,
                                                      'height_ratios': [data.shape[0], *list(map(len, targets_list))]
                                                      })

        heatmap = sns.heatmap(data,
                              yticklabels=data.index,
                              annot=True,
                              fmt='.2f',
                              ax=axs[0],
                              cmap="BrBG",
                              center=0,
                              cbar=False)

        for n, i in enumerate(targets_list):
            sns.heatmap(targets.loc[i],
                        yticklabels=targets.loc[i].index,
                        annot=True,
                        fmt='.2f',
                        ax=axs[1 + n],
                        cmap=next(target_cmaps),
                        center=0,
                        cbar=False)

        for ax in axs:
            sns.mpl.pyplot.sca(ax)
            sns.mpl.pyplot.yticks(rotation=0)

        for _, spine in heatmap.spines.items():
            spine.set_visible(True)

        f.suptitle(title)

    plot_name = plot_name or 'step_matrix_{}'.format(datetime.now()).replace(':', '_').replace('.', '_') + '.svg'
    plot_name = data.rete.retention_config['experiments_folder'] + '/' + plot_name


    return heatmap, plot_name, None, data.rete.retention_config


@__altair_save_plot__
def altair_cluster_tsne(data, clusters, target, plot_name=None, **kwargs):
    if hasattr(data.retention, '_tsne'):
        tsne = data.rete._tsne.copy()
    else:
        tsne = data.rete.learn_tsne(clusters, **kwargs)
    tsne['color'] = clusters
    tsne.columns = ['x', 'y', 'color']

    scatter = alt.Chart(tsne).mark_point().encode(
        x='x',
        y='y',
        color=alt.Color(
            'color',
            scale=alt.Scale(scheme='plasma')
        )
    ).properties(
        width=800,
        height=600
    )
    return scatter, plot_name, tsne, data.rete.retention_config


@__save_plot__
def cluster_tsne(data, clusters, target, plot_name=None, **kwargs):
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

    if hasattr(data.retention, '_tsne') and not kwargs.get('refit'):
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


@__altair_save_plot__
def altair_cluster_bar(data, clusters, target, plot_name=None, plot_cnt=None, metrics=None, **kwargs):
    cl = pd.DataFrame([clusters, target], index=['clusters', 'target']).T
    cl['cnt'] = 1
    cl.target = cl.target.astype(int)
    bars = cl.groupby('clusters').agg({
        'cnt': 'sum',
        'target': 'mean'
    }).reset_index()
    bars.cnt /= bars.cnt.sum()
    bars = bars.loc[:, ['clusters', 'cnt']].append(bars.loc[:, ['clusters', 'target']], ignore_index=True, sort=False)
    bars['target'] = np.where(bars.target.isnull(), bars.cnt, bars.target)
    bars['Metric'] = np.where(bars['cnt'].isnull(), 'Average CR', 'Cluster size')
    # print(bars, type(bars))
    bar = alt.Chart(bars).mark_bar().encode(
        x='Metric:O',
        y='target:Q',
        color='Metric:N',
        column='clusters:N'
    ).properties(
        width=60,
        height=200
    )

    return bar, plot_name, None, data.rete.retention_config


@__save_plot__
def cluster_bar(data, clusters, target, plot_name=None, plot_cnt=None, metrics=None, **kwargs):
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
    cl = pd.DataFrame([clusters, target], index=['clusters', 'target']).T
    cl['cnt'] = 1
    cl.target = cl.target.astype(int)
    bars = cl.groupby('clusters').agg({
        'cnt': 'sum',
        'target': 'mean'
    }).reset_index()
    bars.cnt /= bars.cnt.sum()
    bars = bars.loc[:, ['clusters', 'cnt']].append(bars.loc[:, ['clusters', 'target']], ignore_index=True, sort=False)
    bars['target'] = np.where(bars.target.isnull(), bars.cnt, bars.target)
    bars['Metric'] = np.where(bars['cnt'].isnull(), 'Average CR', 'Cluster size')
    bar = sns.barplot(x='clusters', y='target', hue='Metric', hue_order=['Cluster size', 'Average CR'], data=bars)
    y_value = ['{:,.2f}'.format(x * 100) + '%' for x in bar.get_yticks()]
    bar.set_yticklabels(y_value)
    bar.set(ylabel=None)

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
def cluster_pie(data, clusters, target, plot_name=None, plot_cnt=None, metrics=None, **kwargs):
    """
    Plots pie-charts of target distribution for different clusters.

    Parameters
    -------
    data: pd.DataFrame
        Feature matrix
    clusters: np.array
        Array of cluster IDs.
    target: np.array
        Boolean vector, if ``True``, then user has `positive_target_event` in trajectory.
    plot_name: str, optional
        Name of plot to save. Default: ``'clusters_pie_{timestamp}.svg'``
    kwargs: optional
        Width and height of plot.

    Returns
    -------
    Saves plot to ``retention_config.experiments_folder``

    Return type
    -------
    PNG
    """
    cl = pd.DataFrame([clusters, target], index=['clusters', 'target']).T
    cl.target = np.where(cl.target, data.rete.retention_config['positive_target_event'],
                         data.rete.retention_config['negative_target_event'])
    pie_data = cl.groupby(['clusters', 'target']).size().rename('target_dist').reset_index()
    targets = list(set(pie_data.target))

    if plot_cnt is None:
        plot_cnt = len(set(clusters))

    if kwargs.get('vol', True):  # vol = False in kwargs in case you want to disable
        _, counts = np.unique(clusters, return_counts=True)
        volumes = 100 * (counts / sum(counts))
    else:
        volumes = [None] * plot_cnt

    fig, ax = sns.mpl.pyplot.subplots(1 if plot_cnt <= 2 else (plot_cnt // 2 + plot_cnt % 2), 2)
    fig.suptitle(
        'Distribution of targets in clusters. Silhouette: {:.2f}, Homogeneity: {:.2f}, Cluster stability: {:.2f}'.format(
            metrics.get('silhouette') if (metrics or {}).get('silhouette') is not None else 0,
            metrics.get('homogen') if metrics is not None else 0,
            metrics.get('csi') if (metrics or {}).get('csi') is not None else 0
        ))
    fig.set_size_inches(kwargs.get('width', 20), kwargs.get('height', 10))
    for i, j in enumerate(pie_data.clusters.unique()):
        tmp = pie_data[pie_data.clusters == j]
        tmp.index = tmp.target
        if plot_cnt <= 2:
            ax[i].pie(tmp.target_dist.reindex(targets).fillna(0).values, labels=targets, autopct='%1.1f%%')
            ax[i].set_title('Class {}\nCluster volume {}%\nMean dist from center {:.2f}'.format(
                i, round(volumes[i], 1), metrics['mean_fc'][j] if (metrics or {}).get('mean_fc') is not None else 0))
        else:
            ax[i // 2][i % 2].pie(tmp.target_dist.reindex(targets).fillna(0).values, labels=targets, autopct='%1.1f%%')
            ax[i // 2][i % 2].set_title('Class {}\nCluster volume {}%\nMean dist from center {:.2f}'.format(
                i, round(volumes[i], 1), metrics['mean_fc'][j] if (metrics or {}).get('mean_fc') is not None else 0))
    if plot_cnt % 2 == 1:
        fig.delaxes(ax[plot_cnt // 2, 1])

    plot_name = plot_name or 'cluster_pie_{}'.format(datetime.now()).replace(':', '_').replace('.', '_') + '.svg'
    plot_name = data.rete.retention_config['experiments_folder'] + '/' + plot_name
    return ___FigureWrapper__(fig), plot_name, None, data.rete.retention_config


@__save_plot__
def cluster_heatmap(data, clusters, target, plot_name=None, **kwargs):
    """
    Visualizes feature usage with heatmap.

    Parameters
    --------
    data: pd.DataFrame
        Feature matrix.
    clusters: np.array
        Array of cluster IDs.
    target: np.array
        Boolean vector, if ``True``, then user has `positive_target_event` in trajectory.
    plot_name: str, optional
        Name of plot to save. Default: ``'clusters_heatmap_{timestamp}.svg'``

    Returns
    -------
    Saves plot to ``retention_config.experiments_folder``

    Return type
    -------
    PNG
    """
    heatmap = sns.clustermap(data.values,
                             cmap="BrBG",
                             xticklabels=data.columns,
                             yticklabels=False,
                             row_cluster=True,
                             col_cluster=False)

    heatmap.ax_row_dendrogram.set_visible(False)
    heatmap = heatmap.ax_heatmap

    plot_name = plot_name or 'cluster_heatmap_{}'.format(datetime.now()).replace(':', '_').replace('.', '_') + '.svg'
    plot_name = data.rete.retention_config['experiments_folder'] + '/' + plot_name
    return heatmap, plot_name, None, data.rete.retention_config


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


@__save_plot__
def tsne_3d(data, clusters, target, plot_name=None, use_coloring=False, **kwargs):
    if hasattr(data.retention, '_tsne'):
        tsne2 = data.rete._tsne.copy()
    else:
        tsne2 = data.rete.learn_tsne(clusters, **kwargs)
    tsne = tsne2.values
    fig = sns.mpl.pyplot.figure(figsize=(10, 10))
    ax = fig.add_subplot(111, projection='3d')

    if target is not None:
        scatter = ax.scatter(tsne[:, 0], tsne[:, 1], target, c=['C{}'.format(i) for i in clusters])
        lgs = []
        for i in set(clusters):
            lgs.append(sns.mpl.lines.Line2D([0], [0], linestyle="none", c='C{}'.format(i), marker='o'))
        ax.legend(lgs, set(clusters), numpoints=1)
    else:
        scatter = ax.scatter(tsne[:, 0], tsne[:, 1], clusters)

    ax.set_xlabel('TSNE 0')
    ax.set_ylabel('TSNE 1')
    ax.set_zlabel('Target')

    scatter = ___FigureWrapper__(fig)
    plot_name = plot_name or 'tsne_3d_{}'.format(datetime.now()).replace(':', '_').replace('.', '_') + '.svg'
    plot_name = data.rete.retention_config['experiments_folder'] + '/' + plot_name
    return scatter, plot_name, None, data.rete.retention_config


class ___FigureWrapper__(object):
    def __init__(self, fig):
        self.fig = fig

    def get_figure(self):
        return self.fig

    def axis(self):
        if len(self.fig.axes) > 1:
            x = self.fig.axes[1].axis()
        else:
            x = self.fig.axes[0].axis()
        return (x[0] / 64, x[0] + (x[1] - x[0]) / 50, x[2] / 1.5, x[3] / 1.5)

    def text(self, *args, **kwargs):
        self.fig.text(*args, **kwargs)


class __SaveFigWrapper__(object):
    def __init__(self, data, interactive=True, width=1000, height=700):
        self.data = data
        self.interactive = interactive
        self.width = width
        self.height = height

    def savefig(self, name, **kwargs):
        with open(name, 'w', encoding="utf-8") as f:
            f.write(self.data)
        if self.interactive:
            display(IFrame(name, width=self.width + 200, height=self.height + 200))


class ___DynamicFigureWrapper__(object):
    def __init__(self, fig, interactive, width, height, links):
        self.fig = fig
        self.interactive, self.width, self.height = interactive, width, height
        self.links = links

    def get_figure(self):
        savefig = __SaveFigWrapper__(self.fig, self.interactive, self.width, self.height)
        return savefig

    def text(self, x, y, text, *args, **kwargs):
        # parts = self.fig.split('<main>')
        # res = parts[:1] + [f'<p>{text}</p>'] + parts[1:]
        # self.fig = '\n'.join(res)
        pass

    def get_raw(self, path):
        base = '.'.join(path.split('.')[:-1])
        with open(base + '_config.json', 'w', encoding="utf-8") as f:
            json.dump(self.links, f)
        return base + '_config.json'

    @staticmethod
    def axis():
        return 4 * [0]
