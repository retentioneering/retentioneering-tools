# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md


import json
from datetime import datetime

import networkx as nx
import pandas as pd

from retentioneering.visualization import templates
from .plot_utils import __save_plot__, ___DynamicFigureWrapper__


_ = pd.DataFrame()


def _calc_layout(data,
                 node_params,
                 width=500,
                 height=500,
                 **kwargs):
    G = nx.DiGraph()

    G.add_weighted_edges_from(data.loc[:, ['source', 'target', 'weight']].values)

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
    data['weight_norm'] = data['weight'] / data['weight'].abs().max()
    for idx, row in data.iterrows():
        edges.append({
            "source": nodes.get(row.source),
            "target": nodes.get(row.target),
            "weight": row.weight_norm,
            "weight_text": row.weight,
            "type": row['type']
        })
    return edges, list(nodes.values())


def _filter_edgelist(data,
                     thresh,
                     node_params,
                     targets=None):
    if targets is None:
        x = pd.Series(node_params).astype(str).str.contains('target')
        targets = set(x[x].index)
    f = data.weight.abs() >= thresh
    nodes = set(data[f].source) | set(data[f].target)
    f |= (data.source.isin(targets) & data.target.isin(nodes))
    f |= (data.target.isin(targets) & data.source.isin(nodes))
    return data[f].copy()


def _make_json_data(data,
                    node_params,
                    layout_dump,
                    thresh=0.0,
                    width=500,
                    height=500,
                    **kwargs):
    res = {}
    data.columns = ['source', 'target', 'weight']
    data = _filter_edgelist(data, thresh, node_params)

    data["type"] = data.apply(
        lambda x: node_params.get(x.source) if node_params.get(x.source) == 'source' else node_params.get(
            x.target) or 'suit', 1)

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
    return nodes


@__save_plot__
def graph(data, *,
          node_params=None,
          thresh=0.0,
          width=800,
          height=500,
          interactive=True,
          layout_dump=None,
          show_percent=True,
          plot_name=None,
          node_weights=None,
          **kwargs):
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
    scale = data['edge_weight'].abs().max()

    if node_params is None:
        node_params = _prepare_node_params(node_params, data)
    res = _make_json_data(data,
                          node_params,
                          layout_dump,
                          thresh=thresh,
                          width=round(width - width / 3),
                          height=round(height - height / 3),
                          node_weights=node_weights,
                          **kwargs)

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
            thresh=thresh/scale,
            scale=scale
    )

    plot_name = 'graph_{}'.format(datetime.now()).replace(':', '_').replace('.', '_') + '.html'
    plot_name = _.rete.retention_config['experiments_folder'] + '/' + plot_name

    return (
        ___DynamicFigureWrapper__(x, interactive, width, height, res),
        plot_name,
        plot_name,
        data.rete.retention_config
    )


