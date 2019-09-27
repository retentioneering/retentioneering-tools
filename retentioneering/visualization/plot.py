# Copyright (C) 2019 Maxim Godzi, Anatoly Zaytsev, Dmitrii Kiselev
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.


import networkx as nx
import seaborn as sns
from IPython.display import IFrame, display # TODO understand how to use visualization without it
import numpy as np
from datetime import datetime
import pandas as pd
import json
from functools import wraps

# from MulticoreTSNE import MulticoreTSNE as TSNE


__TEMPLATE__ = """
<!DOCTYPE html>
<meta charset="utf-8">
<style>
                circle {{
                  fill: #ccc;
                  stroke: #333;
                  stroke-width: 1.5px;
                }}

                .circle.source_node {{
                  fill: #f3f310;
                }}

                .circle.nice_node {{
                  fill: green;
                }}

                .circle.bad_node {{
                  fill: red;
                }}

                .link {{
                  fill: none;
                  stroke: #666;
                  stroke-opacity: 0.7;
                }}

                #nice_target {{
                  fill: green;
                }}

                .link.nice_target {{
                  stroke: green;
                }}

                #source {{
                  fill: yellow;
                }}

                .link.source {{
                  stroke: #f3f310;
                }}
                
                .link.positive {{
                  stroke: green;
                }}
                
                .link.negative {{
                  stroke: red;
                }}

                #source {{
                  fill: orange;
                }}

                .link.source1 {{
                  stroke: orange;
                }}

                #bad_target {{
                  fill: red;
                }}

                .link.bad_target {{
                  stroke: red;
                }}
                text {{
                  font: 12px sans-serif;
                  pointer-events: none;
                  text-shadow: 0 1px 0 #fff, 1px 0 0 #fff, 0 -1px 0 #fff, -1px 0 0 #fff;
                }}

</style>
<body>
<script src="https://api.retentioneering.com/files/d3.v4.min.js"></script>
<div>
  <input type="checkbox" class="checkbox" value="weighted"><label> Show weights </label>
</div>
<div id="option">
    <input name="downloadButton" 
           type="button" 
           value="download" 
           onclick="downloadLayout()" />
</div>
<script>

var links = {links};
var node_params = {node_params};

var nodes = {nodes};

var width = {width},
    height = {height};

var svg = d3.select("body").append("svg")
    .attr("width", width)
    .attr("height", height);

let defs = svg.append("g").selectAll("marker")
    .data(links)
  .enter().append("marker")
    .attr("id", function(d) {{ return d.source.index + '-' + d.target.index; }})
    .attr("viewBox", "0 -5 10 10")
    .attr("refX", function(d) {{
        if (d.target.name !== d.source.name) {{
            return 7 + d.target.degree; 
        }} else {{
            return 0;
        }}
    }})
    .attr("refY", calcMarkers)
    .attr("markerWidth", 10)
    .attr("markerHeight", 10)
    .attr("markerUnits", "userSpaceOnUse")
    .attr("orient", "auto");

defs.append("path")
    .attr("d", "M0,-5L10,0L0,5");

function calcMarkers(d) {{
    let dist = Math.sqrt((nodes[d.target.index].x - nodes[d.source.index].x) ** 2 + (nodes[d.target.index].y - nodes[d.source.index].y) ** 2);
    if (dist > 0 && dist <= 200){{
        return - Math.sqrt((0.5 - (d.target.degree ) / 2 / dist)) * (d.target.degree) / 2;
    }} else {{
        return 0;
    }}
}}

var path = svg.append("g").selectAll("path")
    .data(links)
  .enter().append("path")
    .attr("class", function(d) {{ return "link " + d.type; }})
    .attr("stroke-width", function(d) {{ return Math.max(d.weight * 20, 1); }})
    .attr("marker-end", function(d) {{ return "url(#" + d.source.index + '-' + d.target.index + ")"; }})
    .attr("id", function(d,i) {{ return "link_"+i; }})
    .attr("d", linkArc)
    ;

var edgetext = svg.append("g").selectAll("text")
    .data(links)
   .enter().append("text")
   .append("textPath")
    .attr("xlink:href",function(d,i){{return "#link_"+i;}})
    .style("text-anchor","middle")
    .attr("startOffset", "50%")
    ;
    
function update() {{
    d3.selectAll(".checkbox").each(function(d) {{
        cb = d3.select(this);
        if (cb.property("checked")) {{
            edgetext = edgetext.text(function(d) {{
                if ({show_percent}) {{
                    return Math.round(d.weight_text * 100) / 100;
                }} else {{
                    return Math.round(d.weight_text * 100) + "%";
                }}
            }})
        }} else {{
            edgetext = edgetext.text(function(d) {{ return ; }})
        }}
    }})
}};

d3.selectAll(".checkbox").on("change",update);

function dragstarted(d) {{
  d3.select(this).raise().classed("active", true);
}}

function dragged(d) {{
  d3.select(this).attr("cx", d.x = d3.event.x).attr("cy", d.y = d3.event.y);
}}

function dragended(d) {{
  d3.select(this).classed("active", false);
  path = path.attr("d", linkArc);
  text = text
        .attr('x', function(d) {{ return d.x; }})
        .attr('y', function(d) {{ return d.y; }})
        ;
  defs = defs.attr("refY", calcMarkers);
  defs.append("path")
    .attr("d", "M0,-5L10,0L0,5");
}};

var circle = svg.append("g").selectAll("circle")
    .data(nodes)
  .enter().append("circle")
    .attr("class", function(d) {{ return "circle " + d.type; }})
    .attr("r", function(d) {{ return d.degree; }})
    .attr('cx', function(d) {{ return d.x; }})
    .attr('cy', function(d) {{ return d.y; }})
    .call(d3.drag()
        .on("start", dragstarted)
        .on("drag", dragged)
        .on("end", dragended));

var text = svg.append("g").selectAll("text")
    .data(nodes)
  .enter().append("text")
    .attr('x', function(d) {{ return d.x; }})
    .attr('y', function(d) {{ return d.y; }})
    .text(function(d) {{ return d.name; }});

function linkArc(d) {{
  var dx = nodes[d.target.index].x - nodes[d.source.index].x,
      dy = nodes[d.target.index].y - nodes[d.source.index].y,
      dr = dx * dx + dy * dy;
      dr = Math.sqrt(dr);
      if (dr > 200) {{
        dr *= 5
      }} else {{
        dr /= 2
      }};
      if (dr > 0) {{return "M" + nodes[d.source.index].x + "," + nodes[d.source.index].y + "A" + (dr * 1.1) + "," + (dr * 1.1) + " 0 0,1 " + nodes[d.target.index].x + "," + nodes[d.target.index].y;}}
      else {{return "M" + nodes[d.source.index].x + "," + nodes[d.source.index].y + "A" + 20 + "," + 20 + " 0 1,0 " + (nodes[d.target.index].x + 0.1) + "," + (nodes[d.target.index].y + 0.1);}}
}}

function downloadLayout() {{
    var a = document.createElement("a");
    var file = new Blob([JSON.stringify(nodes)], {{type: "text/json;charset=utf-8"}});
    a.href = URL.createObjectURL(file);
    a.download = "node_params.json";
    a.click();
}}


</script>
"""


def _calc_layout(data, node_params, width=500, height=500, **kwargs):
    G = nx.DiGraph()
    G.add_weighted_edges_from(data.loc[:, ['source', 'target', 'weight']].values)
    if nx.algorithms.check_planarity(G)[0]:
        pos_new = nx.layout.planar_layout(G)
    else:
        pos_new = nx.layout.spring_layout(G, k=kwargs.get('k', .1),
                                          iterations=kwargs.get('iterations', 300),
                                          threshold=kwargs.get('nx_threshold', 1e-4),
                                          seed=0)
    min_x = min([j[0] for i, j in pos_new.items()])
    min_y = min([j[1] for i, j in pos_new.items()])
    max_x = max([j[0] for i, j in pos_new.items()])
    max_y = max([j[1] for i, j in pos_new.items()])
    pos_new = {
        i: [(j[0] - min_x) / (max_x - min_x) * (width - 150) + 75, (j[1] - min_y) / (max_y - min_y) * (height - 100) + 50]
        for i, j in pos_new.items()
    }
        # pos_new.update({i: [j[0] * width, j[1] * height] for i, j in pos.items()})
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
            "degree": (abs(degrees.get(node, 0)) + 1/120) / abs(max_degree) * 30
        }})
    return nodes


def _prepare_edges(data, nodes):
    edges = []
    data['weight_norm'] = data.weight / data.weight.abs().max()
    for idx, row in data.iterrows():
        edges.append({
            "source": nodes.get(row.source),
            "target": nodes.get(row.target),
            "weight": np.log1p(row.weight_norm) * 1.5,
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
            name = data.retention.retention_config.get(key)
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
        sns.mpl.pyplot.close()
        res = func(*args, **kwargs)
        if len(res) == 2:
            (vis_object, name), res = res, None
        else:
            vis_object, name, res = res
        idx = 'id: ' + str(int(datetime.now().timestamp()))
        coords = vis_object.axis()
        vis_object.text((coords[0] - (coords[1] - coords[0]) / 10), (coords[3] + (coords[3] - coords[2]) / 10), idx, fontsize=8)
        vis_object.get_figure().savefig(name, bbox_inches="tight", dpi=200)
        return res
    return save_plot_wrapper


@__save_plot__
def graph(data, node_params=None, thresh=.05, width=500, height=500, interactive=True,
          layout_dump=None, show_percent=True, plot_name=None, **kwargs):
    """
    Plots graph by its edgelist representation

    :param data: graph in edgelist form
    :param node_params: mapping describes which node should be highlighted by target or source type
            Node param should be represented in the following form
            ```{
                    'lost': 'bad_target',
                    'passed': 'nice_target',
                    'onboarding_welcome_screen': 'source',
                }```
            If mapping is not given, it will be constracted from config
    :param thresh: threshold for filtering low frequency edges
    :param width: width of plot
    :param height: height of plot
    :param interactive: if True, then opens graph visualization in Jupyter Notebook IFrame
    :param layout_dump: path to layout dump
    :param show_percent: if True, then all edge weights are converted to percents
    :param kwargs: do nothing, needs for plot.graph usage with other functions
    :return: saves to `experiments_folder` webpage with js graph visualization
    """
    if node_params is None:
        node_params = _prepare_node_params(node_params, data)
    res = _make_json_data(data, node_params, layout_dump, thresh=thresh,
                          width=width - 100, height=height - 100, **kwargs)
    x = __TEMPLATE__.format(
        width=width,
        height=height,
        links=json.dumps(res.get('links')).encode('latin1').decode('utf-8'),
        node_params=json.dumps(node_params).encode('latin1').decode('utf-8'),
        nodes=json.dumps(res.get('nodes')).encode('latin1').decode('utf-8'),
        show_percent="1 !== 1" if show_percent else "1 === 1"
    )
    if hasattr(data, 'trajectory'):
        if plot_name is None:
            plot_name = f'{data.trajectory.retention_config["experiments_folder"]}/index_{datetime.now()}'
    else:
        if plot_name is None:
            plot_name = 'index'
    plot_name = plot_name.replace(':', '_').replace('.', '_') + '.html'
    return ___DynamicFigureWrapper__(x, interactive, width, height), plot_name, plot_name


@__save_plot__
def step_matrix(diff, plot_name=None, title='', vmin=None, vmax=None, **kwargs):
    """
    Plots heatmap with distribution of events over event steps (ordering in the session by event time)

    :param diff: table for heatmap visualization
    :param plot_name: name of plot to save
    :param kwargs: do nothing, needs for plot usage with other functions
    :return: saves heatmap to `experiments_folder`
    """
    sns.mpl.pyplot.figure(figsize=(20, 10))
    heatmap = sns.heatmap(diff, annot=True, cmap="BrBG", center=0, vmin=vmin, vmax=vmax)
    heatmap.set_title(title)
    plot_name = 'desc_table_{}.png'.format(plot_name or datetime.now()).replace(':', '_').replace('.', '_')
    plot_name = diff.retention.retention_config['experiments_folder'] + '/' + plot_name
    return heatmap, plot_name


@__save_plot__
def core_event_dist(rates, thresh, plot_name=None, **kwargs):
    hist = sns.distplot(rates.values, hist=True, bins=kwargs.get('bins'), kde=kwargs.get('kde'))
    if thresh is not None:
        sns.mpl.pyplot.axvline(thresh, c='C1')

    plot_name = plot_name if plot_name is not None else 'clusters_heatmap_{}.svg'.format(
        datetime.now()).replace(':', '_').replace('.', '_')
    rates = rates.reset_index()
    plot_name = rates.retention.retention_config['experiments_folder'] + '/' + plot_name
    return hist, plot_name


@__save_plot__
def cluster_tsne(data, clusters, target, plot_name=None, **kwargs):
    """
    Plots TSNE projection of user stories and colors by founded clusters

    :param data: feature matrix
    :param clusters: np.array of cluster ids
    :param target: do nothing, need for compatibility with other cluster visualization methods
    :param plot_name: name of plot to save
    :param kwargs: do nothing, needs for plot usage with other functions
    :return: saves plot to `experiments_folder`
    """

    if hasattr(data.retention, '_tsne'):
        tsne2 = data.retention._tsne.copy()
    else:
        tsne2 = data.retention.learn_tsne(clusters, **kwargs)
    tsne = tsne2.values
    if np.unique(clusters).shape[0] > 10:
        f, ax = sns.mpl.pyplot.subplots()
        points = ax.scatter(tsne[:, 0], tsne[:, 1], c=clusters, cmap="BrBG")
        f.colorbar(points)
        scatter = ___FigureWrapper__(f)
    else:
        scatter = sns.scatterplot(tsne[:, 0], tsne[:, 1], hue=clusters, legend='full', palette="BrBG")
    plot_name = plot_name if plot_name is not None else 'clusters_tsne_{}.svg'.format(
        datetime.now()).replace(':', '_').replace('.', '_')
    plot_name = data.retention.retention_config['experiments_folder'] + '/' + plot_name
    return scatter, plot_name, tsne2


@__save_plot__
def cluster_bar(data, clusters, target, plot_name=None, plot_cnt=None, metrics=None, **kwargs):
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

    plot_name = plot_name if plot_name is not None else 'clusters_bar_{}.svg'.format(
        datetime.now()).replace(':', '_').replace('.', '_')
    plot_name = data.retention.retention_config['experiments_folder'] + '/' + plot_name
    return bar, plot_name


@__save_plot__
def cluster_event_dist(bars, event_col, cl1, sizes, crs, cl2=None, plot_name=None):
    bar = sns.barplot(x=event_col, y='freq', hue='hue',
                      hue_order=[f'cluster {cl1}','all' if cl2 is None else f'cluster {cl2}'], data=bars)
    y_value = ['{:,.2f}'.format(x * 100) + '%' for x in bar.get_yticks()]
    bar.set_yticklabels(y_value)
    bar.set_xticklabels(bar.get_xticklabels(), rotation=30)
    bar.set(ylabel=None)
    tit = f'Distribution of top {bars.shape[0] // 2} events in cluster {cl1} (size: {round(sizes[0] * 100, 2)}%, CR: {round(crs[0] * 100, 2)}% ) '
    tit += f'vs. all data (CR: {round(crs[1] * 100, 2)}%)' if cl2 is None else f'vs. cluster {cl2} (size: {round(sizes[1] * 100, 2)}%, CR: {round(crs[1] * 100, 2)}%)'
    bar.set_title(tit)

    plot_name = plot_name if plot_name is not None else 'clusters_event_dist_{}.svg'.format(
        datetime.now()).replace(':', '_').replace('.', '_')
    plot_name = bars.retention.retention_config['experiments_folder'] + '/' + plot_name
    return bar, plot_name


@__save_plot__
def cluster_pie(data, clusters, target, plot_name=None, plot_cnt=None, metrics=None, **kwargs):
    """
    Plots pie-charts of target distribution for different clusters

    :param data: feature matrix
    :param clusters: np.array of cluster ids
    :param target: boolean vector, if True, then user have `positive_target_event` in trajectory
    :param plot_name: name of plot to save
    :param plot_cnt: number of clusters to plot
    :param kwargs: width and height of plot
    :return: saves plot to `experiments_folder`
    """
    cl = pd.DataFrame([clusters, target], index=['clusters', 'target']).T
    cl.target = np.where(cl.target, data.retention.retention_config['positive_target_event'],
                         data.retention.retention_config['negative_target_event'])
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
    fig.suptitle('Distribution of targets in clusters. Silhouette: {:.2f}, Homogeneity: {:.2f}, Cluster stability: {:.2f}'.format(
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

    plot_name = plot_name if plot_name is not None else 'clusters_pie_{}.svg'.format(
        datetime.now()).replace(':', '_').replace('.', '_')
    plot_name = data.retention.retention_config['experiments_folder'] + '/' + plot_name
    return ___FigureWrapper__(fig), plot_name


@__save_plot__
def cluster_heatmap(data, clusters, target, plot_name=None, **kwargs):
    """
    Visualizes features for users with heatmap

    :param data: feature matrix
    :param clusters: do nothing, need for compatibility with other cluster visualization methods
    :param target: do nothing, need for compatibility with other cluster visualization methods
    :param plot_name: name of plot to save
    :param kwargs: do nothing, need for compatibility with other cluster visualization methods
    :return: saves plot to `experiments_folder`
    """
    heatmap = sns.clustermap(data.values,
                             cmap="BrBG",
                             xticklabels=data.columns,
                             yticklabels=False,
                             row_cluster=True,
                             col_cluster=False)

    heatmap.ax_row_dendrogram.set_visible(False)
    heatmap = heatmap.ax_heatmap

    plot_name = plot_name if plot_name is not None else 'clusters_heatmap_{}.svg'.format(
        datetime.now()).replace(':', '_').replace('.', '_')
    plot_name = data.retention.retention_config['experiments_folder'] + '/' + plot_name
    return heatmap, plot_name


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
        return (x[0]/ 64, x[0] + (x[1] - x[0]) / 50, x[2] / 1.5, x[3] / 1.5)

    def text(self, *args, **kwargs):
        self.fig.text(*args, **kwargs)


class __SaveFigWrapper__(object):
    def __init__(self, data, interactive=True, width=1000, height=700):
        self.data = data
        self.interactive = interactive
        self.width = width
        self.height = height

    def savefig(self, name, **kwargs):
        with open(name, 'w') as f:
            f.write(self.data)
        if self.interactive:
            display(IFrame(name, width=self.width + 200, height=self.height + 200))


class ___DynamicFigureWrapper__(object):
    def __init__(self, fig, interactive, width, height):
        self.fig = fig
        self.interactive, self.width, self.height = interactive, width, height

    def get_figure(self):
        savefig = __SaveFigWrapper__(self.fig, self.interactive, self.width, self.height)
        return savefig

    def text(self, x, y, text, *args, **kwargs):
        parts = self.fig.split('<body>')
        res = parts[:1] + [f'<p>{text}</p>'] + parts[1:]
        self.fig = '\n'.join(res)

    def axis(self):
        return 4 * [0]
