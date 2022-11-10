from __future__ import annotations

from typing import Any, MutableMapping, Sequence

import pandas as pd

from backend import ServerManager
from eventstream.types import EventstreamType

from .typing import AllowedColors, Edge, Node, PlotParamsType, TransitionGraphProtocol, PreparedNode

Threshold = MutableMapping[str, float]
NodeParams = MutableMapping[str, str]
Position = MutableMapping[str, Sequence[float]]


class PlotParams(PlotParamsType):
    show_weights: bool
    show_percents: bool
    show_nodes_names: bool
    show_all_edges_for_targets: bool
    show_nodes_without_links: bool
    nodes_threshold: Threshold
    links_threshold: Threshold


class TransitionGraph(TransitionGraphProtocol):

    def show_graph(self) -> Any:
        ...

    def __init__(
        self,
        eventstream: EventstreamType,
        # graph: dict,  # preprocessed graph
        plot_params: PlotParams,
        nodes: list[Node],
        edges: list[Edge],
    ) -> None:
        sm = ServerManager()
        self.env = sm.check_env()
        self.server = sm.create_server()
        self.eventstream = eventstream

        self.spring_layout_config = {"k": 0.1, "iterations": 300, "nx_threshold": 1e-4}

        self.layout: pd.DataFrame = eventstream.to_dataframe()
        self.plot_params = plot_params
        self.nodes = nodes
        self.edges = edges

    def _replace_grouped_events(self, grouped: pd.Series[Any], row):
        event_col = self.eventstream.schema.event_name
        event_name = row[event_col]
        mathced = grouped[grouped[event_col] == event_name]

        if len(mathced) > 0:
            parent_node_name = mathced.iloc[0]["parent"]
            row[event_col] = parent_node_name

        return row

    def _make_node_params(self, targets: MutableMapping[str, str] = None):
        if targets is not None:
            return targets
        else:
            node_params: NodeParams = {}
            if self.config.positive_target_event is not None:
                node_params[self.config.positive_target_event] = "nice"
            if self.config.negative_target_event is not None:
                node_params[self.config.negative_target_event] = "bad"
            if self.config.source_event is not None:
                node_params[self.config.source_event] = "source"

            return node_params

    def _get_norm_link_threshold(self, links_threshold: Threshold = None):
        nodelist_default_col = self.config.get_nodelist_default_col()
        edgelist_default_col = self.config.get_edgelist_default_col()
        scale = float(cast(float, self.edgelist[edgelist_default_col].abs().max()))
        norm_links_threshold = None

        if links_threshold is not None:
            norm_links_threshold = {}
            for key in links_threshold:
                if key == nodelist_default_col:
                    norm_links_threshold[nodelist_default_col] = links_threshold[nodelist_default_col] / scale
                else:
                    s = float(cast(float, self.edgelist[key].abs().max()))
                    norm_links_threshold[key] = links_threshold[key] / s
        return norm_links_threshold

    def _get_norm_node_threshold(self, nodes_threshold: Threshold = None):
        norm_nodes_threshold = None
        if nodes_threshold is not None:
            norm_nodes_threshold = {}
            for key in nodes_threshold:
                scale = float(cast(float, self.nodelist[key].abs().max()))
                norm_nodes_threshold[key] = nodes_threshold[key] / scale

        return norm_nodes_threshold

    def _calc_layout(self, edgelist: pd.DataFrame, width: int, height: int):
        G = nx.DiGraph()
        source_col = edgelist.columns[0]
        target_col = edgelist.columns[1]
        weight_col = edgelist.columns[2]

        G.add_weighted_edges_from(edgelist.loc[:, [source_col, target_col, weight_col]].values)

        pos_new = nx.layout.spring_layout(
            G,
            k=self.spring_layout_config["k"],
            iterations=self.spring_layout_config["iterations"],
            threshold=self.spring_layout_config["nx_threshold"],
            seed=0,
        )

        all_x_coords: list[float] = []
        all_y_coords: list[float] = []

        for j in pos_new.values():
            all_x_coords.append(j[0])
            all_y_coords.append(j[1])

        min_x = min(all_x_coords)
        min_y = min(all_y_coords)
        max_x = max(all_x_coords)
        max_y = max(all_y_coords)

        pos_new: Position = {
            i: [
                (j[0] - min_x) / (max_x - min_x) * (width - 150) + 75,
                (j[1] - min_y) / (max_y - min_y) * (height - 100) + 50,
            ]
            for i, j in pos_new.items()
        }
        return pos_new

    def _prepare_nodes(self, nodelist: pd.DataFrame, node_params: NodeParams = None, pos: Position = None):
        event_col = self.config.event_col
        node_names = set(nodelist[event_col])

        cols = self.config.get_nodelist_cols()

        nodes_set: MutableMapping[str, PreparedNode] = {}
        for idx, node_name in enumerate(node_names):
            row = nodelist.loc[nodelist[event_col] == node_name]
            degree = {}
            for weight_col in cols:
                max_degree = cast(float, nodelist[weight_col].max())
                r = row[weight_col]
                r = r.tolist()
                value = r[0]
                curr_degree = {}
                curr_degree["degree"] = (abs(value)) / abs(max_degree) * 30 + 4
                curr_degree["source"] = value
                degree[weight_col] = curr_degree

            node_pos = pos.get(node_name) if pos is not None else None
            active = cast(bool, row["active"].tolist()[0])
            alias = cast(str, row["alias"].to_list()[0])
            parent = cast(str, row["parent"].to_list()[0])

            type = node_params.get(node_name) or "suit" if node_params is not None else "suit"

            node: PreparedNode = {
                "index": idx,
                "name": node_name,
                "degree": degree,
                "type": type + "_node",
                "active": active,
                "alias": alias,
                "parent": parent,
                "changed_name": None,
                "x": None,
                "y": None,
            }

            if node_pos is not None:
                node["x"] = node_pos[0]
                node["y"] = node_pos[1]

            nodes_set.update({node_name: node})

        return list(nodes_set.values()), nodes_set

    def _prepare_edges(self, edgelist: pd.DataFrame, nodes_set: MutableMapping[str, PreparedNode]):
        default_col = self.config.get_nodelist_default_col()
        weight_col = edgelist.columns[2]
        source_col = edgelist.columns[0]
        target_col = edgelist.columns[1]
        custom_cols = self.config.get_custom_cols()
        edges: MutableSequence[PreparedLink] = []

        edgelist["weight_norm"] = edgelist[weight_col] / edgelist[weight_col].abs().max()

        for _, row in edgelist.iterrows():
            default_col_weight: Weight = {
                "weight_norm": row.weight_norm,
                "weight": cast(float, row[weight_col]),
            }
            weights = {
                default_col: default_col_weight,
            }
            for custom_weight_col in custom_cols:
                weight = cast(float, row[custom_weight_col])
                max_weight = cast(float, edgelist[custom_weight_col].abs().max())
                weight_norm = weight / max_weight
                col_weight: Weight = {
                    "weight_norm": weight_norm,
                    "weight": cast(float, row[custom_weight_col]),
                }
                weights[custom_weight_col] = col_weight

            source_node_name = cast(str, row[source_col])
            target_node_name = cast(str, row[target_col])

            source_node = nodes_set.get(source_node_name)
            target_node = nodes_set.get(target_node_name)

            if source_node is not None:
                if target_node is not None:
                    edges.append(
                        {
                            "sourceIndex": source_node["index"],
                            "targetIndex": target_node["index"],
                            "weights": weights,
                            "type": cast(str, row["type"]),
                        }
                    )

        return edges

    def _make_template_data(self, node_params: NodeParams, width: int, height: int):
        edgelist = self.edgelist.copy()
        nodelist = self.nodelist.copy()

        source_col = edgelist.columns[0]
        target_col = edgelist.columns[1]

        # calc edge type
        edgelist["type"] = edgelist.apply(
            lambda x: node_params.get(x[source_col])
            if node_params.get(x[source_col]) == "source"
            else node_params.get(x[target_col]) or "suit",
            1,
        )

        pos = self._use_layout(self._calc_layout(edgelist=edgelist, width=width, height=height))

        nodes, nodes_set = self._prepare_nodes(nodelist=nodelist, pos=pos, node_params=node_params)

        links = self._prepare_edges(edgelist=edgelist, nodes_set=nodes_set)

        return nodes, links

    def _use_layout(self, position: Position):
        if self.layout is None:
            return position
        for node_name in position:
            matched = self.layout[self.layout["name"] == node_name]
            if not matched.empty:
                x = cast(float, matched["x"].item())
                y = cast(float, matched["y"].item())
                position[node_name] = [x, y]

        return position

    def _to_json(self, data):
        return json.dumps(data).encode("latin1").decode("utf-8")

    def _apply_settings(
        self,
        show_weights: bool = None,
        show_percents: bool = None,
        show_nodes_names: bool = None,
        show_all_edges_for_targets: bool = None,
        show_nodes_without_links: bool = None,
    ):
        settings = {
            "show_weights": show_weights,
            "show_percents": show_percents,
            "show_nodes_names": show_nodes_names,
            "show_all_edges_for_targets": show_all_edges_for_targets,
            "show_nodes_without_links": show_nodes_without_links,
        }
        merged = {**self.graph_settings, **clear_dict(settings)}
        return cast(GraphSettings, merged)

    def plot_graph(
        self,
        targets: MutableMapping[str, str] = None,
        width: int = 960,
        height: int = 900,
        weight_template: str = None,
        show_weights: bool = None,
        show_percents: bool = None,
        show_nodes_names: bool = None,
        show_all_edges_for_targets: bool = None,
        show_nodes_without_links: bool = None,
        nodes_threshold: Threshold = None,
        links_threshold: Threshold = None,
    ):

        settings = self._apply_settings(
            show_weights=show_weights,
            show_percents=show_percents,
            show_nodes_names=show_nodes_names,
            show_all_edges_for_targets=show_all_edges_for_targets,
            show_nodes_without_links=show_nodes_without_links,
        )

        node_params = self._make_node_params(targets)

        norm_nodes_threshold = (
            settings["nodes_threshold"]
            if "nodes_threshold" in settings
            else self._get_norm_node_threshold(nodes_threshold)
        )
        norm_links_threshold = (
            settings["links_threshold"]
            if "links_threshold" in settings
            else self._get_norm_link_threshold(links_threshold)
        )
        cols = self.config.get_nodelist_cols()

        nodes, links = self._make_template_data(
            node_params=node_params,
            width=width,
            height=height,
        )

        def to_js_val(val=None):
            return self._to_json(val) if val is not None else "undefined"

        def get_option(name: str):
            if name in settings:
                return self._to_json(settings[name])
            return "undefined"

        init_graph_js = templates.__INIT_GRAPH__.format(
            server_id="'" + self.server.id + "'",
            env="'" + self.env + "'",
            links=self._to_json(links),
            node_params=self._to_json(node_params),
            nodes=self._to_json(nodes),
            layout_dump=1 if self.layout is not None else 0,
            links_weights_names=cols,
            node_cols_names=cols,
            show_weights=get_option("show_weights"),
            show_percents=get_option("show_percents"),
            show_nodes_names=get_option("show_nodes_names"),
            show_all_edges_for_targets=get_option("show_all_edges_for_targets"),
            show_nodes_without_links=get_option("show_nodes_without_links"),
            nodes_threshold=to_js_val(norm_nodes_threshold),
            links_threshold=to_js_val(norm_links_threshold),
            weight_template="'" + weight_template + "'" if weight_template is not None else "undefined",
        )

        graph_styles = templates.__GRAPH_STYLES__.format()
        graph_body = templates.__GRAPH_BODY__.format()

        graph_script_src = "https://static.server.retentioneering.com/viztools/graph/rete-graph.js"

        init_graph_template = templates.__INIT_GRAPH__.format(
            server_id="'" + self.server.id + "'",
            env="'" + self.env + "'",
            node_params=self._to_json(node_params),
            links="<%= links %>",
            nodes="<%= nodes %>",
            layout_dump=1,
            links_weights_names=cols,
            node_cols_names=cols,
            show_weights="<%= show_weights %>",
            show_percents="<%= show_percents %>",
            show_nodes_names="<%= show_nodes_names %>",
            show_all_edges_for_targets="<%= show_all_edges_for_targets %>",
            show_nodes_without_links="<%= show_nodes_without_links %>",
            nodes_threshold="<%= nodes_threshold %>",
            links_threshold="<%= links_threshold %>",
            weight_template="undefined",
        )

        html_template = templates.__FULL_HTML__.format(
            content=templates.__RENDER_INNER_IFRAME__.format(
                id=generateId(),
                width=width,
                height=height,
                graph_body=graph_body,
                graph_styles=graph_styles,
                graph_script_src=graph_script_src,
                init_graph_js=init_graph_template,
                template="",
            ),
        )

        html = templates.__RENDER_INNER_IFRAME__.format(
            id=generateId(),
            width=width,
            height=height,
            graph_body=graph_body,
            graph_styles=graph_styles,
            graph_script_src=graph_script_src,
            init_graph_js=init_graph_js,
            template=html_template,
        )

        full_html_page = templates.__FULL_HTML__.format(
            content=html,
        )

        display(HTML(html))
