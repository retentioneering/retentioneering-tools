from __future__ import annotations

import json
from typing import Any, MutableMapping, MutableSequence, cast

import networkx as nx
import pandas as pd
from IPython.core.display import HTML
from IPython.core.display_functions import display

from backend import ServerManager
from eventstream.types import EventstreamType
from src.templates.translition_graph import TransitionGraphRenderer

from .typing import (
    Edge,
    GraphSettings,
    LayoutNode,
    Node,
    NodeParams,
    Position,
    PreparedLink,
    PreparedNode,
    Threshold,
    Weight,
)


def clear_dict(d: dict):
    for k, v in dict(d).items():
        if v is None:
            del d[k]
    return d


class TransitionGraph:
    def __init__(
        self,
        eventstream: EventstreamType,  # graph: dict,  # preprocessed graph
        graph_settings: GraphSettings,
        nodes: list[Node],
        edges: list[Edge],  # add to config
        positive_target_event: str | None = None,
        negative_target_event: str | None = None,
        source_event: str | None = None,
        edgelist_default_col: str = "edge_weight",
        nodelist_default_col: str = "number_of_events",
    ) -> None:
        sm = ServerManager()
        self.env = sm.check_env()
        self.server = sm.create_server()

        self.server.register_action("save-nodelist", lambda n: self._on_nodelist_updated(n))
        self.server.register_action("save-layout", lambda n: self._on_layout_request(n))
        self.server.register_action("save-graph-settings", lambda n: self._on_graph_settings_request(n))

        self.eventstream = eventstream

        self.spring_layout_config = {"k": 0.1, "iterations": 300, "nx_threshold": 1e-4}

        self.layout: pd.DataFrame = eventstream.to_dataframe()
        self.graph_settings = graph_settings
        self.nodes = nodes
        self.edges = edges

        self.event_col = self.eventstream.schema.event_name
        self.event_time_col = self.eventstream.schema.event_timestamp
        self.user_col = self.eventstream.schema.user_id
        self.id_col = self.eventstream.schema.event_id

        self.positive_target_event = positive_target_event
        self.negative_target_event = negative_target_event
        self.source_event = source_event
        self.nodelist_default_col = nodelist_default_col
        self.edgelist_default_col = edgelist_default_col
        self.edgelist = self.__get_edgelist(norm_type="full")
        self.nodelist = pd.DataFrame()

        self.render: TransitionGraphRenderer = TransitionGraphRenderer()

    def _on_graph_settings_request(self, settings: GraphSettings):
        self.graph_settings = settings

    def _on_layout_request(self, layout_nodes: MutableSequence[LayoutNode]):
        self.graph_updates = layout_nodes
        self.layout = pd.DataFrame(layout_nodes)

    def _on_nodelist_updated(self, nodes: MutableSequence[PreparedNode]):
        self.updates = nodes
        # prepare data, map cols
        mapped_nodes = []
        for i, n in enumerate(nodes):
            source_node = cast(dict, n)
            mapped_node = {}
            for key, source_value in source_node.items():
                if key == "degree":
                    for col_name, deg in source_value.items():
                        mapped_node[col_name] = deg["source"]
                    continue
                if key == "name":
                    mapped_node[self.event_col] = source_value
                    continue
                if key == "index":
                    mapped_node["index"] = source_value
                    continue
                # filter fields
                if key not in self.nodelist.columns:
                    continue
                mapped_node[key] = source_value
            mapped_nodes.append(mapped_node)

        self.nodelist = pd.DataFrame(data=mapped_nodes)
        self.nodelist.set_index("index")
        self.nodelist = self.nodelist.drop(columns=["index"])

    def _get_shift(self) -> pd.DataFrame:

        data = self.eventstream.to_dataframe().copy()
        data.sort_values([self.user_col, self.id_col], inplace=True)
        shift = data.groupby(self.user_col).shift(-1)

        data["next_" + self.event_col] = shift[self.event_col]
        data["next_" + str(self.id_col)] = shift[self.id_col]

        return data

    def __get_edgelist(self, weight_col=None, norm_type=None, edge_attributes="edge_weight") -> pd.DataFrame:
        """
        Creates weighted table of the transitions between events.

        Parameters
        ----------
        weight_col: str (optional, default=None)
            Aggregation column for transitions weighting. To calculate weights
            as number of transion events use None. To calculate number
            of unique users passed through given transition 'user_id'.
             For any other aggreagtion, like number of sessions, pass the column name.

        norm_type: {None, 'full', 'node'} (optional, default=None)
            Type of normalization. If None return raw number of transtions
            or other selected aggregation column. 'full' - normalized over
            entire dataset. 'node' weight for edge A --> B normalized over
            user in A

        edge_attributes: str (optional, default 'edge_weight')
            Name for edge_weight columns

        Returns
        -------
        Dataframe with number of rows equal to all transitions with weight
        non-zero weight

        Return type
        -----------
        pd.DataFrame
        """
        if norm_type not in [None, "full", "node"]:
            raise ValueError(f"unknown normalization type: {norm_type}")

        cols = [self.event_col, "next_" + str(self.event_col)]

        data = self._get_shift().copy()

        # get aggregation:
        if weight_col is None:
            agg = data.groupby(cols)[self.event_time_col].count().reset_index()
            agg.rename(columns={self.event_time_col: edge_attributes}, inplace=True)
        else:
            agg = data.groupby(cols)[weight_col].nunique().reset_index()
            agg.rename(columns={weight_col: edge_attributes}, inplace=True)

        # apply normalization:
        if norm_type == "full":
            if weight_col is None:
                agg[edge_attributes] /= agg[edge_attributes].sum()
            else:
                agg[edge_attributes] /= data[weight_col].nunique()

        if norm_type == "node":
            if weight_col is None:
                event_transitions_counter = data.groupby(self.event_col)[cols[1]].count().to_dict()
                agg[edge_attributes] /= agg[cols[0]].map(event_transitions_counter)
            else:
                user_counter = data.groupby(cols[0])[weight_col].nunique().to_dict()
                agg[edge_attributes] /= agg[cols[0]].map(user_counter)

        return agg

    def _replace_grouped_events(self, grouped: pd.Series[Any], row):
        event_name = row[self.event_col]
        mathced = grouped[grouped[self.event_col] == event_name]

        if len(mathced) > 0:
            parent_node_name = mathced.iloc[0]["parent"]
            row[self.event_col] = parent_node_name

        return row

    def _make_node_params(self, targets: MutableMapping[str, str] | None = None):
        if targets is not None:
            return targets
        else:
            node_params: NodeParams = {}
            if self.positive_target_event is not None:
                node_params[self.positive_target_event] = "nice"
            if self.negative_target_event is not None:
                node_params[self.negative_target_event] = "bad"
            if self.source_event is not None:
                node_params[self.source_event] = "source"

            return node_params

    def _get_norm_link_threshold(self, links_threshold: Threshold | None = None):
        nodelist_default_col = self.nodelist_default_col
        edgelist_default_col = self.edgelist_default_col
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

    def _get_norm_node_threshold(self, nodes_threshold: Threshold | None = None):
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

        pos = nx.layout.spring_layout(
            G,
            k=self.spring_layout_config["k"],
            iterations=self.spring_layout_config["iterations"],
            threshold=self.spring_layout_config["nx_threshold"],
            seed=0,
        )

        all_x_coords: list[float] = []
        all_y_coords: list[float] = []

        for j in pos.values():
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
            for i, j in pos.items()
        }
        return pos_new

    def __get_nodelist_cols(self):
        default_col = self.nodelist_default_col
        custom_cols = self.eventstream.schema.custom_cols
        return list([default_col]) + list(custom_cols)

    def _prepare_nodes(
        self, nodelist: pd.DataFrame, node_params: NodeParams | None = None, pos: Position | None = None
    ) -> tuple[list, MutableMapping]:
        node_names = set(nodelist[self.event_col])

        cols = self.__get_nodelist_cols()

        nodes_set: MutableMapping[str, PreparedNode] = {}
        for idx, node_name in enumerate(node_names):
            row = nodelist.loc[nodelist[self.event_col] == node_name]
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

    def _prepare_edges(
        self, edgelist: pd.DataFrame, nodes_set: MutableMapping[str, PreparedNode]
    ) -> MutableSequence[PreparedLink]:
        default_col = self.nodelist_default_col
        source_col = edgelist.columns[0]
        target_col = edgelist.columns[1]
        weight_col = edgelist.columns[2]
        custom_cols: list[str] = self.eventstream.schema.custom_cols
        edges: MutableSequence[PreparedLink] = []

        edgelist["weight_norm"] = edgelist[weight_col] / edgelist[weight_col].abs().max()

        for _, row in edgelist.iterrows():
            default_col_weight: Weight = {
                "weight_norm": row.weight_norm,
                "weight": cast(float, row[weight_col]),  # type: ignore
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

            source_node_name = str(row[source_col])  # type: ignore
            target_node_name = str(row[target_col])  # type: ignore

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

    def _make_template_data(
        self, node_params: NodeParams, width: int, height: int
    ) -> tuple[MutableSequence, MutableSequence]:
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

    def _use_layout(self, position: Position) -> Position:
        if self.layout is None:
            return position
        for node_name in position:
            matched = self.layout[self.layout["name"] == node_name]
            if not matched.empty:
                x = cast(float, matched["x"].item())
                y = cast(float, matched["y"].item())
                position[node_name] = [x, y]

        return position

    def _to_json(self, data) -> str:
        return json.dumps(data).encode("latin1").decode("utf-8")

    def _apply_settings(
        self,
        show_weights: bool | None = None,
        show_percents: bool | None = None,
        show_nodes_names: bool | None = None,
        show_all_edges_for_targets: bool | None = None,
        show_nodes_without_links: bool | None = None,
    ) -> dict[str, Any]:
        settings = {
            "show_weights": show_weights,
            "show_percents": show_percents,
            "show_nodes_names": show_nodes_names,
            "show_all_edges_for_targets": show_all_edges_for_targets,
            "show_nodes_without_links": show_nodes_without_links,
        }
        merged = {**self.graph_settings, **clear_dict(settings)}

        return clear_dict(merged)

    def _to_js_val(self, val=None) -> str:
        return self._to_json(val) if val is not None else "undefined"

    def _get_option(self, name: str, settings: dict[str, Any]) -> str:
        if name in settings:
            return self._to_json(settings[name])
        return "undefined"

    def plot_graph(
        self,
        nodes_threshold: Threshold,
        links_threshold: Threshold,
        targets: MutableMapping[str, str],
        width: int = 960,
        height: int = 900,
        weight_template: str | None = None,
        show_weights: bool | None = None,
        show_percents: bool | None = None,
        show_nodes_names: bool | None = None,
        show_all_edges_for_targets: bool | None = None,
        show_nodes_without_links: bool | None = None,
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
        cols = self.__get_nodelist_cols()

        nodes, links = self._make_template_data(
            node_params=node_params,
            width=width,
            height=height,
        )

        init_graph_js = self.render.init(
            **dict(
                server_id="'" + self.server.pk + "'",
                env="'" + self.env + "'",
                links=self._to_json(links),
                node_params=self._to_json(node_params),
                nodes=self._to_json(nodes),
                layout_dump=1 if self.layout is not None else 0,
                links_weights_names=cols,
                node_cols_names=cols,
                show_weights=self._get_option("show_weights", settings),
                show_percents=self._get_option("show_percents", settings),
                show_nodes_names=self._get_option("show_nodes_names", settings),
                show_all_edges_for_targets=self._get_option("show_all_edges_for_targets", settings),
                show_nodes_without_links=self._get_option("show_nodes_without_links", settings),
                nodes_threshold=self._to_js_val(norm_nodes_threshold),
                links_threshold=self._to_js_val(norm_links_threshold),
                weight_template="'" + weight_template + "'" if weight_template is not None else "undefined",
            )
        )

        graph_styles = self.render.graph_stype()
        graph_body = self.render.body()

        graph_script_src = "https://static.server.retentioneering.com/viztools/graph/rete-graph.js"

        init_graph_template = self.render.init(
            **dict(
                server_id="'" + self.server.pk + "'",
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
        )

        html_template = self.render.full(
            **dict(
                content=self.render.inner_iframe(
                    **dict(
                        id=self.server.pk,
                        width=width,
                        height=height,
                        graph_body=graph_body,
                        graph_styles=graph_styles,
                        graph_script_src=graph_script_src,
                        init_graph_js=init_graph_template,
                        template="",
                    )
                ),
            )
        )

        html = self.render.inner_iframe(
            **dict(
                id=self.server.pk,
                width=width,
                height=height,
                graph_body=graph_body,
                graph_styles=graph_styles,
                graph_script_src=graph_script_src,
                init_graph_js=init_graph_js,
                template=html_template,
            )
        )

        display(HTML(html))
