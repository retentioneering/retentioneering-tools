from __future__ import annotations

import json
import random
import string
from typing import Any, Dict, List, MutableMapping, MutableSequence, cast

import networkx as nx
import pandas as pd
from IPython.display import HTML, display

from src.backend import ServerManager
from src.edgelist import Edgelist
from src.eventstream.types import EventstreamType
from src.nodelist import Nodelist
from src.templates.transition_graph import TransitionGraphRenderer

from .typing import (
    GraphSettings,
    LayoutNode,
    NodeParams,
    NormType,
    Position,
    PreparedLink,
    PreparedNode,
    Threshold,
    Weight,
)

RenameRule = Dict[str, List[str]]


def clear_dict(d: dict) -> dict:
    for k, v in dict(d).items():
        if v is None:
            del d[k]
    return d


class TransitionGraph:
    def __init__(
        self,
        eventstream: EventstreamType,  # graph: dict,  # preprocessed graph
        graph_settings: GraphSettings,
        norm_type: NormType = None,
        weights: MutableMapping[str, str] | None = None,
        targets: MutableMapping[str, str | None] | None = None,
        thresholds: dict[str, Threshold] | None = None,
    ) -> None:
        from src.eventstream.eventstream import Eventstream

        self.weights = weights if weights else {"edges": "edge_weight", "nodes": "number_of_events"}
        self.targets = targets if targets else {"positive": None, "negative": None, "source": None}
        self.thresholds = (
            thresholds if thresholds else {"edges": {"count_of_events": 0.03}, "nodes": {"count_of_events": 0.03}}
        )
        sm = ServerManager()
        self.env = sm.check_env()
        self.server = sm.create_server()

        self.server.register_action("save-nodelist", lambda n: self._on_nodelist_updated(n))
        self.server.register_action("save-layout", lambda n: self._on_layout_request(n))
        self.server.register_action("save-graph-settings", lambda n: self._on_graph_settings_request(n))

        self.eventstream: Eventstream = eventstream  # type: ignore

        self.spring_layout_config = {"k": 0.1, "iterations": 300, "nx_threshold": 1e-4}

        self.layout: pd.DataFrame | None = None
        self.graph_settings = graph_settings

        self.event_col = self.eventstream.schema.event_name
        self.event_time_col = self.eventstream.schema.event_timestamp
        self.user_col = self.eventstream.schema.user_id
        self.id_col = self.eventstream.schema.event_id
        self.custom_cols = self.eventstream.schema.custom_cols

        self.nodelist_default_col = self.weights["nodes"]
        self.norm_type: NormType | None = norm_type

        self.nodelist: Nodelist = Nodelist(
            nodelist_default_col=self.nodelist_default_col,
            custom_cols=self.custom_cols,
            time_col=self.event_time_col,
            event_col=self.event_col,
        )

        self.nodelist.calculate_nodelist(data=self.eventstream.to_dataframe())

        self.edgelist_default_col = self.weights["edges"]
        self.edgelist: Edgelist = Edgelist(
            event_col=self.event_col,
            time_col=self.event_time_col,
            default_weight_col=self.edgelist_default_col,
            nodelist=self.nodelist.nodelist_df,
            index_col=self.user_col,
        )
        self.edgelist.calculate_edgelist(
            norm_type=self.norm_type, custom_cols=self.custom_cols, data=self.eventstream.to_dataframe()
        )

        self.render: TransitionGraphRenderer = TransitionGraphRenderer()

    def _on_graph_settings_request(self, settings: GraphSettings) -> None:
        self.graph_settings = settings

    def _on_layout_request(self, layout_nodes: MutableSequence[LayoutNode]) -> None:
        self.graph_updates = layout_nodes
        self.layout = pd.DataFrame(layout_nodes)

    def _on_nodelist_updated(self, nodes: MutableSequence[PreparedNode]) -> None:
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
                if key not in self.nodelist.nodelist_df.columns:
                    continue
                mapped_node[key] = source_value
            mapped_nodes.append(mapped_node)

        self.nodelist.nodelist_df = pd.DataFrame(data=mapped_nodes)
        self.nodelist.nodelist_df.set_index("index")
        self.nodelist.nodelist_df = self.nodelist.nodelist_df.drop(columns=["index"])

    def _make_node_params(
        self, targets: MutableMapping[str, str | None] | None = None
    ) -> MutableMapping[str, str | None] | dict[str, str | None]:
        if targets is not None:
            return targets
        else:
            return self.targets  # type: ignore

    def _get_norm_link_threshold(self, links_threshold: Threshold | None = None) -> dict[str, float] | None:
        nodelist_default_col = self.nodelist_default_col
        edgelist_default_col = self.edgelist_default_col
        scale = float(cast(float, self.edgelist.edgelist_df[edgelist_default_col].abs().max()))
        norm_links_threshold = None

        if links_threshold is not None:
            norm_links_threshold = {}
            for key in links_threshold:
                if key == nodelist_default_col:
                    norm_links_threshold[nodelist_default_col] = links_threshold[nodelist_default_col] / scale
                else:
                    s = float(cast(float, self.edgelist.edgelist_df[key].abs().max()))
                    norm_links_threshold[key] = links_threshold[key] / s
        return norm_links_threshold

    def _get_norm_node_threshold(self, nodes_threshold: Threshold | None = None) -> Threshold | None:
        norm_nodes_threshold = None
        if nodes_threshold is not None:
            norm_nodes_threshold = {}
            for key in nodes_threshold:
                scale = float(cast(float, self.nodelist.nodelist_df[key].abs().max()))
                norm_nodes_threshold[key] = nodes_threshold[key] / scale

        return norm_nodes_threshold

    def _calc_layout(self, edgelist: pd.DataFrame, width: int, height: int) -> Position:
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

    def __get_nodelist_cols(self) -> list[str]:
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
        edgelist = self.edgelist.edgelist_df.copy()
        nodelist = self.nodelist.nodelist_df.copy()

        source_col = edgelist.columns[0]
        target_col = edgelist.columns[1]

        # calc edge type
        edgelist["type"] = edgelist.apply(
            lambda x: node_params.get(x[source_col])  # type: ignore
            if node_params.get(x[source_col]) == "source"
            else node_params.get(x[target_col]) or "suit",
            1,  # type: ignore
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

    def _to_json(self, data: Any) -> str:
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

    def _to_js_val(self, val: Any = None) -> str:
        return self._to_json(val) if val is not None else "undefined"

    @staticmethod
    def generateId(size: int = 6, chars: str = string.ascii_uppercase + string.digits) -> str:
        return "el" + "".join(random.choice(chars) for _ in range(size))

    def plot_graph(
        self,
        thresholds: dict[str, Threshold] | None = None,
        targets: MutableMapping[str, str | None] | None = None,
        weights: MutableMapping[str, str] | None = None,
        norm_type: NormType | None = None,
        width: int = 960,
        height: int = 900,
        weight_template: str | None = None,
        show_weights: bool | None = None,
        show_percents: bool | None = None,
        show_nodes_names: bool | None = None,
        show_all_edges_for_targets: bool | None = None,
        show_nodes_without_links: bool | None = None,
    ) -> None:
        if targets:
            self.targets = targets
        if weights:
            self.weights = weights
        self.norm_type = norm_type

        settings = self._apply_settings(
            show_weights=show_weights,
            show_percents=show_percents,
            show_nodes_names=show_nodes_names,
            show_all_edges_for_targets=show_all_edges_for_targets,
            show_nodes_without_links=show_nodes_without_links,
        )

        nodes_threshold = thresholds.get("nodes", self.thresholds.get("nodes", None)) if thresholds else None
        links_threshold = thresholds.get("edges", self.thresholds.get("edges", None)) if thresholds else None

        self.edgelist.calculate_edgelist(
            norm_type=self.norm_type, custom_cols=self.custom_cols, data=self.eventstream.to_dataframe()
        )
        node_params = self._make_node_params(targets)

        cols = self.__get_nodelist_cols()

        nodes, links = self._make_template_data(
            node_params=node_params,
            width=width,
            height=height,
        )

        init_graph_js = self.render.init(
            **dict(
                server_id=self.server.pk,
                env=self.env,
                links=self._to_json(links),
                nodes=self._to_json(nodes),
                node_params=self._to_json(node_params),
                layout_dump=1 if self.layout is not None else 0,
                links_weights_names=cols,
                node_cols_names=cols,
                show_weights=self._get_option("show_weights", settings),
                show_percents=self._get_option("show_percents", settings),
                show_nodes_names=self._get_option("show_nodes_names", settings),
                show_all_edges_for_targets=self._get_option("show_all_edges_for_targets", settings),
                show_nodes_without_links=self._get_option("show_nodes_without_links", settings),
                nodes_threshold=self._to_js_val(nodes_threshold),
                links_threshold=self._to_js_val(links_threshold),
                weight_template=weight_template if weight_template is not None else "undefined",
            )
        )

        graph_body = self.render.body()

        graph_script_src = (
            "https://static.server.retentioneering.com/viztools/transition-graph/v3/transition-graph.umd.js?id="
            + self.generateId()
        )

        init_graph_template = self.render.init(
            **dict(
                server_id=self.server.pk,
                env=self.env,
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
                        id=self.generateId(),
                        width=width,
                        height=height,
                        graph_body=graph_body,
                        graph_script_src=graph_script_src,
                        init_graph_js=init_graph_template,
                        template="",
                    )
                ),
            )
        )

        html = self.render.inner_iframe(
            **dict(
                id=self.generateId(),
                width=width,
                height=height,
                graph_body=graph_body,
                graph_script_src=graph_script_src,
                init_graph_js=init_graph_js,
                template=html_template,
            )
        )
        display(HTML(html))

    def _get_option(self, name: str, settings: dict[str, Any]) -> str:
        if name in settings:
            return self._to_json(settings[name])
        return "undefined"

    def get_adjacency(self, weights: list[str] | None, norm_type: NormType) -> pd.DataFrame:
        self.edgelist.calculate_edgelist(data=self.eventstream.to_dataframe(), norm_type=norm_type, custom_cols=weights)
        edgelist: pd.DataFrame = self.edgelist.edgelist_df
        graph = nx.DiGraph()
        graph.add_weighted_edges_from(edgelist.values)
        return nx.to_pandas_adjacency(G=graph)
