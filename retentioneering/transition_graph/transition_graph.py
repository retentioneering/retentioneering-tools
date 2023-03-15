from __future__ import annotations

import json
import random
import string
from typing import Any, Dict, List, MutableMapping, MutableSequence, Union, cast

import networkx as nx
import pandas as pd
from IPython.core.display import HTML, display

from retentioneering.backend import ServerManager
from retentioneering.edgelist import Edgelist
from retentioneering.eventstream.types import EventstreamType
from retentioneering.nodelist import Nodelist
from retentioneering.templates.transition_graph import TransitionGraphRenderer
from retentioneering.tooling.typing.transition_graph import (
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
from retentioneering.utils.dict import clear_dict

RenameRule = Dict[str, Union[List[str], str]]


class TransitionGraph:
    """
    A class that holds methods for transition graph visualization.

    Parameters
    ----------
    eventstream: EventstreamType
        Sourcing eventstream.

    edge_norm_type: {"full", "node", None}, default None
        Type of normalization that is used to calculate weights for graph nodes and edges. See
        :ref:`Transition graph user guide <transition_graph_weights>` for the details.

    weights: dict, optional
        Weighting columns for nodes and edges. See :ref:`Transition graph user guide <transition_graph_weights>`
        for the details.

        - Possible keys: "nodes", "edges".
        - Possible values: "event_id", user column (typically "user_id") or custom columns.

        If None, {'nodes': 'event_id', 'edges': 'event_id'} dict is used.

    thresholds: dict, optional
        Threshold values for hiding nodes and edges from the canvas.

        - Possible keys: "nodes", "edges".
        - Possible values: dict with weighting columns as dict keys and threshold values as dict values.

        Example: {'nodes': {'event_id': 0.03}, 'edges': {'event_id': 0.03, user_id: 0.05}}

        If None, all the threshold values are set to 0.

    targets: dict, optional
        Events mapping that defines which nodes and edges should be colored for better visualization.

        - Possible keys: "positive" (green), "negative" (red), "source" (orange).
        - Possible values: list of events of a given type.

    graph_settings: dict, optional
        Visual boolean settings related to :ref:`Settings block <transition_graph_visual_settings>`
        in the control of transition graph interface.

        Possible keys:
        - show_weights,
        - show_percents,
        - show_nodes_names,
        - show_all_edges_for_targets,
        - show_nodes_without_links.

    """

    _weights: MutableMapping[str, str] | None = None
    _edge_norm_type: NormType = None
    _nodes_threshold: Threshold
    _edges_threshold: Threshold

    @property
    def nodes_thresholds(self) -> Threshold:
        return self._nodes_threshold

    @nodes_thresholds.setter
    def nodes_thresholds(self, value: Threshold) -> None:
        if self._check_thresholds_for_norm_type(value):
            self._nodes_threshold = value

    @property
    def edges_thresholds(self) -> Threshold:
        return self._edges_threshold

    @edges_thresholds.setter
    def edges_thresholds(self, value: Threshold) -> None:
        if self._check_thresholds_for_norm_type(value):
            self._edges_threshold = value

    def _check_thresholds_for_norm_type(self, value: Threshold) -> bool:
        if self.edge_norm_type == "full":
            if not all(map(lambda x: isinstance(x, int), value.values())):
                raise ValueError(f"For normalization type {self.edge_norm_type} all thresholds must be int")
            if not all(map(lambda x: x >= 0, value.values())):
                raise ValueError(f"For normalization type {self.edge_norm_type} all thresholds must be positive")
        else:
            if not all(map(lambda x: type(x) in (float, None), value.values())):
                raise ValueError(f"For normalization type {self.edge_norm_type} all thresholds must be float or None")
            if not all(map(lambda x: x is None or 0 <= x <= 1, value.values())):
                raise ValueError(
                    f"For normalization type {self.edge_norm_type} all thresholds must be between 0 and 1 or None"
                )
        return True

    def __init__(
        self,
        eventstream: EventstreamType,  # graph: dict,  # preprocessed graph
        graph_settings: GraphSettings | dict[str, Any] | None = None,
        edge_norm_type: NormType = None,
        targets: MutableMapping[str, str | None] | None = None,
        nodes_threshold: Threshold | None = None,
        edges_threshold: Threshold | None = None,
        nodes_weight_col: str | None = None,
        edges_weight_col: str | None = None,
        custom_weights_list: list[str] | None = None,
    ) -> None:
        from retentioneering.eventstream.eventstream import Eventstream

        if graph_settings is None:
            graph_settings = {}  # type: ignore
        if nodes_threshold is None:
            nodes_threshold = {"user_id": 0, "event_id": 0}
        self.nodes_thresholds = nodes_threshold

        if edges_threshold is None:
            edges_threshold = {"user_id": 0, "event_id": 0}
        self.edges_thresholds = edges_threshold

        self.nodelist_default_col = eventstream.schema.event_id
        self.edgelist_default_col = eventstream.schema.event_id

        self.targets = targets if targets else {"positive": None, "negative": None, "source": None}
        sm = ServerManager()
        self.env = sm.check_env()
        self.server = sm.create_server()

        self.server.register_action("save-nodelist", lambda n: self._on_nodelist_updated(n))
        self.server.register_action("save-layout", lambda n: self._on_layout_request(n))
        self.server.register_action("save-graph-settings", lambda n: self._on_graph_settings_request(n))
        self.server.register_action("recalculate", lambda n: self._on_recalc_request(n))

        self.eventstream: Eventstream = eventstream  # type: ignore

        self.event_col = self.eventstream.schema.event_name
        self.event_time_col = self.eventstream.schema.event_timestamp
        self.user_col = self.eventstream.schema.user_id
        self.id_col = self.eventstream.schema.event_id
        self.custom_cols = [
            self.eventstream.schema.user_id,
            self.eventstream.schema.event_id,
            *self.eventstream.schema.custom_cols,
        ]
        if "session_id" in self.eventstream.schema.custom_cols:
            self.custom_cols.append("session_id")
        if isinstance(custom_weights_list, list):
            self.custom_cols.extend(custom_weights_list)

        self.nodes_weight_col = nodes_weight_col if nodes_weight_col else eventstream.schema.event_id
        self.edges_weight_col = edges_weight_col if edges_weight_col else eventstream.schema.event_id

        self.spring_layout_config = {"k": 0.1, "iterations": 300, "nx_threshold": 1e-4}

        self.layout: pd.DataFrame | None = None
        self.graph_settings = graph_settings

        self.edge_norm_type: NormType | None = edge_norm_type

        self.nodelist: Nodelist = Nodelist(
            nodelist_default_col=self.nodelist_default_col,
            custom_cols=self.custom_cols,
            time_col=self.event_time_col,
            event_col=self.event_col,
        )

        self.nodelist.calculate_nodelist(data=self.eventstream.to_dataframe())
        self.rename_cols = {self.eventstream.schema.event_id: self.edges_weight_col}
        self.edgelist: Edgelist = Edgelist(eventstream=self.eventstream)
        self.edgelist.calculate_edgelist(
            weight_cols=self.custom_cols, norm_type=self.edge_norm_type, rename_cols=self.rename_cols
        )

        self.render: TransitionGraphRenderer = TransitionGraphRenderer()

    @property
    def weights(self) -> MutableMapping[str, str] | None:
        return self._weights

    @weights.setter
    def weights(self, value: MutableMapping[str, str] | None) -> None:
        available_cols = self.__get_nodelist_cols()

        if value and ("edges" not in value or "nodes" not in value):
            raise ValueError("Allowed only: %s" % {"edges": "col_name", "nodes": "col_name"})

        if value and (value["edges"] not in available_cols or value["nodes"] not in available_cols):
            raise ValueError("Allowed only: %s" % {"edges": "col_name", "nodes": "col_name"})

        self._weights = value

    @property
    def edge_norm_type(self) -> NormType:  # type: ignore
        return self._edge_norm_type

    @edge_norm_type.setter
    def edge_norm_type(self, edge_norm_type: NormType) -> None:  # type: ignore
        allowed_edge_norm_types: list[str | None] = [None, "full", "node"]
        if edge_norm_type in allowed_edge_norm_types:
            self._edge_norm_type = edge_norm_type
        else:
            raise ValueError("Norm type should be one of: %s" % allowed_edge_norm_types)

    def _on_recalc_request(
        self, rename_rules: list[RenameRule]
    ) -> dict[str, MutableSequence[PreparedNode] | MutableSequence[PreparedLink] | list]:
        try:
            self._recalculate(rename_rules=rename_rules)

            nodes, nodes_set = self._prepare_nodes(
                nodelist=self.nodelist.nodelist_df,
            )
            self._on_nodelist_updated(nodes)
            edgelist = self.edgelist.edgelist_df
            edgelist["type"] = "suit"
            links = self._prepare_edges(edgelist=edgelist, nodes_set=nodes_set)
            result: dict[str, MutableSequence[PreparedNode] | MutableSequence[PreparedLink] | list] = {
                "nodes": nodes,
                "links": links,
            }

            return result
        except Exception as err:
            raise ValueError("error! %s" % err)

    def _recalculate(self, rename_rules: list[RenameRule]) -> None:
        eventstream = self.eventstream.copy()
        renamed_eventstream = eventstream.rename(rules=rename_rules)
        renamed_df = renamed_eventstream.to_dataframe()

        # save norm type
        recalculated_nodelist = self.nodelist.calculate_nodelist(data=renamed_df)
        self.edgelist.eventstream = renamed_eventstream
        recalculated_edgelist = self.edgelist.calculate_edgelist(
            weight_cols=self.custom_cols, norm_type=self.edge_norm_type, rename_cols=self.rename_cols
        )

        curr_nodelist = self.nodelist.nodelist_df

        self.nodelist.nodelist_df = curr_nodelist.apply(
            lambda x: self._update_node_after_recalc(recalculated_nodelist, x), axis=1
        )
        self.edgelist.edgelist_df = recalculated_edgelist

    def _replace_grouped_events(self, grouped: pd.Series, row: pd.Series) -> pd.Series:
        event_name = row[self.event_col]
        mathced = grouped[grouped[self.event_col] == event_name]

        if len(mathced) > 0:
            parent_node_name = mathced.iloc[0]["parent"]
            row[self.event_col] = parent_node_name

        return row

    def _update_node_after_recalc(self, recalculated_nodelist: pd.DataFrame, row: pd.Series) -> pd.Series:
        cols = self.__get_nodelist_cols()
        node_name = row[self.event_col]
        matched: pd.Series[Any] = recalculated_nodelist[recalculated_nodelist[self.event_col] == node_name]

        if len(matched) > 0:
            recalculated_node = matched.iloc[0]
            for col in cols:
                row[col] = recalculated_node[col]
        return row.copy()

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
            return self._map_targets(targets)  # type: ignore
        else:
            return self._map_targets(self.targets)  # type: ignore

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
        custom_cols = self.custom_cols
        return list([default_col]) + list(custom_cols)

    def __round_value(self, value: float) -> float:
        if self.edge_norm_type in ["full", "node"]:
            # @TODO: make this magical number as constant or variable from config dict. Vladimir Makhanov
            return round(value, 5)
        return value

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
                curr_degree["degree"] = self.__round_value((abs(value)) / abs(max_degree) * 30 + 4)
                curr_degree["source"] = self.__round_value(value)
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
        custom_cols: list[str] = self.custom_cols
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
        # @FIXME: idk why pyright doesn't like this. Vladimir Makhanov
        merged = {**self.graph_settings, **clear_dict(settings)}  # type: ignore

        return clear_dict(merged)

    def _map_targets(self, targets: dict[str, str | list[str]]) -> dict[str, str]:
        targets_mapping = {
            "positive": "nice",
            "negative": "bad",
            "source": "source",
        }
        mapped_targets = {}

        for target, nodes in targets.items():
            if nodes is None:
                pass
            if isinstance(nodes, list):
                for node in nodes:
                    mapped_targets[node] = targets_mapping[target]
            else:
                mapped_targets[nodes] = targets_mapping[target]

        return mapped_targets

    def _to_js_val(self, val: Any = None) -> str:
        return self._to_json(val) if val is not None else "undefined"

    @staticmethod
    def generateId(size: int = 6, chars: str = string.ascii_uppercase + string.digits) -> str:
        return "el" + "".join(random.choice(chars) for _ in range(size))

    def _edge_norm_type_to_json_value(self, edge_norm_type: NormType) -> str:
        return "none" if edge_norm_type is None else str(edge_norm_type).lower()

    def plot_graph(
        self,
        targets: MutableMapping[str, str | None] | None = None,
        edge_norm_type: NormType | None = None,
        width: int = 960,
        height: int = 900,
        weight_template: str | None = None,
        show_weights: bool | None = None,
        show_percents: bool | None = None,
        show_nodes_names: bool | None = None,
        show_all_edges_for_targets: bool | None = None,
        show_nodes_without_links: bool | None = None,
        layout_dump: str | None = None,
    ) -> None:
        """
        Create interactive transition graph visualization with callback to sourcing eventstream.

        Parameters
        ----------
        edge_norm_type: {"full", "node", None}, default None
            Type of normalization that is used to calculate weights for graph nodes and edges. See
            :ref:`Transition graph user guide <transition_graph_weights>` for the details.

        weights: dict, optional
            Weighting columns for nodes and edges. See :ref:`Transition graph user guide <transition_graph_weights>`
            for the details.

            - Possible keys: "nodes", "edges".
            - Possible values: "event_id", user column (typically "user_id") or custom columns.

            If None, {'nodes': 'event_id', 'edges': 'event_id'} dict is used.

        thresholds: dict, optional
            Threshold values for hiding nodes and edges from the canvas.

            - Possible keys: "nodes", "edges".
            - Possible values: dict with weighting columns as dict keys and threshold values as dict values.

            Example: {'nodes': {'event_id': 0.03}, 'edges': {'event_id': 0.03, user_id: 0.05}}

            If None, all the threshold values are set to 0.

        targets: dict, optional
            Events mapping that defines which nodes and edges should be colored for better visualization.

            - Possible keys: "positive" (green), "negative" (red), "source" (orange).
            - Possible values: list of events of a given type.

        width : int, default 960
            Width of plot in pixels.
        height : int, default 960
            Height of plot in pixels.
        weight_template : str, optional
        show_weights : bool, optional
        show_percents : bool, optional
        show_nodes_names : bool, optional
        show_all_edges_for_targets : bool, optional
        show_nodes_without_links : bool, optional

        Returns
        -------
            Rendered IFrame graph

        Notes
        -----
        To get the definition of ``show_*`` visual parameters see
        :ref:`Settings block <transition_graph_visual_settings>` in the control of transition graph interface.

        """
        if targets:
            self.targets = targets
        self.edge_norm_type = edge_norm_type

        settings = self._apply_settings(
            show_weights=show_weights,
            show_percents=show_percents,
            show_nodes_names=show_nodes_names,
            show_all_edges_for_targets=show_all_edges_for_targets,
            show_nodes_without_links=show_nodes_without_links,
        )

        norm_nodes_threshold = self._get_norm_node_threshold(self.nodes_thresholds)
        norm_links_threshold = self._get_norm_link_threshold(self.edges_thresholds)

        self.edgelist.calculate_edgelist(weight_cols=self.custom_cols, norm_type=self.edge_norm_type)
        node_params = self._make_node_params(targets)
        cols = self.__get_nodelist_cols()

        nodes, links = self._make_template_data(
            node_params=node_params,
            width=width,
            height=height,
        )

        shown_nodes_col = self.nodes_weight_col
        shown_links_weight = self.edges_weight_col
        selected_nodes_col_for_thresholds = shown_nodes_col
        selected_links_weight_for_thresholds = shown_links_weight

        init_graph_js = self.render.init(
            **dict(
                server_id=self.server.pk,
                env=self.env,
                edge_norm_type=self._edge_norm_type_to_json_value(self.edge_norm_type),
                links=self._to_json(links),
                nodes=self._to_json(nodes),
                node_params=self._to_json(node_params),
                layout_dump=1 if layout_dump is not None or self.layout is not None else 0,
                links_weights_names=cols,
                node_cols_names=cols,
                shown_nodes_col=shown_nodes_col,
                shown_links_weight=shown_links_weight,
                selected_nodes_col_for_thresholds=selected_nodes_col_for_thresholds,
                selected_links_weight_for_thresholds=selected_links_weight_for_thresholds,
                show_weights=self._get_option("show_weights", settings),
                show_percents=self._get_option("show_percents", settings),
                show_nodes_names=self._get_option("show_nodes_names", settings),
                show_all_edges_for_targets=self._get_option("show_all_edges_for_targets", settings),
                show_nodes_without_links=self._get_option("show_nodes_without_links", settings),
                nodes_threshold=self._to_js_val(norm_nodes_threshold),
                links_threshold=self._to_js_val(norm_links_threshold),
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
                edge_norm_type=self._edge_norm_type_to_json_value(self.edge_norm_type),
                node_params=self._to_json(node_params),
                links="<%= links %>",
                nodes="<%= nodes %>",
                layout_dump=1,
                links_weights_names=cols,
                node_cols_names=cols,
                shown_nodes_col="<%= shown_nodes_col %>",
                shown_links_weight="<%= shown_links_weight %>",
                selected_nodes_col_for_thresholds="<%= selected_nodes_col_for_thresholds %>",
                selected_links_weight_for_thresholds="<%= selected_links_weight_for_thresholds %>",
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
