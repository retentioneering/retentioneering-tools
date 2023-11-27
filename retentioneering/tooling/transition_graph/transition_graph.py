from __future__ import annotations

import copy
import json
import os
import random
import string
import warnings
from dataclasses import asdict
from typing import Any, MutableMapping, MutableSequence, cast

import networkx as nx
import pandas as pd
from IPython.core.display import HTML, display
from nanoid import generate

from retentioneering import RETE_CONFIG
from retentioneering import __version__ as RETE_VERSION
from retentioneering.backend import ServerManager
from retentioneering.backend.tracker import (
    collect_data_performance,
    time_performance,
    track,
    tracker,
)
from retentioneering.edgelist import Edgelist
from retentioneering.eventstream.types import EventstreamType
from retentioneering.nodelist import Nodelist
from retentioneering.templates.transition_graph import TransitionGraphRenderer
from retentioneering.tooling.transition_graph.interface import (
    EdgeId,
    EnvId,
    NodeId,
    NodeLayout,
    RecalculationEdge,
    RecalculationNode,
    RecalculationSuccessResult,
    TargetId,
)
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

from .interface import (
    Column,
    EdgeItem,
    Edges,
    EdgesCustomColors,
    Env,
    InitializationParams,
    NodeItem,
    Nodes,
    NodesCusomColors,
    Normalization,
    RecalculationSuccessResponse,
    RenameRule,
    SerializedState,
    Settings,
    StateChanges,
    SyncStatePayload,
    SyncStateSuccessResponse,
    Target,
    Tracker,
)

# RenameRule = Dict[str, Union[List[str], str]]

SESSION_ID_COL = "session_id"


class TransitionGraph:
    """
    A class that holds methods for transition graph visualization.

    Parameters
    ----------
    eventstream: EventstreamType
        Source eventstream.


    See Also
    --------
    .Eventstream.transition_graph : Call TransitionGraph tool as an eventstream method.
    .Eventstream.transition_matrix : Matrix representation of transition graph.
    .EventstreamSchema : Schema of eventstream columns, that could be used as weights.
    .TransitionGraph.plot : Interactive transition graph visualization.


    Notes
    -----
    See :doc:`transition graph user guide</user_guides/transition_graph>` for the details.

    """

    DEFAULT_GRAPH_URL = "https://static.server.retentioneering.com/package/@rete/transition-graph/version/2/dist/transition-graph.umd.js"
    _weights: MutableMapping[str, str] | None = None
    _edges_norm_type: NormType = None
    _nodes_norm_type: NormType = None
    _nodes_threshold: Threshold
    _edges_threshold: Threshold
    _recalculation_result: EventstreamType

    sync_data: SyncStatePayload | None = None
    node_layout: dict[str, NodeLayout]
    __renamed_nodes: dict[str, str]

    @property
    def graph_url(self) -> str:
        env_url: str = os.getenv("RETE_TRANSITION_GRAPH_URL", "")
        return env_url if env_url else self.DEFAULT_GRAPH_URL

    @property
    def nodes_thresholds(self) -> Threshold:
        return self._nodes_threshold

    @nodes_thresholds.setter
    def nodes_thresholds(self, value: Threshold) -> None:
        if self._check_thresholds_for_norm_type(value=value, norm_type=self.nodes_norm_type):
            self._nodes_threshold = value

    @property
    def edges_thresholds(self) -> Threshold:
        return self._edges_threshold

    @edges_thresholds.setter
    def edges_thresholds(self, value: Threshold) -> None:
        if self._check_thresholds_for_norm_type(value=value, norm_type=self.edges_norm_type):
            self._edges_threshold = value

    def _check_thresholds_for_norm_type(self, value: Threshold, norm_type: NormType) -> bool:
        if norm_type is None:
            if not all(map(lambda x: x is None or x >= 0, value.values())):  # type: ignore
                raise ValueError(f"For normalization type {norm_type} all thresholds must be positive or None")
        else:
            if not all(map(lambda x: x is None or 0 <= x <= 1, value.values())):  # type: ignore
                raise ValueError(f"For normalization type {norm_type} all thresholds must be between 0 and 1 or None")

        return True

    @time_performance(
        scope="transition_graph",
        event_name="init",
    )
    def __init__(
        self,
        eventstream: EventstreamType,  # graph: dict,  # preprocessed graph
    ) -> None:
        from retentioneering.eventstream.eventstream import Eventstream

        sm = ServerManager()
        self.env: EnvId = sm.check_env()
        self.server = sm.create_server()

        self.server.register_action("save-graph-settings", lambda n: self._on_graph_settings_request(n))
        self.server.register_action("recalculate", lambda n: self._on_recalc_request(n))
        self.server.register_action("sync-state", lambda n: self._on_sync_state_request(n))

        self.eventstream: Eventstream = eventstream  # type: ignore

        self.event_col = self.eventstream.schema.event_name
        self.event_time_col = self.eventstream.schema.event_timestamp
        self.user_col = self.eventstream.schema.user_id

        self.spring_layout_config = {"k": 0.1, "iterations": 300, "nx_threshold": 1e-4}

        self.is_layout_loaded: bool = False
        self.graph_settings: GraphSettings | dict[str, Any] = {}
        self.render: TransitionGraphRenderer = TransitionGraphRenderer()
        self.normalizations: list[Normalization] = [
            Normalization(id="none", name="none", type="absolute"),
            Normalization(id="full", name="full", type="relative"),
            Normalization(id="node", name="node", type="relative"),
        ]
        self.node_layout = {}

        self._recalculation_result = eventstream
        self.allowed_targets = self.__build_targets()

    @property
    @time_performance(
        scope="transition_graph",
        event_name="recalculation_result",
    )
    def recalculation_result(self) -> EventstreamType:
        """
        Export an eventstream after GUI actions that affect eventstream.

        Returns
        -------
        EventstreamType
            The modified event stream.

        Notes
        -----
        Renaming groups, nodes, and nested nodes in the GUI will not affect the resulting eventstream.
        The default group and node names will be returned.
        """
        return self._recalculation_result

    def __build_targets(self) -> list[Target]:
        nice_node = Target(
            id="nice_node",
            name="Positive",
            ignoreThreshold=True,
            edgeDirection="in",
            position="top-right",
        )
        bad_node = Target(
            id="bad_node", name="Negative", ignoreThreshold=True, edgeDirection="in", position="bottom-right"
        )
        source_node = Target(
            id="source_node",
            name="Source",
            ignoreThreshold=False,
            edgeDirection="both",
            position="top-left",
        )

        return [nice_node, bad_node, source_node]

    def _on_sync_state_request(self, sync_data: dict[str, Any]) -> dict:
        self.debug_sync_data = sync_data
        try:
            self.sync_data = SyncStatePayload(**sync_data)
            return asdict(SyncStateSuccessResponse(serverId=self.server.pk, requestId=""))
        except Exception as e:
            raise e  # for now for debugging purposes. Vladimir Makhanov

    def _define_weight_cols(self, custom_weight_cols: list[str] | None) -> list[str]:
        weight_cols = [
            self.eventstream.schema.event_id,
            self.eventstream.schema.user_id,
        ]
        if SESSION_ID_COL in self.eventstream.schema.custom_cols:
            weight_cols.append(SESSION_ID_COL)
        if isinstance(custom_weight_cols, list):
            for col in custom_weight_cols:
                if col not in weight_cols:
                    if col not in self.eventstream.schema.custom_cols:
                        raise ValueError(f"Custom weights column {col} not found in eventstream schema")
                    else:
                        weight_cols.append(col)
        return weight_cols

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
    def edges_norm_type(self) -> NormType:  # type: ignore
        return self._edges_norm_type

    @edges_norm_type.setter
    def edges_norm_type(self, edges_norm_type: NormType) -> None:  # type: ignore
        allowed_edges_norm_types: list[str | None] = [None, "full", "node"]
        if edges_norm_type in allowed_edges_norm_types:
            self._edges_norm_type = edges_norm_type
        else:
            raise ValueError("Norm type should be one of: %s" % allowed_edges_norm_types)

    @property
    def render_edge_norm_type(self) -> NormType:  # type: ignore
        if self.edges_norm_type is None:
            return "none"
        else:
            return self.edges_norm_type

    @property
    def nodes_norm_type(self) -> NormType:  # type: ignore
        return self._nodes_norm_type

    @nodes_norm_type.setter
    def nodes_norm_type(self, nodes_norm_type: NormType) -> None:  # type: ignore
        if nodes_norm_type is not None:
            warnings.warn(f"Currently nodes_norm_type allowed to be None only")
        self._nodes_norm_type = None

    @property
    def nodes_edge_norm_type(self) -> NormType:  # type: ignore
        if self.nodes_norm_type is None:
            return "none"
        else:
            return self.edges_norm_type

    def _on_recalc_request(self, recalculate_data: dict[str, Any]) -> dict[str, Any]:
        try:
            serialized_state: SerializedState = SerializedState(**recalculate_data)
        except Exception as e:
            raise Exception("Invalid recalculate data")

        self._recalculation_result = self.eventstream.copy()
        self.serialized_state = serialized_state

        # remove disabled nodes
        disabled_nodes_names: list[str] = [
            node["name"] for node in serialized_state.nodes["items"] if node["isDisabledByUser"] is True  # type: ignore
        ]

        # grouping
        rename_rules: list[RenameRule] = []
        renamed_nodes = {}

        for node_item in serialized_state.nodes["items"]:
            renamed_nodes[node_item["id"]] = node_item["name"]

        for old_node_name, new_node_name in renamed_nodes.items():
            if old_node_name != new_node_name:
                rename_rules.append(dict(group_name=new_node_name, child_events=[old_node_name]))

        for node_aggregation in serialized_state.stateChanges["nodesAggregation"]:  # type: ignore
            parent_node_id = renamed_nodes[node_aggregation[0]]

            child_events: list[str] = node_aggregation[1]["ids"]
            rename_rules.append(dict(group_name=parent_node_id, child_events=child_events))

        self.__renamed_nodes = renamed_nodes
        try:
            self._recalculate(rename_rules=rename_rules, disabled_nodes=disabled_nodes_names)

            nodes = self._prepare_nodes(
                nodelist=self.nodelist.nodelist_df,
            )
            self._on_nodelist_updated(nodes)
            edgelist = self.edgelist.edgelist_df
            edgelist["type"] = "suit"
            links = self._prepare_edges(edgelist=edgelist, nodes_set=nodes)

            recalculation_answer = self._build_recalculation_answer(
                serialized_state=serialized_state, nodes=nodes, edges=links
            )

            return asdict(recalculation_answer)
        except Exception as err:
            raise ValueError("error! %s" % err)

    def _edge_id_by_map(self, edge: EdgeItem, mapping: dict[str, EdgeId]) -> EdgeId:
        return mapping.get(f'{[edge["sourceNodeId"], edge["targetNodeId"]]}', edge["id"])

    def _build_recalculation_answer(
        self, serialized_state: SerializedState, nodes: list[NodeItem], edges: list[EdgeItem]
    ) -> RecalculationSuccessResult:
        node_edge_map: dict[str, EdgeId] = {}
        node_id_name_mapping: dict[NodeId, str] = {node["id"]: node["name"] for node in serialized_state.nodes["items"]}
        node_name_id_mapping: dict[NodeId, str] = {node["name"]: node["id"] for node in serialized_state.nodes["items"]}
        for edge in serialized_state.edges["items"]:
            node_edge_map[
                f'{[node_id_name_mapping[edge["sourceNodeId"]]]}, {node_id_name_mapping[edge["targetNodeId"]]}'
            ] = edge["id"]

        response_nodes: dict[NodeId, RecalculationNode] = {
            node_name_id_mapping.get(node["id"], node["id"]): RecalculationNode(
                id=node_name_id_mapping.get(node["id"], node["id"]),
                size=node["size"],
                weight=node["weight"],
                targetId=node["targetId"],  # type: ignore
            )
            for node in nodes
        }
        self._response_nodes = response_nodes

        response_edges: list[RecalculationEdge] = []
        for edge in edges:
            edge_actual_id = self._edge_id_by_map(edge=edge, mapping=node_edge_map)
            response_edges.append(
                RecalculationEdge(
                    id=edge_actual_id,
                    sourceNodeId=node_name_id_mapping[
                        self.__renamed_nodes.get(edge["sourceNodeId"], edge["sourceNodeId"])
                    ],
                    targetNodeId=node_name_id_mapping[
                        self.__renamed_nodes.get(edge["targetNodeId"], edge["targetNodeId"])
                    ],
                    size=edge["size"],
                    weight=edge["weight"],
                )
            )

        res = RecalculationSuccessResult(nodes=response_nodes, edges=response_edges)
        return res

    def _recalculate(self, rename_rules: list[RenameRule], disabled_nodes: list[str] | None = None) -> None:
        eventstream = self.eventstream.copy()
        # frontend can ask recalculate without grouping or renaming
        if disabled_nodes is None:
            disabled_nodes = []
        if len(disabled_nodes) > 0:
            from retentioneering.eventstream import EventstreamSchema

            def _filter(df: pd.DataFrame, schema: EventstreamSchema) -> pd.DataFrame:
                return ~df[schema.event_name].isin(disabled_nodes)  # type: ignore

            eventstream = eventstream.filter_events(func=_filter)  # type: ignore

        if len(rename_rules) > 0:
            eventstream = eventstream.rename(rules=rename_rules)  # type: ignore
        self._recalculation_result = eventstream
        renamed_df = eventstream.to_dataframe()

        # save norm type
        recalculated_nodelist = self.nodelist.calculate_nodelist(data=renamed_df, rename_rules=rename_rules)
        self.edgelist.eventstream = eventstream
        recalculated_edgelist = self.edgelist.calculate_edgelist(
            weight_cols=self.weight_cols, norm_type=self.edges_norm_type
        )

        curr_nodelist = self.nodelist.nodelist_df

        self.nodelist.nodelist_df = curr_nodelist.apply(
            lambda x: self._update_node_after_recalc(recalculated_nodelist, x), axis=1
        )
        self.edgelist.edgelist_df = recalculated_edgelist

        collect_data_performance(
            scope="transition_graph",
            event_name="recalculate",
            called_params={"rename_rules": rename_rules},
            performance_data={
                "parent": {"index": self.eventstream._eventstream_index, "hash": self.eventstream._hash},
                "child": {
                    "index": self._recalculation_result._eventstream_index,
                    "hash": self._recalculation_result._hash,
                },
            },
            eventstream_index=self.eventstream._eventstream_index,
            parent_eventstream_index=self.eventstream._eventstream_index,
            child_eventstream_index=self._recalculation_result._eventstream_index,
        )

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
        self.node_layout = {x.get("name", None): NodeLayout(**x) for x in layout_nodes}  # type: ignore

    def _on_nodelist_updated(self, nodes: list[NodeItem]) -> None:
        # prepare data, map cols
        mapped_nodes = []
        for idx, node in enumerate(nodes):
            mapped_node = {
                "index": idx,
                self.event_col: node["name"],
                "active": True,
                "parent": None,
                "alias": False,
                "changed_name": None,
            }
            source_value = node["weight"]
            for col_name, deg in source_value.items():
                mapped_node[col_name] = deg  # type: ignore
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
        custom_cols = self.weight_cols
        return list([default_col]) + list(custom_cols)

    def __round_value(self, value: float) -> float:
        if self.edges_norm_type in ["full", "node"]:
            # @TODO: make this magical number as constant or variable from config dict. Vladimir Makhanov
            return round(value, 5)
        else:
            return value

    def _prepare_nodes(
        self,
        nodelist: pd.DataFrame,
        node_params: NodeParams | None = None,
        pos: Position | None = None,
        nodes_custom_colors: NodesCusomColors | None = None,
    ) -> list[NodeItem]:
        node_names = set(nodelist[self.event_col])
        nodes_custom_colors = nodes_custom_colors if nodes_custom_colors else {}

        cols = self.__get_nodelist_cols()
        nodes_set: list[NodeItem] = []
        for idx, node_name in enumerate(node_names):
            row = nodelist.loc[nodelist[self.event_col] == node_name]
            custom_color = nodes_custom_colors.get(node_name)
            degree = {}
            weight = {}
            size = {}
            for weight_col in cols:
                max_degree = cast(float, nodelist[weight_col].max())
                r = row[weight_col]
                r = r.tolist()
                value = r[0]
                curr_degree = {}
                curr_degree["degree"] = self.__round_value((abs(value)) / abs(max_degree) * 30 + 4)
                curr_degree["source"] = self.__round_value(value)
                degree[weight_col] = curr_degree
                size[weight_col] = curr_degree["degree"]
                weight[weight_col] = curr_degree["source"]

            node_pos = self.node_layout.get(node_name, None)
            target_id: TargetId = node_params.get(node_name, "suit_node") if node_params is not None else "suit_node"  # type: ignore

            node: NodeItem = NodeItem(
                id=node_name,
                name=node_name,
                size=size,
                weight=weight,
                children=[],
                targetId=target_id,
            )
            parent = None
            if parent is not None:
                node["parentNodeId"] = parent

            if custom_color is not None:
                node["customColor"] = custom_color

            if node_pos is not None:
                node["x"] = node_pos.x
                node["y"] = node_pos.y

            nodes_set.append(node)

        return nodes_set

    def _prepare_edges(
        self,
        edgelist: pd.DataFrame,
        nodes_set: list[NodeItem],
        edges_custom_colors: EdgesCustomColors | None = None,
    ) -> list[EdgeItem]:
        default_col = self.nodelist_default_col
        source_col = edgelist.columns[0]
        target_col = edgelist.columns[1]
        weight_col = edgelist.columns[2]
        custom_cols: list[str] = self.weight_cols
        edges: list[EdgeItem] = []
        edges_custom_colors = edges_custom_colors if edges_custom_colors else {}

        edgelist["weight_norm"] = edgelist[weight_col] / edgelist[weight_col].abs().max()
        for _, row in edgelist.iterrows():
            default_col_weight: Weight = {
                "weight_norm": self.__round_value(row.weight_norm),
                "weight": self.__round_value(cast(float, row[weight_col])),  # type: ignore
            }
            weights = {
                default_col: default_col_weight,
            }
            for custom_weight_col in custom_cols:
                weight = self.__round_value(cast(float, row[custom_weight_col]))
                max_weight = cast(float, edgelist[custom_weight_col].abs().max())
                weight_norm = self.__round_value(weight / max_weight)
                col_weight: Weight = {
                    "weight_norm": weight_norm,
                    "weight": weight,
                }
                weights[custom_weight_col] = col_weight

            source_node_name = str(row[source_col])  # type: ignore
            target_node_name = str(row[target_col])  # type: ignore

            custom_color = edges_custom_colors.get((source_node_name, target_node_name))

            # list comprehension faster than filter
            source_node = [node for node in nodes_set if node["name"] == source_node_name][0]
            target_node = [node for node in nodes_set if node["name"] == target_node_name][0]

            if source_node is not None and target_node is not None:  # type: ignore
                edge_item = EdgeItem(
                    id=generate(),
                    sourceNodeId=source_node_name,
                    targetNodeId=target_node_name,
                    weight={col_name: round(weight["weight"], 2) for col_name, weight in weights.items()},
                    size={col_name: round(weight["weight_norm"], 2) for col_name, weight in weights.items()},
                    aggregatedEdges=[],
                )

                if custom_color is not None:
                    edge_item["customColor"] = custom_color

                edges.append(edge_item)

        return edges

    def _make_template_data(
        self,
        node_params: NodeParams,
        nodes_custom_colors: NodesCusomColors | None = None,
        edges_custom_colors: EdgesCustomColors | None = None,
    ) -> tuple[list[NodeItem], list[EdgeItem]]:
        edgelist = self.edgelist.edgelist_df.copy()
        nodelist = self.nodelist.nodelist_df.copy()

        source_col = edgelist.columns[0]
        target_col = edgelist.columns[1]

        # calc edge type
        edgelist["type"] = edgelist.apply(
            lambda x: node_params.get(x[source_col])  # type: ignore
            if node_params.get(x[source_col]) == "source_node"
            else node_params.get(x[target_col]) or "suit",
            1,  # type: ignore
        )

        nodes = self._prepare_nodes(nodelist=nodelist, node_params=node_params, nodes_custom_colors=nodes_custom_colors)

        links = self._prepare_edges(edgelist=edgelist, nodes_set=nodes, edges_custom_colors=edges_custom_colors)
        return nodes, links

    def _to_json(self, data: Any) -> str:
        return json.dumps(data).encode("latin1").decode("utf-8")

    def _to_json_links(self, data: MutableSequence[PreparedLink]) -> str:
        # We need to remove links with zero weight
        cleaned_data = []
        for link in data:
            cleaned_link = copy.deepcopy(link)
            cleaned_link["weights"] = {
                weight_col: weight for weight_col, weight in link["weights"].items() if weight["weight"] > 0
            }
            cleaned_data.append(cleaned_link)
        return self._to_json(cleaned_data)

    def _apply_settings(
        self,
        show_weights: bool | None = None,
        show_percents: bool | None = None,
        show_nodes_names: bool | None = None,
        show_all_edges_for_targets: bool | None = None,
        show_nodes_without_links: bool | None = None,
        show_edges_info_on_hover: bool | None = None,
    ) -> dict[str, Any]:
        settings = {
            "show_weights": show_weights,
            "show_percents": show_percents,
            "show_nodes_names": show_nodes_names,
            "show_all_edges_for_targets": show_all_edges_for_targets,
            "show_nodes_without_links": show_nodes_without_links,
            "show_edges_info_on_hover": show_edges_info_on_hover,
        }
        # @FIXME: idk why pyright doesn't like this. Vladimir Makhanov
        merged = {**self.graph_settings, **clear_dict(settings)}  # type: ignore

        return clear_dict(merged)

    def _map_targets(self, targets: dict[str, str | list[str]]) -> dict[str, str]:
        targets_mapping = {
            "positive": "nice_node",
            "negative": "bad_node",
            "source": "source_node",
        }
        mapped_targets = {}

        for target, nodes in targets.items():
            if nodes is None:  # type: ignore
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

    def _edges_norm_type_to_json_value(self, edges_norm_type: NormType) -> str:
        return "none" if edges_norm_type is None else str(edges_norm_type).lower()

    def _load_layout_from_dump(self, layout_dump: str) -> dict[str, NodeLayout]:
        # check if file exists on path
        node_layout: dict[str, NodeLayout] = {}

        # load layout from json file
        with open(layout_dump, "r") as f:
            layout_data: list[dict[str, str | int]] = json.load(f)

        # convert to node_layout format
        for node in layout_data:
            node_layout[node["name"]] = NodeLayout(**node)  # type: ignore
        return node_layout

    @time_performance(
        scope="transition_graph",
        event_name="plot",
    )
    def plot(
        self,
        targets: MutableMapping[str, str | None] | None = None,
        edges_norm_type: NormType | None = None,
        nodes_threshold: Threshold | None = None,
        nodes_norm_type: NormType | None = None,
        edges_threshold: Threshold | None = None,
        nodes_weight_col: str | None = None,
        edges_weight_col: str | None = None,
        custom_weight_cols: list[str] | None = None,
        width: str | int | float = "100%",
        height: str | int | float = "60vh",
        show_weights: bool = True,
        show_percents: bool = False,
        show_nodes_names: bool = True,
        show_all_edges_for_targets: bool = True,
        show_nodes_without_links: bool = False,
        show_edge_info_on_hover: bool = True,
        layout_dump: str | None = None,
        nodes_custom_colors: NodesCusomColors | None = None,
        edges_custom_colors: EdgesCustomColors | None = None,
    ) -> None:
        """
        Create interactive transition graph visualization with callback to sourcing eventstream.

        Parameters
        ----------
        edges_norm_type : {"full", "node", None}, default None
            Type of normalization that is used to calculate weights for graph edges.
            Based on ``edges_weight_col`` parameter the weight values are calculated.

            - If ``None``, normalization is not used, the absolute values are taken.
            - If ``full``, normalization across the whole eventstream.
            - If ``node``, normalization across each node (or outgoing transitions from each node).

            See :ref:`Transition graph user guide <transition_graph_weights>` for the details.

        nodes_norm_type : {"full", "node", None}, default None
            Currently not implemented. Always None.

        edges_weight_col : str, optional
            A column name from the :py:class:`.EventstreamSchema` which values will control the final
            edges' weights and displayed width as well.

            For each edge is calculated:

            - If ``None`` or ``user_id`` - the number of unique users.
            - If ``event_id`` - the number of transitions.
            - If ``session_id`` - the number of unique sessions.
            - If ``custom_col`` - the number of unique values in selected column.

            See :ref:`Transition graph user guide <transition_graph_weights>` for the details.

        edges_threshold : dict, optional
            Threshold mapping that defines the minimal weights for edges displayed on the canvas.

            - Keys should be of type str and contain the weight column names (the values from the
              :py:class:`.EventstreamSchema`).
            - Values of the dict are the thresholds for the edges that will be displayed.

            Support multiple weighting columns. In that case, logical OR will be applied.
            Edges with value less than at least one of thresholds will be hidden.
            Example: {'event_id': 100, user_id: 50}.

            See :ref:`Transition graph user guide<transition_graph_thresholds>` for the details.

        nodes_weight_col : str, optional
            A column name from the :py:class:`.EventstreamSchema` which values control the final
            nodes' weights and displayed diameter as well.

            For each node is calculated:

            - If ``None`` or ``user_id`` - the number of unique users.
            - If ``event_id`` - the number of events.
            - If ``session_id`` - the number of unique sessions.
            - If ``custom_col`` - the number of unique values in selected column.

            See :ref:`Transition graph user guide <transition_graph_weights>` for the details.

        nodes_threshold : dict, optional
            Threshold mapping that defines the minimal weights for nodes displayed on the canvas.

            - Keys should be of type str and contain the weight column names (the values from the
              :py:class:`.EventstreamSchema`).
            - Values of the dict are the thresholds for the nodes that will be displayed.
              They should be of type int or float.

            Support multiple weighting columns. In that case, logical OR will be applied.
            Nodes with value less than at least one of thresholds will be hidden.
            Example: {'event_id': 100, user_id: 50}.

            See :ref:`Transition graph user guide<transition_graph_thresholds>` for the details.

        custom_weight_cols : list of str, optional
            Custom columns from the :py:class:`.EventstreamSchema` that can be selected in ``edges_weight_col``
            and ``nodes_weight_col`` parameters. If ``session_col=session_id`` exists,
            it is added by default to this list.

        targets : dict, optional
            Events mapping that defines which nodes and edges should be colored for better visualization.

            - Possible keys: "positive" (green), "negative" (red), "source" (orange).
            - Possible values: list of events of a given type.

            See :ref:`Transition graph user guide<transition_graph_color_settings>` for the details.

        nodes_custom_colors : dict, optional
            Set nodes color explicitly. The dict keys are node names, the values are the corresponding colors.
            A color can be defined either as an HTML standard color name or a HEX code.
            See :ref:`Transition graph user guide<transition_graph_color_settings>` for the details.

        edges_custom_colors : dict, optional
            Set edges color explicitly. The dict keys are tuples of length 2, e.g. (path_start', 'catalog'),
            the values are the corresponding colors.
            A color can be defined either as an HTML standard color name or a HEX code.
            See :ref:`Transition graph user guide<transition_graph_color_settings>` for the details.

        width : str, int or float, default "100%"
            The width of the plot can be specified in the following ways:

            - In pixels (int or float);
            - In other CSS units (str). For example, the default value of "100%" means the plot will occupy 100%
              of the width of the Jupyter Notebook cell.
        height : str, int or float, default "60vh"
            The height of the plot can be specified as follows:

            - In pixels (int or float);
            - In other CSS units (str). For example, the default value of "60vh" means the plot will occupy 60%
              of the height of the browser window.

            The resulting height can't be lower than 600px.
        show_weights : bool, default True
            Hide/display the edge weight labels. By default, weights are shown.
        show_percents : bool, default False
            Display edge weights as percents. Available only if an edge normalization type is chosen.
            By default, weights are displayed in fractions.
        show_nodes_names : bool, default True
            Hide/display the node names. By default, names are shown.
        show_all_edges_for_targets : bool, default True
            This displaying option allows to ignore the threshold filters and always display
            any edge connected to a target node. By default, all such edges are shown.
        show_nodes_without_links : bool, default False
            Setting a threshold filter might remove all the edges connected to a node.
            Such isolated nodes might be considered as useless. This displaying option
            hides them in the canvas as well.
        show_edge_info_on_hover : bool, default True
            This parameter determines whether information about an edge (weight, source node, target node)
            is displayed when hovering the mouse over it.
        layout_dump : str, default None
            A string path to the JSON file containing the configuration for node positioning.
            This parameter enables applying the saved mutual positioning of nodes, exported as JSON, to the graph.

        Returns
        -------
            Rendered IFrame graph.

        Notes
        -----
        1. If all the edges connected to a node are hidden, the node becomes hidden as well.
           In order to avoid it - use ``show_nodes_without_links=True`` parameter in code or in the interface.
        2. The thresholds may use their own weighting columns both for nodes and for edges independently
           of weighting columns defined in ``edges_weight_col`` and ``nodes_weight_col`` arguments.

        See :doc:`TransitionGraph user guide </user_guides/transition_graph>` for the details.
        """
        if edges_norm_type is None and show_percents:
            raise ValueError("If show_percents=True, edges_norm_type should be 'full' or 'node'!")

        called_params = {
            "edges_norm_type": edges_norm_type,
            "nodes_norm_type": nodes_norm_type,
            "targets": targets,
            "nodes_threshold": nodes_threshold,
            "edges_threshold": edges_threshold,
            "nodes_weight_col": nodes_weight_col,
            "edges_weight_col": edges_weight_col,
            "custom_weight_cols": custom_weight_cols,
            "width": width,
            "height": height,
            "show_weights": show_weights,
            "show_percents": show_percents,
            "show_nodes_names": show_nodes_names,
            "show_all_edges_for_targets": show_all_edges_for_targets,
            "show_nodes_without_links": show_nodes_without_links,
            "show_edge_info_on_hover": show_edge_info_on_hover,
            "layout_dump": layout_dump,
        }
        not_hash_values = ["edges_norm_type", "targets", "width", "height"]

        if layout_dump is not None:
            try:
                self.node_layout = self._load_layout_from_dump(layout_dump)
                self.is_layout_loaded = True
            except Exception:
                warnings.warn(f"Failed to load layout dump")
                self.is_layout_loaded = False

        self.__prepare_graph_for_plot(
            edges_weight_col=edges_weight_col,
            edges_threshold=edges_threshold,
            edges_norm_type=edges_norm_type,
            nodes_norm_type=nodes_norm_type,
            nodes_weight_col=nodes_weight_col,
            nodes_threshold=nodes_threshold,
            targets=targets,
            custom_weight_cols=custom_weight_cols,
        )

        norm_nodes_threshold = (
            self.nodes_thresholds if self.nodes_thresholds else self._get_norm_node_threshold(self.nodes_thresholds)
        )
        norm_links_threshold = (
            self.edges_thresholds if self.edges_thresholds else self._get_norm_link_threshold(self.edges_thresholds)
        )

        node_params = self._make_node_params(targets)
        cols = self.__get_nodelist_cols()

        nodes, links = self._make_template_data(
            node_params=node_params,
            nodes_custom_colors=nodes_custom_colors,
            edges_custom_colors=edges_custom_colors,
        )

        prepared_nodes = self._prepare_nodes_for_plot(node_list=nodes)

        env = Env(
            id=self.env,
            serverId=self.server.pk,
            kernelId=self.server.kernel_id,
            kernelName="",
            libVersion=RETE_VERSION,
        )
        tracker = Tracker(
            hwid=RETE_CONFIG.user.pk,
            scope="transition_graph",
            eventstreamIndex=self.eventstream._eventstream_index,
        )
        edges = Edges(
            items=links,
            normalizations=self.normalizations,
            selectedNormalizationId=self.render_edge_norm_type,
            columns=prepared_nodes["columns"],
            threshold=self.edges_thresholds,
            selectedThresholdColumnId=self.edges_weight_col,
            selectedWeightsColumnId=self.edges_weight_col,
        )
        settings = Settings(
            showEdgesWeightsOnCanvas=show_weights,
            convertWeightsToPercents=show_percents,
            doNotFilterTargetNodes=show_all_edges_for_targets,
            showEdgesInfoOnHover=show_edge_info_on_hover,
            showNodesNamesOnCanvas=show_nodes_names,
            showNodesWithoutEdges=show_nodes_without_links,
        )

        init_params = SerializedState(
            env=env,
            tracker=tracker,
            useLayoutDump=self.is_layout_loaded,
            nodes=prepared_nodes,
            edges=edges,
            settings=settings,
        )

        widget_id = self.generateId()

        valid_width: str = f"{width}px" if isinstance(width, (int, float)) else width
        valid_height: str = f"{height}px" if isinstance(height, (int, float)) else height
        display(
            HTML(
                self.render.show(
                    widget_id=widget_id,
                    script_url=f"{self.graph_url}?id={widget_id}",
                    style=f"width: 100%; width: {valid_width}; height: 60vh; height: {valid_height}; min-height: 600px; box-sizing: border-box;",
                    state=json.dumps(asdict(init_params)),
                )
            )
        )
        collect_data_performance(
            scope="transition_graph",
            event_name="metadata",
            called_params=called_params,
            not_hash_values=not_hash_values,
            performance_data={"unique_nodes": len(nodes), "unique_links": len(links)},
            eventstream_index=self.eventstream._eventstream_index,
        )

    def _prepare_nodes_for_plot(self, node_list: list[NodeItem]) -> Nodes:
        columns = [Column(id=col, name=col) for col in self.weight_cols]
        nodes = Nodes(
            normalizations=self.normalizations,
            selectedNormalizationId=self.render_edge_norm_type,
            items=node_list,
            columns=columns,
            threshold=self.nodes_thresholds,
            selectedThresholdColumnId=self.nodes_weight_col,
            selectedWeightsColumnId=self.nodes_weight_col,
            targets=self.allowed_targets,
            defaultColumnId=self.nodelist_default_col,
            sortField="name",
            sortOrder="asc",
        )
        return nodes

    def __prepare_graph_for_plot(
        self,
        edges_weight_col: str | None = None,
        edges_threshold: Threshold | None = None,
        nodes_weight_col: str | None = None,
        nodes_threshold: Threshold | None = None,
        edges_norm_type: NormType | None = None,
        nodes_norm_type: NormType | None = None,
        targets: MutableMapping[str, str | None] | None = None,
        custom_weight_cols: list[str] | None = None,
    ) -> None:
        if targets:
            self.targets = targets
        self.edges_norm_type = edges_norm_type
        if nodes_threshold is None:
            nodes_threshold = {"user_id": 0.0, "event_id": 0.0}
        self.nodes_thresholds = nodes_threshold
        if edges_threshold is None:
            edges_threshold = {"user_id": 0.0, "event_id": 0.0}
        self.edges_thresholds = edges_threshold
        self.nodelist_default_col = self.eventstream.schema.event_id
        self.edgelist_default_col = self.eventstream.schema.event_id
        self.targets = targets if targets else {"positive": None, "negative": None, "source": None}
        self.weight_cols = self._define_weight_cols(custom_weight_cols)
        self.nodes_weight_col = nodes_weight_col if nodes_weight_col else self.eventstream.schema.user_id
        self.edges_weight_col = edges_weight_col if edges_weight_col else self.eventstream.schema.user_id

        self.nodes_norm_type = nodes_norm_type
        self.nodelist: Nodelist = Nodelist(
            weight_cols=self.weight_cols,
            time_col=self.event_time_col,
            event_col=self.event_col,
        )
        self.nodelist.calculate_nodelist(data=self.eventstream.to_dataframe())
        self.edges_norm_type: NormType | None = edges_norm_type
        self.edgelist: Edgelist = Edgelist(eventstream=self.eventstream)
        self.edgelist.calculate_edgelist(
            weight_cols=self.weight_cols,
            norm_type=self.edges_norm_type,
        )

    def _get_option(self, name: str, settings: dict[str, Any]) -> str:
        if name in settings:
            return self._to_json(settings[name])
        return "undefined"
