from __future__ import annotations

from typing import List, Optional, TypedDict, cast

import networkx
from IPython.display import HTML, DisplayHandle, display

from src.backend import JupyterServer, ServerManager
from src.backend.callback import list_dataprocessor
from src.eventstream.eventstream import Eventstream
from src.graph.node import EventsNode, MergeNode, Node, SourceNode, build_node
from src.templates import PGraphRenderer


class NodeData(TypedDict):
    name: str
    pk: str
    processor: Optional[dict]


class NodeLink(TypedDict):
    source: NodeData
    target: NodeData


class Payload(TypedDict):
    nodes: list[NodeData]
    links: list[NodeLink]


class PGraph:
    root: SourceNode
    __ngraph: networkx.DiGraph
    __server_manager: ServerManager | None = None
    __server: JupyterServer | None = None

    def __init__(self, source_stream: Eventstream) -> None:
        self.root = SourceNode(source=source_stream)
        self.__ngraph = networkx.DiGraph()
        self.__ngraph.add_node(self.root)

    def add_node(self, node: Node, parents: List[Node]) -> None:
        self.__valiate_already_exists(node)
        self.__validate_not_found(parents)

        if node.events is not None:
            self.__validate_schema(node.events)

        if not isinstance(node, MergeNode) and len(parents) > 1:
            raise ValueError("multiple parents are only allowed for merge nodes!")

        self.__ngraph.add_node(node)

        for parent in parents:
            self.__ngraph.add_edge(parent, node)

    def combine(self, node: Node) -> Eventstream:
        self.__validate_not_found([node])

        if isinstance(node, SourceNode):
            return node.events.copy()

        if isinstance(node, EventsNode):
            return self.combine_events_node(node)

        return self.combine_merge_node(node)

    def combine_events_node(self, node: EventsNode) -> Eventstream:
        parent = self.get_events_node_parent(node)
        parent_events = self.combine(parent)
        events = node.processor.apply(parent_events)
        parent_events.join_eventstream(events)
        return parent_events

    def combine_merge_node(self, node: MergeNode) -> Eventstream:
        parents = self.get_merge_node_parents(node)
        curr_eventstream: Optional[Eventstream] = None

        for parent_node in parents:
            if curr_eventstream is None:
                curr_eventstream = self.combine(parent_node)
            else:
                new_eventstream = self.combine(parent_node)
                curr_eventstream.append_eventstream(new_eventstream)

        node.events = curr_eventstream

        return cast(Eventstream, curr_eventstream)

    def get_parents(self, node: Node) -> List[Node]:
        self.__validate_not_found([node])
        parents: List[Node] = []

        for parent in self.__ngraph.predecessors(node):
            parents.append(parent)
        return parents

    def get_merge_node_parents(self, node: MergeNode) -> List[Node]:
        parents = self.get_parents(node)
        if len(parents) == 0:
            raise ValueError("orphan merge node!")

        return parents

    def get_events_node_parent(self, node: EventsNode) -> Node:
        parents = self.get_parents(node)
        if len(parents) > 1:
            raise ValueError("invalid graph: events node has more than 1 parent")

        return parents[0]

    def __validate_schema(self, eventstream: Eventstream) -> bool:
        return self.root.events.schema.is_equal(eventstream.schema)

    def __valiate_already_exists(self, node: Node) -> None:
        if node in self.__ngraph.nodes:
            raise ValueError("node already exists!")

    def __validate_not_found(self, nodes: List[Node]) -> None:
        for node in nodes:
            if node not in self.__ngraph.nodes:
                raise ValueError("node not found!")

    def display(self) -> DisplayHandle:
        if not self.__server_manager:
            self.__server_manager = ServerManager()

        if not self.__server:
            self.__server = self.__server_manager.create_server()
            self.__server.register_action("list-dataprocessor", list_dataprocessor)

        render = PGraphRenderer()
        return display(HTML(render.show(server_id=self.__server.pk, env=self.__server_manager.check_env())))

    def _set_graph(self, payload: Payload) -> None:
        """
        Payload example:

        {
            "nodes": [
                {
                    "name": "SourceNode",
                    "pk": "0dc3b706-e6cc-401e-96f7-6a45d3947d5c"
                },
                {
                    "name": "EventsNode",
                    "pk": "07921cb0-60b8-45af-928d-272d1b622b25",
                    "processor": {
                        "name": "SimpleGroup",
                        "values": {"event_name": "add_to_cart", "event_type": "group_alias"},
                    },
                },
                {
                    "name": "EventsNode",
                    "pk": "114251ae-0f03-45e6-a163-af51bb02dfd5",
                    "processor": {
                        "name": "SimpleGroup",
                        "values": {"event_name": "logout", "event_type": "group_alias"},
                    },
                },
            ],
            "links": [
                {
                    'source': {'name': 'SourceNode', 'pk': '0dc3b706-e6cc-401e-96f7-6a45d3947d5c'},
                    'target': {'name': 'EventsNode', 'pk': '07921cb0-60b8-45af-928d-272d1b622b25'}
                },
                {
                    'source': {'name': 'EventsNode', 'pk': '07921cb0-60b8-45af-928d-272d1b622b25'},
                    'target': {'name': 'EventsNode', 'pk': '114251ae-0f03-45e6-a163-af51bb02dfd5'}
                }
            ]
        }

        """
        for node in payload["nodes"]:
            # node: dict[str, str | dict[str, Any]]
            node_pk: str = node["pk"]
            if actual_node := self._find_node(pk=node_pk):
                if getattr(actual_node, "processor", None):
                    actual_node.processor.params(**node["processor"])  # type: ignore
            else:
                actual_node = build_node(
                    node_name=node["name"],
                    processor_name=node.get("processor", {}).get("name", None),  # type: ignore
                    processor_params=node.get("processor", {}).get("values", None),  # type: ignore
                )
                parents, child = self._find_linked_nodes(target_node=node_pk, link_list=payload["links"])
                self.insert_node(parents=parents, child=child, node=actual_node)

    def insert_node(self, parents: list[Node], child: Node, node: Node) -> None:
        [self.__ngraph.remove_edge(parent, child) for parent in parents]
        self.add_node(node=node, parents=parents)
        self.__ngraph.add_edge(node, child)

    def _find_linked_nodes(self, target_node: str, link_list: list[NodeLink]) -> tuple[list[Node], Node]:
        parents: list[str] = []
        child: str = ""
        for node in link_list:
            if node["source"]["pk"] == target_node:
                parents.append(node["source"]["pk"])

            if node["target"]["pk"] == target_node:
                child = node["target"]["pk"]
        parent_nodes = [self._find_node(parent) for parent in parents]
        child_node = self._find_node(child)
        return parent_nodes, child_node  # type: ignore

    def _find_node(self, pk: str) -> Node | None:
        for node in self.__ngraph:
            if node.pk == pk:
                return node
        else:
            return None
