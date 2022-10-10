from __future__ import annotations

import json
from itertools import chain
from typing import List, Optional, cast

import networkx
from IPython.display import HTML, DisplayHandle, display

from src.backend import JupyterServer, ServerManager
from src.backend.callback import list_dataprocessor
from src.eventstream.eventstream import Eventstream
from src.graph.nodes import EventsNode, MergeNode, Node, SourceNode
from src.templates import PGraphRenderer


class PGraph:
    root: SourceNode
    _ngraph: networkx.DiGraph
    __server_manager: ServerManager | None = None
    __server: JupyterServer | None = None

    def __init__(self, source_stream: Eventstream) -> None:
        self.root = SourceNode(source=source_stream)
        self._ngraph = networkx.DiGraph()
        self._ngraph.add_node(self.root)

    def add_node(self, node: Node, parents: List[Node]) -> None:
        self.__valiate_already_exists(node)
        self.__validate_not_found(parents)

        if node.events is not None:
            self.__validate_schema(node.events)

        if not isinstance(node, MergeNode) and len(parents) > 1:
            raise ValueError("multiple parents are only allowed for merge nodes!")

        self._ngraph.add_node(node)

        for parent in parents:
            self._ngraph.add_edge(parent, node)

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

        for parent in self._ngraph.predecessors(node):
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
        if node in self._ngraph.nodes:
            raise ValueError("node already exists!")

    def __validate_not_found(self, nodes: List[Node]) -> None:
        for node in nodes:
            if node not in self._ngraph.nodes:
                raise ValueError("node not found!")

    def display(self) -> DisplayHandle:
        if not self.__server_manager:
            self.__server_manager = ServerManager()

        if not self.__server:
            self.__server = self.__server_manager.create_server()
            self.__server.register_action("list-dataprocessor", list_dataprocessor)

        render = PGraphRenderer()
        return display(HTML(render.show(server_id=self.__server.pk, env=self.__server_manager.check_env())))

    def export(self) -> dict:
        source, target, link = "source", "target", "links"
        graph = self._ngraph
        data = {
            "directed": graph.is_directed(),
            "nodes": [n.export() for n in graph],
            link: [dict(chain(d.items(), [(source, u), (target, v)])) for u, v, d in graph.edges(data=True)],
        }
        return data

    def _export_to_json(self) -> str:
        data = self.export()
        return json.dumps(data)
