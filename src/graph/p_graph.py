from typing import List, Optional, Union, cast

import networkx

from src.data_processor.data_processor import ReteDataProcessor
from src.eventstream.eventstream import Eventstream


class SourceNode:
    events: Eventstream

    def __init__(self, source: Eventstream) -> None:
        self.events = source


class EventsNode:
    processor: ReteDataProcessor
    events: Optional[Eventstream]

    def __init__(self, processor: ReteDataProcessor) -> None:
        self.processor = processor
        self.events = None

    def calc_events(self, parent: Eventstream):
        self.events = self.processor.apply(parent)


class MergeNode:
    events: Optional[Eventstream]

    def __init__(self) -> None:
        self.events = None


Node = Union[SourceNode, EventsNode, MergeNode]


class PGraph:
    root: SourceNode
    __ngraph: networkx.DiGraph

    def __init__(self, source_stream: Eventstream) -> None:
        self.root = SourceNode(source=source_stream)
        self.__ngraph = networkx.DiGraph()
        self.__ngraph.add_node(self.root)

    def add_node(self, node: Node, parents: List[Node]) -> None:
        self.__valiate_already_exists(node)
        self.__validate_not_found(parents)

        if node.events is not None:
            self.__validate_schema(node.events)

        if (not isinstance(node, MergeNode) and len(parents) > 1):
            raise ValueError(
                "multiple parents are only allowed for merge nodes!")

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
        if (len(parents) == 0):
            raise ValueError("orphan merge node!")

        return parents

    def get_events_node_parent(self, node: EventsNode) -> Node:
        parents = self.get_parents(node)
        if len(parents) > 1:
            raise ValueError(
                "invalid graph: events node has more than 1 parent")

        return parents[0]

    def __validate_schema(self, eventstream: Eventstream) -> bool:
        return self.root.events.schema.is_equal(eventstream.schema)

    def __valiate_already_exists(self, node: Node) -> None:
        if (node in self.__ngraph.nodes):
            raise ValueError("node already exists!")

    def __validate_not_found(self, nodes: List[Node]) -> None:
        for node in nodes:
            if (node not in self.__ngraph.nodes):
                raise ValueError("node not found!")
