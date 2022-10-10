from __future__ import annotations

from typing import Optional, Union, Any, Type

from src.data_processor.data_processor import DataProcessor
from src.eventstream import Eventstream


def get_registry(processor_name: str) -> Type[DataProcessor]:
    """
    mock function!!!
    :return:
    """
    return DataProcessor


class SourceNode:
    events: Eventstream

    def __init__(self, source: Eventstream) -> None:
        self.events = source


class EventsNode:
    processor: DataProcessor
    events: Optional[Eventstream]

    def __init__(self, processor: DataProcessor) -> None:
        self.processor = processor
        self.events = None

    def calc_events(self, parent: Eventstream):
        self.events = self.processor.apply(parent)


class MergeNode:
    events: Optional[Eventstream]

    def __init__(self) -> None:
        self.events = None


Node = Union[SourceNode, EventsNode, MergeNode]


def build_node(node_name: str, processor_name: str | None, processor_params: dict[str, Any] | None) -> Node:
    nodes = {
        'MergeNode': MergeNode,
        'EventsNode': EventsNode,
        'SourceNode': SourceNode,
    }
    _node = nodes.get(node_name)
    node_kwargs = {}
    if processor_name:
        _processor = get_registry(processor_name)
        processor = _processor(**processor_params)
        node_kwargs['processor'] = processor

    node = _node(**node_kwargs)
    return node
