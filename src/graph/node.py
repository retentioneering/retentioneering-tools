from __future__ import annotations

from typing import Any, Optional, Type, Union

from src.data_processor.data_processor import DataProcessor
from src.eventstream import Eventstream


def get_registry_dataprocessor(processor_name: str) -> Type[DataProcessor]:
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
        "MergeNode": MergeNode,
        "EventsNode": EventsNode,
        "SourceNode": SourceNode,
    }
    _node = nodes[node_name]
    node_kwargs = {}
    if processor_name:
        _processor: Type[DataProcessor] = get_registry_dataprocessor(processor_name)
        processor: DataProcessor = _processor(params=processor_params)  # type: ignore
        node_kwargs["processor"] = processor

    node = _node(**node_kwargs)
    return node
