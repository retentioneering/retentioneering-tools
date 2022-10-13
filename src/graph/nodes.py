from __future__ import annotations

import uuid
from typing import Any, Optional, Type, Union

from src.data_processor.data_processor import DataProcessor
from src.eventstream import Eventstream


def get_registry_dataprocessor(processor_name: str) -> Type[DataProcessor]:
    """
    mock function!!!
    :return:
    """
    return DataProcessor


class BaseNode:
    processor: Optional[DataProcessor]
    events: Optional[Eventstream]
    pk: str

    def __init__(self, **kwargs) -> None:
        self.pk = str(uuid.uuid4())

    def __str__(self) -> str:
        data = {"name": self.__class__.__name__, "pk": self.pk}
        return str(data)

    __repr__ = __str__

    def export(self) -> dict:
        data: dict[str, Any] = {"name": self.__class__.__name__, "pk": self.pk}
        if processor := getattr(self, "processor", None):
            data["processor"] = processor.to_dict()
        return data


class SourceNode(BaseNode):
    events: Eventstream

    def __init__(self, source: Eventstream) -> None:
        super().__init__()
        self.events = source


class EventsNode(BaseNode):
    processor: DataProcessor
    events: Optional[Eventstream]

    def __init__(self, processor: DataProcessor) -> None:
        super().__init__()
        self.processor = processor
        self.events = None

    def calc_events(self, parent: Eventstream):
        self.events = self.processor.apply(parent)


class MergeNode(BaseNode):
    events: Optional[Eventstream]

    def __init__(self) -> None:
        super().__init__()
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
