from __future__ import annotations

import uuid
from typing import Any, Optional, Type, Union

from retentioneering.data_processor import DataProcessor
from retentioneering.data_processor.registry import dataprocessor_registry
from retentioneering.eventstream.types import EventstreamType
from retentioneering.params_model.registry import params_model_registry


class BaseNode:
    processor: Optional[DataProcessor]
    events: Optional[EventstreamType]
    pk: str

    def __init__(self, **kwargs: Any) -> None:
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
    events: EventstreamType

    def __init__(self, source: EventstreamType) -> None:
        super().__init__()
        self.events = source


class EventsNode(BaseNode):
    """
    A class for a regular node of a preprocessing graph
    """

    processor: DataProcessor
    events: Optional[EventstreamType]

    def __init__(self, processor: DataProcessor) -> None:
        super().__init__()
        self.processor = processor
        self.events = None

    def calc_events(self, parent: EventstreamType) -> None:
        self.events = self.processor.apply(parent)


class MergeNode(BaseNode):
    """
    A class for a merging node of a preprocessing graph
    """

    events: Optional[EventstreamType]

    def __init__(self) -> None:
        super().__init__()
        self.events = None


Node = Union[SourceNode, EventsNode, MergeNode]
nodes = {
    "MergeNode": MergeNode,
    "EventsNode": EventsNode,
    "SourceNode": SourceNode,
}


class NotFoundDataprocessor(Exception):
    pass


def build_node(node_name: str, processor_name: str, processor_params: dict[str, Any] | None = None) -> Node | None:
    if node_name == "SourceNode":
        return None

    _node = nodes[node_name]
    node_kwargs = {}
    if processor_name:
        _params_model_registry = params_model_registry.get_registry()
        _dataprocessor_registry = dataprocessor_registry.get_registry()

        _processor: Type[DataProcessor] = _dataprocessor_registry[processor_name]  # type: ignore
        params_name = _processor.__annotations__["params"]
        _params_model = _params_model_registry[params_name]
        params_model = _params_model(**processor_params)

        processor: DataProcessor = _processor(params=params_model)

        node_kwargs["processor"] = processor

    node = _node(**node_kwargs)
    return node
