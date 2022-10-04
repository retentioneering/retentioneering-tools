from typing import Optional, Union

from src.data_processor.data_processor import DataProcessor
from src.eventstream import Eventstream


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
