from __future__ import annotations

from typing import TypedDict

from src.data_processor.data_processor import DataProcessor, ParamsModel
from src.eventstream.types import EventstreamType


class RenameRule(TypedDict):
    group_name: str
    child_events: list[str]


class MergeParams(ParamsModel):
    """
    [{
        "group_name": "some_group",
        "child_events": ["eventA", "eventB"]
    }]
    Ну и как мне это сделать? Тройной уровень вложенности!
    """

    rules: list[RenameRule]


class MergeProcessor(DataProcessor):
    params: MergeParams

    def __init__(self, params: MergeParams):
        super().__init__(params=params)

    def apply(self, eventstream: EventstreamType) -> EventstreamType:
        from src.eventstream.eventstream import Eventstream

        events = eventstream.to_dataframe(copy=True)
        events["ref"] = events[eventstream.schema.event_id]

        eventstream = Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=events,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )
        return eventstream
