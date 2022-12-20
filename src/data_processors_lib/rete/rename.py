from __future__ import annotations

from typing import List

from pydantic.dataclasses import dataclass

from src.data_processor.data_processor import DataProcessor, ParamsModel
from src.eventstream.types import EventstreamType


@dataclass
class RenameRule:
    group_name: str
    child_events: List[str]


class RenameParams(ParamsModel):
    """
    [{
        "group_name": "some_group",
        "child_events": ["eventA", "eventB"]
    }]
    """

    rules: List[RenameRule]


class RenameProcessor(DataProcessor):
    params: RenameParams

    def __init__(self, params: RenameParams):
        super().__init__(params=params)

    def apply(self, eventstream: EventstreamType) -> EventstreamType:
        from src.eventstream.eventstream import Eventstream

        events = eventstream.to_dataframe(copy=True)
        event_col = eventstream.schema.event_name

        rename_rules: dict[str, str] = dict()
        for rule in self.params.rules:
            to_ = rule.group_name
            for from_ in rule.child_events:
                rename_rules[from_] = to_

        affected_names = list(rename_rules.keys())
        affected_events = events[events[event_col].isin(affected_names)]
        affected_events = events.replace(rename_rules)
        affected_events["ref"] = events[eventstream.schema.event_id]

        eventstream = Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=affected_events,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )
        return eventstream
