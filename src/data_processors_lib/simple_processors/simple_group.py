import pandas as pd
from typing import Any, Callable, Optional

from src.data_processor.data_processor import DataProcessor
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema
from src.params_model import ParamsModel

EventstreamFilter = Callable[[pd.DataFrame, EventstreamSchema], Any]


class GroupEventsParams(ParamsModel):
    event_name: str
    filter: EventstreamFilter
    event_type: Optional[str] = "group_alias"


class GroupEvents(DataProcessor):
    params: GroupEventsParams

    def __init__(self, params: GroupEventsParams) -> None:
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        event_name = self.params.event_name
        filter_: Callable = self.params.filter
        event_type = self.params.event_type

        events = eventstream.to_dataframe()
        mask = filter_(events, eventstream.schema)
        matched_events = events[mask]

        with pd.option_context('mode.chained_assignment', None):
            if event_type is not None:
                matched_events[eventstream.schema.event_type] = event_type

            matched_events[eventstream.schema.event_name] = event_name
            matched_events["ref"] = matched_events[eventstream.schema.event_id]

        return Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=matched_events,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )
