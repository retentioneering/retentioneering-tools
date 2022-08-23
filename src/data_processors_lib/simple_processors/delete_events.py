from typing import Any, Callable

import pandas as pd
from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema
from src.params_model import ParamsModel

EventstreamFilter = Callable[[DataFrame, EventstreamSchema], Any]


class DeleteEventsParams(ParamsModel):
    filter: Callable[[DataFrame, EventstreamSchema], Any]


class DeleteEvents(DataProcessor):
    params: DeleteEventsParams

    def __init__(self, params: DeleteEventsParams):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        filter_: Callable[[DataFrame, EventstreamSchema], Any] = self.params.filter  # type: ignore
        events: pd.DataFrame = eventstream.to_dataframe()
        mathed_events_q = filter_(events, eventstream.schema)
        matched_events = events[mathed_events_q].copy()

        matched_events["ref"] = matched_events[eventstream.schema.event_id]

        eventstream = Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=matched_events,
            relations=[{"raw_col": "ref", "evenstream": eventstream}],
        )
        eventstream.soft_delete(eventstream.to_dataframe())
        return eventstream
