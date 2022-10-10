from typing import Any, Callable

import pandas as pd
from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema
from src.params_model import ParamsModel

EventstreamFilter = Callable[[DataFrame, EventstreamSchema], Any]


class FilterEventsParams(ParamsModel):
    filter: Callable[[DataFrame, EventstreamSchema], Any]


class FilterEvents(DataProcessor):
    params: FilterEventsParams

    def __init__(self, params: FilterEventsParams):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        filter_: Callable[[DataFrame, EventstreamSchema], Any] = self.params.filter  # type: ignore
        events: pd.DataFrame = eventstream.to_dataframe()
        mask = filter_(events, eventstream.schema)
        events_to_delete = events[~mask]

        with pd.option_context('mode.chained_assignment', None):
            events_to_delete["ref"] = events_to_delete[eventstream.schema.event_id]

        eventstream = Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=events_to_delete,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )
        eventstream.soft_delete(eventstream.to_dataframe())

        return eventstream
