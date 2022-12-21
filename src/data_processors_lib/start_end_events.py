from __future__ import annotations

import pandas as pd
from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.eventstream.types import EventstreamType
from src.params_model import ParamsModel


class StartEndEventsParams(ParamsModel):
    pass


class StartEndEvents(DataProcessor):
    """
    Creates two synthetic events in each user's path:
    ``path_start`` and ``path_end``.

    Returns
    -------
    Eventstream
        ``Eventstream`` with new synthetic events only - two for each user:

        +----------------+----------------+----------------+
        | **event_name** | **event_type** | **timestamp**  |
        +----------------+----------------+----------------+
        | path_start     | path_start     | first_event    |
        +----------------+----------------+----------------+
        | path_end       | path_end       | last_event     |
        +----------------+----------------+----------------+


    """

    params: StartEndEventsParams

    def __init__(self, params: StartEndEventsParams) -> None:
        super().__init__(params=params)

    def apply(self, eventstream: EventstreamType) -> EventstreamType:
        from src.eventstream.eventstream import Eventstream

        events: DataFrame = eventstream.to_dataframe(copy=True)
        user_col = eventstream.schema.user_id
        time_col = eventstream.schema.event_timestamp
        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name

        matched_events_start: DataFrame = events.groupby(user_col, as_index=False)[time_col].min()  # type: ignore
        matched_events_start[type_col] = "path_start"
        matched_events_start[event_col] = "path_start"
        matched_events_start["ref"] = None

        matched_events_end: DataFrame = events.groupby(user_col, as_index=False)[time_col].max()  # type: ignore
        matched_events_end[type_col] = "path_end"
        matched_events_end[event_col] = "path_end"
        matched_events_end["ref"] = None

        matched_events = pd.concat([matched_events_start, matched_events_end])

        eventstream = Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=matched_events,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )
        return eventstream