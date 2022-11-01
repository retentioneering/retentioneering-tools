from __future__ import annotations

import pandas as pd
from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.eventstream.eventstream import Eventstream
from src.params_model import ParamsModel


class StartEndEventsParams(ParamsModel):
    pass


class StartEndEvents(DataProcessor):
    """
    Creates two synthetic events in each user's path:
    ``start`` and ``end``

    Returns
    -------
    Eventstream
        Eventstream with new synthetic events - two for each user:

        +----------------+----------------+----------------+
        | **event_name** | **event_type** | **timestamp**  |
        +----------------+----------------+----------------+
        | start          | start          | first_event    |
        +----------------+----------------+----------------+
        | end            | end            | last_event     |
        +----------------+----------------+----------------+

    Note
    -------
    Output Eventstream could be added to the original Eventstream using :py:func:`src.graph.p_graph.PGraph.combine`

    See Also
    -------
    :py:func:`src.graph.p_graph.PGraph`
    :py:func:`src.graph.p_graph.EventsNode`
    :py:func:`src.graph.p_graph.PGraph.add_node`
    :py:func:`src.graph.p_graph.PGraph.combine`
    """

    params: StartEndEventsParams

    def __init__(self, params: StartEndEventsParams) -> None:
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        events: DataFrame = eventstream.to_dataframe(copy=True)
        user_col = eventstream.schema.user_id
        time_col = eventstream.schema.event_timestamp
        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name

        matched_events_start: DataFrame = events.groupby(user_col, as_index=False)[time_col].min()  # type: ignore
        matched_events_start[type_col] = "start"
        matched_events_start[event_col] = "start"
        matched_events_start["ref"] = None

        matched_events_end: DataFrame = events.groupby(user_col, as_index=False)[time_col].max()  # type: ignore
        matched_events_end[type_col] = "end"
        matched_events_end[event_col] = "end"
        matched_events_end["ref"] = None

        matched_events = pd.concat([matched_events_start, matched_events_end])

        eventstream = Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=matched_events,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )
        return eventstream
