from __future__ import annotations

from typing import Any, Callable, List

import pandas as pd
from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema
from src.params_model import ParamsModel
from src.widget.widgets import ReteFunction

EventstreamFilter = Callable[[DataFrame, EventstreamSchema], Any]


def _default_func_negative(eventstream: Eventstream, negative_target_events: list[str]) -> pd.DataFrame:
    # TODO добавить доку в тесты test_negative_target__export tests/p_graph/test_export.py
    """
    Filters rows with target events from the input eventstream on the base

    Parameters
    ----------
    eventstream : Eventstream
        Source eventstream or output from previous nodes

    negative_target_events : list[str]
        Each event from that list is associated with the bad result (scenario)
        of user behaviour (experience) in the product
        If there are several target events in user path - the event with minimum timestamp is taken

    Returns
    -------
    Filtered DataFrame

    Return type
    ----------
    pd.DataFrame

    """
    user_col = eventstream.schema.user_id
    time_col = eventstream.schema.event_timestamp
    event_col = eventstream.schema.event_name
    df = eventstream.to_dataframe()

    negative_events_index = (
        df[df[event_col].isin(negative_target_events)].groupby(user_col)[time_col].idxmin()  # type: ignore
    )
    return df.iloc[negative_events_index]  # type: ignore


class NegativeTargetParams(ParamsModel):
    negative_target_events: List[str]
    negative_function: Callable = _default_func_negative

    _widgets = {"negative_function": ReteFunction}


class NegativeTarget(DataProcessor):
    """
    Creates new synthetic events for users who have had specified event(s) in their paths

    Parameters
    ----------
    negative_target_events : List(str)
        Each event from that list is associated with the negative user behaviour in the product
        If there are several target events in user path - the event with minimum timestamp is taken

    negative_function : Callable, default=_default_func_negative
        Filter rows with target events from the input eventstream

    Note
    -------



    Returns
    -------
    Eventstream with new synthetic events for users who fit the conditions (details in the table below)

        +--------------------------------------+------------------+-----------------------------------------+
        | event_name                           | event_type       | timestamp                               |
        +--------------------------------------+------------------+-----------------------------------------+
        | negative_target_ORIGINAL_EVENT_NAME  | negative_target  | min(timestamp(negative_target_events))  |
        +--------------------------------------+------------------+-----------------------------------------+


    See Also
    -------
    """

    params: NegativeTargetParams

    def __init__(self, params: NegativeTargetParams):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name

        negative_function = self.params.negative_function
        negative_target_events = self.params.negative_target_events

        negative_targets = negative_function(eventstream, negative_target_events)
        negative_targets[type_col] = "negative_target"
        negative_targets[event_col] = "negative_target_" + negative_targets[event_col]
        negative_targets["ref"] = None

        eventstream = Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=negative_targets,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )
        return eventstream
