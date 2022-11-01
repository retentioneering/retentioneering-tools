from __future__ import annotations

from typing import Any, Callable, List

import pandas as pd
from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema
from src.params_model import ParamsModel
from src.widget.widgets import ListOfString, ReteFunction

EventstreamFilter = Callable[[DataFrame, EventstreamSchema], Any]


def _default_func_negative(eventstream, negative_target_events) -> pd.DataFrame:
    user_col = eventstream.schema.user_id
    time_col = eventstream.schema.event_timestamp
    event_col = eventstream.schema.event_name
    df = eventstream.to_dataframe()

    negative_events_index = df[df[event_col].isin(negative_target_events)].groupby(user_col)[time_col].idxmin()

    return df.iloc[negative_events_index]


class NegativeTargetParams(ParamsModel):
    negative_target_events: List[str]
    negative_function: Callable = _default_func_negative

    _widgets = {"negative_function": ReteFunction, "negative_target_events": ListOfString}


class NegativeTarget(DataProcessor):
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
