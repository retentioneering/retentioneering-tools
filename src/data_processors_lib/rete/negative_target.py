from __future__ import annotations

import logging
from typing import Any, Callable, List

import pandas as pd
from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema
from src.params_model import ParamsModel

log = logging.getLogger(__name__)
EventstreamFilter = Callable[[DataFrame, EventstreamSchema], Any]


def _default_func_negative(eventstream, negative_target_events):
    user_col = eventstream.schema.user_id
    time_col = eventstream.schema.event_timestamp
    event_col = eventstream.schema.event_name
    df = eventstream.to_dataframe(copy=True)

    data_neg = df[df[event_col].isin(negative_target_events)]
    data_neg = (
        data_neg.groupby(user_col, as_index=False)
        .apply(lambda group: group.nsmallest(1, columns=time_col))
        .reset_index(drop=True)
    )

    return data_neg


class NegativeTargetParams(ParamsModel):
    negative_target_events: List[str]
    negative_function: Callable = _default_func_negative


class NegativeTarget(DataProcessor):
    params: NegativeTargetParams

    def __init__(self, params: NegativeTargetParams):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name

        negative_function = self.params.negative_function
        negative_target_events = self.params.negative_target_events

        df = eventstream.to_dataframe(copy=True)

        negative_targets = negative_function(eventstream, negative_target_events)
        negative_targets[type_col] = "negative_target"
        negative_targets[event_col] = "negative_target_" + negative_targets[event_col]

        negative_targets["event_type"] = "negative_target"
        negative_targets["ref"] = negative_targets[eventstream.schema.event_id]
        df = pd.concat([df, negative_targets])

        eventstream = Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=df,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )
        return eventstream
