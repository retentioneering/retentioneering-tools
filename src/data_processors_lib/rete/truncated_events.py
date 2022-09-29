from __future__ import annotations

from typing import Optional, Tuple

import numpy as np
import pandas as pd
from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.data_processors_lib.rete.constants import DATETIME_UNITS
from src.eventstream.eventstream import Eventstream
from src.params_model import ParamsModel


class TruncatedParams(ParamsModel):
    left_truncated_cutoff: Optional[Tuple[float, DATETIME_UNITS]]
    right_truncated_cutoff: Optional[Tuple[float, DATETIME_UNITS]]


class TruncatedEvents(DataProcessor):
    params: TruncatedParams

    def __init__(self, params: TruncatedParams):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        events: DataFrame = eventstream.to_dataframe(copy=True)
        user_col = eventstream.schema.user_id
        time_col = eventstream.schema.event_timestamp
        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name

        left_truncated_cutoff, left_truncated_unit = None, None
        right_truncated_cutoff, right_truncated_unit = None, None

        if self.params.left_truncated_cutoff:
            left_truncated_cutoff, left_truncated_unit = self.params.left_truncated_cutoff

        if self.params.right_truncated_cutoff:
            right_truncated_cutoff, right_truncated_unit = self.params.right_truncated_cutoff
        truncated_events = pd.DataFrame()

        if not left_truncated_cutoff and not right_truncated_cutoff:
            raise ValueError("Either left_truncated_cutoff or right_truncated_cutoff must be specified!")

        userpath = (
            events.groupby(user_col)[time_col].agg([np.min, np.max]).rename(columns={"amin": "start", "amax": "end"})
        )

        if left_truncated_cutoff:
            timedelta = (userpath["end"] - events[time_col].min()) / np.timedelta64(1, left_truncated_unit)
            left_truncated_events = (
                userpath[timedelta < left_truncated_cutoff][["start"]]
                .rename(columns={"start": time_col})  # type: ignore
                .reset_index()
            )
            left_truncated_events[event_col] = "truncated_left"
            left_truncated_events[type_col] = "truncated_left"
            left_truncated_events["ref"] = None
            truncated_events = pd.concat([truncated_events, left_truncated_events])

        if right_truncated_cutoff:
            timedelta = (events[time_col].max() - userpath["start"]) / np.timedelta64(1, right_truncated_unit)
            right_truncated_events = (
                userpath[timedelta < right_truncated_cutoff][["end"]]
                .rename(columns={"end": time_col})  # type: ignore
                .reset_index()
            )
            right_truncated_events[event_col] = "truncated_right"
            right_truncated_events[type_col] = "truncated_right"
            right_truncated_events["ref"] = None
            truncated_events = pd.concat([truncated_events, right_truncated_events])

        eventstream = Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=truncated_events,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )
        return eventstream