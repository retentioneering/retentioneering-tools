from __future__ import annotations

import logging
from typing import Any, Callable, Optional, Tuple

import pandas as pd
from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.data_processors_lib.rete.constants import UOM_DICT
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema
from src.params_model import ParamsModel

log = logging.getLogger(__name__)

EventstreamFilter = Callable[[DataFrame, EventstreamSchema], Any]


class TruncatedParams(ParamsModel):
    left_truncated_cutoff: Optional[Tuple[float, str]]
    right_truncated_cutoff: Optional[Tuple[float, str]]


class TruncatedEvents(DataProcessor):
    params: TruncatedParams

    def __init__(self, params: TruncatedParams = None):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        events: DataFrame = eventstream.to_dataframe()
        user_col = eventstream.schema.user_id
        time_col = eventstream.schema.event_timestamp
        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name

        left_truncated_cutoff = self.params.left_truncated_cutoff
        right_truncated_cutoff = self.params.right_truncated_cutoff
        if left_truncated_cutoff:

            df_end_to_end = (
                events.groupby(user_col, as_index=False)
                .apply(lambda group: group.nlargest(1, columns=time_col))
                .reset_index(drop=True)
            )

            df_end_to_end["diff_end_to_end"] = df_end_to_end[time_col].max() - df_end_to_end[time_col]
            df_end_to_end["diff_end_to_end"] = df_end_to_end["diff_end_to_end"].dt.total_seconds()

            if left_truncated_cutoff[1] != "s":
                df_end_to_end["diff_end_to_end"] = df_end_to_end["diff_end_to_end"] / UOM_DICT[left_truncated_cutoff[1]]
            df_end_to_end.loc[
                df_end_to_end["diff_end_to_end"] >= left_truncated_cutoff[0], [type_col, event_col]
            ] = "truncated_left"
            df_end_to_end[[type_col, event_col]] = "truncated_left"

            df_end_to_end = df_end_to_end[df_end_to_end[type_col] != 0]
            df_end_to_end["ref"] = df_end_to_end[eventstream.schema.event_id]
            del df_end_to_end["diff_end_to_end"]
            print(df_end_to_end)
            events = pd.concat([events, df_end_to_end])

        if right_truncated_cutoff:

            df_start_to_start = (
                events.groupby(user_col, as_index=False)
                .apply(lambda group: group.nsmallest(1, columns=time_col))
                .reset_index(drop=True)
            )
            df_start_to_start["diff_start_to_start"] = df_start_to_start[time_col] - df_start_to_start[time_col].min()
            df_start_to_start["diff_start_to_start"] = df_start_to_start["diff_start_to_start"].dt.total_seconds()
            df_start_to_start = df_start_to_start[df_start_to_start["diff_start_to_start"] != 0]

            if right_truncated_cutoff[1] != "s":
                df_start_to_start["diff_start_to_start"] = (
                    df_start_to_start["diff_start_to_start"] / UOM_DICT[right_truncated_cutoff[1]]
                )

            df_start_to_start.loc[
                df_start_to_start["diff_start_to_start"] >= right_truncated_cutoff[0], [type_col, event_col]
            ] = "truncated_right"
            df_start_to_start[[type_col, event_col]] = "truncated_right"

            df_start_to_start = df_start_to_start[df_start_to_start[type_col] != 0]
            df_start_to_start["ref"] = df_start_to_start[eventstream.schema.event_id]

            del df_start_to_start["diff_start_to_start"]

            events = pd.concat([events, df_start_to_start])

        eventstream = Eventstream(
            raw_data=events,
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            relations=[{"raw_col": "ref", "evenstream": eventstream}],
        )
        return eventstream
