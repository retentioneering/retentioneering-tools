from __future__ import annotations

import logging
from typing import Any, Callable, Optional, Tuple

import numpy as np
import pandas as pd
from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.data_processors_lib.rete.constants import DATETIME_UNITS
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema
from src.params_model import ParamsModel

log = logging.getLogger(__name__)
EventstreamFilter = Callable[[DataFrame, EventstreamSchema], Any]


class SplitSessionsParams(ParamsModel):
    session_cutoff: Tuple[float, DATETIME_UNITS]
    mark_truncated: Optional[bool] = False
    session_col: str


class SplitSessions(DataProcessor):
    params: SplitSessionsParams

    def __init__(self, params: SplitSessionsParams) -> None:
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        user_col = eventstream.schema.user_id
        time_col = eventstream.schema.event_timestamp
        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name
        session_col = self.params.session_col
        session_cutoff, session_cutoff_unit = self.params.session_cutoff
        mark_truncated = self.params.mark_truncated

        df = eventstream.to_dataframe(copy=True)
        df['ref'] = df[eventstream.schema.event_id]

        df['prev_timedelta'] = (df[time_col] - df.groupby(user_col)[time_col].shift(1))
        df['next_timedelta'] = (df.groupby(user_col)[time_col].shift(-1) - df[time_col])
        df['prev_timedelta'] /= np.timedelta64(1, session_cutoff_unit)
        df['next_timedelta'] /= np.timedelta64(1, session_cutoff_unit)

        session_starts_mask = (df['prev_timedelta'] > session_cutoff) | (df['prev_timedelta'].isnull())
        session_ends_mask = (df['next_timedelta'] > session_cutoff) | (df['next_timedelta'].isnull())

        df['is_session_start'] = session_starts_mask
        df[session_col] = df.groupby(user_col)['is_session_start'].transform(np.cumsum)
        df[session_col] = df[user_col].astype(str) + '_' + df[session_col].astype(str)

        session_starts = df[session_starts_mask][[user_col, time_col, session_col]]
        session_ends = df[session_ends_mask][[user_col, time_col, session_col]]

        session_starts[event_col] = 'session_start'
        session_starts[type_col] = 'session_start'
        session_starts['ref'] = None

        session_ends[event_col] = 'session_end'
        session_ends[type_col] = 'session_end'
        session_ends['ref'] = None

        df = df.drop(['prev_timedelta', 'next_timedelta', 'is_session_start'], axis=1)

        if mark_truncated:
            dataset_start = df[time_col].min()
            dataset_end = df[time_col].max()
            start_to_start = (session_starts[time_col] - dataset_start) / np.timedelta64(1, session_cutoff_unit)
            end_to_end = (dataset_end - session_ends[time_col]) / np.timedelta64(1, session_cutoff_unit)

            session_starts_truncated = session_starts[start_to_start < session_cutoff].reset_index()
            session_ends_truncated = session_ends[end_to_end < session_cutoff].reset_index()

            session_starts_truncated[event_col] = 'session_start_truncated'
            session_starts_truncated[type_col] = 'session_start_truncated'

            session_ends_truncated[event_col] = 'session_end_truncated'
            session_ends_truncated[type_col] = 'session_end_truncated'

            session_starts = pd.concat([session_starts, session_starts_truncated])
            session_ends = pd.concat([session_ends, session_ends_truncated])

        df = pd.concat([df, session_starts, session_ends])

        raw_data_schema = eventstream.schema.to_raw_data_schema()
        raw_data_schema.custom_cols.append(
            {"custom_col": session_col, "raw_data_col": session_col}
        )

        eventstream = Eventstream(
            schema=EventstreamSchema(custom_cols=[session_col]),
            raw_data_schema=raw_data_schema,
            raw_data=df,
            relations=[{"raw_col": "ref", "eventstream": eventstream}]
        )

        return eventstream
