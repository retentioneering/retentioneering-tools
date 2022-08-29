from __future__ import annotations

import logging
from typing import Callable, Any, Tuple

import pandas as pd
from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema
from src.params_model import ParamsModel

log = logging.getLogger(__name__)
EventstreamFilter = Callable[[DataFrame, EventstreamSchema], Any]
from src.data_processors_lib.rete.constants import UOM_DICT


class SplitSessionsParams(ParamsModel):
    session_cutoff: Tuple[float, str]
    mark_truncated: bool
    session_col: str


class SplitSessions(DataProcessor):
    params: SplitSessionsParams

    def __init__(self, params: SplitSessionsParams = None):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        events: DataFrame = eventstream.to_dataframe()
        user_col = eventstream.schema.user_id
        time_col = eventstream.schema.event_timestamp
        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name
        session_col = self.params.session_col
        session_cutoff = self.params.session_cutoff
        mark_truncated = self.params.mark_truncated

        temp_col = 'temp_col'

        df = eventstream.to_dataframe()

        shift_df = df.groupby(user_col).shift(-1)

        time_delta = shift_df[time_col] - df[time_col]
        time_delta = time_delta.dt.total_seconds()
        if session_cutoff[1] != 's':
            time_delta = time_delta / UOM_DICT[session_cutoff[1]]

        # get boolean mapper for end_of_session occurrences
        eos_mask = time_delta > session_cutoff[0]
        # add session column:

        df[temp_col] = eos_mask
        df[temp_col] = df.groupby(user_col)[temp_col].cumsum()
        df[temp_col] = df.groupby(user_col)[temp_col].shift(1).fillna(0).map(int).map(str)
        df[session_col] = df[user_col].map(str) + '_' + df[temp_col]

        sessions_end_df = df.groupby([user_col, session_col], as_index=False).apply(
            lambda group: group.nlargest(1, columns=time_col)) \
            .reset_index(drop=True)
        sessions_end_df[event_col] = 'session_end'
        sessions_end_df[type_col] = 'session_end'
        sessions_end_df['ref'] = sessions_end_df[eventstream.schema.event_id]

        sessions_start_df = df.groupby([user_col, session_col], as_index=False).apply(
            lambda group: group.nsmallest(1, columns=time_col)) \
            .reset_index(drop=True)
        sessions_start_df[event_col] = 'session_start'
        sessions_start_df[type_col] = 'session_start'
        sessions_start_df['ref'] = sessions_start_df[eventstream.schema.event_id]

        df_sessions = pd.concat([sessions_end_df, sessions_start_df])

        if mark_truncated:
            df_start_to_start_time = df.groupby(user_col)[[time_col]].min().reset_index()
            df_start_to_start_time['diff_start_to_start'] = df_start_to_start_time[time_col] - df_start_to_start_time[
                time_col].min()
            df_start_to_start_time['diff_start_to_start'] = df_start_to_start_time[
                'diff_start_to_start'].dt.total_seconds()

            cut_df = df.groupby([user_col, session_col])[[time_col]].agg([min, max])
            cut_df.columns = cut_df.columns.get_level_values(1)
            cut_df.reset_index(inplace=True)
            cut_df['diff_end_to_end'] = (cut_df['max'].max() - cut_df['max']).dt.total_seconds()
            cut_df['diff_start_to_start'] = (cut_df['min'] - cut_df['min'].min()).dt.total_seconds()

            if session_cutoff[1] != 's':
                cut_df['diff_end_to_end'] = cut_df['diff_end_to_end'] / UOM_DICT[session_cutoff[1]]
                cut_df['diff_start_to_start'] = cut_df['diff_start_to_start'] / UOM_DICT[session_cutoff[1]]

            cut_df['diff_end_to_end'] = cut_df['diff_end_to_end'] < session_cutoff[0]
            cut_df['diff_start_to_start'] = cut_df['diff_start_to_start'] < session_cutoff[0]

            end_sessions = cut_df[cut_df['diff_end_to_end']][session_col].to_list()
            start_sessions = cut_df[cut_df['diff_start_to_start']][session_col].to_list()
            # TODO dasha - после fix поменять на soft
            df = df[df[session_col].isin(start_sessions) & ~df[type_col].isin(["start", "session_end"])]
            # TODO dasha - после fix поменять на soft
            df = df[df[session_col].isin(end_sessions) & ~df[type_col].isin(["end", "session_start"])]

        eventstream = Eventstream(
            raw_data=df,
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            relations=[{"raw_col": "ref", "evenstream": eventstream}],
        )
        return eventstream
