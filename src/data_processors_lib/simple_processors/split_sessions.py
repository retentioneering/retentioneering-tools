from __future__ import annotations

import logging
from typing import Callable, Any, TypedDict

from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema

log = logging.getLogger(__name__)
EventstreamFilter = Callable[[DataFrame, EventstreamSchema], Any]
UOM_DICT = {'s': 1,
            'm': 60,
            'h': 3600,
            'd': 24 * 3600}


class SplitSessionsParams(TypedDict):
    session_cutoff: tuple[float]
    mark_truncated: bool
    session_col: str
    inplace: bool
    delete_previous: bool


class SplitSessions(DataProcessor[SplitSessionsParams]):
    def __init__(self, params: SplitSessionsParams = None):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        events: DataFrame = eventstream.to_dataframe()
        user_col = eventstream.schema.user_id
        time_col = eventstream.schema.event_timestamp
        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name
        session_col = self.params.fields['session_col']
        inplace = self.params.fields['inplace']
        delete_previous = self.params.fields['delete_previous']
        session_cutoff = self.params.fields['session_cutoff']
        mark_truncated = self.params.fields['mark_truncated']

        target_stream = eventstream if inplace else eventstream.copy()
        if inplace:
            logging.warning(f'original dataframe has been lost')

        temp_col = 'temp_col'

        df = target_stream.to_dataframe()

        check_col = session_col in df.columns
        check = df[type_col].isin(['start_session', 'end_session']).any()

        if not delete_previous and (check or check_col):
            logging.warning(
                f'"start_session" and/or "end_session" event_types and/or {session_col} are already exist! ')

        if delete_previous:
            target_stream.delete_events('event_type == ["start_session", "end_session"]',
                                        hard=False, index=False, inplace=delete_previous)

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

        target_stream.add_raw_custom_col(
            colname=session_col, column=df[session_col])

        sessions_end_df = df.groupby([user_col, session_col], as_index=False).apply(
            lambda group: group.nlargest(1, columns=time_col)) \
            .reset_index(drop=True)
        sessions_end_df[event_col] = 'session_end'
        sessions_end_df[type_col] = 'session_end'

        target_stream.append_raw_events(sessions_end_df, save_raw_cols=False)

        sessions_start_df = df.groupby([user_col, session_col], as_index=False).apply(
            lambda group: group.nsmallest(1, columns=time_col)) \
            .reset_index(drop=True)
        sessions_start_df[event_col] = 'session_start'
        sessions_start_df[type_col] = 'session_start'

        target_stream.append_raw_events(sessions_start_df, save_raw_cols=False)

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
            target_stream.delete_events(query=f'{session_col}.isin({start_sessions}) \
                                                        & (~{type_col}.isin(["start", "session_end"]))',
                                        hard=False, inplace=True)
            # TODO dasha - после fix поменять на soft
            target_stream.delete_events(query=f'{session_col}.isin({end_sessions})\
                                                         & (~{type_col}.isin(["end", "session_start"]))',
                                        hard=False, inplace=True)

        eventstream = Eventstream(
            raw_data=target_stream,
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            relations=[{"raw_col": "ref", "evenstream": eventstream}],
        )
        return eventstream
