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


class TruncatedParams(TypedDict):
    left_truncated_cutoff: tuple[float]
    right_truncated_cutoff: tuple[float]
    inplace: bool
    delete_previous: bool


class TruncatedEvents(DataProcessor[TruncatedParams]):
    def __init__(self, params: TruncatedParams = None):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        events: DataFrame = eventstream.to_dataframe()
        user_col = eventstream.schema.user_id
        time_col = eventstream.schema.event_timestamp
        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name

        left_truncated_cutoff = self.params.fields['left_truncated_cutoff']
        right_truncated_cutoff = self.params.fields['right_truncated_cutoff']
        inplace = self.params.fields['inplace']
        delete_previous = self.params.fields['delete_previous']

        target_stream = eventstream if inplace else eventstream.copy()
        if inplace:
            log.warning(f'original dataframe has been lost')

        df = target_stream.to_dataframe()

        check = df[type_col].isin(['truncated_left', 'truncated_right']).any()

        if not delete_previous and check:
            log.warning(f'"truncated_left" and/or "truncated_right" event_types are already exist!')

        if delete_previous:
            target_stream.delete_events('event_type == ["truncated_left", "truncated_right"]',
                                        hard=False, index=False, inplace=True)

        if left_truncated_cutoff:

            df_end_to_end = df.groupby(user_col, as_index=False).apply(
                lambda group: group.nlargest(1, columns=time_col)) \
                .reset_index(drop=True)

            df_end_to_end['diff_end_to_end'] = df_end_to_end[time_col].max() - df_end_to_end[time_col]
            df_end_to_end['diff_end_to_end'] = df_end_to_end['diff_end_to_end'].dt.total_seconds()

            if left_truncated_cutoff[1] != 's':
                df_end_to_end['diff_end_to_end'] = df_end_to_end['diff_end_to_end'] / UOM_DICT[left_truncated_cutoff[1]]
            df_end_to_end.loc[df_end_to_end['diff_end_to_end'] >= left_truncated_cutoff[0],
                              [type_col, event_col]] = 'truncated_left'

            df_end_to_end = df_end_to_end[df_end_to_end[type_col] != 0]
            del df_end_to_end['diff_end_to_end']

            target_stream.append_raw_events(df_end_to_end, save_raw_cols=False)

        if right_truncated_cutoff:
            df_start_to_start = df.groupby(user_col, as_index=False).apply(
                lambda group: group.nsmallest(1, columns=time_col)) \
                .reset_index(drop=True)
            df_start_to_start['diff_start_to_start'] = df_start_to_start[time_col] - df_start_to_start[time_col].min()
            df_start_to_start['diff_start_to_start'] = df_start_to_start['diff_start_to_start'].dt.total_seconds()
            df_start_to_start = df_start_to_start[df_start_to_start['diff_start_to_start'] != 0]

            if right_truncated_cutoff[1] != 's':
                df_start_to_start['diff_start_to_start'] = \
                    df_start_to_start['diff_start_to_start'] / UOM_DICT[right_truncated_cutoff[1]]

            df_start_to_start.loc[df_start_to_start['diff_start_to_start'] >= right_truncated_cutoff[0],
                                  [type_col, event_col]] = 'truncated_right'

            df_start_to_start = df_start_to_start[df_start_to_start[type_col] != 0]
            del df_start_to_start['diff_start_to_start']

            target_stream.append_raw_events(df_start_to_start, save_raw_cols=False)

        eventstream = Eventstream(
            raw_data=target_stream,
            raw_data_schema=target_stream.schema.to_raw_data_schema(),
            relations=[{"raw_col": "ref", "evenstream": target_stream}],
        )
        return eventstream
