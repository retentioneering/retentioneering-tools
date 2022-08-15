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


class LostPauseParams(TypedDict):
    lost_cutoff: list[float]
    lost_users_list: list
    func: Callable
    inplace: bool
    delete_previous: bool


def _custom_func_lost(eventstream: Eventstream, lost_users_list: list):
    df = eventstream.to_dataframe()
    user_col = eventstream.schema.user_id
    time_col = eventstream.schema.event_timestamp

    data_lost = df.groupby(user_col, as_index=False).apply(lambda group: group.nlargest(1, columns=time_col)) \
        .reset_index(drop=True)
    return data_lost


class LostPauseEvents(DataProcessor[LostPauseParams]):
    def __init__(self, params: LostPauseParams = None):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        user_col = eventstream.schema.user_id
        time_col = eventstream.schema.event_timestamp
        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name

        if self.params.fields['lost_cutoff'] and self.params.fields['func']:
            raise ValueError('lost_cutoff and func parameters cannot be used at the same time!')

        target_stream = eventstream if self.params.fields['inplace'] else eventstream.copy()
        if self.params.fields['inplace']:
            log.warning(f'original dataframe has been lost')

        df = target_stream.to_dataframe()

        check = df[type_col].isin(['lost', 'pause']).any()
        if not self.params.fields['delete_previous'] and check:
            logging.warning(f'"lost" and/or "pause" event_types are already exist!')

        if self.params.fields['delete_previous']:
            target_stream.delete_events('event_type == ["lost", "pause"]', hard=False, index=False, inplace=True)

        if self.params.fields['lost_cutoff']:
            data_lost = df.groupby(user_col, as_index=False).apply(lambda group: group.nlargest(1, columns=time_col)) \
                .reset_index(drop=True)
            data_lost['diff_end_to_end'] = data_lost[time_col].max() - data_lost[time_col]
            data_lost['diff_end_to_end'] = data_lost['diff_end_to_end'].dt.total_seconds()

            if self.params.fields['lost_cutoff'][1] != 's':
                data_lost['diff_end_to_end'] = data_lost['diff_end_to_end'] / UOM_DICT[
                    self.params.fields['lost_cutoff'][1]]

            data_lost[type_col] = data_lost.apply(
                lambda x: 'pause' if x['diff_end_to_end'] < self.params.fields['lost_cutoff'][0] else 'lost',
                axis=1)
            data_lost[event_col] = data_lost[type_col]
            del data_lost['diff_end_to_end']

        elif self.params.fields['func']:
            data_lost = self.params.fields['func'](eventstream, self.params.fields['lost_users_list'])
            # TODO dasha добавить в пример пользоват функции, чтобы добавлялись только lost?
            data_lost[type_col] = 'lost'
            data_lost[event_col] = 'lost'

            data_pause = df.groupby(user_col, as_index=False).apply(lambda group: group.nlargest(1, columns=time_col)) \
                .reset_index(drop=True)

            data_pause.loc[:, [type_col, event_col]] = 'pause'
            target_stream.append_raw_events(data_pause, save_raw_cols=False)

        else:
            data_lost = df.groupby(user_col, as_index=False).apply(lambda group: group.nlargest(1, columns=time_col)) \
                .reset_index(drop=True)
            data_lost[type_col] = 'pause'
            data_lost[event_col] = 'pause'

        target_stream.append_raw_events(data_lost, save_raw_cols=False)

        eventstream = Eventstream(
            raw_data=target_stream,
            raw_data_schema=target_stream.schema.to_raw_data_schema(),
            relations=[{"raw_col": "ref", "evenstream": target_stream}],
        )
        return eventstream
