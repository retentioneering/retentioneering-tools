from __future__ import annotations

import logging
from typing import Callable, Any, TypedDict

from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema

log = logging.getLogger(__name__)
EventstreamFilter = Callable[[DataFrame, EventstreamSchema], Any]


class NegativeTargetParams(TypedDict):
    negative_target_events: list[str]
    negative_function: Callable
    inplace: bool
    delete_previous: bool


class NegativeTarget(DataProcessor[NegativeTargetParams]):
    def __init__(self, params: NegativeTargetParams = None):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        events: DataFrame = eventstream.to_dataframe()
        user_col = eventstream.schema.user_id
        time_col = eventstream.schema.event_timestamp
        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name

        negative_function = self.params.fields['negative_function']
        negative_target_events = self.params.fields['negative_target_events']
        inplace = self.params.fields['inplace']
        delete_previous = self.params.fields['delete_previous']

        target_stream = eventstream if inplace else eventstream.copy()
        if inplace:
            logging.warning(f'original dataframe has been lost')

        event_col, type_col = attrgetter('event_col', 'type_col')(target_stream.config)

        df = target_stream.to_dataframe()

        check = df[type_col].isin(['negative_target']).any()

        if not delete_previous and check:
            logging.warning(f'events with "negative_target" event_type are already exist!')
        if delete_previous:
            target_stream.delete_events(f'{type_col} == ["negative_target"]', hard=False, index=False, inplace=True)

        negative_targets = negative_function(df, negative_target_events)
        negative_targets[type_col] = 'negative_target'
        negative_targets[event_col] = 'negative_target_' + negative_targets[event_col]

        negative_targets['event_type'] = 'negative_target'
        target_stream.append_raw_events(negative_targets, save_raw_cols=False)

        eventstream = Eventstream(
            raw_data=target_stream,
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            relations=[{"raw_col": "ref", "evenstream": eventstream}],
        )
        return eventstream
