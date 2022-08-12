from __future__ import annotations
from typing import Callable, Any, TypedDict

import pandas as pd
from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema

EventstreamFilter = Callable[[DataFrame, EventstreamSchema], Any]


class NewResumeParams(TypedDict):
    user_col: str
    event_col: str
    time_col: str
    type_col: str
    new_users_list: list[int]


class NewResumeEvents(DataProcessor[NewResumeParams]):
    def __init__(self, params: NewResumeParams = None):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        events: DataFrame = eventstream.to_dataframe()
        user_col = eventstream.schema.user_id
        time_col = eventstream.schema.event_timestamp
        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name
        new_users_list = self.params.fields['new_users_list']
        matched_events = events.groupby(user_col, as_index=False)\
            .apply(lambda group: group.nsmallest(1, columns=time_col)) \
            .reset_index(drop=True)

        if self.params.fields['new_users_list'] == 'all':

            matched_events[type_col] = 'new_user'
            matched_events[event_col] = 'new_user'

        else:
            new_user_mask = matched_events[user_col].isin(new_users_list)
            matched_events.loc[new_user_mask, type_col] = 'new_user'
            matched_events.loc[~new_user_mask, type_col] = 'resume'
            matched_events[event_col] = matched_events[type_col]

        matched_events["ref"] = matched_events[eventstream.schema.event_id]

        eventstream = Eventstream(
            raw_data=matched_events,
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            relations=[{"raw_col": "ref", "evenstream": eventstream}],
        )
        return eventstream
