from __future__ import annotations

from typing import Any, Callable, List, Union, Literal

from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema
from src.params_model import ParamsModel

EventstreamFilter = Callable[[DataFrame, EventstreamSchema], Any]


class NewResumeParams(ParamsModel):
    new_users_list: Union[List[int], Literal['all']]


class NewResumeEvents(DataProcessor):
    params: NewResumeParams

    def __init__(self, params: NewResumeParams):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        events: DataFrame = eventstream.to_dataframe(copy=True)
        user_col = eventstream.schema.user_id
        time_col = eventstream.schema.event_timestamp
        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name
        new_users_list = self.params.new_users_list
        # TODO - продумать текст отбивки
        if isinstance(new_users_list, str) and new_users_list != "all":
            raise ValueError('Should be list of users or "all"!')
        matched_events = (
            events.groupby(user_col, as_index=False)
            .apply(lambda group: group.nsmallest(1, columns=time_col))
            .reset_index(drop=True)
        )

        if new_users_list == "all":

            matched_events[type_col] = "new_user"
            matched_events[event_col] = "new_user"

        else:
            new_user_mask = matched_events[user_col].isin(new_users_list)
            matched_events.loc[new_user_mask, type_col] = "new_user"
            matched_events.loc[~new_user_mask, type_col] = "resume"
            matched_events[event_col] = matched_events[type_col]

        matched_events["ref"] = None

        eventstream = Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=matched_events,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )
        return eventstream
