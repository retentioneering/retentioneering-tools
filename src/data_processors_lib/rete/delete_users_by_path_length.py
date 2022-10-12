from __future__ import annotations

from typing import Optional, Tuple

import numpy as np

from src.data_processor.data_processor import DataProcessor
from src.data_processors_lib.rete.constants import DATETIME_UNITS
from src.eventstream.eventstream import Eventstream
from src.params_model import ParamsModel


class DeleteUsersByPathLengthParams(ParamsModel):
    events_num: Optional[int]
    cutoff: Optional[Tuple[float, DATETIME_UNITS]]


class DeleteUsersByPathLength(DataProcessor):
    params: DeleteUsersByPathLengthParams

    def __init__(self, params: DeleteUsersByPathLengthParams):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        user_col = eventstream.schema.user_id
        time_col = eventstream.schema.event_timestamp

        cutoff, cutoff_unit = None, None
        events_num = self.params.events_num

        if self.params.cutoff:
            cutoff, cutoff_unit = self.params.cutoff

        if events_num and cutoff:
            raise ValueError("Events_num and cutoff parameters cannot be used simultaneously!")

        if not events_num and not cutoff:
            raise ValueError("Either events_num or cutoff must be specified!")

        events = eventstream.to_dataframe(copy=True)

        if cutoff and cutoff_unit:
            userpath = (
                events.groupby(user_col)[time_col]
                .agg([np.min, np.max])
                .rename(columns={"amin": "start", "amax": "end"})
            )
            mask_ = (userpath["end"] - userpath["start"]) / np.timedelta64(1, cutoff_unit) < cutoff  # type: ignore

        else:
            userpath = events.groupby([user_col])[[time_col]].nunique().rename(columns={time_col: "length"})
            mask_ = userpath["length"] < events_num

        users_to_delete = userpath[mask_].index
        events = events[events[user_col].isin(users_to_delete)]
        events["ref"] = events.loc[:, eventstream.schema.event_id]

        eventstream = Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=events,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )

        if not events.empty:
            eventstream.soft_delete(eventstream.to_dataframe())

        return eventstream
