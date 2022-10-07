from __future__ import annotations

from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from src.data_processor.data_processor import DataProcessor
from src.data_processors_lib.rete.constants import DATETIME_UNITS
from src.eventstream.eventstream import Eventstream
from src.params_model import ParamsModel


class LostUsersParams(ParamsModel):
    lost_cutoff: Optional[Tuple[float, DATETIME_UNITS]]
    lost_users_list: Optional[List[int]]


class LostUsersEvents(DataProcessor):
    params: LostUsersParams

    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls, *args, **kwargs)
        obj.params = LostUsersParams  # type: ignore
        return obj

    def __init__(self, params: LostUsersParams):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        user_col = eventstream.schema.user_id
        time_col = eventstream.schema.event_timestamp
        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name

        lost_cutoff, lost_cutoff_unit = None, None
        lost_users_list = self.params.lost_users_list
        data_lost = pd.DataFrame()

        if self.params.lost_cutoff:
            lost_cutoff, lost_cutoff_unit = self.params.lost_cutoff

        if lost_cutoff and lost_users_list:
            raise ValueError("lost_cutoff and lost_users_list parameters cannot be used simultaneously!")

        if not lost_cutoff and not lost_users_list:
            raise ValueError("Either lost_cutoff or lost_users_list must be specified!")

        df = eventstream.to_dataframe(copy=True)

        if lost_cutoff and lost_cutoff_unit:
            data_lost = df.groupby(user_col, as_index=False)[time_col].max()
            data_lost["diff_end_to_end"] = data_lost[time_col].max() - data_lost[time_col]
            data_lost["diff_end_to_end"] /= np.timedelta64(1, lost_cutoff_unit)  # type: ignore

            data_lost[type_col] = np.where(data_lost["diff_end_to_end"] < lost_cutoff, "absent_user", "lost_user")
            data_lost[event_col] = data_lost[type_col]
            data_lost["ref"] = None
            del data_lost["diff_end_to_end"]

        if lost_users_list:
            data_lost = df.groupby(user_col, as_index=False)[time_col].max()
            data_lost[type_col] = np.where(data_lost["user_id"].isin(lost_users_list), "lost_user", "absent_user")
            data_lost[event_col] = data_lost[type_col]
            data_lost["ref"] = None

        eventstream = Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=data_lost,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )
        return eventstream
