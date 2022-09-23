from __future__ import annotations

import logging
from typing import Any, Callable, List, Literal, Optional, Tuple

import numpy as np
import pandas as pd
from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema
from src.params_model import ParamsModel

log = logging.getLogger(__name__)

EventstreamFilter = Callable[[DataFrame, EventstreamSchema], Any]


class LostPauseParams(ParamsModel):
    lost_cutoff: Optional[Tuple[float, Literal['Y', 'M', 'D', 'h', 'm', 's']]]
    lost_users_list: Optional[List[int]]


class LostPauseEvents(DataProcessor):
    params: LostPauseParams

    def __init__(self, params: LostPauseParams):
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

        if lost_cutoff:
            data_lost = (
                df.groupby(user_col, as_index=False)
                .apply(lambda group: group.nlargest(1, columns=time_col))
                .reset_index(drop=True)
            )
            data_lost["diff_end_to_end"] = data_lost[time_col].max() - data_lost[time_col]
            data_lost["diff_end_to_end"] /= np.timedelta64(1, lost_cutoff_unit)

            data_lost[type_col] = data_lost.apply(
                lambda x: "pause" if x["diff_end_to_end"] < lost_cutoff else "lost", axis=1
            )
            data_lost[event_col] = data_lost[type_col]
            data_lost["ref"] = None
            del data_lost["diff_end_to_end"]
        # TODO dasha продумать правильное условие
        if lost_users_list:
            data_lost = df[df[user_col].isin(lost_users_list)]
            data_lost = (
                data_lost.groupby(user_col, as_index=False)
                .apply(lambda group: group.nlargest(1, columns=time_col))
                .reset_index(drop=True)
            )

            data_lost[type_col] = "lost"
            data_lost[event_col] = "lost"
            data_lost["ref"] = None

            data_pause = df[~df[user_col].isin(data_lost[user_col].unique())]
            data_pause = (
                data_pause.groupby(user_col, as_index=False)
                .apply(lambda group: group.nlargest(1, columns=time_col))
                .reset_index(drop=True)
            )

            data_pause.loc[:, [type_col, event_col]] = "pause"
            data_pause["ref"] = None
            data_lost = pd.concat([data_lost, data_pause])

        eventstream = Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=data_lost,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )
        return eventstream
