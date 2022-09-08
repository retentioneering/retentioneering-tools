from __future__ import annotations

import logging
from typing import Callable, Any, Tuple, Optional, List

import pandas as pd
from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.data_processors_lib.rete.constants import UOM_DICT
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema
from src.params_model import ParamsModel

log = logging.getLogger(__name__)

EventstreamFilter = Callable[[DataFrame, EventstreamSchema], Any]

class LostPauseParams(ParamsModel):
    lost_cutoff: Optional[Tuple[float, str]]
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

        lost_cutoff = self.params.lost_cutoff
        lost_users_list = self.params.lost_users_list

        if lost_cutoff and lost_users_list:
            raise ValueError("lost_cutoff and lost_users_list parameters cannot be used at the same time!")
        # TODO dasha нужна сформулировать нормальную отбивку
        if not lost_cutoff and not lost_users_list:
            raise ValueError('lost_cutoff or lost_users_list should be specified!')

        df = eventstream.to_dataframe(copy=True)

        if lost_cutoff:
            data_lost = (
                df.groupby(user_col, as_index=False)
                .apply(lambda group: group.nlargest(1, columns=time_col))
                .reset_index(drop=True)
            )
            data_lost["diff_end_to_end"] = data_lost[time_col].max() - data_lost[time_col]
            data_lost["diff_end_to_end"] = data_lost["diff_end_to_end"].dt.total_seconds()
            if lost_cutoff[1] != "s":
                data_lost["diff_end_to_end"] = data_lost["diff_end_to_end"] / UOM_DICT[lost_cutoff[1]]

            data_lost[type_col] = data_lost.apply(
                lambda x: "pause" if x["diff_end_to_end"] < lost_cutoff[0] else "lost", axis=1
            )
            data_lost[event_col] = data_lost[type_col]
            data_lost["ref"] = None
            del data_lost["diff_end_to_end"]

        elif lost_users_list:
            data_lost = df[df[user_col].isin(lost_users_list)]
            data_lost = data_lost.groupby(user_col, as_index=False).apply(
                lambda group: group.nlargest(1, columns=time_col)) \
                .reset_index(drop=True)

            data_lost[type_col] = "lost"
            data_lost[event_col] = "lost"
            data_lost["ref"] = df[eventstream.schema.event_id]

            data_pause = df[~df[user_col].isin(data_lost[user_col].unique())]
            data_pause = (data_pause.groupby(user_col, as_index=False)
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
