from __future__ import annotations

import logging
from typing import Any, Callable, List, Optional

import pandas as pd
from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema
from src.params_model import ParamsModel

log = logging.getLogger(__name__)

EventstreamFilter = Callable[[DataFrame, EventstreamSchema], Any]
from src.data_processors_lib.rete.constants import UOM_DICT


class LostPauseParams(ParamsModel):
    lost_cutoff: Optional[List[int]]
    lost_users_list: Optional[list]
    func: Optional[Callable]


def _custom_func_lost(eventstream: Eventstream, lost_users_list: list):
    df = eventstream.to_dataframe()
    user_col = eventstream.schema.user_id
    time_col = eventstream.schema.event_timestamp

    data_lost = (
        df.groupby(user_col, as_index=False)
        .apply(lambda group: group.nlargest(1, columns=time_col))
        .reset_index(drop=True)
    )
    return data_lost


class LostPauseEvents(DataProcessor):
    params: LostPauseParams

    def __init__(self, params: LostPauseParams = None):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        user_col = eventstream.schema.user_id
        time_col = eventstream.schema.event_timestamp
        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name

        lost_cutoff = self.params.lost_cutoff
        func = self.params.func
        lost_users_list = self.params.lost_users_list

        if lost_cutoff and func:
            raise ValueError("lost_cutoff and func parameters cannot be used at the same time!")

        df = eventstream.to_dataframe()

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
            data_lost["ref"] = df[eventstream.schema.event_id]
            del data_lost["diff_end_to_end"]

        elif func:
            data_lost = func(eventstream, lost_users_list)
            # TODO dasha добавить в пример пользоват функции, чтобы добавлялись только lost?
            data_lost[type_col] = "lost"
            data_lost[event_col] = "lost"
            data_lost["ref"] = df[eventstream.schema.event_id]

            data_pause = (
                df.groupby(user_col, as_index=False)
                .apply(lambda group: group.nlargest(1, columns=time_col))
                .reset_index(drop=True)
            )

            data_pause.loc[:, [type_col, event_col]] = "pause"
            data_pause["ref"] = df[eventstream.schema.event_id]
            df = pd.concat(df, data_pause)

        else:
            data_lost = (
                df.groupby(user_col, as_index=False)
                .apply(lambda group: group.nlargest(1, columns=time_col))
                .reset_index(drop=True)
            )
            data_lost[type_col] = "pause"
            data_lost[event_col] = "pause"
            data_lost["ref"] = df[eventstream.schema.event_id]

        result = pd.concat([df, data_lost])

        eventstream = Eventstream(
            raw_data=result,
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            relations=[{"raw_col": "ref", "evenstream": eventstream}],
        )
        return eventstream
