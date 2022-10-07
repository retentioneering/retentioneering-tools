from __future__ import annotations

from typing import List

import pandas as pd

from src.data_processor.data_processor import DataProcessor
from src.eventstream.eventstream import Eventstream
from src.params_model import ParamsModel


class CutPathAfterEventParams(ParamsModel):
    cutoff_events: List[str]
    cut_shift: int
    min_cjm: int


class CutPathAfterEvent(DataProcessor):
    params: CutPathAfterEventParams

    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls)
        obj.params = CutPathAfterEventParams  # type: ignore
        return obj

    def __init__(self, params: CutPathAfterEventParams):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        user_col = eventstream.schema.user_id
        time_col = eventstream.schema.event_timestamp
        event_col = eventstream.schema.event_name
        id_col = eventstream.schema.event_id

        cutoff_events = self.params.cutoff_events
        min_cjm = self.params.min_cjm
        cut_shift = self.params.cut_shift

        df = eventstream.to_dataframe(copy=True)

        df["_point"] = 0
        df.loc[df[event_col].isin(cutoff_events), "_point"] = 1
        df["_point"] = df.groupby([user_col, time_col])["_point"].transform(max)
        #  накопленная сумма _point внутри юзера
        df["_cumsum"] = df.groupby([user_col])["_point"].cumsum()
        # номер группы внутри юзера
        df["num_groups"] = df.groupby([user_col])[time_col].transform(
            lambda x: x.diff().dt.total_seconds().ne(0).cumsum()
        )

        df["find_1"] = df.groupby([user_col, "num_groups"])["_cumsum"].transform(lambda x: x.eq(1).any().astype(int))

        mask = (df["_cumsum"] == 0) | ((df["_cumsum"] > 0) & (df["_point"] == 1) & (df["find_1"] == 1))

        df_cut = df[mask]
        ids_to_del = df[~mask][id_col].to_list()
        df_cut["rw_cumsum"] = (
            df_cut.loc[::-1]
            .groupby([user_col])[time_col]
            .transform(lambda x: x.diff().dt.total_seconds().ne(0).cumsum())
        )
        if cut_shift > 0:
            users_with_target = df[df[event_col].isin(cutoff_events)][user_col].to_list()
            mask_shift = (df_cut["rw_cumsum"] <= cut_shift) & (df_cut[user_col].isin(users_with_target))
            ids_to_del = ids_to_del + df_cut[mask_shift][id_col].to_list()
            df_cut = df_cut[~mask_shift]

        df_to_del = df.loc[df[id_col].apply(lambda x: x in ids_to_del)]  # type: ignore
        if min_cjm > 0:
            df_cut = df_cut.groupby([user_col])[["num_groups"]].max().reset_index()
            users_to_del = df_cut[df_cut["num_groups"] < min_cjm][user_col].to_list()
            df_users_to_del = df.loc[df[user_col].apply(lambda x: x in users_to_del)]  # type: ignore
            df_to_del = pd.concat([df_to_del, df_users_to_del])
            df_to_del.drop_duplicates(inplace=True)

        df_to_del["ref"] = df_to_del[eventstream.schema.event_id]

        eventstream = Eventstream(
            raw_data=df_to_del,
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )

        eventstream.soft_delete(events=df_to_del)

        return eventstream
