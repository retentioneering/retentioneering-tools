from __future__ import annotations

from typing import Literal

import pandas as pd

from src.data_processor.data_processor import DataProcessor
from src.eventstream.eventstream import Eventstream
from src.params_model import ParamsModel


class CollapseLoopsParams(ParamsModel):
    full_collapse: bool = (True,)
    timestamp_aggregation_type: Literal["max", "min", "mean"] = "max"


class CollapseLoops(DataProcessor):
    params: CollapseLoopsParams

    def __init__(self, params: CollapseLoopsParams):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        user_col = eventstream.schema.user_id
        time_col = eventstream.schema.event_timestamp
        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name

        full_collapse = self.params.full_collapse
        timestamp_aggregation_type = self.params.timestamp_aggregation_type
        df = eventstream.to_dataframe(copy=True)
        df["ref"] = df[eventstream.schema.event_id]

        df["grp"] = df.groupby(user_col)[event_col].apply(lambda x: x != x.shift())
        # Столбец в котором считается порядковый номер по группам одинаковых событий
        df["cumgroup"] = df.groupby(user_col)["grp"].cumsum()
        df["count"] = df.groupby([user_col, "cumgroup"]).cumcount() + 1
        df["collapsed"] = df.groupby([user_col, "cumgroup", event_col])["count"].transform(max)
        df["collapsed"] = df["collapsed"].apply(lambda x: False if x == 1 else True)

        loops = (
            df[df["collapsed"] == 1]
            .groupby([user_col, "cumgroup", event_col])
            .agg({time_col: timestamp_aggregation_type, "count": "max"})
            .reset_index()
        )

        if full_collapse:
            loops[event_col] = loops[event_col].map(str) + "_loop"
        else:
            loops[event_col] = loops[event_col].map(str) + "_loop_" + loops["count"].map(str)
        loops[type_col] = "group_alias"
        loops["ref"] = None

        df_to_del = df[df["collapsed"] == 1]

        df_loops = pd.concat([loops, df_to_del])
        eventstream = Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=df_loops,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )
        if not df_to_del.empty:
            eventstream.soft_delete(df_to_del)

        return eventstream
