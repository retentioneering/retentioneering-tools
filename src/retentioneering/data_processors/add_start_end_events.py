from dataclasses import dataclass, field
from typing import Tuple

import pandas as pd

from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.event_type import EventTypes
from retentioneering.eventstream.schema import EventstreamSchema


@dataclass
class AddStartEndEvents(DataProcessor):
    path_id_col: str | None = field(default=None)

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        event_types = EventTypes()
        path_id_col = self.path_id_col or schema.path_col

        # NB: select whole boundary ROWS via drop_duplicates, not
        # groupby().first()/.last() — those return the first/last NON-NULL value
        # per column independently (pandas skipna semantics), which fabricates
        # chimera rows mixing values from different events when nullable
        # extra/segment columns are present.
        df_sorted = df.sort_values([path_id_col, schema.timestamp, schema.subindex])

        df_start = (
            df_sorted.drop_duplicates(subset=path_id_col, keep="first")
            .loc[lambda _df: _df[schema.event_col] != event_types.PATH_START.name]
            .copy()
        )
        df_start[schema.event_col] = event_types.PATH_START.name
        df_start[schema.event_type] = event_types.PATH_START.type
        df_start[schema.subindex] = event_types.PATH_START.index

        df_end = (
            df_sorted.drop_duplicates(subset=path_id_col, keep="last")
            .loc[lambda _df: _df[schema.event_col] != event_types.PATH_END.name]
            .copy()
        )
        df_end[schema.event_col] = event_types.PATH_END.name
        df_end[schema.event_type] = event_types.PATH_END.type
        df_end[schema.subindex] = event_types.PATH_END.index

        df = (
            pd.concat([df, df_start, df_end])
            .sort_values([schema.path_col, schema.index, schema.subindex])
            .reset_index(drop=True)
        )
        df[schema.event_col] = df[schema.event_col].astype("category")
        return df, schema
