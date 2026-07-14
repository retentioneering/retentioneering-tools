from dataclasses import dataclass, field
from typing import Tuple

import pandas as pd

from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.event_type import EventTypes
from retentioneering.eventstream.schema import EventstreamSchema


@dataclass
class AddStartEndEvents(DataProcessor):
    path_col: str | None = field(default=None)

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        event_types = EventTypes()
        path_col = self.path_col or schema.path_col

        # Drop any path_start/path_end markers already present -- from a prior
        # call at this grain, or (the bug this fixes) at a *different*
        # path_col grain, whose markers would otherwise be stranded mid-path
        # once boundaries are recomputed at the new grain -- and recompute
        # both boundaries fresh. Matched by event_type, not event name, so a
        # genuine event that happens to be named "path_start"/"path_end" is
        # never mistaken for a synthetic marker.
        clean = df[
            ~df[schema.event_type].isin(
                [event_types.PATH_START.type, event_types.PATH_END.type]
            )
        ]

        # NB: select whole boundary ROWS via drop_duplicates, not
        # groupby().first()/.last() — those return the first/last NON-NULL value
        # per column independently (pandas skipna semantics), which fabricates
        # chimera rows mixing values from different events when nullable
        # extra/segment columns are present.
        clean_sorted = clean.sort_values(
            [path_col, schema.timestamp_col, schema.subindex]
        )

        df_start = clean_sorted.drop_duplicates(subset=path_col, keep="first").copy()
        df_start[schema.event_col] = event_types.PATH_START.name
        df_start[schema.event_type] = event_types.PATH_START.type
        df_start[schema.subindex] = event_types.PATH_START.index

        df_end = clean_sorted.drop_duplicates(subset=path_col, keep="last").copy()
        df_end[schema.event_col] = event_types.PATH_END.name
        df_end[schema.event_type] = event_types.PATH_END.type
        df_end[schema.subindex] = event_types.PATH_END.index

        df = (
            pd.concat([clean, df_start, df_end])
            .sort_values([schema.path_col, schema.index, schema.subindex])
            .reset_index(drop=True)
        )
        df[schema.event_col] = df[schema.event_col].astype("category")
        return df, schema
