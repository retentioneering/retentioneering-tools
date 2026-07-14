from typing import Dict, Tuple

import pandas as pd

from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import PreprocessingConfigError
from retentioneering.utils.sequences import find_delimiter_collisions

PROCESSOR_NAME = "rename_events"


class RenameEvents(DataProcessor):
    mapping: Dict[str, str]

    def __init__(self, mapping: Dict[str, str]) -> None:
        if not isinstance(mapping, dict):
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "Argument 'mapping' must be a dictionary."
            )
        if not all(
            isinstance(k, str) and isinstance(v, str) for k, v in mapping.items()
        ):
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "All keys and values in 'mapping' must be strings."
            )

        offenders = find_delimiter_collisions(mapping.values())
        if offenders:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"New event name(s) {offenders} in 'mapping' contain '->', which "
                f"retentioneering uses as the path delimiter in matches_pattern/"
                f"step_matrix pattern matching. Choose different names.",
            )

        self.mapping = mapping
        super().__init__()

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        if not self.mapping:
            return df, schema

        df = df.copy()

        for event_col in schema.event_cols:
            existing = set(df[event_col].cat.categories.tolist())
            unknown = set(self.mapping.keys()) - existing
            if unknown:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME,
                    f"Unknown event names in 'mapping': {sorted(unknown)}. "
                    f"Available events: {sorted(existing)}.",
                )

            series = df[event_col].astype(str).replace(self.mapping)
            df[event_col] = series.astype("category")
            df[event_col] = df[event_col].cat.remove_unused_categories()
            df[event_col] = df[event_col].cat.as_unordered()

        return df, schema
