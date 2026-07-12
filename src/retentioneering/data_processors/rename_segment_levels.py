from typing import Dict, Tuple

import pandas as pd

from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import PreprocessingConfigError

PROCESSOR_NAME = "rename_segment_levels"


class RenameSegmentLevels(DataProcessor):
    segment_col: str
    mapping: Dict[str, str]

    def __init__(self, segment_col: str, mapping: Dict[str, str]) -> None:
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

        self.segment_col = segment_col
        self.mapping = mapping
        super().__init__()

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        if self.segment_col not in schema.segment_cols:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, f"Segment '{self.segment_col}' is not found."
            )

        if not self.mapping:
            return df, schema

        existing = set(df[self.segment_col].cat.categories.tolist())
        unknown = set(self.mapping.keys()) - existing
        if unknown:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"Unknown levels in 'mapping' for segment '{self.segment_col}': "
                f"{sorted(unknown)}. Available levels: {sorted(existing)}.",
            )

        df = df.copy()
        series = df[self.segment_col].astype(str).replace(self.mapping)
        df[self.segment_col] = series.astype("category")
        df[self.segment_col] = df[self.segment_col].cat.remove_unused_categories()
        df[self.segment_col] = df[self.segment_col].cat.as_unordered()

        return df, schema
