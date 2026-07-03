from typing import Tuple

import pandas as pd

from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import PreprocessingConfigError

PROCESSOR_NAME = "drop_segment"


class DropSegment(DataProcessor):
    name: str

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__()

    def apply(self, df, schema) -> Tuple[pd.DataFrame, EventstreamSchema]:
        if self.name not in schema.segment_cols:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, f"Segment '{self.name}' is not found."
            )

        new_df = df.drop(columns=self.name)
        new_schema = schema.copy()
        new_schema.segment_cols.remove(self.name)
        return new_df, new_schema
