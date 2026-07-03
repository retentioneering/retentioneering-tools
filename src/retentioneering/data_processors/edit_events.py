from typing import Dict, List, Tuple

import pandas as pd

from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.data_processors.filter_events import FilterEvents
from retentioneering.data_processors.rename_events import RenameEvents
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import PreprocessingConfigError

PROCESSOR_NAME = "edit_events"


class EditEvents(DataProcessor):
    rename: Dict[str, str]
    delete: List[str]

    def __init__(
        self,
        rename: Dict[str, str] | None = None,
        delete: List[str] | None = None,
    ) -> None:
        self.rename = rename if rename is not None else {}
        self.delete = delete if delete is not None else []

        if not isinstance(self.rename, dict):
            raise PreprocessingConfigError(PROCESSOR_NAME, "Argument 'rename' must be a dictionary.")
        if not all(isinstance(k, str) and isinstance(v, str) for k, v in self.rename.items()):
            raise PreprocessingConfigError(PROCESSOR_NAME, "All keys and values in 'rename' must be strings.")

        if not isinstance(self.delete, list):
            raise PreprocessingConfigError(PROCESSOR_NAME, "Argument 'delete' must be a list.")
        if not all(isinstance(e, str) for e in self.delete):
            raise PreprocessingConfigError(PROCESSOR_NAME, "All elements in 'delete' must be strings.")

        overlap = set(self.delete) & set(self.rename.keys())
        if overlap:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"Events cannot be both deleted and renamed: {sorted(overlap)}."
            )

        super().__init__()

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        if self.delete:
            existing = set(df[schema.event_col].cat.categories.tolist())
            unknown = set(self.delete) - existing
            if unknown:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME,
                    f"Unknown event names in 'delete': {sorted(unknown)}. "
                    f"Available events: {sorted(existing)}."
                )
            dp = FilterEvents(values={"column": schema.event_col, "values": self.delete, "exclude": True})
            df, schema = dp.apply(df, schema)

        if self.rename:
            dp = RenameEvents(self.rename)
            df, schema = dp.apply(df, schema)

        return df, schema
