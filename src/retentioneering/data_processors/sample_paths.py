from dataclasses import dataclass
from typing import Tuple

import duckdb
import pandas as pd

from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import PreprocessingConfigError

PROCESSOR_NAME = "sample_paths"


@dataclass
class SamplePaths(DataProcessor):
    sample_size: float | int
    random_state: int | None
    path_id_col: str | None

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        path_id_col = self.path_id_col or schema.path_col

        if self.sample_size == 1.0 and isinstance(self.sample_size, float):
            return df, schema

        if isinstance(self.sample_size, float) and (
            self.sample_size > 1.0 or self.sample_size <= 0.0
        ):
            raise PreprocessingConfigError(PROCESSOR_NAME, "Float sample size must be between 0 and 1.")

        query_template = """
            {set_threads}
             with sampled_paths as (
                select {path_id_col}
                from (select distinct {path_id_col} from df)
                using sample reservoir({sample_chunk}) {seed_chunk}
             )
             select * from df join sampled_paths using({path_id_col})
         """

        sample_chunk = seed_chunk = set_threads = ""

        if self.random_state is not None:
            seed_chunk = f"repeatable({self.random_state})"
            set_threads = "set threads = 1;"

        if isinstance(self.sample_size, float):
            sample_chunk = f"{self.sample_size * 100}%"
        elif isinstance(self.sample_size, int):
            sample_chunk = f"{self.sample_size} rows"
        else:
            raise PreprocessingConfigError(PROCESSOR_NAME, "Sample size must be either a float or an integer.")

        query = query_template.format(
            path_id_col=path_id_col,
            sample_chunk=sample_chunk,
            seed_chunk=seed_chunk,
            set_threads=set_threads,
        )

        df = duckdb.query(query).df()

        for col in schema.event_cols + schema.segment_cols:
            df[col] = df[col].astype("category")
            df[col] = df[col].cat.remove_unused_categories()
            df[col] = df[col].cat.as_unordered()

        return df, schema
