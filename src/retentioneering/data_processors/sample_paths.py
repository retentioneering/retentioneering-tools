from dataclasses import dataclass
from typing import Tuple

import pandas as pd

from retentioneering import engine
from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import PreprocessingConfigError

PROCESSOR_NAME = "sample_paths"


@dataclass
class SamplePaths(DataProcessor):
    # Cannot break the event order: keeps whole paths with a boolean mask, in input order.
    may_break_order = False

    n: int | None
    frac: float | None
    random_state: int | None
    path_col: str | None

    def __init__(
        self,
        n: int | None = None,
        frac: float | None = None,
        random_state: int | None = None,
        path_col: str | None = None,
    ) -> None:
        if (n is None) == (frac is None):
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "Exactly one of 'n' or 'frac' must be provided."
            )
        if n is not None and (not isinstance(n, int) or isinstance(n, bool) or n <= 0):
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "Argument 'n' must be a positive integer."
            )
        if frac is not None:
            if not isinstance(frac, (int, float)) or isinstance(frac, bool):
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "Argument 'frac' must be a float in (0.0, 1.0]."
                )
            if frac > 1.0 or frac <= 0.0:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "Argument 'frac' must be in the range (0.0, 1.0]."
                )

        self.n = n
        self.frac = float(frac) if frac is not None else None
        self.random_state = random_state
        self.path_col = path_col
        super().__init__()

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        path_col = self.path_col or schema.path_col

        if self.frac == 1.0:
            return df, schema

        path_col_q = engine.quote_ident(path_col)

        # DuckDB picks the paths; the rows are then kept with a mask, which
        # leaves them in the input layout (ADR-0017). A JOIN would not
        # guarantee any row order.
        query_template = """
            {set_threads}
            select {path_col}
            from (select distinct {path_col} from df)
            using sample reservoir({sample_chunk}) {seed_chunk}
         """

        seed_chunk = set_threads = ""

        if self.random_state is not None:
            seed_chunk = f"repeatable({self.random_state})"
            set_threads = "set threads = 1;"

        if self.frac is not None:
            sample_chunk = f"{self.frac * 100}%"
        else:
            sample_chunk = f"{self.n} rows"

        query = query_template.format(
            path_col=path_col_q,
            sample_chunk=sample_chunk,
            seed_chunk=seed_chunk,
            set_threads=set_threads,
        )

        sampled = engine.run(query, df=df)[path_col]
        df = df[df[path_col].isin(sampled)].reset_index(drop=True)

        for col in [schema.event_col] + schema.segment_cols:
            df[col] = df[col].astype("category")
            df[col] = df[col].cat.remove_unused_categories()
            df[col] = df[col].cat.as_unordered()

        return df, schema
