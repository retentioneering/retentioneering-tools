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
    # Cannot break the event order: keeps whole paths. The JOIN may return paths
    # in any order, which the layout contract allows; within a path the scan
    # order is kept (the test suite's strict check verifies it).
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

        query_template = """
             with sampled_paths as (
                select {path_col}
                from (select distinct {path_col} from df {order_chunk})
                using sample reservoir({sample_chunk}) {seed_chunk}
             )
             select * from df join sampled_paths using({path_col})
         """

        seed_chunk = order_chunk = ""

        if self.random_state is not None:
            seed_chunk = f"repeatable({int(self.random_state)})"
            # A seed reproduces the sample only over the same input order, and
            # DISTINCT returns paths in an order that depends on threading and
            # on the frame's row order. Sorting the IDs makes the sample depend
            # on the seed and the set of paths alone.
            order_chunk = f"order by {path_col_q}"

        if self.frac is not None:
            sample_chunk = f"{self.frac * 100}%"
        else:
            sample_chunk = f"{self.n} rows"

        query = query_template.format(
            path_col=path_col_q,
            sample_chunk=sample_chunk,
            seed_chunk=seed_chunk,
            order_chunk=order_chunk,
        )

        df = engine.run(query, df=df)

        for col in [schema.event_col] + schema.segment_cols:
            df[col] = df[col].astype("category")
            df[col] = df[col].cat.remove_unused_categories()
            df[col] = df[col].cat.as_unordered()

        return df, schema
