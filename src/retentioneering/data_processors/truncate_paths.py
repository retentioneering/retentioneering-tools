from typing import Tuple

import duckdb
import pandas as pd

from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import PreprocessingConfigError

PROCESSOR_NAME = "truncate_paths"


class TruncatePaths(DataProcessor):
    """
    Truncate paths by keeping only events between two boundary events (inclusive).

    Parameters
    ----------
    left : str
        The left boundary event name. The truncated path will start from this event.
    right : str
        The right boundary event name. The truncated path will end at this event.
    path_id_col : str, optional
        Path ID column name. If None, taken from schema.
    event_col : str, optional
        Event column name. If None, taken from schema.
    """

    left: str
    right: str
    path_id_col: str | None
    event_col: str | None

    def __init__(
        self,
        left: str,
        right: str,
        path_id_col: str | None = None,
        event_col: str | None = None,
    ) -> None:
        if not left:
            raise PreprocessingConfigError(PROCESSOR_NAME, "Parameter 'left' must be a non-empty string.")
        if not right:
            raise PreprocessingConfigError(PROCESSOR_NAME, "Parameter 'right' must be a non-empty string.")

        self.left = left
        self.right = right
        self.path_id_col = path_id_col
        self.event_col = event_col
        super().__init__()

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        path_id_col = self.path_id_col or schema.path_col
        event_col = self.event_col or schema.event_col
        timestamp_col = schema.timestamp

        # SQL query to find the first occurrence of left and right events in each path
        # and keep only events between them (inclusive)
        query = f"""
        WITH left_bounds AS (
            SELECT
                {path_id_col},
                MIN(CASE WHEN {event_col} = '{self.left}' THEN {schema.index} END) AS left_idx
            FROM df
            GROUP BY {path_id_col}
        ),
        right_bounds AS (
            SELECT
                df.{path_id_col},
                MIN(CASE WHEN df.{event_col} = '{self.right}' THEN df.{schema.index} END) AS right_idx
            FROM df
            INNER JOIN left_bounds lb ON df.{path_id_col} = lb.{path_id_col}
            WHERE df.{schema.index} > lb.left_idx OR (df.{schema.index} = lb.left_idx AND '{self.left}' = '{self.right}')
            GROUP BY df.{path_id_col}
        ),
        path_bounds AS (
            SELECT
                lb.{path_id_col},
                lb.left_idx,
                rb.right_idx
            FROM left_bounds lb
            INNER JOIN right_bounds rb ON lb.{path_id_col} = rb.{path_id_col}
        )
        SELECT df.*
        FROM df
        INNER JOIN path_bounds pb
            ON df.{path_id_col} = pb.{path_id_col}
        WHERE
            df.{schema.index} BETWEEN pb.left_idx AND pb.right_idx
        ORDER BY df.{path_id_col}, df.{timestamp_col}, df.{schema.subindex}
        """

        result = duckdb.query(query).df()

        # Restore categorical dtypes and remove unused categories
        for col in schema.event_cols + schema.segment_cols:
            if col in result.columns:
                result[col] = result[col].astype("category")
                result[col] = result[col].cat.remove_unused_categories()
                result[col] = result[col].cat.as_unordered()

        return result, schema
