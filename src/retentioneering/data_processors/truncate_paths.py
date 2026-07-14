from typing import Tuple

import pandas as pd

from retentioneering import engine
from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.event_type import EventTypes
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import PreprocessingConfigError

PROCESSOR_NAME = "truncate_paths"

_PATH_START = EventTypes().PATH_START.name
_PATH_END = EventTypes().PATH_END.name


class TruncatePaths(DataProcessor):
    """
    Truncate paths by keeping only events between two anchor events (inclusive).

    Parameters
    ----------
    start_event : str
        The event that marks the start of the window. The truncated path will
        start from this event. The reserved name `"path_start"` anchors the
        window at the path's actual first event instead of a named one.
    end_event : str
        The event that marks the end of the window. The truncated path will end
        at this event. The reserved name `"path_end"` anchors the window at the
        path's actual last event instead of a named one.
    path_col : str, optional
        Path ID column name. If None, taken from schema.
    event_col : str, optional
        Event column name. If None, taken from schema.
    """

    start_event: str
    end_event: str
    path_col: str | None
    event_col: str | None

    def __init__(
        self,
        start_event: str,
        end_event: str,
        path_col: str | None = None,
        event_col: str | None = None,
    ) -> None:
        if not start_event:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "Parameter 'start_event' must be a non-empty string."
            )
        if not end_event:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "Parameter 'end_event' must be a non-empty string."
            )

        self.start_event = start_event
        self.end_event = end_event
        self.path_col = path_col
        self.event_col = event_col
        super().__init__()

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        path_col = self.path_col or schema.path_col
        if path_col not in schema.path_cols:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"path_col '{path_col}' must be one of schema.path_cols: {schema.path_cols}.",
            )
        event_col = self.event_col or schema.event_col
        timestamp_col = schema.timestamp_col

        start_literal = "'" + self.start_event.replace("'", "''") + "'"
        end_literal = "'" + self.end_event.replace("'", "''") + "'"
        path_col_q = engine.quote_ident(path_col)
        event_col_q = engine.quote_ident(event_col)
        timestamp_col_q = engine.quote_ident(timestamp_col)
        index_col_q = engine.quote_ident(schema.index)
        subindex_col_q = engine.quote_ident(schema.subindex)

        # "path_start"/"path_end" are reserved sentinels (see EventTypes): they
        # anchor the window at the path's actual first/last event rather than
        # searching for a named event, so no path is ever dropped on that side.
        start_is_path_start = self.start_event == _PATH_START
        end_is_path_end = self.end_event == _PATH_END

        start_idx_expr = (
            f"MIN({index_col_q})"
            if start_is_path_start
            else f"MIN(CASE WHEN {event_col_q} = {start_literal} THEN {index_col_q} END)"
        )

        if end_is_path_end:
            end_idx_expr = f"MAX(df.{index_col_q})"
            end_filter = "sb.start_idx IS NOT NULL"
        else:
            end_idx_expr = f"MIN(CASE WHEN df.{event_col_q} = {end_literal} THEN df.{index_col_q} END)"
            end_filter = (
                f"df.{index_col_q} >= sb.start_idx"
                if start_is_path_start
                else f"df.{index_col_q} > sb.start_idx OR (df.{index_col_q} = sb.start_idx AND {start_literal} = {end_literal})"
            )

        # path_cols is validated (coarsest-first, strictly nested) at Eventstream
        # construction time, and path_col is restricted to schema.path_cols
        # above, so comparing schema.index directly is correct at any accepted
        # grain (see ADR-0004).
        #
        # SQL query to find the first occurrence of the start and end events in
        # each path and keep only events between them (inclusive)
        query = f"""
        WITH start_bounds AS (
            SELECT
                {path_col_q},
                {start_idx_expr} AS start_idx
            FROM df
            GROUP BY {path_col_q}
        ),
        end_bounds AS (
            SELECT
                df.{path_col_q},
                {end_idx_expr} AS end_idx
            FROM df
            INNER JOIN start_bounds sb ON df.{path_col_q} = sb.{path_col_q}
            WHERE {end_filter}
            GROUP BY df.{path_col_q}
        ),
        path_bounds AS (
            SELECT
                sb.{path_col_q},
                sb.start_idx,
                eb.end_idx
            FROM start_bounds sb
            INNER JOIN end_bounds eb ON sb.{path_col_q} = eb.{path_col_q}
        )
        SELECT df.*
        FROM df
        INNER JOIN path_bounds pb
            ON df.{path_col_q} = pb.{path_col_q}
        WHERE
            df.{index_col_q} BETWEEN pb.start_idx AND pb.end_idx
        ORDER BY df.{path_col_q}, df.{timestamp_col_q}, df.{subindex_col_q}
        """

        result = engine.run(query, df=df)

        # Restore categorical dtypes and remove unused categories
        for col in schema.event_cols + schema.segment_cols:
            if col in result.columns:
                result[col] = result[col].astype("category")
                result[col] = result[col].cat.remove_unused_categories()
                result[col] = result[col].cat.as_unordered()

        return result, schema
