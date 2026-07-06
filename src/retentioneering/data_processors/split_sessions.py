import duckdb
import pandas as pd
from dataclasses import dataclass
from typing import Any, List, Tuple

from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import PreprocessingConfigError
from retentioneering.utils.session_detection import (
    build_session_ctes,
    parse_timeout,
    sql_list,
    to_list,
)

PROCESSOR_NAME = "split_sessions"


@dataclass
class SplitSessions(DataProcessor):
    session_id_col: str
    session_index_col: str
    separator: List[str] | None
    start_event: List[str] | None
    end_event: List[str] | None
    timeout_seconds: float | None
    path_col: str | None
    event_col: str | None

    def __init__(
        self,
        session_id_col: str = "session_id",
        session_index_col: str = "session_index",
        separator: str | List[str] | None = None,
        start_event: str | List[str] | None = None,
        end_event: str | List[str] | None = None,
        timeout: "str | pd.Timedelta | None" = None,
        path_col: str | None = None,
        event_col: str | None = None,
    ) -> None:
        self.session_id_col = session_id_col
        self.session_index_col = session_index_col
        self.separator = to_list(separator) if separator else None
        self.start_event = to_list(start_event) if start_event else None
        self.end_event = to_list(end_event) if end_event else None
        if timeout is not None:
            try:
                self.timeout_seconds = parse_timeout(timeout)
            except ValueError as exc:
                raise PreprocessingConfigError(PROCESSOR_NAME, str(exc)) from exc
        else:
            self.timeout_seconds = None
        self.path_col = path_col
        self.event_col = event_col
        super().__init__()

        self._validate()

    def _validate(self) -> None:
        boundary_count = sum(
            [
                bool(self.separator),
                bool(self.start_event) or bool(self.end_event),
            ]
        )
        if boundary_count > 1:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                "specify at most one boundary mode: 'separator' or 'start_event'+'end_event'",
            )
        if self.timeout_seconds is None and boundary_count == 0:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                "specify at least one of: 'separator', 'start_event'+'end_event', 'timeout'",
            )
        if bool(self.start_event) != bool(self.end_event):
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                "'start_event' and 'end_event' must be specified together",
            )

    def _as_group(self) -> dict:
        g: dict[str, Any] = {}
        if self.separator:
            g["separator"] = self.separator
        if self.start_event:
            g["start_event"] = self.start_event
        if self.end_event:
            g["end_event"] = self.end_event
        if self.timeout_seconds is not None:
            g["timeout"] = self.timeout_seconds
        return g

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        path_col = self.path_col or schema.path_col
        event_col = self.event_col or schema.event_col
        ts_col = schema.timestamp_col
        subindex_col = schema.subindex

        for col in (self.session_id_col, self.session_index_col):
            if col in schema.cols:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, f"column '{col}' already exists in the eventstream"
                )

        group = self._as_group()
        session_ctes = build_session_ctes(
            group, path_col, event_col, ts_col, subindex_col, separator_starts=True
        )

        cols_with_prefix = ", ".join(f"w.{c}" for c in schema.cols)
        if self.separator:
            where_clause = "WHERE w._is_sep = 0"
        elif self.start_event:
            where_clause = f"WHERE {event_col} NOT IN ({sql_list(self.start_event + self.end_event)})"
        else:
            where_clause = ""

        query = f"""
        WITH
        {session_ctes}
        SELECT
            {cols_with_prefix},
            CASE WHEN w._in_session = 1 THEN CAST(w._session_counter AS INTEGER) END AS {self.session_index_col},
            CASE WHEN w._in_session = 1
                 THEN CONCAT(CAST(w.{path_col} AS VARCHAR), '_', CAST(w._session_counter AS VARCHAR))
            END AS {self.session_id_col}
        FROM with_session_id w
        {where_clause}
        ORDER BY w.{path_col}, w.{ts_col}, w.{subindex_col}
        """

        result = duckdb.query(query).df()

        new_schema = schema.copy()
        new_schema.custom_cols = schema.custom_cols + [self.session_index_col]
        new_schema.path_cols = schema.path_cols + [self.session_id_col]

        return result, new_schema
