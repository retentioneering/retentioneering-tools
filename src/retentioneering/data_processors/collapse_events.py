import json
import duckdb
import pandas as pd
from dataclasses import dataclass
from typing import Any, Dict, List, Set, Tuple

from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.data_processors.filter_paths import FilterPaths
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.eventstream.event_type import EventTypes
from retentioneering.exceptions import PreprocessingConfigError
from retentioneering.utils.session_detection import (
    build_session_ctes,
    detect_mode,
    to_list,
    _MODE_TIMEOUT,
)

PROCESSOR_NAME = "collapse_events"


@dataclass
class CollapseEvents(DataProcessor):
    repetitive: bool | List[str] | None
    event_groups: List[Dict[str, Any]] | None
    event_from_col: str | None
    session_id_col: str | None
    session_type_col: str | None
    agg: Dict[str, str]
    path_id_col: str | None
    event_col: str | None

    def __init__(
        self,
        repetitive: bool | List[str] | None = None,
        event_groups: List[Dict[str, Any]] | None = None,
        event_from_col: str | None = None,
        session_id_col: str | None = None,
        session_type_col: str | None = None,
        agg: Dict[str, str] | None = None,
        path_id_col: str | None = None,
        event_col: str | None = None,
    ) -> None:
        self.repetitive = repetitive
        self.event_groups = event_groups
        self.event_from_col = event_from_col
        self.session_id_col = session_id_col
        self.session_type_col = session_type_col
        self.agg = agg or {}
        self.path_id_col = path_id_col
        self.event_col = event_col
        super().__init__()

        modes = [
            self.repetitive is not None,
            bool(self.event_groups),
            self.event_from_col is not None,
            self.session_id_col is not None,
        ]
        if sum(modes) != 1:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                "Provide exactly one of: repetitive, event_groups, event_from_col, session_id_col",
            )

        if self.session_id_col is not None and self.session_type_col is None:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "'session_id_col' requires 'session_type_col'"
            )

        if event_groups is not None:
            if not event_groups:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "event_groups must not be empty"
                )
            for g in event_groups:
                self._validate_group(g)

    @staticmethod
    def _validate_group(g: Dict[str, Any]) -> None:
        has_events = bool(g.get("events"))
        has_separator = bool(g.get("separator"))
        has_start = bool(g.get("start_event"))
        has_end = bool(g.get("end_event"))

        boundary_count = sum([has_events, has_separator, has_start or has_end])
        if boundary_count != 1:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                "each group must define exactly one boundary mode: 'events', 'separator', or 'start_event'+'end_event'",
            )
        if has_start and not has_end:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "'start_event' requires 'end_event'"
            )
        if has_end and not has_start:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "'end_event' requires 'start_event'"
            )
        if not g.get("default") and not g.get("cases"):
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                "each group must specify 'default' or at least one item in 'cases'",
            )

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        path_id_col = self.path_id_col or schema.path_col
        event_col = self.event_col or schema.event_col

        if self.repetitive is not None:
            result = self._collapse_repetitive(df, schema, path_id_col, event_col)
        elif self.event_groups:
            result = df
            for group in self.event_groups:
                result = self._collapse_group(
                    result, schema, path_id_col, event_col, group
                )
        elif self.event_from_col is not None:
            return self._collapse_by_col(df, schema, path_id_col, event_col)
        else:
            return self._collapse_by_session_type(df, schema, path_id_col, event_col)

        for col in schema.event_cols + schema.segment_cols:
            if col in result.columns:
                result[col] = result[col].astype("category")
                result[col] = result[col].cat.remove_unused_categories()
                result[col] = result[col].cat.as_unordered()

        return result, schema

    @staticmethod
    def _session_agg_exprs(
        df: pd.DataFrame,
        agg_config: Dict[str, str],
        exclude_cols: Set[str],
        ts_col: str,
    ) -> List[str]:
        agg_exprs = []
        for c in df.columns:
            if c in exclude_cols:
                continue
            agg_name = agg_config.get(c, "first")
            if agg_name == "first":
                agg_exprs.append(f"ARG_MIN({c}, {ts_col}) AS {c}")
            elif agg_name == "last":
                agg_exprs.append(f"ARG_MAX({c}, {ts_col}) AS {c}")
            elif agg_name == "min":
                agg_exprs.append(f"MIN({c}) AS {c}")
            elif agg_name == "max":
                agg_exprs.append(f"MAX({c}) AS {c}")
            elif agg_name == "mean":
                agg_exprs.append(f"AVG({c}) AS {c}")
            elif agg_name == "mode":
                agg_exprs.append(f"MODE({c}) AS {c}")
            elif agg_name == "any":
                agg_exprs.append(f"ANY_VALUE({c}) AS {c}")
            else:
                agg_exprs.append(f"ARG_MIN({c}, {ts_col}) AS {c}")
        return agg_exprs

    @staticmethod
    def _metric_agg_sql(
        mc: Dict[str, Any], event_col: str, ts_col: str
    ) -> List[Tuple[str, str]]:
        metric = mc["metric"]
        args = mc.get("metric_args") or {}

        if metric == "has":
            events = to_list(args.get("events", []))
            return [
                (f"has_{e}", f"MAX(CASE WHEN {event_col} = '{e}' THEN 1 ELSE 0 END)")
                for e in events
            ]
        elif metric == "event_count":
            events = to_list(args.get("events", []))
            return [
                (
                    f"event_count_{e}",
                    f"COUNT(CASE WHEN {event_col} = '{e}' THEN 1 ELSE NULL END)",
                )
                for e in events
            ]
        elif metric == "duration":
            return [("duration", f"EPOCH(MAX({ts_col}) - MIN({ts_col}))")]
        elif metric == "length":
            return [("length", "COUNT(*)")]
        elif metric == "time_between":
            ef = args.get("event_from", "")
            et = args.get("event_to", "")
            agg_sql = (
                f"EPOCH("
                f"MIN(CASE WHEN {event_col} = '{et}' THEN {ts_col} END) - "
                f"MIN(CASE WHEN {event_col} = '{ef}' THEN {ts_col} END))"
            )
            return [(f"time_from_{ef}_to_{et}", agg_sql)]
        elif metric == "active_days":
            return [("active_days", f"COUNT(DISTINCT CAST({ts_col} AS DATE))")]
        else:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"Metric '{metric}' is not supported in event_groups cases. "
                f"Supported metrics: has, event_count, duration, length, time_between, active_days.",
            )

    def _collapse_repetitive(
        self,
        df: pd.DataFrame,
        schema: EventstreamSchema,
        path_id_col: str,
        event_col: str,
    ) -> pd.DataFrame:
        timestamp_col = schema.timestamp
        collapsed_event_type = EventTypes().COLLAPSED_EVENT.type
        exclude = {path_id_col, schema.event_col, schema.event_type}
        agg_exprs = self._session_agg_exprs(df, self.agg, exclude, timestamp_col)
        agg_chunk = (", ".join(agg_exprs)) if agg_exprs else ""

        if self.repetitive is True:
            is_start_condition = f"LAG({event_col}) OVER (PARTITION BY {path_id_col} ORDER BY {timestamp_col}, {schema.subindex}) = {event_col}"
        else:
            events_list = ", ".join(f"'{event}'" for event in self.repetitive)
            is_start_condition = (
                f"LAG({event_col}) OVER (PARTITION BY {path_id_col} ORDER BY {timestamp_col}, {schema.subindex}) = {event_col}"
                f" AND {event_col} IN ({events_list})"
            )

        query = f"""
        WITH event_group_starts AS (
            SELECT *,
                CASE WHEN {is_start_condition}
                     THEN 0 ELSE 1 END AS is_start
            FROM df
        ),
        event_groups AS (
            SELECT *,
                SUM(is_start) OVER (PARTITION BY {path_id_col} ORDER BY {timestamp_col}, {schema.subindex}) AS grp
            FROM event_group_starts
        )
        SELECT
            {path_id_col},
            ANY_VALUE({event_col}) AS {event_col},
            CASE WHEN COUNT(*) > 1 THEN '{collapsed_event_type}' ELSE ARG_MIN({schema.event_type}, {timestamp_col}) END AS {schema.event_type},
            {agg_chunk}
        FROM event_groups
        GROUP BY {path_id_col}, grp
        ORDER BY {path_id_col}, MIN({timestamp_col}), MIN({schema.subindex})
        """
        res = duckdb.query(query).df()
        res = res[schema.cols]
        return res

    def _collapse_group(
        self,
        df: pd.DataFrame,
        schema: EventstreamSchema,
        path_id_col: str,
        event_col: str,
        group: Dict[str, Any],
    ) -> pd.DataFrame:
        ts_col = schema.timestamp
        subindex_col = schema.subindex
        event_type_col = schema.event_type
        collapsed_event_type = EventTypes().COLLAPSED_EVENT.type

        cases: List[Dict[str, Any]] = group.get("cases", [])
        mode = detect_mode(group)
        if mode == _MODE_TIMEOUT:
            default = group.get("default", "session")
        else:
            default = group.get(
                "default", to_list(group.get("events") or ["session"])[0]
            )

        session_ctes = build_session_ctes(
            group, path_id_col, event_col, ts_col, subindex_col
        )

        fp = FilterPaths(None, None, None)
        all_metric_configs: List[Dict[str, Any]] = []
        seen_keys: Set[str] = set()
        for case in cases:
            for mc in FilterPaths._extract_metric_configs(case["condition"]):
                key = (mc["metric"], str(sorted((mc.get("metric_args") or {}).items())))
                if key not in seen_keys:
                    seen_keys.add(key)
                    all_metric_configs.append(mc)

        metric_col_names: List[str] = []
        metric_agg_parts: List[str] = []
        for mc in all_metric_configs:
            for col_name, agg_sql in self._metric_agg_sql(mc, event_col, ts_col):
                if col_name not in metric_col_names:
                    metric_col_names.append(col_name)
                    metric_agg_parts.append(f'{agg_sql} AS "{col_name}"')

        metric_agg_chunk = (
            (", " + ", ".join(metric_agg_parts)) if metric_agg_parts else ""
        )

        if cases:
            case_when_parts = [
                f"WHEN {fp._ast_to_sql(case['condition'])} THEN '{case['new_name'].replace(chr(39), chr(39) * 2)}'"
                for case in cases
            ]
            default_escaped = default.replace("'", "''")
            classify_expr = (
                f"CASE {' '.join(case_when_parts)} ELSE '{default_escaped}' END"
            )
        else:
            classify_expr = f"'{default.replace(chr(39), chr(39) * 2)}'"

        exclude_cols = {path_id_col, event_col, event_type_col, ts_col}
        agg_exprs = self._session_agg_exprs(df, self.agg, exclude_cols, ts_col)
        agg_chunk = (", " + ", ".join(agg_exprs)) if agg_exprs else ""

        collapsed_select = ", ".join(
            f"{classify_expr} AS {event_col}" if c == event_col else c
            for c in schema.cols
        )
        cols_list = json.dumps(schema.cols)[1:-1]

        query = f"""
        WITH
        {session_ctes},
        session_raw AS (
            SELECT
                {path_id_col},
                _session_counter,
                MIN({ts_col}) AS {ts_col},
                '{collapsed_event_type}' AS {event_type_col}
                {metric_agg_chunk}
                {agg_chunk}
            FROM with_session_id
            WHERE _in_session = 1
            GROUP BY {path_id_col}, _session_counter
        ),
        collapsed AS (
            SELECT {collapsed_select}
            FROM session_raw
        ),
        uncollapsed AS (
            SELECT {cols_list}
            FROM with_session_id
            WHERE _in_session = 0
        )
        SELECT {cols_list} FROM collapsed
        UNION ALL
        SELECT {cols_list} FROM uncollapsed
        ORDER BY {path_id_col}, {ts_col}, {subindex_col}
        """

        return duckdb.query(query).df()

    def _collapse_by_col(
        self,
        df: pd.DataFrame,
        schema: EventstreamSchema,
        path_id_col: str,
        event_col: str,
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        ts_col = schema.timestamp
        subindex_col = schema.subindex
        event_type_col = schema.event_type
        collapsed_event_type = EventTypes().COLLAPSED_EVENT.type
        col = self.event_from_col

        if col not in df.columns:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, f"column '{col}' not found in eventstream"
            )
        if col == event_col:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"'event_from_col' must differ from event column '{event_col}'",
            )

        explicit_cols = {
            path_id_col,
            event_col,
            event_type_col,
            ts_col,
            subindex_col,
            col,
        }
        agg_exprs = self._session_agg_exprs(df, self.agg, explicit_cols, ts_col)
        agg_chunk = (", " + ", ".join(agg_exprs)) if agg_exprs else ""
        group_col_select = f", {col}" if col in schema.cols else ""
        cols_list = json.dumps(schema.cols)[1:-1]

        query = f"""
        WITH
        ordered AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY {path_id_col} ORDER BY {ts_col}, {subindex_col}
                ) AS _rn
            FROM df
        ),
        group_starts AS (
            SELECT *,
                CASE WHEN {col} IS DISTINCT FROM
                          LAG({col}) OVER (PARTITION BY {path_id_col} ORDER BY _rn)
                     THEN 1 ELSE 0 END AS _is_new_group
            FROM ordered
        ),
        with_group AS (
            SELECT *,
                SUM(_is_new_group) OVER (
                    PARTITION BY {path_id_col} ORDER BY _rn
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS _grp
            FROM group_starts
        ),
        collapsed AS (
            SELECT
                {path_id_col},
                MIN({ts_col}) AS {ts_col},
                MIN({subindex_col}) AS {subindex_col},
                CAST({col} AS VARCHAR) AS {event_col},
                '{collapsed_event_type}' AS {event_type_col}
                {group_col_select}
                {agg_chunk}
            FROM with_group
            GROUP BY {path_id_col}, _grp, {col}
        )
        SELECT {cols_list}
        FROM collapsed
        ORDER BY {path_id_col}, {ts_col}, {subindex_col}
        """

        result = duckdb.query(query).df()

        for c in schema.event_cols + schema.segment_cols:
            if c in result.columns:
                result[c] = result[c].astype("category")
                result[c] = result[c].cat.remove_unused_categories()
                result[c] = result[c].cat.as_unordered()

        return result, schema

    def _collapse_by_session_type(
        self,
        df: pd.DataFrame,
        schema: EventstreamSchema,
        path_id_col: str,
        event_col: str,
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        ts_col = schema.timestamp
        subindex_col = schema.subindex
        event_type_col = schema.event_type
        collapsed_event_type = EventTypes().COLLAPSED_EVENT.type
        session_id_col = self.session_id_col
        session_type_col = self.session_type_col

        if session_id_col not in df.columns:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, f"column '{session_id_col}' not found in eventstream"
            )
        if session_type_col not in df.columns:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, f"column '{session_type_col}' not found in eventstream"
            )

        explicit_cols = {
            path_id_col,
            event_col,
            event_type_col,
            ts_col,
            subindex_col,
            session_id_col,
            session_type_col,
        }
        agg_exprs = self._session_agg_exprs(df, self.agg, explicit_cols, ts_col)
        agg_chunk = (", " + ", ".join(agg_exprs)) if agg_exprs else ""
        cols_list = json.dumps(schema.cols)[1:-1]

        # session_id_col and session_type_col may be in schema.cols (custom_cols),
        # so include them explicitly to satisfy the final SELECT.
        extra_session_cols = ""
        if session_id_col in schema.cols:
            extra_session_cols += f", ANY_VALUE({session_id_col}) AS {session_id_col}"
        if session_type_col in schema.cols:
            extra_session_cols += (
                f", ANY_VALUE({session_type_col}) AS {session_type_col}"
            )

        query = f"""
        WITH collapsed AS (
            SELECT
                {path_id_col},
                MIN({ts_col}) AS {ts_col},
                MIN({subindex_col}) AS {subindex_col},
                CAST(ANY_VALUE({session_type_col}) AS VARCHAR) AS {event_col},
                '{collapsed_event_type}' AS {event_type_col}
                {extra_session_cols}
                {agg_chunk}
            FROM df
            GROUP BY {path_id_col}, {session_id_col}
        )
        SELECT {cols_list}
        FROM collapsed
        ORDER BY {path_id_col}, {ts_col}, {subindex_col}
        """

        result = duckdb.query(query).df()

        for c in schema.event_cols + schema.segment_cols:
            if c in result.columns:
                result[c] = result[c].astype("category")
                result[c] = result[c].cat.remove_unused_categories()
                result[c] = result[c].cat.as_unordered()

        return result, schema


# Module-level alias so daily_states.py can import this without depending on the class
_session_agg_exprs = CollapseEvents._session_agg_exprs
