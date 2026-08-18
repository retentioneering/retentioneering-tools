import pandas as pd
from dataclasses import dataclass
from typing import Any, Dict, List, Set, Tuple

from retentioneering import engine
from retentioneering.engine import dialect
from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.eventstream.event_type import EventTypes
from retentioneering.exceptions import PreprocessingConfigError
from retentioneering.metrics.condition_ast import ast_to_sql, extract_metric_configs
from retentioneering.metrics.metric_builder import combined_metric_name
from retentioneering.utils.session_detection import (
    build_session_ctes,
    sql_list,
    to_list,
)
from retentioneering.utils.sequences import find_delimiter_collisions
from retentioneering.utils.sql_quoting import quote_literal

PROCESSOR_NAME = "collapse_events"

BOUNDS_KEYS = ("start_event", "end_event")

#: Modes that chunk a path by its events, sharing `split_sessions`' vocabulary.
BOUNDARY_MODES = ("event_groups", "bounds")
#: Modes that chunk a path by runs of equal values.
RUN_MODES = ("loops", "group_col")


@dataclass
class CollapseEvents(DataProcessor):
    """
    Merge a run of events into one, and give the result a name.

    Two decisions, two argument groups. *How to chunk the path* is one
    mutually-exclusive mode — `loops` / `group_col` group adjacent rows
    sharing a value, `event_groups` / `bounds` cut it into windows, sharing
    `split_sessions`' `build_session_ctes`.
    Inactivity is deliberately not one of them: breaking on a time gap is
    `split_sessions(timeout=...)`, whose session column this processor then
    collapses through `group_col`.
    *How to name the merged event* is `name`, orthogonal to all of them: a
    literal, another column's value, or conditions evaluated over the group's
    own events.
    """

    loops: bool | List[str] | None
    event_groups: List[str] | None
    start_event: List[str] | None
    end_event: List[str] | None
    group_col: str | None
    name: Any
    agg: Dict[str, str]
    path_col: str | None
    event_col: str | None

    def __init__(
        self,
        loops: bool | List[str] | None = None,
        event_groups: str | List[str] | None = None,
        bounds: Dict[str, Any] | None = None,
        group_col: str | None = None,
        name: Any = None,
        agg: Dict[str, str] | None = None,
        path_col: str | None = None,
        event_col: str | None = None,
    ) -> None:
        self.loops = loops
        self.event_groups = self._parse_event_groups(event_groups)
        self.start_event, self.end_event = self._parse_bounds(bounds)
        self.group_col = group_col
        self.name = name
        self.agg = agg or {}
        self.path_col = path_col
        self.event_col = event_col
        super().__init__()

        self._validate_modes()
        self.cases, self.name_col, self.literal_name = self._parse_name(name)

    # ── modes ────────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_event_groups(value) -> List[str] | None:
        """
        Normalize `event_groups` to a list of event names.

        5.1 spelled this parameter `event_groups` too, but as a list of *group
        spec dicts* carrying their own boundary mode and name. That shape is
        gone, and silently reading a dict as an event name would match nothing
        — so say what happened instead.
        """
        if not value:
            return None
        names = to_list(value)
        if any(not isinstance(n, str) for n in names):
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                "'event_groups' now takes event names — the events that form one "
                "group — not group spec dicts. A spec's boundary mode is a "
                "top-level argument now ('event_groups' / 'separator' / 'bounds') "
                "and its label is 'name'.",
            )
        return names

    @staticmethod
    def _parse_bounds(bounds: Dict[str, Any] | None) -> Tuple[Any, Any]:
        """Unpack the `bounds` mode dict into its two anchors."""
        if bounds is None:
            return None, None
        if not isinstance(bounds, dict):
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"'bounds' must be a dict with keys {list(BOUNDS_KEYS)}, "
                f"got {type(bounds).__name__}",
            )
        unknown = sorted(set(bounds) - set(BOUNDS_KEYS))
        if unknown:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"unknown 'bounds' key(s) {unknown}; allowed keys are {list(BOUNDS_KEYS)}",
            )
        missing = [k for k in BOUNDS_KEYS if not bounds.get(k)]
        if missing:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"'bounds' requires both {list(BOUNDS_KEYS)}; missing {missing}",
            )
        return to_list(bounds["start_event"]), to_list(bounds["end_event"])

    def _validate_modes(self) -> None:
        run_modes = [m for m in RUN_MODES if getattr(self, m) is not None]
        boundary_modes = [
            m
            for m in BOUNDARY_MODES
            if (self.start_event if m == "bounds" else getattr(self, m))
        ]
        if len(boundary_modes) > 1:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"specify at most one boundary mode, got {boundary_modes}: "
                f"{list(BOUNDARY_MODES)} are alternatives",
            )
        if run_modes and boundary_modes:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"{run_modes[0]!r} groups adjacent rows sharing a value and cannot "
                f"be combined with the boundary modes {list(BOUNDARY_MODES)}",
            )
        if len(run_modes) > 1:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"specify at most one of {list(RUN_MODES)}, got {run_modes}",
            )
        if not run_modes and not boundary_modes:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"provide exactly one mode: {list(RUN_MODES) + list(BOUNDARY_MODES)}",
            )

    @property
    def mode(self) -> str:
        """Which chunking mode this call selected."""
        for m in RUN_MODES:
            if getattr(self, m) is not None:
                return m
        if self.event_groups:
            return "event_groups"
        return "bounds"

    def _as_group(self) -> dict:
        """The boundary spec in the shared `session_detection` shape."""
        g: Dict[str, Any] = {}
        if self.event_groups:
            g["events"] = self.event_groups
        if self.start_event:
            g["start_event"] = self.start_event
            g["end_event"] = self.end_event
        return g

    # ── naming ───────────────────────────────────────────────────────────────

    def _parse_name(
        self, name: Any
    ) -> Tuple[List[Dict[str, Any]], str | None, str | None]:
        """
        Split `name` into its three forms: conditions, a column, or a literal.

        Returns `(cases, name_col, literal)`, of which at most `cases` +
        a literal fallback are set at once.
        """
        if name is None:
            if self.mode in BOUNDARY_MODES:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME,
                    f"the {self.mode!r} mode needs a 'name' for the merged event: "
                    f"a string, {{'col': ...}}, or a list of conditions",
                )
            return [], None, None

        if isinstance(name, str):
            self._check_delimiters([name])
            return [], None, name

        if isinstance(name, dict):
            unknown = sorted(set(name) - {"col"})
            if unknown or "col" not in name:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME,
                    f"a dict 'name' takes exactly one key, 'col' — got {sorted(name)}",
                )
            return [], name["col"], None

        if isinstance(name, (list, tuple)):
            entries = list(name)
            if not entries:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "a list 'name' must hold at least one case"
                )
            fallback = None
            if isinstance(entries[-1], str):
                fallback = entries.pop()
            if not entries:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME,
                    "a list 'name' holding only a fallback is just that string — "
                    "pass it directly as name=",
                )
            for case in entries:
                if (
                    not isinstance(case, dict)
                    or "condition" not in case
                    or not case.get("name")
                ):
                    raise PreprocessingConfigError(
                        PROCESSOR_NAME,
                        "each case in a list 'name' is a dict with 'condition' and "
                        "'name'; the last entry may be a plain string used as the "
                        "fallback for groups no case matched",
                    )
            self._check_delimiters(
                [c["name"] for c in entries] + ([fallback] if fallback else [])
            )
            return entries, None, fallback

        raise PreprocessingConfigError(
            PROCESSOR_NAME,
            f"'name' must be a string, {{'col': ...}} or a list of cases, "
            f"got {type(name).__name__}",
        )

    @staticmethod
    def _check_delimiters(names: List[str]) -> None:
        offenders = find_delimiter_collisions(names)
        if offenders:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"New event name(s) {offenders} contain '->', which retentioneering "
                f"uses as the path delimiter in matches_pattern/step_matrix pattern "
                f"matching. Choose different names.",
            )

    # ── application ──────────────────────────────────────────────────────────

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        path_col = self.path_col or schema.path_col
        event_col = self.event_col or schema.event_col

        if self.mode == "loops":
            result = self._collapse_loops(df, schema, path_col, event_col)
        elif self.mode == "group_col":
            result = self._collapse_group_col(df, schema, path_col, event_col)
        else:
            result = self._collapse_boundary(df, schema, path_col, event_col)

        for col in schema.event_cols + schema.segment_cols:
            if col in result.columns:
                result[col] = result[col].astype("category")
                result[col] = result[col].cat.remove_unused_categories()
                result[col] = result[col].cat.as_unordered()

        return result, schema

    # ── naming SQL ───────────────────────────────────────────────────────────

    def _name_sql(
        self, default_expr: str, event_col: str, ts_col: str
    ) -> Tuple[str, str]:
        """
        Build the merged event's name expression, plus any aggregates it needs.

        Returns `(name_expr, metric_agg_chunk)`. `name_expr` is evaluated *after*
        the GROUP BY, so `cases` can compare group-level aggregates; the chunk
        carries those aggregates into the grouped CTE. `default_expr` is the name
        the mode implies when `name` says nothing — the event itself, or the
        column being grouped on.
        """
        # Every form below is evaluated after the GROUP BY, so a column-valued
        # name has to ride into the grouped CTE as an aggregate first.
        if self.name_col is not None:
            col_q = engine.quote_ident(self.name_col)
            return (
                "CAST(_name_value AS VARCHAR)",
                f", ANY_VALUE({col_q}) AS _name_value",
            )

        fallback = (
            quote_literal(self.literal_name)
            if self.literal_name is not None
            else default_expr
        )
        if not self.cases:
            return fallback, ""

        metric_col_names: List[str] = []
        metric_agg_parts: List[str] = []
        seen_keys: Set[str] = set()
        for case in self.cases:
            for mc in extract_metric_configs(case["condition"], PROCESSOR_NAME):
                key = (mc["metric"], str(sorted((mc.get("metric_args") or {}).items())))
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                for col_name, agg_sql in self._metric_agg_sql(mc, event_col, ts_col):
                    if col_name not in metric_col_names:
                        metric_col_names.append(col_name)
                        metric_agg_parts.append(
                            f"{agg_sql} AS {engine.quote_ident(col_name)}"
                        )

        case_when_parts = [
            f"WHEN {ast_to_sql(case['condition'], PROCESSOR_NAME)} "
            f"THEN {quote_literal(case['name'])}"
            for case in self.cases
        ]
        name_expr = f"CASE {' '.join(case_when_parts)} ELSE {fallback} END"
        chunk = (", " + ", ".join(metric_agg_parts)) if metric_agg_parts else ""
        return name_expr, chunk

    def _validate_name_col(self, df: pd.DataFrame) -> None:
        if self.name_col is not None and self.name_col not in df.columns:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"name column {self.name_col!r} not found in eventstream",
            )

    @staticmethod
    def _session_agg_exprs(
        df: pd.DataFrame,
        agg_config: Dict[str, str],
        exclude_cols: Set[str],
        ts_col: str,
    ) -> List[str]:
        agg_exprs = []
        ts_col_q = engine.quote_ident(ts_col)
        for c in df.columns:
            if c in exclude_cols:
                continue
            c_q = engine.quote_ident(c)
            agg_name = agg_config.get(c, "first")
            if agg_name == "first":
                agg_exprs.append(f"ARG_MIN({c_q}, {ts_col_q}) AS {c_q}")
            elif agg_name == "last":
                agg_exprs.append(f"ARG_MAX({c_q}, {ts_col_q}) AS {c_q}")
            elif agg_name == "min":
                agg_exprs.append(f"MIN({c_q}) AS {c_q}")
            elif agg_name == "max":
                agg_exprs.append(f"MAX({c_q}) AS {c_q}")
            elif agg_name == "mean":
                agg_exprs.append(f"AVG({c_q}) AS {c_q}")
            elif agg_name == "mode":
                agg_exprs.append(f"MODE({c_q}) AS {c_q}")
            elif agg_name == "any":
                agg_exprs.append(f"ANY_VALUE({c_q}) AS {c_q}")
            else:
                agg_exprs.append(f"ARG_MIN({c_q}, {ts_col_q}) AS {c_q}")
        return agg_exprs

    @staticmethod
    def _metric_agg_sql(
        mc: Dict[str, Any], event_col: str, ts_col: str
    ) -> List[Tuple[str, str]]:
        metric = mc["metric"]
        args = mc.get("metric_args") or {}
        event_col_q = engine.quote_ident(event_col)
        ts_col_q = engine.quote_ident(ts_col)

        if metric == "has_event":
            event = args.get("event")
            return [
                (
                    f"has_event_{event}",
                    f"MAX(CASE WHEN {event_col_q} = {quote_literal(event)} THEN 1 ELSE 0 END)",
                )
            ]
        elif metric == "event_count":
            event = args.get("event")
            return [
                (
                    f"event_count_{event}",
                    f"COUNT(CASE WHEN {event_col_q} = {quote_literal(event)} THEN 1 ELSE NULL END)",
                )
            ]
        elif metric == "has_all_events":
            events = to_list(args.get("events", []))
            conds = " AND ".join(
                f"MAX(CASE WHEN {event_col_q} = {quote_literal(e)} THEN 1 ELSE 0 END) = 1"
                for e in events
            )
            return [
                (
                    combined_metric_name(metric, events),
                    f"CASE WHEN ({conds}) THEN 1 ELSE 0 END",
                )
            ]
        elif metric == "has_any_event":
            events = to_list(args.get("events", []))
            conds = " OR ".join(
                f"MAX(CASE WHEN {event_col_q} = {quote_literal(e)} THEN 1 ELSE 0 END) = 1"
                for e in events
            )
            return [
                (
                    combined_metric_name(metric, events),
                    f"CASE WHEN ({conds}) THEN 1 ELSE 0 END",
                )
            ]
        elif metric == "duration":
            return [("duration", dialect.epoch(f"MAX({ts_col_q}) - MIN({ts_col_q})"))]
        elif metric == "length":
            return [("length", "COUNT(*)")]
        elif metric == "time_between":
            ef = args.get("start_event", "")
            et = args.get("end_event", "")
            agg_sql = dialect.epoch(
                f"MIN(CASE WHEN {event_col_q} = {quote_literal(et)} THEN {ts_col_q} END) - "
                f"MIN(CASE WHEN {event_col_q} = {quote_literal(ef)} THEN {ts_col_q} END)"
            )
            return [(f"time_from_{ef}_to_{et}", agg_sql)]
        elif metric == "active_days":
            return [("active_days", f"COUNT(DISTINCT CAST({ts_col_q} AS DATE))")]
        else:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"Metric '{metric}' is not supported in event_groups cases. "
                f"Supported metrics: has_event, event_count, has_all_events, has_any_event, "
                f"duration, length, time_between, active_days. "
                f"(has_event_bulk/event_count_bulk are never supported here - they produce "
                f"multiple columns.)",
            )

    # ── builders ─────────────────────────────────────────────────────────────

    def _collapse_loops(
        self,
        df: pd.DataFrame,
        schema: EventstreamSchema,
        path_col: str,
        event_col: str,
    ) -> pd.DataFrame:
        self._validate_name_col(df)
        ts_col = schema.timestamp_col
        collapsed_event_type = EventTypes().COLLAPSED_EVENT.type
        exclude = {path_col, event_col, schema.event_type}
        agg_exprs = self._session_agg_exprs(df, self.agg, exclude, ts_col)
        agg_chunk = (", " + ", ".join(agg_exprs)) if agg_exprs else ""

        path_col_q = engine.quote_ident(path_col)
        event_col_q = engine.quote_ident(event_col)
        ts_col_q = engine.quote_ident(ts_col)
        event_type_col_q = engine.quote_ident(schema.event_type)
        subindex_col_q = engine.quote_ident(schema.subindex)

        # A run of one event keeps that event's own name unless `name` overrides it.
        name_expr, metric_agg_chunk = self._name_sql("_event", event_col, ts_col)

        # LAG/SUM below must be ordered by a unique key: (timestamp, subindex) can
        # tie (subindex is the same for all raw events), and DuckDB's default RANGE
        # window frame lumps tied peer rows into one group, silently merging
        # distinct consecutive events that share a timestamp. A precomputed
        # ROW_NUMBER (_rn) plus an explicit ROWS frame makes grouping deterministic.
        if self.loops is True:
            is_start_condition = f"LAG({event_col_q}) OVER (PARTITION BY {path_col_q} ORDER BY _rn) = {event_col_q}"
        else:
            events_list = sql_list(to_list(self.loops))
            is_start_condition = (
                f"LAG({event_col_q}) OVER (PARTITION BY {path_col_q} ORDER BY _rn) = {event_col_q}"
                f" AND {event_col_q} IN ({events_list})"
            )

        collapsed_select = ", ".join(
            f"{name_expr} AS {event_col_q}" if c == event_col else engine.quote_ident(c)
            for c in schema.cols
        )

        query = f"""
        WITH ordered AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY {path_col_q} ORDER BY {ts_col_q}, {subindex_col_q}
                ) AS _rn
            FROM df
        ),
        event_group_starts AS (
            SELECT *,
                CASE WHEN {is_start_condition}
                     THEN 0 ELSE 1 END AS is_start
            FROM ordered
        ),
        event_groups AS (
            SELECT *,
                SUM(is_start) OVER (
                    PARTITION BY {path_col_q} ORDER BY _rn
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS grp
            FROM event_group_starts
        ),
        grouped AS (
            SELECT
                {path_col_q},
                ANY_VALUE({event_col_q}) AS _event,
                CASE WHEN COUNT(*) > 1 THEN '{collapsed_event_type}' ELSE ARG_MIN({event_type_col_q}, {ts_col_q}) END AS {event_type_col_q},
                MIN(_rn) AS _first_rn
                {metric_agg_chunk}
                {agg_chunk}
            FROM event_groups
            GROUP BY {path_col_q}, grp
        )
        SELECT {collapsed_select}
        FROM grouped
        ORDER BY {path_col_q}, _first_rn
        """
        res = engine.run(query, df=df)
        return res[schema.cols]

    def _collapse_group_col(
        self,
        df: pd.DataFrame,
        schema: EventstreamSchema,
        path_col: str,
        event_col: str,
    ) -> pd.DataFrame:
        """
        Collapse each run of equal values in `group_col` into one event.

        Grouping is by *runs*, not by value — this is not SQL's `GROUP BY`. A
        value that comes back later in the path starts a second event rather
        than joining the first one across the gap, which would produce an event
        whose span swallows everything in between.
        """
        self._validate_name_col(df)
        ts_col = schema.timestamp_col
        subindex_col = schema.subindex
        event_type_col = schema.event_type
        collapsed_event_type = EventTypes().COLLAPSED_EVENT.type
        col = self.group_col

        if col not in df.columns:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, f"column '{col}' not found in eventstream"
            )
        if col == event_col:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"'group_col' must differ from the event column '{event_col}'; to collapse "
                f"repeats of the same event use loops=True",
            )

        explicit_cols = {path_col, event_col, event_type_col, ts_col, subindex_col, col}
        agg_exprs = self._session_agg_exprs(df, self.agg, explicit_cols, ts_col)
        agg_chunk = (", " + ", ".join(agg_exprs)) if agg_exprs else ""

        path_col_q = engine.quote_ident(path_col)
        event_col_q = engine.quote_ident(event_col)
        ts_col_q = engine.quote_ident(ts_col)
        subindex_col_q = engine.quote_ident(subindex_col)
        event_type_col_q = engine.quote_ident(event_type_col)
        col_q = engine.quote_ident(col)

        # Without `name`, the group is named after the value it is a run of.
        name_expr, metric_agg_chunk = self._name_sql(
            "CAST(_run_value AS VARCHAR)", event_col, ts_col
        )

        run_col_select = (
            f", ANY_VALUE({col_q}) AS {col_q}" if col in schema.cols else ""
        )
        collapsed_select = ", ".join(
            f"{name_expr} AS {event_col_q}" if c == event_col else engine.quote_ident(c)
            for c in schema.cols
        )

        query = f"""
        WITH
        ordered AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY {path_col_q} ORDER BY {ts_col_q}, {subindex_col_q}
                ) AS _rn
            FROM df
        ),
        group_starts AS (
            SELECT *,
                CASE WHEN {col_q} IS DISTINCT FROM
                          LAG({col_q}) OVER (PARTITION BY {path_col_q} ORDER BY _rn)
                     THEN 1 ELSE 0 END AS _is_new_group
            FROM ordered
        ),
        with_group AS (
            SELECT *,
                SUM(_is_new_group) OVER (
                    PARTITION BY {path_col_q} ORDER BY _rn
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS _grp
            FROM group_starts
        ),
        grouped AS (
            SELECT
                {path_col_q},
                MIN({ts_col_q}) AS {ts_col_q},
                MIN({subindex_col_q}) AS {subindex_col_q},
                ANY_VALUE({col_q}) AS _run_value,
                '{collapsed_event_type}' AS {event_type_col_q}
                {run_col_select}
                {metric_agg_chunk}
                {agg_chunk}
            FROM with_group
            GROUP BY {path_col_q}, _grp
        )
        SELECT {collapsed_select}
        FROM grouped
        ORDER BY {path_col_q}, {ts_col_q}, {subindex_col_q}
        """
        return engine.run(query, df=df)

    def _collapse_boundary(
        self,
        df: pd.DataFrame,
        schema: EventstreamSchema,
        path_col: str,
        event_col: str,
    ) -> pd.DataFrame:
        """
        Collapse each window the boundary spec cuts out; rows outside stay put.

        The windows come from `session_detection.build_session_ctes`, the same
        chunking `split_sessions` writes into a column.
        """
        self._validate_name_col(df)
        ts_col = schema.timestamp_col
        subindex_col = schema.subindex
        event_type_col = schema.event_type
        collapsed_event_type = EventTypes().COLLAPSED_EVENT.type

        path_col_q = engine.quote_ident(path_col)
        ts_col_q = engine.quote_ident(ts_col)
        subindex_col_q = engine.quote_ident(subindex_col)
        event_type_col_q = engine.quote_ident(event_type_col)
        event_col_q = engine.quote_ident(event_col)

        # A window has no natural name of its own — `name` is required here, so
        # the default below is only ever reached through a cases fallback.
        default = quote_literal(
            self.event_groups[0] if self.event_groups else "session"
        )
        name_expr, metric_agg_chunk = self._name_sql(default, event_col, ts_col)

        session_ctes = build_session_ctes(
            self._as_group(), path_col, event_col, ts_col, subindex_col
        )

        exclude_cols = {path_col, event_col, event_type_col, ts_col}
        agg_exprs = self._session_agg_exprs(df, self.agg, exclude_cols, ts_col)
        agg_chunk = (", " + ", ".join(agg_exprs)) if agg_exprs else ""

        collapsed_select = ", ".join(
            f"{name_expr} AS {event_col_q}" if c == event_col else engine.quote_ident(c)
            for c in schema.cols
        )
        cols_list = ", ".join(engine.quote_ident(c) for c in schema.cols)

        query = f"""
        WITH
        {session_ctes},
        session_raw AS (
            SELECT
                {path_col_q},
                _session_counter,
                MIN({ts_col_q}) AS {ts_col_q},
                '{collapsed_event_type}' AS {event_type_col_q}
                {metric_agg_chunk}
                {agg_chunk}
            FROM with_session_id
            WHERE _in_session = 1
            GROUP BY {path_col_q}, _session_counter
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
        ORDER BY {path_col_q}, {ts_col_q}, {subindex_col_q}
        """
        return engine.run(query, df=df)


# Module-level alias so daily_states.py can import this without depending on the class
_session_agg_exprs = CollapseEvents._session_agg_exprs
