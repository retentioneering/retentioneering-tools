from typing import Callable, Collection, Tuple

import pandas as pd

from retentioneering import engine
from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import PreprocessingConfigError
from retentioneering.utils.sql_quoting import quote_literal

PROCESSOR_NAME = "add_segment"

_ROW_IDX_COL = "__retentioneering_row_idx__"


def _inject_row_idx(sql: str) -> str:
    """
    Injects _ROW_IDX_COL into the outermost SELECT of a SQL query.

    DuckDB reorders rows when executing window functions with PARTITION BY,
    so we inject a row index column into the result to restore the original order.

    Works for both plain SELECT and CTEs (WITH ... SELECT ...).
    """
    stripped = sql.strip()
    depth = 0
    i = 0
    in_single_quote = False
    in_double_quote = False

    while i < len(stripped):
        c = stripped[i]

        if in_single_quote:
            if c == "'" and (i == 0 or stripped[i - 1] != "\\"):
                in_single_quote = False
        elif in_double_quote:
            if c == '"' and (i == 0 or stripped[i - 1] != "\\"):
                in_double_quote = False
        elif c == "'":
            in_single_quote = True
        elif c == '"':
            in_double_quote = True
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif depth == 0 and stripped[i : i + 6].upper() == "SELECT":
            before_ok = i == 0 or not (
                stripped[i - 1].isalnum() or stripped[i - 1] == "_"
            )
            end_pos = i + 6
            after_ok = end_pos >= len(stripped) or not (
                stripped[end_pos].isalnum() or stripped[end_pos] == "_"
            )
            if before_ok and after_ok:
                return stripped[:end_pos] + f" {_ROW_IDX_COL}," + stripped[end_pos:]

        i += 1

    raise PreprocessingConfigError(
        PROCESSOR_NAME, "Could not find SELECT statement in SQL query."
    )


def _build_funnel_segment_query(
    path_col: str,
    event_col: str,
    index_col: str,
    funnel_events: list[str],
    result_col: str,
) -> str:
    """
    Per-row DuckDB query, chained-CTE approach identical in spirit to
    tools/funnel.py: a path reaches step k iff there exist event indices
    i_0 < i_1 < ... < i_k with event(i_j) = funnel_events[j] — i.e. an
    increasing subsequence of positions, not a comparison of *last*
    occurrences. step_k holds, per path, the earliest occurrence of
    funnel_events[k - 1] that comes strictly after the path's step_{k - 1}
    match. This correctly handles paths that revisit an earlier funnel
    event after completing a later one (see funnel.py's
    test_funnel_repeated_event_after_step for the equivalent regression).

    path_cols is validated (coarsest-first, strictly nested) at Eventstream
    construction time, and the caller restricts path_col to schema.path_cols,
    so comparing index_col directly is correct at any accepted grain (see
    ADR-0004).

    Segment values (named after the deepest step completed in order):
      funnel_events[k]  — path completed steps 0..k in order, but not step k+1
                          in order (missing, or out of sequence)
      'out_of_funnel'   — path never completed step 0 in order

    Reads from the 'eventstream' variable in caller scope (same as sql= mode).
    Row order is restored via _ROW_IDX_COL.

    Example output for funnel_events=['add_to_cart', 'purchase'], path_id='session',
    event='event', index='__index__':

        WITH step_1 AS (
                SELECT "session" AS path_id, MIN("__index__") AS idx
                FROM eventstream WHERE "event" = 'add_to_cart' GROUP BY "session"
             ),
             step_2 AS (
                SELECT eventstream."session" AS path_id, MIN(eventstream."__index__") AS idx
                FROM eventstream JOIN step_1 ON eventstream."session" = step_1.path_id
                WHERE eventstream."event" = 'purchase' AND eventstream."__index__" > step_1.idx
                GROUP BY eventstream."session"
             ),
             paths AS (SELECT DISTINCT "session" AS path_id FROM eventstream),
             levels AS (
                SELECT paths.path_id,
                    CASE
                        WHEN step_2.idx IS NOT NULL THEN 'purchase'
                        WHEN step_1.idx IS NOT NULL THEN 'add_to_cart'
                        ELSE 'out_of_funnel'
                    END AS funnel
                FROM paths
                LEFT JOIN step_1 ON step_1.path_id = paths.path_id
                LEFT JOIN step_2 ON step_2.path_id = paths.path_id
             )
        SELECT eventstream.__retentioneering_row_idx__, levels.funnel
        FROM eventstream
        JOIN levels ON eventstream."session" = levels.path_id
    """
    path_col_q = engine.quote_ident(path_col)
    event_col_q = engine.quote_ident(event_col)
    index_col_q = engine.quote_ident(index_col)
    n = len(funnel_events)

    ctes = []
    for step_num, step_event in enumerate(funnel_events, start=1):
        ev = quote_literal(step_event)
        if step_num == 1:
            ctes.append(
                f"step_1 AS ("
                f"SELECT {path_col_q} AS path_id, MIN({index_col_q}) AS idx "
                f"FROM eventstream "
                f"WHERE {event_col_q} = {ev} "
                f"GROUP BY {path_col_q}"
                f")"
            )
        else:
            prev = f"step_{step_num - 1}"
            ctes.append(
                f"step_{step_num} AS ("
                f"SELECT eventstream.{path_col_q} AS path_id, MIN(eventstream.{index_col_q}) AS idx "
                f"FROM eventstream "
                f"JOIN {prev} ON eventstream.{path_col_q} = {prev}.path_id "
                f"WHERE eventstream.{event_col_q} = {ev} AND eventstream.{index_col_q} > {prev}.idx "
                f"GROUP BY eventstream.{path_col_q}"
                f")"
            )

    ctes.append(f"paths AS (SELECT DISTINCT {path_col_q} AS path_id FROM eventstream)")

    joins = " ".join(
        f"LEFT JOIN step_{k} ON step_{k}.path_id = paths.path_id"
        for k in range(1, n + 1)
    )
    whens = "\n        ".join(
        f"WHEN step_{k}.idx IS NOT NULL THEN {quote_literal(funnel_events[k - 1])}"
        for k in range(n, 0, -1)
    )
    ctes.append(
        f"levels AS ("
        f"SELECT paths.path_id,\n"
        f"    CASE\n"
        f"        {whens}\n"
        f"        ELSE 'out_of_funnel'\n"
        f"    END AS {result_col}\n"
        f"FROM paths {joins}"
        f")"
    )

    return (
        f"WITH {', '.join(ctes)}\n"
        f"SELECT eventstream.{_ROW_IDX_COL}, levels.{result_col}\n"
        f"FROM eventstream\n"
        f"JOIN levels ON eventstream.{path_col_q} = levels.path_id"
    )


_BIN_KEYS = frozenset({"metric", "edges", "quantiles", "segment_levels"})

#: Level given to paths whose metric has no value — `time_between` on a path
#: missing either of its events, the only metric `build_metrics` leaves as NaN
#: rather than filling with 0. Not a bin of the split, so it is never counted
#: against `segment_levels`; rename it with `rename_segment_levels` if it clashes.
UNDEFINED_LEVEL = "undefined"


def _validate_metric_bins(metric_bins: dict) -> None:
    if not isinstance(metric_bins, dict):
        raise PreprocessingConfigError(PROCESSOR_NAME, "metric_bins must be a dict.")
    unknown = sorted(set(metric_bins) - _BIN_KEYS)
    if unknown:
        raise PreprocessingConfigError(
            PROCESSOR_NAME,
            f"metric_bins has unknown key(s) {unknown}. Valid keys: {sorted(_BIN_KEYS)}.",
        )
    if "metric" not in metric_bins:
        raise PreprocessingConfigError(
            PROCESSOR_NAME, "metric_bins requires a 'metric' configuration."
        )
    has_edges = metric_bins.get("edges") is not None
    has_quantiles = metric_bins.get("quantiles") is not None
    if has_edges == has_quantiles:
        raise PreprocessingConfigError(
            PROCESSOR_NAME,
            "metric_bins requires exactly one of 'edges' or 'quantiles'.",
        )


def _bin_edges_and_levels(
    metric_bins: dict, values: pd.Series
) -> tuple[list[float], list[str]]:
    """
    Resolve `metric_bins` to interior cut points and one level name per bin.

    Cut points are *interior*: N of them always produce N + 1 bins, so no value
    can fall outside the split. This deliberately differs from `pd.cut`, where
    the list is the outer edges and out-of-range values become NaN — a segment
    column has no room for NaN, since `diff` and `in_segment` both need every
    path to carry a level.
    """
    if metric_bins.get("edges") is not None:
        edges = list(metric_bins["edges"])
        if not edges:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "metric_bins 'edges' must not be empty."
            )
        auto_levels = _interval_levels(edges)
    else:
        quantiles = metric_bins["quantiles"]
        if isinstance(quantiles, bool) or not isinstance(quantiles, (int, list, tuple)):
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                "metric_bins 'quantiles' must be a number of bins (int) or a list "
                "of cut quantiles between 0 and 1.",
            )
        if isinstance(quantiles, int):
            if quantiles < 2:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME,
                    f"metric_bins 'quantiles' must ask for at least 2 bins, got {quantiles}.",
                )
            qs = [i / quantiles for i in range(1, quantiles)]
        else:
            qs = list(quantiles)
            if not qs:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "metric_bins 'quantiles' must not be empty."
                )
            if any(not 0 < q < 1 for q in qs):
                raise PreprocessingConfigError(
                    PROCESSOR_NAME,
                    f"metric_bins 'quantiles' must be strictly between 0 and 1, got {qs}.",
                )
        edges = [float(values.quantile(q)) for q in qs]
        auto_levels = [f"q{i + 1}" for i in range(len(edges) + 1)]

    if any(b <= a for a, b in zip(edges, edges[1:])):
        raise PreprocessingConfigError(
            PROCESSOR_NAME,
            f"metric_bins cut points must be strictly increasing, got {edges}.",
        )

    levels = metric_bins.get("segment_levels")
    if levels is None:
        return edges, auto_levels
    levels = list(levels)
    if len(levels) != len(edges) + 1:
        raise PreprocessingConfigError(
            PROCESSOR_NAME,
            f"metric_bins has {len(edges)} cut point(s) → {len(edges) + 1} bins, "
            f"but 'segment_levels' has {len(levels)} entr{'y' if len(levels) == 1 else 'ies'}.",
        )
    if len(set(levels)) != len(levels):
        raise PreprocessingConfigError(
            PROCESSOR_NAME,
            f"metric_bins 'segment_levels' must be unique, got {levels}.",
        )
    return edges, [str(level) for level in levels]


def _interval_levels(edges: list[float]) -> list[str]:
    """Readable auto-labels. Short and stable — they end up in widget legends."""
    labels = [f"(-∞, {edges[0]})"]
    labels += [f"[{lo}, {hi})" for lo, hi in zip(edges, edges[1:])]
    labels.append(f"[{edges[-1]}, ∞)")
    return labels


class AddSegment(DataProcessor):
    name: str
    rules: Collection | None
    func: Callable | None
    sql: str | None
    funnel_events: list | None
    time_range: Collection | None
    metric_bins: dict | None
    path_col: str | None

    def __init__(
        self,
        name: str,
        rules: Collection | None = None,
        func: Callable | None = None,
        sql: str | None = None,
        funnel_events: list | None = None,
        time_range: Collection | None = None,
        metric_bins: dict | None = None,
        path_col: str | None = None,
        eventstream=None,
    ) -> None:
        arg_is_not_none = [
            func is not None,
            rules is not None,
            sql is not None,
            funnel_events is not None,
            time_range is not None,
            metric_bins is not None,
        ]
        if sum(arg_is_not_none) > 1:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                "At most one of the arguments must be defined: "
                "rules, func, sql, funnel_events, time_range, metric_bins.",
            )
        if funnel_events is not None and len(funnel_events) < 2:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "funnel_events must have at least 2 events."
            )
        if time_range is not None and len(time_range) != 2:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "time_range must have exactly 2 elements: (start, end)."
            )
        if metric_bins is not None:
            _validate_metric_bins(metric_bins)

        self.name = name
        self.rules = rules
        self.func = func
        self.sql = sql
        self.funnel_events = funnel_events
        self.time_range = time_range
        self.metric_bins = metric_bins
        self.path_col = path_col
        self.eventstream = eventstream
        super().__init__()

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        has_mode = any(
            [
                self.rules is not None,
                self.func is not None,
                self.sql is not None,
                self.funnel_events is not None,
                self.time_range is not None,
                self.metric_bins is not None,
            ]
        )

        if self.name in df.columns:
            if self.name in schema.segment_cols:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, f"Segment '{self.name}' already exists."
                )
            if self.name in schema.custom_cols and not has_mode:
                new_df = df.copy()
                new_df[self.name] = new_df[self.name].astype("category")
                new_schema = schema.copy()
                new_schema.custom_cols = [
                    c for c in new_schema.custom_cols if c != self.name
                ]
                new_schema.segment_cols.append(self.name)
                return new_df, new_schema
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"Name '{self.name}' is already reserved in the eventstream.",
            )

        if not has_mode:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                "One of the arguments must be defined: rules, func, sql, "
                "funnel_events, time_range, metric_bins — unless promoting an existing custom "
                f"column to a segment, in which case '{self.name}' must already be "
                "a custom column and none of them should be set.",
            )

        values = None

        if self.rules is not None:
            if not isinstance(self.rules, Collection):
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "Segment rules must be a collection."
                )

            cases = "CASE"
            for item in self.rules[:-1]:
                column, op, value, segment_level = item
                column_q = engine.quote_ident(column)
                if isinstance(value, str) and op.lower() != "in":
                    value = quote_literal(value)
                cases += f"\nWHEN {column_q} {op} {value} THEN {quote_literal(str(segment_level))}"
            else_segment_level = self.rules[-1][0]
            cases += f"\nELSE {quote_literal(str(else_segment_level))}"
            cases += f"\nEND AS {engine.quote_ident(self.name)}"

            sql = f"SELECT {cases} FROM df"
            result = engine.run(sql, df=df)
            values = result[result.columns[0]].tolist()

        elif self.sql is not None:
            if not isinstance(self.sql, str):
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "SQL query must be a string."
                )

            # Copy df and add a row index so we can restore original order after
            # DuckDB reorders rows during window function (PARTITION BY) execution.
            eventstream = df.copy()
            eventstream[_ROW_IDX_COL] = range(len(df))

            tracking_sql = _inject_row_idx(self.sql)
            result = engine.run(tracking_sql, eventstream=eventstream)

            if len(result.columns) != 2:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "SQL script must return a single column."
                )

            result = result.sort_values(_ROW_IDX_COL).reset_index(drop=True)
            values = result.iloc[:, 1].tolist()

        elif self.func is not None:
            if not isinstance(self.func, Callable):
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "Function must be callable."
                )
            result = self.func(df)
            if not isinstance(result, Collection):
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "Function must return a collection."
                )
            values = list(result)

        elif self.funnel_events is not None:
            pid_col = self.path_col or schema.path_col
            if pid_col not in schema.path_cols:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME,
                    f"path_col '{pid_col}' must be one of schema.path_cols: {schema.path_cols}.",
                )
            # Add row index so we can restore original order after DuckDB execution
            eventstream = df.copy()
            eventstream[_ROW_IDX_COL] = range(len(df))
            query = _build_funnel_segment_query(
                pid_col,
                schema.event_col,
                schema.index,
                self.funnel_events,
                "__funnel_level__",
            )
            result = engine.run(query, eventstream=eventstream)
            result = result.sort_values(_ROW_IDX_COL).reset_index(drop=True)
            values = result["__funnel_level__"].tolist()

        elif self.time_range is not None:
            start, end = self.time_range
            start_ts = pd.to_datetime(start)
            end_ts = pd.to_datetime(end)
            if start_ts > end_ts:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "time_range start must not be after end."
                )
            ts_col = schema.timestamp_col
            in_range = df[ts_col].between(start_ts, end_ts)
            values = in_range.map({True: "inside", False: "outside"}).tolist()

        elif self.metric_bins is not None:
            values = self._binned_levels(df, schema)

        new_df = df.copy()
        new_df[self.name] = values
        new_df[self.name] = new_df[self.name].astype("category")
        new_schema = schema.copy()
        new_schema.segment_cols.append(self.name)

        return new_df, new_schema

    def _binned_levels(self, df: pd.DataFrame, schema: EventstreamSchema) -> list:
        """
        Bin a per-path metric into segment levels, one per row of the eventstream.

        Quantile cut points are computed from the eventstream *as it is at this
        call* — putting `add_segment` before or after a `filter_paths` in a chain
        gives different boundaries. That is intended, but easy to miss.
        """
        from retentioneering.metrics.metric_builder import MetricBuilder

        if self.eventstream is None:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "metric_bins mode requires an eventstream."
            )
        path_col = self.path_col or schema.path_col
        if path_col not in schema.path_cols:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"path_col '{path_col}' must be one of schema.path_cols: {schema.path_cols}.",
            )

        metrics = MetricBuilder(self.eventstream).build_metrics(
            [self.metric_bins["metric"]], path_col
        )
        if metrics.shape[1] != 1:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"metric_bins needs a metric that produces exactly one value per path, "
                f"but this one produced {metrics.shape[1]} columns: "
                f"{list(metrics.columns)}.",
            )
        values = metrics.iloc[:, 0]
        defined = values.dropna()
        if defined.empty:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                "metric_bins metric has no value for any path — nothing to bin.",
            )

        edges, levels = _bin_edges_and_levels(self.metric_bins, defined)
        if UNDEFINED_LEVEL in levels:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"metric_bins 'segment_levels' must not contain '{UNDEFINED_LEVEL}' — "
                "that name is reserved for paths the metric has no value for.",
            )

        binned = pd.cut(
            values,
            bins=[-float("inf"), *edges, float("inf")],
            labels=levels,
            right=False,
        )
        per_path = binned.astype(object).where(values.notna(), UNDEFINED_LEVEL)
        return df[path_col].map(per_path).fillna(UNDEFINED_LEVEL).tolist()
