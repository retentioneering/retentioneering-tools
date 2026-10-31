"""Explicit match tables and per-match outcomes; no implicit unit of analysis."""

from __future__ import annotations

import math

from retentioneering import engine
from retentioneering.engine import dialect
from retentioneering.exceptions import InvalidParameterError
from retentioneering.paths import anchors
from retentioneering.tools.conversion import parse_within
from retentioneering.utils.sql_quoting import quote_literal

MATCH_COLUMNS = [
    "match_id",
    "start_step",
    "end_step",
    "start_index",
    "end_index",
    "start_timestamp",
    "end_timestamp",
]


def find_matches(stream, pattern, *, path_col, event_col=None):
    schema, df = stream.schema, stream.df
    if path_col not in schema.path_cols:
        raise InvalidParameterError("path_col", path_col, schema.path_cols)
    if path_col in MATCH_COLUMNS or path_col in {
        "converted",
        "target_index",
        "target_timestamp",
        "target_step",
        "unit",
        "attribution",
        "from_boundary",
    }:
        raise InvalidParameterError(
            "path_col",
            path_col,
            ["a path column not colliding with match result columns"],
        )
    event_col = schema.event_col if event_col is None else event_col
    if event_col not in df.columns:
        raise InvalidParameterError("event_col", event_col, list(df.columns))
    spec = anchors.parse_spec(
        {
            "pattern": pattern,
            "at": "start",
            "occurrence": "non_overlapping",
            "event_col": event_col,
        }
    )
    anchors.validate_pattern_tokens(
        spec.pattern, df[event_col].unique().tolist(), param="pattern"
    )
    result = anchors.resolve_anchors(
        df,
        schema,
        spec.pattern,
        occurrence="non_overlapping",
        source_pattern=pattern,
        path_col=path_col,
        event_col=event_col,
    )
    matches = result.frame.drop_duplicates([path_col, "match_id"])[
        [path_col, *MATCH_COLUMNS[:5]]
    ].copy()
    # Synthetic rows sharing an index have the source event's timestamp.
    times = df[[path_col, schema.index, schema.timestamp_col]].drop_duplicates(
        [path_col, schema.index]
    )
    for edge in ["start", "end"]:
        lookup = times.rename(
            columns={
                schema.index: f"{edge}_index",
                schema.timestamp_col: f"{edge}_timestamp",
            }
        )
        matches = matches.merge(
            lookup, on=[path_col, f"{edge}_index"], how="left", validate="many_to_one"
        )
    matches = matches[[path_col, *MATCH_COLUMNS]].sort_values(
        [path_col, "match_id"], ignore_index=True
    )
    matches.attrs.update(
        unit="match",
        pattern=pattern,
        path_col=path_col,
        selection="leftmost non-overlapping; greedy groups; lazy gaps",
    )
    return matches


def get_match_conversion(
    stream, pattern, target_event, *, path_col, from_boundary, attribution, within=None
):
    """One row per match: does the first subsequent target fit the window?"""
    if from_boundary not in {"start", "end"}:
        raise InvalidParameterError("from_boundary", from_boundary, ["start", "end"])
    if attribution != "any_later":
        raise InvalidParameterError(
            "attribution",
            attribution,
            ["any_later (one target may convert several matches)"],
        )
    schema, df = stream.schema, stream.df
    if (
        not isinstance(target_event, str)
        or target_event not in df[schema.event_col].unique()
    ):
        raise InvalidParameterError(
            "target_event",
            target_event,
            ["an existing literal event name (not a pattern)"],
        )
    try:
        window = parse_within(within)
    except ValueError as exc:
        raise InvalidParameterError(
            "within",
            within,
            ["a finite positive event count or duration such as '30m'"],
        ) from exc
    if window is not None and (not math.isfinite(window[1]) or window[1] <= 0):
        raise InvalidParameterError(
            "within", within, ["a finite positive event count or duration"]
        )
    matches = find_matches(stream, pattern, path_col=path_col)
    path_q, idx_q, sub_q, ts_q, event_q = map(
        engine.quote_ident,
        [
            path_col,
            schema.index,
            schema.subindex,
            schema.timestamp_col,
            schema.event_col,
        ],
    )
    condition = "TRUE"
    if window is not None:
        unit, size = window
        condition = (
            f"t.step <= m.{from_boundary}_step + {int(size)}"
            if unit == "steps"
            else f"t.ts <= m.{from_boundary}_timestamp + {dialect.interval_seconds(size)}"
        )
    query = f"""
        WITH base AS (
            SELECT {path_q} AS p, {idx_q} AS idx, {ts_q} AS ts, {event_q} AS e,
                   row_number() OVER (PARTITION BY {path_q} ORDER BY {idx_q}, {sub_q}) AS step
            FROM df
        ), targets AS (SELECT * FROM base WHERE e = {quote_literal(target_event)})
        SELECT m.*, t.step IS NOT NULL AS converted,
               t.step AS target_step, t.idx AS target_index, t.ts AS target_timestamp
        FROM matches m LEFT JOIN targets t ON t.p = m.{path_q}
            AND t.step > m.{from_boundary}_step AND {condition}
        QUALIFY row_number() OVER (PARTITION BY m.{path_q}, m.match_id ORDER BY t.step) = 1
        ORDER BY m.{path_q}, m.match_id
    """
    output = engine.run(query, df=df, matches=matches)
    output["unit"] = "match"
    output["from_boundary"] = from_boundary
    output["attribution"] = attribution
    output.attrs.update(
        matches.attrs,
        target_event=target_event,
        within=within,
        from_boundary=from_boundary,
        attribution=attribution,
    )
    return output
