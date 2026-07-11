"""
Shared session detection SQL logic for collapse_events and split_sessions.

Each public function builds a chain of CTEs (from `tagged` through `with_session_id`)
that can be embedded directly into a WITH clause.

The last CTE is always `with_session_id`, which exposes:
  _rn              — row number within path (ORDER BY ts, subindex)
  _in_session      — 1 if the event belongs to a session, 0 otherwise
  _session_counter — monotonically increasing session counter within path
                     (same value for all events in the same session)
"""

from typing import Any, Dict, List

from retentioneering import engine
from retentioneering.engine import dialect
from retentioneering.utils.sql_quoting import quote_list as sql_list

_MODE_EVENTS = "events"
_MODE_SEPARATOR = "separator"
_MODE_START_END = "start_end"
_MODE_TIMEOUT = "timeout"


def to_list(value: str | List[str]) -> List[str]:
    return [value] if isinstance(value, str) else list(value)


def detect_mode(group: Dict[str, Any]) -> str:
    if group.get("events"):
        return _MODE_EVENTS
    if group.get("separator"):
        return _MODE_SEPARATOR
    if group.get("start_event"):
        return _MODE_START_END
    return _MODE_TIMEOUT


def build_session_ctes(
    group: Dict[str, Any],
    path_col: str,
    event_col: str,
    ts_col: str,
    subindex_col: str,
    separator_starts: bool = False,
) -> str:
    """
    Dispatches to the appropriate CTE builder based on the boundary mode in `group`.
    `group` may additionally carry `timeout` (seconds) to add time-gap breaks.

    separator_starts: if True, the separator event marks the START of a new session
    (the separator itself is not in-session). If False (default), the separator marks
    the END of a session (the separator is included in the session being collapsed).
    """
    mode = detect_mode(group)
    if mode == _MODE_EVENTS:
        return _ctes_events(group, path_col, event_col, ts_col, subindex_col)
    if mode == _MODE_SEPARATOR:
        if separator_starts:
            return _ctes_separator_start(
                group, path_col, event_col, ts_col, subindex_col
            )
        return _ctes_separator(group, path_col, event_col, ts_col, subindex_col)
    if mode == _MODE_START_END:
        return _ctes_start_end(group, path_col, event_col, ts_col, subindex_col)
    return _ctes_timeout(group["timeout"], path_col, ts_col, subindex_col)


# ---------------------------------------------------------------------------
# Internal builders
# ---------------------------------------------------------------------------


def _timeout_or_clause(group: Dict[str, Any], path_col: str, ts_col: str) -> str:
    timeout = group.get("timeout")
    if timeout is None:
        return ""
    path_col_q = engine.quote_ident(path_col)
    ts_col_q = engine.quote_ident(ts_col)
    return (
        f" OR {dialect.epoch(f'{ts_col_q} - LAG({ts_col_q}) OVER (PARTITION BY {path_col_q} ORDER BY _rn)')}"
        f" > {timeout}"
    )


def _ctes_events(group, path_col, event_col, ts_col, subindex_col):
    events = to_list(group["events"])
    skip = to_list(group.get("skip", []))
    timeout_or = _timeout_or_clause(group, path_col, ts_col)

    path_col_q = engine.quote_ident(path_col)
    event_col_q = engine.quote_ident(event_col)
    ts_col_q = engine.quote_ident(ts_col)
    subindex_col_q = engine.quote_ident(subindex_col)

    tag_expr = f"CASE WHEN {event_col_q} IN ({sql_list(events)}) THEN 'group'"
    if skip:
        tag_expr += f" WHEN {event_col_q} IN ({sql_list(skip)}) THEN 'skip'"
    tag_expr += " ELSE 'other' END"

    return f"""
tagged AS (
    SELECT *,
        {tag_expr} AS _tag,
        ROW_NUMBER() OVER (
            PARTITION BY {path_col_q} ORDER BY {ts_col_q}, {subindex_col_q}
        ) AS _rn
    FROM df
),
positions AS (
    SELECT *,
        MAX(CASE WHEN _tag = 'group' THEN _rn END) OVER (
            PARTITION BY {path_col_q} ORDER BY _rn
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS _last_group_rn,
        MAX(CASE WHEN _tag = 'other' THEN _rn END) OVER (
            PARTITION BY {path_col_q} ORDER BY _rn
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS _last_other_rn
    FROM tagged
),
in_session AS (
    SELECT *,
        CASE WHEN _tag IN ('group', 'skip')
                  AND _last_group_rn IS NOT NULL
                  AND COALESCE(_last_other_rn, -1) < _last_group_rn
             THEN 1 ELSE 0 END AS _in_session
    FROM positions
),
session_starts AS (
    SELECT *,
        CASE WHEN _in_session = 1
                  AND (
                      COALESCE(LAG(_in_session) OVER (PARTITION BY {path_col_q} ORDER BY _rn), 0) = 0
                      {timeout_or}
                  )
             THEN 1 ELSE 0 END AS _is_new_session
    FROM in_session
),
with_session_id AS (
    SELECT *,
        SUM(_is_new_session) OVER (
            PARTITION BY {path_col_q} ORDER BY _rn
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS _session_counter
    FROM session_starts
)"""


def _ctes_separator(group, path_col, event_col, ts_col, subindex_col):
    separators = to_list(group["separator"])
    timeout_or = _timeout_or_clause(group, path_col, ts_col)

    path_col_q = engine.quote_ident(path_col)
    event_col_q = engine.quote_ident(event_col)
    ts_col_q = engine.quote_ident(ts_col)
    subindex_col_q = engine.quote_ident(subindex_col)

    return f"""
tagged AS (
    SELECT *,
        CASE WHEN {event_col_q} IN ({sql_list(separators)}) THEN 1 ELSE 0 END AS _is_sep,
        ROW_NUMBER() OVER (
            PARTITION BY {path_col_q} ORDER BY {ts_col_q}, {subindex_col_q}
        ) AS _rn
    FROM df
),
sep_lookahead AS (
    SELECT *,
        MIN(CASE WHEN _is_sep = 1 THEN _rn END) OVER (
            PARTITION BY {path_col_q} ORDER BY _rn
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS _next_sep_rn
    FROM tagged
),
in_session AS (
    SELECT *,
        CASE WHEN _next_sep_rn IS NOT NULL THEN 1 ELSE 0 END AS _in_session
    FROM sep_lookahead
),
session_starts AS (
    SELECT *,
        CASE WHEN _in_session = 1
                  AND (
                      COALESCE(LAG(_in_session) OVER (PARTITION BY {path_col_q} ORDER BY _rn), 0) = 0
                      OR LAG(_is_sep) OVER (PARTITION BY {path_col_q} ORDER BY _rn) = 1
                      {timeout_or}
                  )
             THEN 1 ELSE 0 END AS _is_new_session
    FROM in_session
),
with_session_id AS (
    SELECT *,
        SUM(_is_new_session) OVER (
            PARTITION BY {path_col_q} ORDER BY _rn
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS _session_counter
    FROM session_starts
)"""


def _ctes_separator_start(group, path_col, event_col, ts_col, subindex_col):
    """
    Separator marks the START of a new session. Events after the separator are in-session;
    the separator itself is not (_in_session = 0, but _is_sep = 1 so callers can filter it).
    Events before the first separator are also not in-session.
    """
    separators = to_list(group["separator"])
    timeout_or = _timeout_or_clause(group, path_col, ts_col)

    path_col_q = engine.quote_ident(path_col)
    event_col_q = engine.quote_ident(event_col)
    ts_col_q = engine.quote_ident(ts_col)
    subindex_col_q = engine.quote_ident(subindex_col)

    return f"""
tagged AS (
    SELECT *,
        CASE WHEN {event_col_q} IN ({sql_list(separators)}) THEN 1 ELSE 0 END AS _is_sep,
        ROW_NUMBER() OVER (
            PARTITION BY {path_col_q} ORDER BY {ts_col_q},
            CASE WHEN {event_col_q} IN ({sql_list(separators)}) THEN 0 ELSE 1 END,
            {subindex_col_q}
        ) AS _rn
    FROM df
),
sep_count AS (
    SELECT *,
        SUM(_is_sep) OVER (
            PARTITION BY {path_col_q} ORDER BY _rn
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS _sep_count
    FROM tagged
),
in_session AS (
    SELECT *,
        CASE WHEN _is_sep = 0 AND _sep_count > 0 THEN 1 ELSE 0 END AS _in_session
    FROM sep_count
),
session_starts AS (
    SELECT *,
        CASE WHEN _in_session = 1
                  AND (
                      COALESCE(LAG(_in_session) OVER (PARTITION BY {path_col_q} ORDER BY _rn), 0) = 0
                      OR LAG(_is_sep) OVER (PARTITION BY {path_col_q} ORDER BY _rn) = 1
                      {timeout_or}
                  )
             THEN 1 ELSE 0 END AS _is_new_session
    FROM in_session
),
with_session_id AS (
    SELECT *,
        SUM(_is_new_session) OVER (
            PARTITION BY {path_col_q} ORDER BY _rn
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS _session_counter
    FROM session_starts
)"""


def _ctes_start_end(group, path_col, event_col, ts_col, subindex_col):
    starts = to_list(group["start_event"])
    ends = to_list(group["end_event"])
    timeout_or = _timeout_or_clause(group, path_col, ts_col)

    path_col_q = engine.quote_ident(path_col)
    event_col_q = engine.quote_ident(event_col)
    ts_col_q = engine.quote_ident(ts_col)
    subindex_col_q = engine.quote_ident(subindex_col)

    return f"""
tagged AS (
    SELECT *,
        CASE WHEN {event_col_q} IN ({sql_list(starts)}) THEN 'start'
             WHEN {event_col_q} IN ({sql_list(ends)}) THEN 'end'
             ELSE 'inner' END AS _tag,
        ROW_NUMBER() OVER (
            PARTITION BY {path_col_q} ORDER BY {ts_col_q}, {subindex_col_q}
        ) AS _rn
    FROM df
),
positions AS (
    SELECT *,
        MAX(CASE WHEN _tag = 'start' THEN _rn END) OVER (
            PARTITION BY {path_col_q} ORDER BY _rn
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS _last_start_rn,
        MAX(CASE WHEN _tag = 'end' THEN _rn END) OVER (
            PARTITION BY {path_col_q} ORDER BY _rn
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS _last_end_rn
    FROM tagged
),
in_session AS (
    SELECT *,
        CASE WHEN _last_start_rn IS NOT NULL
                  AND COALESCE(_last_end_rn, -1) < _last_start_rn
             THEN 1 ELSE 0 END AS _in_session
    FROM positions
),
session_starts AS (
    SELECT *,
        CASE WHEN _in_session = 1
                  AND (
                      COALESCE(LAG(_in_session) OVER (PARTITION BY {path_col_q} ORDER BY _rn), 0) = 0
                      OR LAG(_tag) OVER (PARTITION BY {path_col_q} ORDER BY _rn) = 'end'
                      {timeout_or}
                  )
             THEN 1 ELSE 0 END AS _is_new_session
    FROM in_session
),
with_session_id AS (
    SELECT *,
        SUM(_is_new_session) OVER (
            PARTITION BY {path_col_q} ORDER BY _rn
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS _session_counter
    FROM session_starts
)"""


def _ctes_timeout(timeout, path_col, ts_col, subindex_col):
    """
    Pure timeout mode: every event is in-session; a new session starts when the
    gap to the previous event exceeds `timeout` seconds (or at the start of a path).
    """
    path_col_q = engine.quote_ident(path_col)
    ts_col_q = engine.quote_ident(ts_col)
    subindex_col_q = engine.quote_ident(subindex_col)

    return f"""
tagged AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY {path_col_q} ORDER BY {ts_col_q}, {subindex_col_q}
        ) AS _rn
    FROM df
),
with_gap AS (
    SELECT *,
        CASE WHEN LAG({ts_col_q}) OVER (PARTITION BY {path_col_q} ORDER BY _rn) IS NULL
                  OR {dialect.epoch(f"{ts_col_q} - LAG({ts_col_q}) OVER (PARTITION BY {path_col_q} ORDER BY _rn)")} > {timeout}
             THEN 1 ELSE 0 END AS _is_new_session
    FROM tagged
),
with_session_id AS (
    SELECT *,
        1 AS _in_session,
        SUM(_is_new_session) OVER (
            PARTITION BY {path_col_q} ORDER BY _rn
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS _session_counter
    FROM with_gap
)"""


def parse_timeout(value) -> float:
    """
    Convert a public-API timeout value into seconds.

    Accepts a pandas-parseable duration string with an explicit unit
    (e.g. "30m", "1h", "1800s") or a pandas.Timedelta. Bare numbers are
    rejected to avoid unit ambiguity.
    """
    import pandas as pd

    if isinstance(value, pd.Timedelta):
        return float(value.total_seconds())
    if isinstance(value, str):
        # pd.Timedelta("1800") silently means 1800 *nanoseconds* — reject
        # unit-less strings outright instead of inheriting that footgun.
        if not any(c.isalpha() for c in value):
            raise ValueError(
                f"timeout {value!r} has no unit. "
                "Use a duration string with an explicit unit, e.g. '30m', '1h', '1800s'."
            )
        try:
            return float(pd.Timedelta(value).total_seconds())
        except ValueError as exc:
            raise ValueError(
                f"invalid timeout {value!r}: {exc}. "
                "Use a duration string with an explicit unit, e.g. '30m', '1h', '1800s'."
            ) from exc
    raise ValueError(
        "timeout must be a duration string with an explicit unit "
        f"(e.g. '30m', '1h', '1800s') or a pandas.Timedelta, got {type(value).__name__}"
    )
