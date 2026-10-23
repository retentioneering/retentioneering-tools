"""
DuckDB dialect fragments (L1).

The rest of the codebase builds SQL query text with f-strings; the handful of
constructs in that text that are specific to DuckDB (as opposed to portable
SQL-92) are centralized here as small named functions instead of being
inlined ad hoc at each call site. A future warehouse-native or remote-SQL
backend that needs different syntax for these constructs only has to retarget
this one module, not grep every query string in the codebase.

This module does not transpile or validate SQL - it only wraps the
enumerable DuckDB-specific fragments currently in use (EPOCH, path-string
aggregation, regex matching, calendar-day truncation) as plain
string-returning functions.
"""

from __future__ import annotations

__all__ = [
    "calendar_date",
    "event_position",
    "epoch",
    "interval_seconds",
    "path_agg",
    "path_agg_ordered",
    "regexp_match",
]


def event_position(index_expr: str, subindex_expr: str) -> str:
    """Comparable DuckDB row value for a full event position.

    MIN and comparisons on the row are lexicographic: compare index first,
    then subindex. Keep the pair intact when selecting an earliest match.
    Expressions must already contain any required identifier quoting.
    """
    return f"ROW({index_expr}, {subindex_expr})"


def interval_seconds(seconds: float) -> str:
    """
    A DuckDB INTERVAL of `seconds` seconds, for timestamp arithmetic
    (``ts + interval_seconds(1800)``). The inverse of :func:`epoch`.
    """
    return f"to_seconds({float(seconds)})"


def calendar_date(ts_expr: str, *, tz_aware: bool) -> str:
    """
    The calendar day of a timestamp expression, as a DuckDB DATE.

    A ``TIMESTAMP WITH TIME ZONE`` resolves to a calendar day in DuckDB's
    session timezone, which defaults to the host's. Pinning that setting is not
    an option here: it is not inherited by the per-call cursors
    :func:`engine.run` executes on. So a bare ``CAST(ts AS DATE)`` would bucket
    the same tz-aware eventstream into different days on two different
    machines, and the day has to be taken in a fixed zone instead.

    A tz-naive column carries no zone to convert from and is cast directly.
    Routing it through ``AT TIME ZONE`` would read it *as* UTC and hand back a
    tz-aware value, reintroducing the session-timezone dependency the
    conversion exists to remove — hence `tz_aware` rather than one expression
    for both.
    """
    if tz_aware:
        return f"CAST({ts_expr} AT TIME ZONE 'UTC' AS DATE)"
    return f"CAST({ts_expr} AS DATE)"


def epoch(expr: str) -> str:
    """Wrap `expr` (typically an INTERVAL, e.g. a timestamp difference) in DuckDB's EPOCH(), converting it to seconds."""
    return f"EPOCH({expr})"


def path_agg(expr: str, sep: str = "->") -> str:
    """
    Aggregate `expr` (typically an event column) into a single `sep`-delimited
    path string via DuckDB's string_agg(). Aggregation order follows the
    input row order within each group (add an ORDER BY in the surrounding
    query/subquery if a specific order is required).
    """
    sep_literal = sep.replace("'", "''")
    return f"string_agg({expr}, '{sep_literal}')"


def path_agg_ordered(expr: str, sep: str = "->") -> str:
    """
    Aggregate `expr` into a single `sep`-delimited path string via DuckDB's
    list_aggregate(), for callers that already pre-sort rows in a FROM
    subquery (e.g. `SELECT * FROM df ORDER BY ...`) to guarantee aggregation
    order, rather than relying on `string_agg`'s (see :func:`path_agg`).
    """
    sep_literal = sep.replace("'", "''")
    return f"list_aggregate(list({expr}), 'string_agg', '{sep_literal}')"


def regexp_match(expr: str, pattern_sql: str) -> str:
    """
    DuckDB regexp_matches(`expr`, `pattern_sql`). `pattern_sql` must already
    be a quoted SQL string literal (e.g. via a value-escaping helper such as
    ``utils.sql_quoting.quote_literal``) — this function only wraps the
    DuckDB-specific function name, it does not escape its arguments.
    """
    return f"regexp_matches({expr}, {pattern_sql})"
