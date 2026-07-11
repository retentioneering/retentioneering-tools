"""Shared SQL literal/identifier escaping for DuckDB queries built via f-strings.

See ADR-0001's "standing review point" on SQL quoting. Before this module,
near-identical escaping logic existed independently in `metrics/metric_builder.py`
(`format_value_for_sql`), `data_processors/filter_paths.py` (`_quote_ident`/
`_literal_sql`), `data_processors/add_segment.py` and `tools/funnel.py` (both
named `_sql_str`), and `utils/session_detection.py` (`sql_list`) — this module
is the one place that logic now lives; the others delegate to it.
"""

from typing import Any, Iterable


def quote_literal(value: Any) -> str:
    """Format a Python scalar as a DuckDB SQL literal, escaping single quotes.

    Lenient by design: falls back to `str(value)` + escaping for anything
    that isn't None/bool/int/float/str (e.g. numpy scalar types like
    `numpy.int64`/`numpy.bool_` that pandas hands back for a dataframe
    column's unique values — these are NOT `isinstance` of the plain
    Python types despite being numeric/boolean in practice).
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def quote_list(values: Iterable[Any]) -> str:
    """Comma-joined `quote_literal` values, for a SQL `IN (...)` clause."""
    return ", ".join(quote_literal(v) for v in values)


def quote_ident(name: str) -> str:
    """Double-quote a DuckDB identifier (column/table name), escaping embedded quotes."""
    return '"' + name.replace('"', '""') + '"'
