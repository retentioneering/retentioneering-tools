"""
L1 unified query engine.

Centralizes DuckDB execution behind an explicit table-registration API,
replacing the "replacement scan" idiom documented (and now superseded) in
``docs/adr/0002-duckdb-replacement-scan.md``.

Historically, call sites built a SQL string referencing an unqualified table
name (e.g. ``FROM df``) and relied on DuckDB inspecting the caller's stack
frame for a same-named Python variable to resolve it. That trick is invisible
to static analysis (hence the ``# noqa: F841`` comments it required) and ties
execution to whatever frame happens to be sitting in local scope. ``run()``
replaces it with an explicit, ordinary function call: pass each pandas frame
the query needs as a keyword argument named after the table it should be
addressable as in the SQL text.

    >>> run("SELECT count(*) AS n FROM df", df=some_dataframe)

Centralizing execution here also gives the codebase one place to swap in a
different execution backend later (e.g. a warehouse-native connection) without
touching call sites, and one place (:func:`quote_ident`) to safely quote
column/event identifiers that are interpolated into SQL text instead of bound
as query parameters.
"""

from __future__ import annotations

import duckdb
import pandas as pd

__all__ = ["run", "quote_ident"]


def run(sql: str, /, **tables: pd.DataFrame) -> pd.DataFrame:
    """
    Execute a SQL query against one or more explicitly named pandas frames.

    A fresh, private DuckDB connection is created for each call, the given
    frames are registered on it under their keyword name, and the query is
    executed and eagerly materialized to a pandas DataFrame before the
    connection is closed. Callers no longer need a same-named local variable
    for DuckDB's replacement-scan to find — the mapping from SQL table name
    to pandas frame is explicit at the call site.

    Parameters
    ----------
    sql:
        The SQL query text. Any table it references by an unqualified name
        (e.g. ``FROM df``) must be passed as a same-named keyword argument.
    **tables:
        Pandas DataFrames to register on the query's connection, keyed by the
        name they are referenced as in `sql`.

    Returns
    -------
    pandas.DataFrame
        The query result, materialized eagerly (equivalent to the previous
        ``duckdb.sql(query).df()`` / ``duckdb.query(query).df()`` call).

    Examples
    --------
        result = engine.run(
            "SELECT {path_col}, count(*) AS n FROM df GROUP BY {path_col}",
            df=self.df,
        )
    """
    con = duckdb.connect()
    try:
        for name, frame in tables.items():
            con.register(name, frame)
        return con.sql(sql).df()
    finally:
        con.close()


def quote_ident(identifier: str) -> str:
    """
    Quote a SQL identifier (column or event name) DuckDB/SQL-92 style.

    Wraps `identifier` in double quotes, doubling any embedded double quotes,
    so column/event names that collide with reserved words or contain spaces
    or other special characters can be safely interpolated into SQL text as
    identifiers (as opposed to string literals — see each call site's value
    escaping helper for that, e.g. ``utils.sql_quoting.quote_literal``).

    Quoting an identifier does not change the (unquoted) name DuckDB reports
    back for it on the resulting DataFrame, so this is always safe to apply
    to an identifier used to build a query, independent of how the result is
    consumed afterwards.
    """
    return '"' + identifier.replace('"', '""') + '"'
