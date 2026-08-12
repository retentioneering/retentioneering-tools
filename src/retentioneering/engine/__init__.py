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

import os
import threading

import duckdb
import pandas as pd

__all__ = ["run", "quote_ident"]


#: The DuckDB instance that every query uses. It is built when the first query
#: runs and then shared for the rest of the process. It is ``None`` before
#: that, and ``None`` again inside a child process after a fork (see
#: :func:`_reset_after_fork`).
_ROOT: duckdb.DuckDBPyConnection | None = None

#: Stops two threads from building `_ROOT` twice when they both send the very
#: first query at the same time.
_ROOT_LOCK = threading.Lock()

#: Instances that a child process got from its parent through ``fork()``. The
#: child keeps them here instead of dropping them. If it dropped the last
#: reference, Python would close the instance, and closing one that the parent
#: still uses is the thing we must avoid. Nothing ever reads this list again.
_ABANDONED: list[duckdb.DuckDBPyConnection] = []


def _root() -> duckdb.DuckDBPyConnection:
    """Return the shared DuckDB instance, building it if there is none yet.

    We build it late, on the first query, and not when the module is imported.
    An instance costs both time and memory, so a program that imports
    retentioneering but never runs a query should not pay for one.
    """
    global _ROOT
    if _ROOT is None:
        with _ROOT_LOCK:
            # Check again inside the lock. Another thread may have built the
            # instance while this one was waiting for its turn.
            if _ROOT is None:
                _ROOT = duckdb.connect()
    return _ROOT


def _reset_after_fork() -> None:
    """Let go of the instance we got from the parent, but never close it.

    A DuckDB instance cannot be used after a ``fork()``. Closing it is not the
    answer either, because the parent still uses it. So the child keeps the
    object in `_ABANDONED`, where it stays alive and Python never closes it,
    and then empties the slot. The next call to :func:`run` builds a new
    instance for the child.

    We replace `_ROOT_LOCK` as well. Only the thread that called ``fork()``
    lives on in the child, so if another thread held the lock at that moment,
    the lock would stay locked for ever.
    """
    global _ROOT, _ROOT_LOCK
    if _ROOT is not None:
        _ABANDONED.append(_ROOT)
        _ROOT = None
    _ROOT_LOCK = threading.Lock()


# Windows has no fork(), so there is nothing to register there.
if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_after_fork)


def run(sql: str, /, **tables: pd.DataFrame) -> pd.DataFrame:
    """
    Execute a SQL query against one or more explicitly named pandas frames.

    The whole process shares one DuckDB instance, built when the first query
    runs, and this call takes a cursor on it. The frames you pass are put on
    that cursor under their keyword name, the query runs, the result is turned
    into a pandas DataFrame, and then the cursor is closed. Every cursor has
    its own temporary catalog, so whatever one call puts there belongs to that
    call alone and is gone when it ends. Calls stay as separate as they were
    when each one opened its own database, but without the cost of building a
    database every time. Callers no longer need a same-named local variable
    for DuckDB's replacement-scan to find — the mapping from SQL table name
    to pandas frame is explicit at the call site.

    Parameters
    ----------
    sql:
        The SQL query text. Any table it references by an unqualified name
        (e.g. ``FROM df``) must be passed as a same-named keyword argument.
    **tables:
        Pandas DataFrames to register on the query's cursor, keyed by the
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
    cur = _root().cursor()
    try:
        for name, frame in tables.items():
            cur.register(name, frame)
        return cur.sql(sql).df()
    finally:
        cur.close()


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
