# ADR-0001: DuckDB-backed Eventstream

Status: Accepted (5.0 rewrite; recorded 2026-07)

## Context

The 3.3.0 engine was pure pandas. Users hit its ceiling on production-scale
data (millions of rows), and every data processor reimplemented windowing,
grouping, and ordering logic in pandas idioms that were hard to review and
easy to get subtly wrong. Analysts and their tooling, meanwhile, are fluent
in SQL.

## Decision

`Eventstream` keeps its data in a pandas/pyarrow DataFrame (`self._df`) but
executes almost all transformations and computations as DuckDB SQL over that
frame. Data processors build SQL strings (window functions, CTEs) and run
them via `duckdb.sql()` / `duckdb.query()`; results come back as DataFrames.

Several processors additionally expose a `sql=` escape hatch where the *user*
writes DuckDB SQL against the stream (see ADR-0002 for the contract).

## Consequences

- Heavy lifting (sessionization, collapsing, per-path metrics) is columnar
  and fast; the pandas ceiling of 3.x is gone.
- Every processor must restore pandas categorical dtypes after a DuckDB
  round-trip (DuckDB returns ordered categoricals; we normalize back to
  unordered and drop unused categories).
- SQL strings are built with f-strings, so every user-supplied value must go
  through the escaping helpers in `utils/sql_quoting.py` (`quote_literal`,
  `quote_list`, `quote_ident` — the single home for this logic; several
  per-module duplicates existed before consolidation and are gone now).
  This is still a standing review point, not an automated check.
- The replacement-scan idiom (ADR-0002) becomes load-bearing and looks like
  dead code to linters.
