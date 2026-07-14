# ADR-0002: DuckDB replacement scan idiom and the `FROM eventstream` SQL contract

Status: Superseded by L1 (see `engine.py`) (recorded 2026-07)

> **Superseded.** The replacement-scan idiom described below is no longer the
> live convention for new code. `src/retentioneering/engine/` (`engine.run()` /
> `engine.quote_ident()`, plus DuckDB-specific SQL fragments in
> `engine/dialect.py`) now centralizes query execution: call sites pass pandas
> frames to `engine.run()` as explicit keyword arguments instead of relying on
> DuckDB inspecting the caller's stack frame for a same-named local variable.
> The `eventstream` alias contract for user-facing `sql=` arguments
> (`filter_events`, `add_events`, `add_segment`) is unchanged — it is still the
> fixed table name those processors register the frame under when calling
> `engine.run()`. The historical context below (why the old idiom existed, and
> why "unused" variables were once load-bearing) is kept for reference; it no
> longer describes how current call sites are written.

## Context

DuckDB resolves an unqualified table name in a SQL string by looking up a
same-named Python variable in the caller's scope ("replacement scan"). Static
analysis cannot see this data flow: a variable that is only referenced from
inside a SQL string looks unused.

## Decision

1. Internal queries reference the frame as `FROM df` (or `df_with_start_end`,
   `metrics`, ...) and bind it with an explicit assignment immediately before
   the query, annotated with
   `# noqa: F841 -- referenced by name via DuckDB replacement scan ...`.
2. User-facing `sql=` arguments (in `filter_events`, `add_events`,
   `add_segment`) expose the data under the fixed alias **`eventstream`** —
   the processor assigns `eventstream = df` so user queries always read
   `SELECT ... FROM eventstream`.

## Consequences

- **Never delete a variable assignment just because it looks unused** — check
  for a matching `FROM <name>` in a nearby SQL string first. This is the
  single most dangerous "cleanup" in the codebase.
- The `eventstream` alias is a public API contract documented in docstrings
  and covered by tests; renaming it is a breaking change.
- `add_segment(sql=)` additionally injects a row-index column into the user's
  outermost SELECT to restore row order after DuckDB's parallel execution
  reorders results (`_inject_row_idx`).
