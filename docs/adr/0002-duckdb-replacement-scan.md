# ADR-0002: DuckDB replacement scan idiom and the `FROM eventstream` SQL contract

Status: Accepted (5.0 rewrite; recorded 2026-07)

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
