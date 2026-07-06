# ADR-0003: Immutable Eventstream and the single-hub API

Status: Accepted (5.0 rewrite; recorded 2026-07)

## Context

3.x had a separate preprocessing-pipeline system (`data_processor` /
`params_model` pydantic GUI schemas) with its own registration machinery.
It was powerful for the visual pipeline builder but heavyweight for code-first
users, and the builder itself was cut from 5.0 (ADR-0013).

## Decision

- `Eventstream` is the single hub: every data processor, tool, and widget is
  exposed as a method on it. There are no sub-namespaces
  (`stream.dp.filter_events()` was considered and rejected — it breaks
  chaining, and the flat surface of ~25 methods is manageable; naming
  conventions provide implicit grouping, see ADR-0008).
- Processors are pure with respect to the stream: each method returns a
  **new** `Eventstream`; the original is never mutated. Chaining is the
  primary composition style.
- Each processor is a class in `data_processors/` with the contract
  `apply(df, schema) -> (new_df, new_schema)`; the Eventstream method is a
  thin wrapper that constructs the processor and rewraps the result with
  `Eventstream(new_df, asdict(new_schema), preprocess=False)`.
- `preprocess=True` (constructor) does the one-time normalization: timestamp
  parsing, categorical casting, sorting, technical columns. Internal
  reconstruction always passes `preprocess=False`.

## Consequences

- No hidden state; any intermediate stream can be kept, compared
  (`equals()`), or branched.
- Copies cost memory; `is_empty()` deliberately avoids the `to_dataframe()`
  deep copy.
- Adding a processor means: class in `data_processors/`, method + docstring
  on `Eventstream`, docs template, tests — the docstring is the documentation
  source (ADR-0014).
