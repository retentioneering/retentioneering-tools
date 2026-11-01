# ADR-0016: Shorthand leaves in metric conditions

Status: Accepted (recorded 2026-10)

## Context

`filter_paths` and `collapse_events` name cases share one condition grammar
(`metrics/condition_ast.py`): comparison leaves
`{"op", "metric", "metric_args", "value"}` joined by `and` / `or` / `not`, with
a top-level list meaning `and`. Most leaves in practice ask whether a 0/1
metric is true — `{"op": "=", "metric": "has_event", "value": True,
"metric_args": {"event": "purchase"}}` — and the ceremony grows with every
branch, e.g. one `matches_pattern` leaf per event to find self-loops.

## Decision

A leaf comparing a 0/1 metric with `True` may be written with the metric name
as its only key:

- `{metric: metric_args}` for any 0/1 metric — `has_event`, `has_any_event`,
  `has_all_events`, `matches_pattern`, `in_segment`;
- `{metric: value}` for those with exactly one required argument —
  `has_event` / `matches_pattern` take a string (`event` / `pattern`),
  `has_any_event` / `has_all_events` a list (`events`). `in_segment` needs two
  and takes the dict form only.

`expand_node` rewrites one node at a time and both tree walkers call it on
every node they visit, so the shorthand works wherever a condition does —
including MCP `filter_paths` steps — and composes with branches and lists.

Rules that keep it unambiguous:

- Before this, every node needed `op`, so a one-key dict without `op` was
  always invalid; no previously valid condition changes meaning.
- Only 0/1 metrics. For `length` or `duration`, `= True` would be a guess about
  the comparison; `{"length": {}}` is refused with the full form to write.
- Exactly one key. `{"has_event": ..., "matches_pattern": ...}` would read as
  an implicit `and`, which already has a spelling (a list); it is refused.
- A node without `op` that is not a shorthand raises a
  `PreprocessingConfigError` naming the node.

The scalar form is a second spelling of the dict form, which ADR-0008 would
normally argue against. It is accepted because the leaves it shortens are the
most common ones, and the mapping is fixed per metric rather than inferred.

## Consequences

- The shorthand is syntax only: what a metric supports does not change.
  `collapse_events` cases still reject `matches_pattern` and `in_segment`,
  shorthand or not.
- A new 0/1 metric must be added to `BOOLEAN_METRICS` (and to `_SCALAR_ARG` if
  it has one required argument) to get a shorthand.
