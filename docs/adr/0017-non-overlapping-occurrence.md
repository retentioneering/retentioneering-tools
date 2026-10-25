# ADR-0017: Non-overlapping occurrences via Python's `re`

Status: Accepted (recorded 2026-10)

## Context

Anchor specs had three occurrence modes: `"first"` and `"last"` give one
position per path, and `"all"` gives every position a token can occupy in
*some* match, overlapping matches included. None of them marks each instance
of a pattern. On `B×5`, `(B){3,}` with `"all"` marks steps 1, 2 and 3 — every
`B` a run of three could start on — where "a marker before each loop" needs
one.

What is needed is `re.finditer`: take the leftmost match, resume after it.
That is sequential by nature — each match depends on where the previous one
ended — and does not fit the relational forward/backward passes the SQL
matcher (ADR-0015) is built on. Greediness also matters here and nowhere else:
for "does a match exist" and for first/last positions `(B){3,}` equals
`(B){3}`, but enumerating matches needs `{3,}` to take the whole run.

## Decision

- New mode `occurrence="non_overlapping"`, resolved in
  `paths/non_overlapping.py` rather than SQL. Each path is encoded as a string,
  one private-use character per event (sentinels included), the pattern is
  compiled to an ordinary Python regex over those characters, and
  `re.finditer` supplies leftmost, non-overlapping, backtracking semantics. The
  library does not reimplement regex.
- **Repeated groups are greedy, gaps are lazy.** `(P){k,}` compiles to the
  first copy, a greedy `(?:P)*`, then the remaining `k − 1` copies, so the
  captures land on the run's first and last repetitions — `at="start"` is the
  start of the run and `at="end"` its end. Gaps compile to lazy quantifiers
  (`.*?`, `.{m,n}?`), so `A->.*->B` pairs each `A` with the next `B` instead of
  making one match from the first `A` to the last `B`.
- Positions come from capture groups emitted in unrolling order, so the capture
  index equals the ordinal of the normalized pattern; `at=` addresses the same
  token in every mode. The spec keeps `source_pattern` (the pattern before
  unrolling), because greediness is a property of the group the unrolled form
  no longer has.
- Like `"all"`, the mode returns several positions per path; `truncate_paths`
  and Step Matrix / Step Sankey anchors reject both (`MULTI_OCCURRENCES`).
  A per-path floor (`not_before`) is refused for now.

## Alternatives considered

- Explicit greedy/lazy syntax (`*?`, `{3,}?`) with regex's defaults. More
  faithful to regex, but a greedy `.*` by default makes `A->.*->B` find one
  match per path, which is almost never what an analysis wants, and the suffix
  would mean nothing in the other modes. Can be added later without changing
  the defaults chosen here.
- Iterating the SQL matcher with a moving floor, plus greedy extension in SQL.
  One query per match index, and greedy backtracking in SQL is the hard part
  regex already solves.

## Consequences

- Cost is one pass over the events in Python (`groupby` + `"".join`) and a C
  regex scan per path; fine for typical event logs, slower than SQL for very
  large ones.
- The name contrasts with `"all"` by mechanism rather than by quantity —
  `"each"` was rejected as a near-synonym of `"all"`.
- A future `pattern_count` metric is the length of this mode's match list.
