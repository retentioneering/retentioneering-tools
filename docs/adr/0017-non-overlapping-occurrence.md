# ADR-0017: Explicit matches with bounded ordered evaluation

Status: Proposed (2026-10; replaces the implementation proposed in PR #207)

## Context

The new occurrence mode must mark each episode rather than every overlapping
position. The first implementation encoded events as characters and delegated
to Python re. Local Lumeo audit found exponential backtracking in a permitted
nested-repeat pattern, SQL/regex differences for NULL and real boundaries, and
analytical ambiguity around captures, defaults, offsets and denominators.

## Decision

- Keep the grammar and the leftmost, non-overlapping selection rule from #207,
  including its later restricted-gap parsing fix. Groups are greedy; gaps lazy.
- Compile consume/split/accept instructions. A stack-based, memoized evaluator
  resolves each (instruction, input position) once per path, sharing capture
  suffixes. Ordered alternatives preserve greedy/lazy choices without exponential
  backtracking or Python recursion depth limits. DuckDB handles row ordering and
  integer vocabulary encoding. No runtime regex scan or new dependency.
- Limit compilation to 4096 instructions and evaluation to 2,000,000 states per
  path. Raise PatternMatchLimitError on exhaustion; never return partial results.
- Keep numeric captures compatible with normalized token ordinals. Track full
  positional bounds independently, so string start/end work for singleton and
  repeated `(B)+` matches. A trailing/leading gap is not a positional boundary.
- Match SQL predicates: positional classes reject NULL; gaps permit NULL and
  boundary rows even when restricted (neither is a SQL gap violation).
- Expose find_matches with explicit grain and (path, match_id) identity. Expose
  get_match_conversion with explicit start/end origin and any_later attribution,
  including non-converted episodes. A target can convert several matches.
- Require explicit at with non_overlapping, path_col for ambiguous grain, and
  unit='path' to use it in get_conversion_rate. Existing modes keep their defaults.
- Add exact-row placement='after' for terminal markers and a named on_existing
  policy. Non-overlapping offsets that leave the path fail instead of clamping;
  repeated annotation defaults to an error rather than duplicate events.

## Consequences and limits

The program uses O(program size * path length) cached states per path, bounded
by the explicit budget. This trades some speed and memory for predictable work;
performance must be measured on representative data, not inherited from #207.
Exhaustive small-path comparisons against an independent regex oracle pin the
selection semantics; large negative cases and resource-limit tests pin safety.

There is deliberately no universal business definition of an episode. Fixed
blocks, maximal runs and target-bounded attempts differ; tables and examples
make that choice inspectable. Match conversion currently accepts a literal
target and any_later attribution only. Matrix/Sankey/window anchors and the
internal not_before floor still reject multi-position occurrence modes.

This changes only the new mode's draft contract, not a released non_overlapping
API. Migration from #207 is documented in Path Patterns. MCP exposes the same
unit and attribution requirements and labels truncated display rows explicitly.
