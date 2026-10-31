# ADR-0015: Path pattern quantifiers and groups

Status: Accepted (recorded 2026-10)

## Context

Path patterns (`paths/tokens.py`, `paths/anchors.py`) are one language shared
by `matches_pattern`, `filter_paths` conditions, `truncate_paths` /
`add_events` anchors and Step Matrix / Step Sankey centring. It borrows Python's
regex with one substitution — a token is an event, not a character — and is
matched relationally in DuckDB, not by an automaton. Two properties hold the
rest of the library together:

- every token that is not a gap occupies one **position** and one **ordinal**,
  which `at=`, `occurrence=` and step-matrix blocks address;
- negation and alternation exist only where their scope is bounded — one
  position, or a run pinned by an anchor on each side.

Before this decision `*` was the only quantifier. That left three gaps: a
positive whitelist gap (`cart->[ship|pay]*->buy`) was satisfied by the empty
run; "within n steps" could only be spelled `.->.->.`, which shifts ordinals and
cannot express a range; and a repeated sequence had to be written out by hand
(#110, #118).

## Decision

**Quantifiers on gaps.** After `.` or a class, the regex family is accepted:
`*`, `+`, `?`, `{n}`, `{m,}`, `{,n}`, `{m,n}` (`{,n}` as in Python's `re`).
Bounds are inclusive and count the events strictly between the two anchors.

- A quantified class is always a **gap**, never a variable number of
  positions. In regex `[ab]{2,3}` is two or three characters; here it takes no
  ordinal, because a position count that varied per path would leave `at=` and
  block layout with nothing stable to address. The set of matched paths equals
  regex's; only the numbering differs.
- A non-plain gap at either end of a pattern is refused, as restricted gaps
  already were: with nothing on its outer side the run can be cut wherever
  suits. The error names `path_start` / `path_end`, and for a required run
  (`[A]{2,}`) the group spelling.
- Only event counts. A unit (`{30m}`) is refused; time bounds (#118) bound the
  anchors' timestamps rather than the run's contents — a different predicate
  that will share this slot via the unit suffix (ADR-0008 rule 4) when built.
- Matching: the forward/backward scans keep their shape. Ordering moves from
  `(step, kind)` to one numeric key `2 * step + kind`, which DuckDB's `RANGE`
  offsets require; the admissible-neighbour window becomes
  `RANGE BETWEEN 2n+1 AND 2m+1 PRECEDING` (mirrored for the backward pass)
  while the violation window stays unbounded. The componentwise-extremum
  property survives both bounds: the componentwise minimum pairs the earlier
  left anchor (lengthening the run, helping `m`) with the earlier right anchor
  (shortening it, helping `n`), each inherited from a valid match.

**Groups.** `(P){k}` repeats a sequence by **unrolling** it in
`normalize_pattern`; the matcher never sees a group and every copy takes its own
ordinals. Copies are joined by `->`, i.e. back to back — the only reading
consistent with what `->` means everywhere else.

- `{k}` already means "at least k" because matching is a search. `{k,}` and
  `+` are accepted only where an unrestricted `.*` sits next to the repeated
  block (at the group's edge, outside it, or at a pattern end) — exactly where
  they equal `{k}`. Between two adjacent tokens (`A->(B->C){2,}->D`) they would
  need counting and are refused.
- Upper bounds (`{,n}`, `{m,n}`, `?`) and zero repetitions are refused. "At
  most n" asserts the absence of an (n+1)-th repetition; it is a predicate on a
  path, expressed as `not matches_pattern("(P){n+1}")` in a condition. The
  error spells that condition out.
- **Glued-copy guard.** A group repeated twice or more with a gap inside but
  none at its edges (`(B->.*->C){2}`) is refused. Its author means
  "somewhere", yet the copies are glued `C->B`. The message offers both
  `(B->.*->C->.*){2}` and the written-out back-to-back form. A gap-free group
  (`(B->C){3}`) is unaffected — back to back is all it can mean.
- Nested groups are refused; unroll the inner one by hand.

**Square vs round brackets.** `[...]` stays one event from a set; `(...)` is a
sequence. A quantifier on `[...]` makes a gap, on `(...)` makes copies. A
quantifier on a bare name (`cart{2}`) stays part of the name, so names ending in
`*` or `}` keep working; an unknown such name gets a hint pointing at both
spellings.

## Consequences

- Every existing pattern parses and matches as before; the scan rewrite is
  verified against the brute-force oracle in `tests/paths/anchors_test.py`,
  which now checks run lengths.
- Patterns reaching the JS frontend may contain bounded gaps and groups. Step
  Sankey splits blocks on any gap token, and its result carries
  `layout_pattern` (the unrolled form) so groups lay out as Python computed them.
- No back-references: "some event repeated" is an `or` of one pattern per event.
- "Exactly k" and ranges are conditions over two patterns. For back-to-back
  repetitions they describe "some run reaches m, none reaches n+1", not the
  length of a maximal run; that would need a counting metric.
