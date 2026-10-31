# ADR-0017: Event order contract — `(index, subindex)` everywhere

Status: Accepted (2026-10)

## Context

Every path-shaped computation in the library — transitions, step matrices,
funnels, patterns, collapsing runs, sessions — depends on the order of events
within a path. 5.0 carried that order in two technical columns written by the
constructor (`preprocess=True`):

- `subindex` — the event type's rank from `EventTypes` (`path_start` 0,
  `synthetic` 1, `raw`/`collapsed` 2, `churn` 3, `path_end` 4);
- `index` — `cumcount() + 1` per path after a stable sort by
  `(path, timestamp, subindex)`, so every raw event has a unique index and raw
  events at equal timestamps keep their input row order.

The order was not one well-defined thing after that:

- **Two keys in use.** Most readers (step matrix, transition matrix, graph
  layout, conversion, funnel, anchors, route stats, `add_start_end_events`,
  `add_segment`) ordered by `(index, subindex)`. Others ordered by
  `(timestamp, subindex)`: session detection, `collapse_events` boundary mode
  and its aggregates (`ARG_MIN(col, timestamp)`, `index` included),
  `add_events` (output sort, churn window), `truncate_paths` and
  `split_sessions` output, `to_daily_states`. With tied timestamps the second
  key cannot tell events apart, so the result depended on physical row order.
  Visible example: path `a, b, a` at one timestamp, then `c` an hour later —
  `split_sessions(separator="b")` put both `a` into the session after `b`.
- **The key was not unique.** `add_events` markers copy the source row's
  `index` and take `subindex` = type rank, so two markers of the same type
  derived from one row tied in `(index, subindex)` regardless of timestamps.
  `es.add_events("x", source_event="c").add_events("y", source_event="c")`
  ties in every path; on 100k paths with x, y, x from one source event,
  identical `collapse_events(loops=True)` calls returned different row counts.
- **Physical order drifted.** Derived streams are rebuilt with
  `preprocess=False` (ADR-0003): no sort, no reindex. Row order was whatever
  the last processor emitted, and each processor sorted by a key of its own.
- **User-supplied `index`/`subindex` were accepted unchecked.**

PR #182 fixed one symptom (`collapse_events` run detection) by switching that
one query to `(index, subindex)`. This ADR makes the same choice library-wide.

## Decision

1. **The order key is `(index, subindex)`, unique within a path.** Position is
   defined by `index`; the timestamp is an attribute of the event, not its
   position. `index` is assigned once and never changes: processors that drop
   rows leave gaps, processors that add rows reuse a source row's `index`.

2. **Raw events tied on timestamp keep their input row order.** The
   constructor's sort is stable, and the library does not invent a tie-break
   (by event name, by type, by anything else). Input order usually carries
   real signal — the order the tracker logged events in — that no rule we
   could pick would reproduce. Users who want a different order sort their
   frame before constructing the `Eventstream`, or pass their own `index`.

3. **`subindex` is fractional for markers sharing a position.** The n-th marker
   (n = 0, 1, 2, …) of rank r at one `index` gets `subindex = r + n/(n+1)` —
   r, r + 1/2, r + 2/3, … — strictly increasing, never reaching r + 1, so the
   rank order between types is kept. Original events keep their integer rank.
   A new marker goes after the existing same-rank markers at that position, so
   `add_events("x").add_events("y")` places `y` after `x`. This applies to all
   `add_events` modes and to any processor that creates rows sharing an index.
   `subindex` becomes `float64`; it is used only for ordering, never compared
   for equality against ranks (in Python or JS).

4. **One ordering helper.** `engine/dialect.py` provides the within-path order
   (`event_order`) next to `event_position`, and `EventstreamSchema.order_cols`
   gives the same key for pandas; every window `ORDER BY`, ordered list
   aggregation, output `ORDER BY` and pandas sort uses them or the same columns.
   No code orders events by timestamp. AGENTS.md carries this rule.

5. **Canonical physical order.** `Eventstream(..., preprocess=False)` sorts by
   `(path, index, subindex)` (stable), so a chain of processors cannot drift
   regardless of what order any one of them emits.

6. **Uniqueness is validated.** The constructor checks that
   `(path, index, subindex)` is unique and raises `SchemaConfigError` naming
   offending rows otherwise. This also covers user-supplied `index`/`subindex`
   columns.

7. **Collapsed rows take the group's first position** — the minimum
   `(index, subindex)` of the run — not `ARG_MIN(index, timestamp)`.

8. **`add_events(sql=...)` rows must carry a source row's position.** The query
   returns rows in the stream's own shape, and each returned row must have the
   `(path, index)` of an existing row; a row with an index that exists nowhere
   in its path raises `PreprocessingConfigError`. The new row is then placed by
   rule 3. A returned timestamp that falls outside the neighbouring positions'
   timestamps is **not** checked or warned about: the timestamp is an
   attribute (rule 1), shifting a marker's time is a legitimate use (e.g. a
   "deadline" marker), and a warning that fires on intended input would be
   noise. Time-based metrics read timestamps as given.

## Consequences

- Every reader gives the same answer on tied data, and repeated calls on the
  same stream give identical results.
- `subindex` dtype changes from integer to `float64` — a visible change for
  anyone reading the technical columns. Raw events still carry `2.0`.
- Frames whose user-supplied `index`/`subindex` collide now fail at
  construction instead of producing order-dependent results.
- `preprocess=False` construction costs one stable sort; the frame is usually
  already in order, so this is cheap relative to the processor that produced it.
- Rows with equal timestamps but different `index` are ordered by `index` —
  e.g. a `churn` marker sorts after its source event even when a later raw
  event shares the source's timestamp, because the marker inherits the
  source's index.
- New processors and tools must order by `(index, subindex)` via the dialect
  helper; reviewers can grep for `ORDER BY` / `sort_values` with a timestamp
  column to find violations.
