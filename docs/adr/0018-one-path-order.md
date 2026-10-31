# ADR-0018: One path order

Status: Accepted (2026-10)

## Context

Every result that reads a path in sequence depends on the order of its rows:
transitions, steps, patterns, funnels, collapsed runs, sessions, and the
`"first"` and `"last"` value of a merged event. Before this decision the library
had two orders.

- **Tools** read a path by `(index, subindex)`. `index` is the event's place in
  the path, counted once at construction (ADR-0004). `subindex` is the rank of
  its event type. Path patterns, the transition and step matrices, funnels,
  conversion and route statistics all order this way, and
  `tests/tools/funnel_order_test.py` pins it.
- **`collapse_events` and `split_sessions`** read a path by
  `(timestamp, subindex)`, with `index` as the tie-breaker since the fix for same-timestamp ties (#225).

Raw logs often store several events at one timestamp, and `add_events` gives
each marker its source event's `index`. Three faults followed:

- **R1.** A marker at a tied timestamp stood next to its own source event in
  the tools, and before every event of that timestamp in the two processors.
  With two `A` at one instant, each with a marker `M`, `matches_pattern("M->A->M->A")`
  was true while `collapse_events(loops=True)` saw `M M A A`.
- **R2.** Two `add_events` calls at one source event gave two rows with the same
  timestamp, subindex and index. Nothing ordered them, in the processors or in
  the tools, so results depended on how DuckDB happened to read the frame.
- **Supplied columns.** A frame that brought its own `index` or `subindex`, for
  example a column called `index` left by `reset_index()`, was used as the
  position without any check.

## Decision

**D1. The path order is `(index, subindex)`.** Every operation that reads a path
in sequence uses it: tools and processors alike. The timestamp is for time
arithmetic only: timeout gaps, durations, inactivity and calendar bins.

- `collapse_events`, `split_sessions`, `truncate_paths`, `to_daily_states`
  (the `"first"` / `"last"` of a day) and the `churn` mode of `add_events` (which
  event of a tie the inactivity follows) order by it.
- The SQL helper is `session_detection.path_order`, the same terms the tools
  write by hand.
- **Exception: `split_sessions(separator=...)`.** A separator sorts first among
  the events of its timestamp, so the events stored with it fall in the session
  it opens. That is the path order with each separator lifted to the front of its
  timestamp. The rule predates this ADR and is pinned by
  `test_separator_same_timestamp_as_regular_event`. It is written out where it
  applies, not folded into `path_order`.

**D2. Positions are unique.** No two rows of a path share `(index, subindex)`.

- The ranks of `EventTypes` are a hundred apart: `path_start` 0, `synthetic` 100,
  `raw` and `collapsed` 200, `churn` 300, `path_end` 400.
- `add_events` gives each new marker the next free rank of its kind at its
  position: synthetic markers 100, 101, …, churn markers 300, 301, …. Markers at
  one position therefore stand in the order they were added. A later synthetic
  marker sits between the earlier ones and its source event, and a later churn
  marker after the earlier ones. That holds when the source is itself a marker:
  the new marker follows it.
- At most `EventTypes.SLOTS` (100) markers of one kind fit at one position;
  `add_events` raises beyond that.
- A collapsed row takes the position of the first row it merges.
- Rejected: renumbering `index` densely after each insertion. It would change
  how pattern gaps count markers and where `truncate_paths` cuts, and ADR-0004
  computes `index` once, at construction.

**D3. Supplied positions are checked.** When the frame given to `Eventstream`
already has the schema's `index` or `subindex` column:

- the values must be non-empty whole numbers; integral floats are cast;
- positions must be unique within each path;
- the timestamp must not decrease along the path order;
- without a `subindex` column, every `event_type` value must be a known type.

Any failure raises `SchemaConfigError`. A frame that passes is laid out in its
own path order.

Ranks saved on the old 0–4 scale are moved to the new one. The new scale never
uses 1 to 4, so a column whose ranks all lie in 0–4 and include one of them can
only be old; multiplying by 100 keeps every rank's meaning and order.

`add_events(sql=...)` gets the same guarantee from the other side: each row the
query returns must be a row of the stream, with its own path, `index` and
timestamp, because the marker is placed at that row's position.

**D4. Invariant.** After every processor:

- positions are unique within each path;
- the timestamp does not decrease along the path order;
- each path's rows come back in path order.

`tests/conftest.py` checks this on the output of every `DataProcessor.apply`
call in the test suite whose input satisfies it. The check itself lives in
`retentioneering.eventstream.positions`.

## Consequences

- Results no longer depend on the order DuckDB reads rows in, for any stream
  that satisfies D4, which is every stream the library builds.
- Streams with markers at tied timestamps, and streams with repeated markers at
  one event, can give different results than before. Raw-only streams, and
  markers at distinct timestamps, give the same results: when no timestamps tie,
  `(timestamp, subindex)` and `(index, subindex)` are the same order.
- `stream.df` and `to_dataframe()` show the new `subindex` values. Code that
  compared `subindex` with literals should compare with
  `EventTypes().get_order()` instead.
- A processor that adds rows must give them positions that keep D4. The test
  fixture catches one that does not.
