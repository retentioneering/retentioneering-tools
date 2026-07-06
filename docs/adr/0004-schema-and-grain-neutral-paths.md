# ADR-0004: Schema model and grain-neutral "path"

Status: Accepted (5.0 rewrite; terminology re-affirmed 2026-07)

## Context

Event data arrives with arbitrary column names and arbitrary notions of what
one trajectory is (user, session, checkout attempt). Different disciplines
name the trajectory differently: *trace/case* (process mining), *journey*
(UX/marketing), *trajectory* (RL, stochastic processes), *flow* (Mixpanel,
but that is an aggregate), *path* (graph theory, Amplitude Pathfinder, GA4).

## Decision

- `EventstreamSchema` declares column roles: `path_cols`, `event_cols`,
  `timestamp_col`, `segment_cols`, `custom_cols`, plus technical columns
  (`event_type`, `index`, `subindex`) managed by the library. The first
  element of `path_cols` / `event_cols` is the primary column.
- The unit of analysis is called **path**, and it is deliberately
  grain-neutral: whatever `path_col` points at *is* the path (user journey,
  session, or fragment). Every tool accepts a `path_col` override, so one
  stream can be analysed at several grains.
- "Path" was chosen over *trace* (collides with stack/distributed tracing),
  *journey* (implies whole-customer grain, clumsy in identifiers), and
  *trajectory* (long, alien to product analysts): it is short, composable
  (`path_col`, `path_start`, `per_path`), native to the transition-graph
  metaphor, and has an industry prior ("path analysis").
- A **segment** is defined by a segment *column*: the column names a
  segmentation, each value is one segment. Segments may be static (one value
  per path) or dynamic (value changes along the path). This differs from
  Amplitude-style "create a segment = define one group" and is documented in
  the Eventstream guide glossary ("Key concepts").

## Consequences

- Docs must keep translating the term for incoming audiences ("your user
  journey, if you come from Amplitude; your trace, if you come from process
  mining") — the glossary owns this.
- Known wart: inside `urls_to_events`, node key `"path"` means *URL path* —
  the word carries two meanings in one library; context disambiguates.
- Reserved event names `path_start` / `path_end` are part of the data-level
  contract (usable as anchors in `truncate_paths`, `time_between`,
  `path_pattern`).
