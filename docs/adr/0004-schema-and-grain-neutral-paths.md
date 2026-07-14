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
- **`path_cols` must be ordered coarsest-first: a strict nesting hierarchy.**
  Every value of `path_cols[i+1]` must belong to exactly one value of
  `path_cols[i]` — e.g. `["user_id", "session_id"]` is valid because every
  session belongs to exactly one user; `["session_id", "user_id"]` is not,
  because a user's sessions aren't nested under a single session. This is a
  data-level invariant, not just a naming convention: `Eventstream` validates
  it against the actual rows at construction time and raises
  `SchemaConfigError` on violation. It exists because the technical `index`
  column (see below) is computed once, scoped to `path_cols[0]` — nesting is
  what guarantees `index` stays chronologically meaningful at every finer
  declared grain, without recomputing it per grain.
- The unit of analysis is called **path**, and it is deliberately
  grain-neutral *within that hierarchy*: whatever `path_col` points at *is*
  the path (user journey, session, or fragment) — but it must be one of the
  declared, nesting-validated `path_cols`. Every tool accepts a `path_col`
  override, so one stream can be analysed at several grains, but passing a
  column outside `path_cols` (e.g. an arbitrary `segment_col`) raises an
  error rather than silently analysing an ungoverned grouping.
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
- Wanting arbitrary (non-nested) grouping columns for analysis — e.g. by
  `device_type` or `campaign_id` — is a legitimate need, but it is not what
  `path_col` is for post-2026-07: use `segment_cols`/`diff` for cross-cutting
  breakdowns instead. This is narrower than the original 5.0-rewrite
  intent ("whatever `path_col` points at is the path"), tightened after a bug
  class where `index`-based ordering silently broke for `path_col` overrides
  that didn't nest under `path_cols[0]`.
