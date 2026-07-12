# Changelog

All notable changes to this project will be documented in this file.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)

## [Unreleased]

### Added

- `schema.custom_cols` now defaults to `None` instead of `[]`: any DataFrame
  column not otherwise declared in the schema is added to it automatically,
  keeping it from being silently dropped by row-reshaping data processors
  (`collapse_events`, `to_daily_states`). Passing an explicit list — even
  `[]` — switches to strict mode: only schema-declared and listed columns
  are kept, everything else is excluded from the eventstream.
- `Eventstream.describe()`: headless summary of an eventstream — schema,
  shape (event/path counts), date range, event frequency, and path
  length/duration distributions (mean/median/min/max/percentiles).
  Replaces 3.3.0's `describe()`/`describe_events()`, dropped in the 5.0.0
  rewrite with no direct replacement.
- All widgets accept a `state_file` argument binding the full widget state
  (data and display parameters, plus widget-specific extras: the transition
  graph's node layout, event visibility, filters, and zoom; the step matrix's
  event visibility/pins, filters, row order, step window, and horizontal
  scroll; the step sankey's event count filter and horizontal scroll; the
  cluster analysis' cluster renames and active tab) to a JSON file: if the
  file exists the state is loaded from it, otherwise it is created, and every
  subsequent change is auto-saved. Explicitly passed arguments override the
  loaded state.
- The transition graph keeps its zoom/pan across recomputes (changing edge
  weight, diff, or path column no longer resets the viewport); step matrix
  and step sankey likewise keep their horizontal scroll and row order.

## [5.0.0]

Complete rewrite of the library's core engine, compared to 3.3.0. The
pandas-based `Eventstream`, the iframe+CDN-loaded Transition Graph /
Preprocessing Graph widgets, and the `params_model` GUI-schema system are
gone, replaced by a DuckDB-backed `Eventstream` and a new generation of
`anywidget`-based interactive widgets. The JS visualization layer is now
open source and lives in this repository — it's built in CI and embedded
directly into the Python wheel, instead of being downloaded from a CDN at
runtime.

### Breaking

- Requires Python 3.11+ (was 3.8–3.11 in 3.3.0)
- `Eventstream` is now backed by DuckDB/pyarrow instead of pandas; the old
  `data_processor`/`preprocessor`/`data_processors_lib` pipeline and the
  `params_model`/`widget` pydantic-based GUI-schema system have both been
  removed — data processors now take plain keyword arguments
- The interactive Preprocessing Graph (the visual, no-code pipeline builder)
  and the `Cohorts`, `StatTests`, and `Sequences` tools from 3.3.0 are not
  part of this release. There is no direct replacement yet — they may return
  in a future version
- `TransitionGraph`, `StepMatrix`, `StepSankey`, `Funnel`, and `Clusters`
  (now `ClusterAnalysis`) are reimplemented from scratch as `anywidget`
  components instead of iframe+CDN-loaded JS; the custom Jupyter kernel-comm
  backend (`backend/`, iframe postMessage bridge) they depended on has been
  removed

#### `Eventstream` API changes

Renamed or changed signature (same concept, different call shape):
- `to_dataframe(copy=False)` → `to_dataframe(exclude_start_end=True)`
- `filter_events(func)` → `filter_events(keep=None, drop=None, func=None, sql=None)`
  — `func` is now one of four alternative filtering modes
- `add_start_end_events()` → `add_start_end_events(path_col=None)`
- `split_sessions(timeout, delimiter_events, delimiter_col, session_col, mark_truncated)`
  → `split_sessions(session_col, session_index_col, separator, start_event, end_event, timeout, path_col, event_col)`
  — `timeout` now takes a duration string with an explicit unit (`"30m"`) or
  a `pandas.Timedelta`; bare numbers are rejected
- `truncate_paths(drop_before, drop_after, occurrence_before, occurrence_after, shift_before, shift_after)`
  → `truncate_paths(start_event, end_event, path_col=None, event_col=None)`
- `rename(rules: list[dict])` → `rename_events(mapping: dict)`
- `collapse_loops(suffix, time_agg)` → `collapse_events(consecutive, event_groups, group_col, session_id_col, session_type_col, agg, path_col, event_col)`

Removed, no equivalent in this release:
`copy()`, `append_eventstream()`, `index_events()`, `add_custom_col()`,
`clusters()` (the stateful `fit()`/`extract_features()` object), `cohorts()`,
`stattests()`, `timedelta_hist()`, `user_lifetime_hist()`,
`event_timestamp_hist()`, `describe()`, `describe_events()`,
`preprocessing_graph()`, `transition_matrix()` as a public method (the
computation still happens internally inside `transition_graph_data()`),
`sequences()`, `add_negative_events()`, `add_positive_events()`,
`drop_paths()`, `group_events()`, `group_events_bulk()`,
`label_cropped_paths()`, `label_lost_users()`, `label_new_users()`, `pipe()`

Added, no equivalent in 3.3.0:
`schema`/`df` properties, `is_empty()`, `equals()`, `get_event_counts()`,
`fingerprint`, `get_segment_levels()`, `urls_to_events()`, `filter_paths()`
(condition-tree based), `get_metrics()`, `add_events()`, `add_segment()`,
`add_clusters()` (a new one-shot processor, unrelated to 3.3.0's `clusters()`),
`to_daily_states()`, `drop_segment()`, `edit_events()`, `drop_events()`,
`sample_paths()`, `transition_graph_data()`, `step_sankey_data()` /
`step_matrix_data()`, `funnel_data()`,
`segment_overview()`/`segment_overview_data()`,
`cluster_analysis()`/`cluster_analysis_data()`, `get_metric_distribution()`

Naming conventions across the new API:
- one column vocabulary everywhere: `path_col`, `event_col`, `timestamp_col`,
  `session_id_col`, `segment_col`
- window anchors are always the `start_event` / `end_event` pair
  (`truncate_paths`, `split_sessions`, the `time_between` metric)
- the diff-mode sentinel for "every other segment value" is `<REST>`
- path metric names: `has_event`, `matches_pattern`, `in_segment`,
  `first_event_time` (plus `length`, `duration`, `event_count`,
  `time_between`, `active_days`); the `complement_distance` aggregation
- transition-graph edge weights: `proba_out`, `proba_in`, `count`,
  `unique_paths`, `share_of_total`, `avg_per_path`, `time_median`, `time_q95`
- `sample_paths(n=, frac=)` mirrors `pandas.DataFrame.sample`

### Added

- **Segment Overview** — a new widget/tool with no equivalent in 3.3.0, for
  comparing metrics across segments
- **MCP server** (`retentioneering.mcp.serve()`) — exposes the eventstream
  to Claude and other MCP clients over SSE, with tools for adding widgets,
  managing a session baseline, validating analysis text, and exporting a
  multi-widget static HTML report with clickable cross-references
- `ipywidgets` is now a core dependency, so widgets work out of the box in
  plain JupyterLab
- `rename_segment_levels(segment_col, mapping)` — rename levels within an existing
  segment column (e.g. cluster labels produced by `add_clusters`, or messy raw
  segment data), analogous to `rename_events` but for segment columns
- `cluster_analysis_data()` and the Cluster Analysis widget now report `best_params`
  (`chosen_params` on the widget) — the concrete clustering parameters that produced
  the current result (e.g. the winning `n_clusters` from a silhouette grid search),
  so they can be passed straight to `add_clusters` to reproduce it
- Cluster Analysis widget: "Save Clusters" sidebar action — write the current
  clustering into the eventstream as a new segment column, optionally renaming
  cluster labels first. Choose either a copy-pasteable `add_clusters(...)` code
  snippet (`stream` stays untouched) or applying it in place immediately

### Fixed

- SQL injection in the funnel data processor's query builder — event names
  are now properly escaped
