# Changelog

All notable changes to this project will be documented in this file.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)

## [Unreleased]

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
- `filter_events(func)` → `filter_events(by_column=None, func=None, sql=None)`
  — `func` is now one of three alternative filtering modes
- `add_start_end_events()` → `add_start_end_events(path_id_col=None)`
- `split_sessions(timeout, delimiter_events, delimiter_col, session_col, mark_truncated)`
  → `split_sessions(session_col, session_index_col, separator, start_event, end_event, timeout, path_id_col, event_col)`
- `truncate_paths(drop_before, drop_after, occurrence_before, occurrence_after, shift_before, shift_after)`
  → `truncate_paths(left, right, path_id_col=None, event_col=None)`
- `rename(rules: list[dict])` → `rename_events(mapping: dict)`
- `collapse_loops(suffix, time_agg)` → `collapse_events(repetitive, event_groups, event_from_col, session_id_col, session_type_col, agg, path_id_col, event_col)`

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
`schema`/`df` properties, `empty()`, `equals()`, `get_event_counts()`,
`fingerprint`, `get_all_segment_levels()`, `url_events()`, `filter_paths()`
(AST-condition based), `get_metrics()`, `add_events()`, `add_segment()`,
`add_clusters()` (a new one-shot processor, unrelated to 3.3.0's `clusters()`),
`daily_states()`, `drop_segment()`, `edit_events()`, `sample_paths()`,
`split_two()`, `transition_graph_data()`, `step_sankey_data()`,
`funnel_data()`, `segment_overview()`/`segment_overview_data()`,
`cluster_analysis()`/`cluster_analysis_data()`, `metric_distribution()`

### Added

- **Segment Overview** — a new widget/tool with no equivalent in 3.3.0, for
  comparing metrics across segments
- **MCP server** (`retentioneering.mcp.serve()`) — exposes the eventstream
  to Claude and other MCP clients over SSE, with tools for adding widgets,
  managing a session baseline, validating analysis text, and exporting a
  multi-widget static HTML report with clickable cross-references
- **Cloud save/load** for the Transition Graph and Step Matrix widgets —
  ships in this release but is disabled by default (no cloud icon shown),
  since retentioneering doesn't yet run a backend for it. Credentials are
  read from environment variables with no built-in default
- `ipywidgets` is now a core dependency, so widgets work out of the box in
  plain JupyterLab

### Fixed

- SQL injection in the funnel data processor's query builder — event names
  are now properly escaped
