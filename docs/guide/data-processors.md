# Data Processors

Data processors transform an [Eventstream](/docs/eventstream) and return a new one. They are available as methods on the `Eventstream` object. In the examples throughout this section, `stream` refers to an `Eventstream` instance.

Processors can be chained:

```python
stream = (
    Eventstream(df)
    .add_start_end_events()
    .filter_events(drop={"event": ["bot_visit"]})
    .rename_events({"btn_clk": "button_click"})
)
```

Each processor returns a new `Eventstream`, so the original is never modified.

## Overview

### Adding events

- [`add_events`](/docs/data-processors/add-events) — insert synthetic events derived from existing events or a SQL query.
- [`add_start_end_events`](/docs/data-processors/add-start-end-events) — prepend a `path_start` and append a `path_end` synthetic event to each path.

### Renaming, editing & merging events

- [`rename_events`](/docs/data-processors/rename-events) — rename events using a mapping dict.
- [`edit_events`](/docs/data-processors/edit-events) — rename and/or delete events in a single operation.
- [`collapse_events`](/docs/data-processors/collapse-events) — merge consecutive or grouped events into a single representative event.
- [`urls_to_events`](/docs/data-processors/urls-to-events) — turn a raw URL column into structured event names using a URL path tree.

### Filtering, truncating & sampling

- [`filter_events`](/docs/data-processors/filter-events) — keep only rows that match a column filter, a Python predicate, or a SQL query.
- [`drop_events`](/docs/data-processors/drop-events) — remove events from the eventstream by name.
- [`filter_paths`](/docs/data-processors/filter-paths) — keep only paths that satisfy a metric condition.
- [`truncate_paths`](/docs/data-processors/truncate-paths) — trim each path to the window between two anchor events.
- [`sample_paths`](/docs/data-processors/sample-paths) — randomly sample paths (and all their events).

### Sessions & lifecycle

- [`split_sessions`](/docs/data-processors/split-sessions) — split each path into sub-sessions and add session ID and index columns.
- [`to_daily_states`](/docs/data-processors/to-daily-states) — convert the eventstream into daily lifecycle-state events (new, current, at-risk, dormant, ...).

### Segments & clustering

- [`add_segment`](/docs/data-processors/add-segment) — add a new categorical segment column to the eventstream.
- [`drop_segment`](/docs/data-processors/drop-segment) — remove a segment column from the eventstream.
- [`add_clusters`](/docs/data-processors/add-clusters) — cluster paths using ML and add a new segment column with integer cluster labels.
- [`rename_segment_levels`](/docs/data-processors/rename-segment-levels) — rename levels within an existing segment column.
