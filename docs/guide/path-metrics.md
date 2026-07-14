# Path Metrics

Path metrics are scalar values computed per path (see [Key concepts](/docs/eventstream#key-concepts) for what counts as a path). All metric-accepting tools share this single registry. Metrics are used in three places:

- [Segment Overview](/docs/widgets/segment-overview) — metrics are aggregated per segment and displayed as a heatmap.
- [Cluster Analysis](/docs/widgets/cluster-analysis) — metrics are used as features for clustering, and separately as overview metrics for the resulting clusters.
- [Filter Paths](/docs/data-processors/filter-paths) — metrics are used as conditions to keep or drop entire paths.

## Available metrics

| Metric | Description | metric_args |
|---|---|---|
| `length` | Total number of events in the path. | — |
| `duration` | Time in seconds between the first and last event. | — |
| `active_days` | Number of distinct calendar days with at least one event. Optionally restricted to specific events. | `active_events`: list[str] (optional) |
| `event_count` | Number of times a single event occurred. | `event`: str (required) |
| `has_event` | 1 if a single event occurred at least once, 0 otherwise. | `event`: str (required) |
| `event_count_bulk` | Number of times each event occurred, expanded into one column per event. Omit `events` (or pass `None`) to count every event in the stream — an explicit empty list is invalid. | `events`: list[str] (optional; omit/`None` for all events) |
| `has_event_bulk` | 1 if each event occurred at least once, 0 otherwise, expanded into one column per event. Omit `events` (or pass `None`) to check every event in the stream — an explicit empty list is invalid. | `events`: list[str] (optional; omit/`None` for all events) |
| `has_all_events` | 1 if **all** of the given events occurred at least once (AND semantics), 0 otherwise. | `events`: list[str] (required, non-empty) |
| `has_any_event` | 1 if **any** of the given events occurred at least once (OR semantics), 0 otherwise. | `events`: list[str] (required, non-empty) |
| `time_between` | Time in seconds between the first occurrences of two events. Returns null if either event is missing. Use `path_start` or `path_end` as anchors. | `start_event`: str, `end_event`: str |
| `first_event_time` | Unix timestamp of the first event in the path. | — |
| `matches_pattern` | 1 if the path matches a sequence pattern, 0 otherwise. Events are separated by `->` and matched as whole tokens (not substrings); `.*` matches any sequence of whole events. Example: `login->.*->purchase`. | `pattern`: str |
| `in_segment` | Checks whether path events belong to a segment value. Mode `any`: at least one event has the value. `all`: all events have the value. `event_share`: at least a threshold share of events have the value. If multiple segment values are selected, a separate metric is created for each value. | `segment_name`: str, `segment_value`: str or list[str], `mode`: `"any"` \| `"all"` \| `"event_share"`, `threshold`: float (for `event_share`) |

`event_count`/`has_event` are strict single-event metrics — one number per path,
comparable directly in a Filter Paths condition. Passing a list of events is a
different, separate concern handled by three distinct metrics rather than by
overloading `events`:

- `event_count_bulk`/`has_event_bulk` are a **shorthand for multiple metrics** —
  they expand into one column per event, for Segment Overview/Cluster Analysis.
  They cannot be used inside a Filter Paths (or `collapse_events` case)
  condition, since a condition needs exactly one comparable value per path.
- `has_all_events`/`has_any_event` are genuine single-valued metrics that
  combine a list of events into one 0/1 result (AND/OR respectively) — these
  **can** be used in Filter Paths conditions.

## Metric config format

Metrics appear in two different config formats depending on where they are used:

- **Segment Overview** (`metrics`) and **Cluster Analysis** (`overview_metrics`) — each metric requires an `agg` field that defines how per-path values are aggregated across paths in a segment.
- **Cluster Analysis** (`features`) — no `agg` field. The raw per-path values are used directly as clustering features.
- **Filter Paths** — metrics appear inside a condition tree with comparison operators. See the [Filter Paths condition format](#filter-paths-condition-format) section below.

| Key | Required | Description |
|---|---|---|
| `metric` | yes | Metric name from the table above. |
| `metric_args` | depends | Additional arguments for the metric. Required for `event_count`, `has_event`, `event_count_bulk`/`has_event_bulk` (unless the wildcard is intended), `has_all_events`/`has_any_event`, `time_between`, `matches_pattern`, and `in_segment`; optional for `active_days`. |
| `agg` | yes (Segment Overview `metrics` and Cluster Analysis `overview_metrics` only) | Aggregation function. See aggregations below. |

```python
metrics=[
    {"metric": "length", "agg": "mean"},
    {"metric": "duration", "agg": "median"},
    {"metric": "event_count", "metric_args": {"event": "purchase"}, "agg": "mean"},
    {"metric": "event_count_bulk", "metric_args": {"events": ["add_to_cart", "purchase"]}, "agg": "mean"},
    {"metric": "time_between", "metric_args": {"start_event": "path_start", "end_event": "purchase"}, "agg": "median"},
    {"metric": "matches_pattern", "metric_args": {"pattern": "login->.*->purchase"}, "agg": "mean"},
]
```

## Aggregations

Aggregations apply to `metrics` in Segment Overview and `overview_metrics` in Cluster Analysis. They are not used for clustering `features` or in Filter Paths conditions.

| Value | Description |
|---|---|
| `mean` | Mean value across all paths in the segment. |
| `median` | Median value (50th percentile). |
| `q5` | 5th percentile. |
| `q25` | 25th percentile. |
| `q75` | 75th percentile. |
| `q95` | 95th percentile. |
| `complement_distance` | Wasserstein distance between this segment's distribution and all other segments combined. Higher means more distinctive from the rest. **Segment Overview only** — requires a segment column to define what "the rest" means. |

## Filter Paths condition format

In [Filter Paths](/docs/data-processors/filter-paths), metrics are used inside a condition tree rather than a flat list. Each leaf node specifies a metric, a comparison operator, and a value:

```python
# Single condition
{"op": ">", "metric": "length", "value": 5}

# Combine conditions
{
    "op": "and",
    "args": [
        {"op": ">", "metric": "length", "value": 3},
        {"op": "=", "metric": "has_event", "metric_args": {"event": "purchase"}, "value": True},
    ]
}

# A top-level list is shorthand for AND — equivalent to the tree above
[
    {"op": ">", "metric": "length", "value": 3},
    {"op": "=", "metric": "has_event", "metric_args": {"event": "purchase"}, "value": True},
]

# Keep paths that contain every one of several events (AND)
{"op": "=", "metric": "has_all_events", "metric_args": {"events": ["add_to_cart", "purchase"]}, "value": True}

# Keep paths that contain at least one of several events (OR)
{"op": "=", "metric": "has_any_event", "metric_args": {"events": ["promo_view", "discount_applied"]}, "value": True}
```

Supported operators: `=` (or `==`), `!=`, `>`, `<`, `>=`, `<=`. Logical nodes use `and`, `or`, `not` with an `args` list.

`event_count_bulk`/`has_event_bulk` cannot appear in a condition leaf — they expand into
multiple columns, and a condition needs exactly one comparable value per path. Use the
non-bulk `event_count`/`has_event` for a single event, or `has_all_events`/`has_any_event`
for a multi-event AND/OR condition.
