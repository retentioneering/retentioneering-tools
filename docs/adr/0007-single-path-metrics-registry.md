# ADR-0007: Single Path Metrics registry

Status: Accepted (5.0 rewrite; recorded 2026-07)

## Context

Per-path scalar features are needed in several places: clustering features,
segment comparison, filtering whole paths, and ad-hoc feature engineering for
ML. 3.x had these scattered per-tool.

## Decision

- One registry (`metrics/metric_builder.py`) defines all path metrics:
  `length`, `duration`, `event_count`, `has_event`, `time_between`,
  `first_event_time`, `active_days`, `matches_pattern`, `in_segment`.
  A metric config is `{"metric": <name>, "metric_args": {...}}` everywhere.
- All consumers use the same registry and config format:
  `get_metrics()` (raw per-path values), `add_clusters` / `cluster_analysis`
  (`features` — clustering role), `segment_overview` (`metrics` +
  `agg` — aggregation role), `cluster_analysis` (`overview_metrics`),
  `filter_paths` (metric + comparison operator in a condition tree).
- Argument names describe the *role* the metrics play (`features`,
  `overview_metrics`, `metrics`); the underlying entity is always a Path
  Metrics config, and docs state this equivalence explicitly.
- Derived column names follow `<metric>_<args>` (`has_event_purchase`,
  `in_segment_<segment>_<value>_<mode>`, `time_from_<a>_to_<b>`), because
  widgets and report anchor links reference metric columns by these names.

## Consequences

- A new metric added to the registry becomes available to clustering,
  overview, and filtering at once — and must be added to the JS metric
  editor (`js/widget/src/metric_config_row.tsx`) and the Path Metrics doc
  page in the same change.
- Metric names are a public string-enum API: renames are breaking changes
  and ripple into JS and MCP docstrings (see ADR-0008 for the naming rules).
