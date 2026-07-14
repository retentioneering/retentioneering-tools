# ADR-0007: Single Path Metrics registry

Status: Superseded (see `metrics/metric_builder.py`) (recorded 2026-07)

> **Superseded.** The dataclass-based `MetricSchema`/`METRIC_SCHEMAS` registry
> described below (`metrics/metric_schema.py`) predated a parallel v5-migration
> rewrite of the metric set itself (`has_event`/`event_count` split into strict
> single-event forms plus `has_event_bulk`/`event_count_bulk` and
> `has_all_events`/`has_any_event` — see `metric_builder.py`'s module
> docstring) that never knew the registry existed. Reconciling the two on
> merge kept the corrected metric set and moved its shape-declarations back
> into `metric_builder.py` directly: `_parse_dict_config` and
> `validate_metric_config` now each dispatch per metric, but share one
> `_normalize_*` helper function per metric (`_normalize_single_event`,
> `_normalize_events_bulk`, `_normalize_events_required`,
> `_normalize_time_between`, `_normalize_pattern`, `_normalize_in_segment`) so
> a metric's `metric_args` shape still can't drift between parsing and
> validation — the **goal** below is unchanged, only the container is: a
> plain function instead of a `MetricSchema` dataclass with `parse`/
> `validate`/`metric_names` callable fields. `metrics/metric_schema.py` and
> `tests/metrics/metric_schema_test.py` have been deleted; the JS codegen
> (`scripts/export_metric_schema.py`, `make export-metric-schema`) now reads
> `metric_builder.VALID_METRICS` instead of `METRIC_SCHEMAS`, still checked by
> `tests/metrics/js_export_test.py`.

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
  overview, and filtering at once — and must be added to the Path Metrics
  doc page in the same change.
- Metric names are a public string-enum API: renames are breaking changes
  and ripple into JS and MCP docstrings (see ADR-0008 for the naming rules).
- Each metric's shape (which `metric_args` keys it takes, required/optional,
  valid modes/ranges) is declared once, in `metrics/metric_schema.py`'s
  `METRIC_SCHEMAS` registry — `MetricConfig._parse_dict_config` and
  `MetricBuilder.validate_metric_config` are both thin dispatchers into it,
  rather than two independently hand-maintained if/elif chains.
- The JS metric editor's dropdown (`js/widget/src/metric_config_row.tsx`)
  imports its metric name list from `js/widget/src/generated/
  metric_names.generated.ts`, generated from `METRIC_SCHEMAS` by
  `scripts/export_metric_schema.py` (`make export-metric-schema`;
  `tests/metrics/js_export_test.py` fails CI if it's stale) — adding a
  metric no longer requires a manual JS edit to make it selectable. Each
  metric's argument-editing UI (which fields to show, e.g. `in_segment`'s
  mode/threshold vs `matches_pattern`'s plain text input) and its tooltip
  text are still hand-written per metric, since they're bespoke UI, not
  data mechanically derivable from the schema; likewise the Path Metrics
  doc page above is still hand-synced.
