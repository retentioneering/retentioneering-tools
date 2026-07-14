# Retentioneering 5.0 — verified API map

Every signature below was executed against retentioneering 5.0 (branch `v5-migration`,
July 2026). If your installed version differs, verify before use:

```python
import retentioneering; print(retentioneering.__version__)
```

Authoritative per-method reference: docstrings in the installed package and
https://retentioneering.com/docs. This map exists so you do not have to guess names,
argument conventions, or return shapes.

## 1. Eventstream — the hub object

```python
from retentioneering import Eventstream

stream = Eventstream(df, schema={
    "path_cols":     ["user_id"],            # path identity; nesting allowed: ["user_id","session_id"]
    "event_cols":    ["event"],
    "timestamp_col": "timestamp",            # REQUIRED; parseable datetimes (tz-aware OK)
    "segment_cols":  ["device", "country"],  # categorical labels usable in every diff/overview
    "custom_cols":   ["price"],              # carried along; visible to sql= modes
})
```

- Defaults match columns named `user_id` / `event` / `timestamp` — `Eventstream(df)` just works then.
- Unknown schema keys raise `SchemaConfigError` listing valid keys (trust this error).
- Rows with null path values are rejected loudly. Numeric path ids stay numeric
  (they are NOT cast to str) — join freely with your source frame.
- `stream.df` is a READ-ONLY property (assignment raises). For a materialized copy use
  `stream.to_dataframe(exclude_start_end=True)`.
- Event names may not contain `->` (raises `SchemaConfigError` — it is the path-pattern delimiter).
- Every processor returns a NEW Eventstream (immutable chaining).

First calls on any new dataset:

```python
d = stream.describe(top_events=20)      # dict: schema, shape, date_range, event_frequency,
                                        #       path_stats, segments
d["event_frequency"].attrs              # {'truncated': True, 'n_total_events': N} when cut;
                                        # pass top_events=None to disable truncation
stream.get_event_counts()               # {event: count}
stream.get_segment_levels()             # {segment_col: [values...]}
```

## 2. Data processors (verb-first, chainable)

| Processor | Use for | Notes |
|---|---|---|
| `filter_events(keep=/drop=/func=/sql=)` | row-level filtering | exactly one mode; `sql=` is full DuckDB over alias `eventstream` (CTEs OK) |
| `filter_paths(condition)` | keep whole paths by metric condition tree | leaves `{"op","metric","value","metric_args"}`; branches `and/or/not`; top-level list = AND; raises `EmptyEventstreamError` when nothing survives |
| `add_segment(name, rules=/func=/sql=/funnel_events=)` | add a segment column | `funnel_events` labels each path by the furthest step reached IN ORDER (closed funnel); non-reachers get `"out_of_funnel"` |
| `add_clusters(name, features, n_clusters, ...)` | materialize clusters as a segment column | labels are strings `"cluster_0"...`; deterministic (seeded) |
| `add_start_end_events()` | explicit `path_start`/`path_end` rows | idempotent; widgets add them implicitly |
| `rename_events(mapping)` | collapse taxonomies (e.g. `"PLP: *"` families by explicit dict) | unknown keys raise |
| `collapse_events(consecutive=True | [...])` | dedupe consecutive repeats | count loops BEFORE collapsing if loops are your subject |
| `split_sessions(timeout="30m" | separator= | start_event=+end_event=)` | derive sessions | duration strings need units; column params use `session_col` naming |
| `truncate_paths(start_event, end_event)` | window each path between two events | `path_start`/`path_end` sentinels supported; paths missing either anchor are DROPPED — see gotchas for the keep-whole pattern |
| `drop_events / drop_segment / edit_events / rename_segment_levels / sample_paths / to_daily_states / urls_to_events / add_events` | as named | `sample_paths(frac=, random_state=)` for stable subsamples |

## 3. Metrics registry — one config format everywhere

Used by `get_metrics(list)`, `filter_paths(condition)`, `cluster_analysis*(features=/overview_metrics=)`,
`segment_overview(metrics=)`.

```python
stream.get_metrics([
    {"metric": "length"},
    {"metric": "duration"},                                            # seconds
    {"metric": "event_count",   "metric_args": {"event": "basket"}},   # SINGLE event → key 'event'
    {"metric": "has_event",     "metric_args": {"event": "purchase"}},
    {"metric": "has_any_event", "metric_args": {"events": ["a","b"]}}, # LIST → key 'events' (OR)
    {"metric": "has_all_events","metric_args": {"events": ["a","b"]}}, # LIST → key 'events' (AND)
    {"metric": "time_between",  "metric_args": {"start_event": "basket",
                                                "end_event": "purchase"}},
    {"metric": "matches_pattern","metric_args": {"pattern": "basket->.*->purchase"}},
])   # -> DataFrame indexed by path id
```

Argument-key convention (this exact split is version-verified):
**single event → `event` (string); multiple events → `events` (list) and only on
`has_any_event` / `has_all_events`.** Wrong keys raise `InvalidMetricConfigError`
naming the requirement.

Other metrics: `active_days`, `first_event_time`, `in_segment` (modes `any/all/event_share`).
`matches_pattern` is token-wise (no substring false-positives) and order-deterministic.
`time_between` measures first occurrence of A to first occurrence of B **globally in the
path** (not "first B after A") — verify fit for your question.

## 4. Tools: widgets + headless twins

Every widget has a headless `<name>_data(...)` twin returning plain data with the same
data parameters. Common widget params: `diff=`, `path_col=`, `height=`, `sidebar_open=`,
`state_file=`.

| Widget | Headless returns | Key params |
|---|---|---|
| `transition_graph` | events×events DataFrame | `edge_weight` ∈ `proba_out, proba_in, count, unique_paths, share_of_total, avg_per_path, time_median, time_q95` |
| `step_matrix` / `step_sankey` | no `path_pattern`: DataFrame; with `path_pattern`: tuple of per-anchor blocks; with pattern+diff: `(blocks, g1_blocks, g2_blocks)` | `max_steps`, `path_pattern=".*->X->.*"` — the anchor event sits at column **0** |
| `funnel` | dict; each step has `unique_paths`, `conversion_rate` (share of ALL paths) **and `step_conversion_rate`** (step-to-step) | `steps=[...]` — closed/ordered semantics: a path counts at step N only after passing all previous |
| `segment_overview` | DataFrame metrics×segment values | `metrics=[...]` with `agg` |
| `cluster_analysis` | dict: `overview_df`, `silhouette` (`{"params": [...], "silhouette": [...]}`), `cluster_labels`, `best_params` | `n_clusters` accepts int, list, or range string `"3-8"`; features = metric configs |

**diff semantics:** `diff=(segment_col, v1, v2)` (also `(path_ids1, path_ids2)`; `v2` may be
`"<REST>"`). Returned diff block = **value1 − value2**. Segment values must match exactly
(check `get_segment_levels()`).

## 5. Deliverables: standalone interactive HTML

```python
w = stream.transition_graph(diff=("device", "mobile", "desktop"))
w.export_html("artifacts/graph.html",
              title="Mobile vs desktop",
              analysis="Markdown text; [event_name] references become clickable node links.")
```

- `export_html` is a method of the **widget**, not the stream.
- Files are fully self-contained (~1.2 MB, no CDN) — safe to attach or email.
- A failed recompute raises `WidgetExportError` (no silent empty exports).
- Widgets construct headlessly in plain scripts — Jupyter is not required for export.

## 6. Reproducibility: lineage recipes

Processor chains are recorded and replayable:

```python
processed = stream.filter_events(drop={"event": ["noise"]}).rename_events({"a": "A"})
rec = processed.recipe()                     # JSON-serializable list of ops
replayed = Eventstream.from_recipe(raw_df, rec)   # re-applies the same chain
```

Persist `rec` in run metadata so any artifact can be regenerated from raw data.

## 7. MCP server (agent-driven analysis)

```python
import retentioneering.mcp as mcp
mcp.serve()                    # data-agnostic: agent loads an eventstream on demand
mcp.serve(stream, port=8765)   # or pre-loaded; raises OSError if port is taken
```

## 8. Environment notes

- Requires Python ≥ 3.11. Backend is an embedded engine (DuckDB family) — millions of rows
  run in seconds on a laptop; no network needed.
- Interactive widgets from a *source checkout* need the JS bundle built (`make build`,
  Node required). pip-installed wheels ship the bundle.
- Telemetry: the library reports anonymous usage (method names, never data);
  see the docs "Tracking" page for scope and opt-out.
