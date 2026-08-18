# Migrating from 3.x

retentioneering 5.0 is a ground-up rewrite, not an incremental release. If you are following a tutorial, a blog post, or a notebook written for 3.3.0, most of the code in it will not run as-is. This page maps the old API onto the new one so you can tell, method by method, whether something was renamed, replaced, or dropped.

If you are new to the library, you can skip this page entirely — start with the [Quick Start](/docs/quick-start).

## What changed, in one paragraph

The pandas-based engine was replaced with a DuckDB-backed one, so the same analyses run at production-log scale on a laptop. The old iframe + CDN-loaded widgets were replaced with [anywidget](https://anywidget.dev)-based ones whose JavaScript is open source, lives in this repository, and ships inside the wheel — nothing is downloaded at runtime, and widgets now work in VS Code and Cursor as well as Jupyter. The `data_processor` / `preprocessor` / `params_model` machinery is gone: data processors are now plain methods with plain keyword arguments. And two things are new with no 3.x counterpart — an [MCP server](/docs/mcp-server) that lets an LLM agent drive the analysis, and the [Segment Overview](/docs/widgets/segment-overview) widget.

`CHANGELOG.md` in the repository holds the exhaustive, itemized delta under `[5.0.0]`.

## Should you upgrade?

Upgrade if you want the new engine, the new widgets, agent support, or ongoing fixes. **Stay on 3.x** if your work depends on the Preprocessing Graph, `Cohorts`, `StatTests`, or `Sequences` — none of them exist in 5.x yet (see [What is gone](#what-is-gone) below).

The 3.x engine is preserved on the [`3.x` branch](https://github.com/retentioneering/retentioneering-tools/tree/3.x) for reference and patches:

```bash
pip install "retentioneering<4"     # the 3.x line
pip install -U retentioneering      # 5.x
```

The two versions install under the same package name, so they cannot coexist in one environment — use separate virtualenvs if you need both.

## Concepts that shifted

Four changes explain most of the surprises when porting old code.

**Paths, not users.** 3.x was built around users. 5.x is built around *paths*, and a path is whatever grain you declare: `path_col="user_id"` reproduces the old behavior, `path_col="session_id"` switches the same eventstream to session grain without rebuilding it. See [Key concepts](/docs/eventstream#key-concepts).

**Everything is immutable.** Every data processor returns a **new** `Eventstream` and never modifies the one it was called on. Code written against 3.x's in-place semantics needs to capture the result:

```python
stream = stream.filter_events(drop={"event": ["checkout_bug"]})   # not just stream.filter_events(...)
```

This is also why `copy()` is gone — there is nothing to protect against.

**Segments replace ad-hoc grouping.** A *segment column* describes a whole split at once (`US` / `DE` / `FR`), not one group, and its values live per event — so a segment can change along a path. Segments drive comparison everywhere: every core widget takes `diff=(segment_col, value1, value2)`. See [Segments](/docs/segments).

**Every widget has a headless twin.** `stream.step_matrix()` renders; `stream.step_matrix_data()` returns the DataFrame behind it, with the same data arguments. If you used 3.x tools to get numbers rather than pictures, the `*_data()` methods are what you want. See [Headless mode](/docs/widgets#headless-mode).

## Renamed or re-signatured

Same concept, different call shape.

| 3.3.0 | 5.x | Note |
|---|---|---|
| `to_dataframe(copy=False)` | [`to_dataframe(exclude_start_end=True)`](/docs/eventstream#getting-your-data-back-out) | The result is always a snapshot, so there is no `copy` flag. |
| `filter_events(func)` | [`filter_events(keep=, drop=, func=, sql=)`](/docs/data-processors/filter-events) | `func` is now one of four alternative modes; `keep`/`drop` cover the common cases without a lambda. |
| `rename(rules: list[dict])` | [`rename_events(mapping: dict)`](/docs/data-processors/rename-events) | A plain `{old: new}` dict. |
| `collapse_loops(suffix, time_agg)` | [`collapse_events(consecutive=, events=, group_col=, ...)`](/docs/data-processors/collapse-events) | Loop-squashing is now one mode of a general merging processor. |
| `group_events` / `group_events_bulk` | [`collapse_events(events=..., name=...)`](/docs/data-processors/collapse-events) or [`rename_events`](/docs/data-processors/rename-events) | Renaming several events to one name is `rename_events`; merging runs of them is `collapse_events`. |
| `split_sessions(timeout, delimiter_events, ...)` | [`split_sessions(timeout=, separator=, bounds=, ...)`](/docs/data-processors/split-sessions) | `timeout` now needs an explicit unit — `"30m"` or a `pd.Timedelta`. Bare numbers are rejected. |
| `truncate_paths(drop_before, drop_after, ...)` | [`truncate_paths(start_anchor, end_anchor)`](/docs/data-processors/truncate-paths) | Window anchors are the `start_anchor`/`end_anchor` pair everywhere in 5.x; each takes an event name, an anchor spec, or a list. |
| `drop_paths()` | [`filter_paths(condition)`](/docs/data-processors/filter-paths) | A condition tree over [path metrics](/docs/path-metrics) — `{"op": ">", "metric": "length", "value": 5}` — instead of fixed thresholds. |
| `add_positive_events` / `add_negative_events` | [`add_events(name, source_event=[...])`](/docs/data-processors/add-events) | One processor for synthetic events; the positive/negative distinction was only naming. |
| `label_new_users` / `label_lost_users` / `label_cropped_paths` | [`add_segment`](/docs/data-processors/add-segment), [`add_events(churn=...)`](/docs/data-processors/add-events), [`to_daily_states`](/docs/data-processors/to-daily-states) | Labelling is a segment; a churn marker is a synthetic event; lifecycle states are their own processor. |
| `clusters()` (stateful `fit()`/`extract_features()`) | [`cluster_analysis()`](/docs/widgets/cluster-analysis) + [`add_clusters()`](/docs/data-processors/add-clusters) | Explore interactively, then persist the split as a segment column. |
| `transition_matrix()` | [`transition_graph_data()`](/docs/widgets/transition-graph#headless-mode) | Same matrix, now the headless twin of the graph. |
| `describe()` / `describe_events()` | [`describe()`](/docs/eventstream#inspecting-your-data) | One dict: schema, shape, date range, event frequency, path length/duration stats. |
| `add_custom_col()` / `index_events()` | `schema={"custom_cols": [...]}` | Extra columns ride along automatically; declare them only if you want strict control. |
| `append_eventstream()` | `pd.concat(...)` before constructing | Combine the frames, then build one `Eventstream`. |
| `pipe()` | plain chaining, or [`recipe()`](/docs/eventstream#reproducing-an-eventstream) | Processors chain directly; `recipe()`/`from_recipe()` replay a pipeline elsewhere. |

Widget classes were reimplemented from scratch, but kept their names and are reached the same way — as methods on the eventstream: `transition_graph`, `step_matrix`, `step_sankey`, `funnel`, and `cluster_analysis` (3.x's `Clusters`).

## What is gone

No equivalent in 5.x today. These were cut deliberately to get the rewrite shipped, and may return — requests and pull requests are welcome on the [issue tracker](https://github.com/retentioneering/retentioneering-tools/issues).

| Gone | Closest thing available now |
|---|---|
| **Preprocessing Graph** (visual no-code pipeline builder) | Chained data processors in code; for agents, the [MCP server](/docs/mcp-server)'s preprocessor lists. |
| **Cohorts** | [`add_segment`](/docs/data-processors/add-segment) over a signup-period column plus [Segment Overview](/docs/widgets/segment-overview); [`to_daily_states`](/docs/data-processors/to-daily-states) for lifecycle-state retention. |
| **StatTests** | Nothing built in — pull per-path values with [`get_metrics()`](/docs/eventstream#per-path-metrics-as-a-feature-table) and run your own test in `scipy`. |
| **Sequences** | The `matches_pattern` [path metric](/docs/path-metrics) and `path_pattern` on [Step Matrix](/docs/widgets/step-matrix) / [Step Sankey](/docs/widgets/step-sankey) answer pattern questions, but there is no n-gram frequency table. |
| `timedelta_hist`, `user_lifetime_hist`, `event_timestamp_hist` | [`get_metrics()`](/docs/eventstream#per-path-metrics-as-a-feature-table) returns `duration`, `time_between`, `first_event_time` and friends per path — plot them with your own matplotlib/plotly call. `describe()` gives the percentiles directly. |

## Naming conventions

5.x follows one vocabulary throughout ([ADR-0008](https://github.com/retentioneering/retentioneering-tools/blob/master/docs/adr/0008-naming-conventions.md)), which is worth skimming before you port a pipeline:

- Column arguments are always `path_col`, `event_col`, `timestamp_col`, `session_col`, `segment_col`.
- Window anchors are always the `start_anchor` / `end_anchor` pair; a plain list of
  boundary event names stays `start_event` / `end_event` (`split_sessions`).
- Durations are strings with an explicit unit (`"30m"`) or a `pd.Timedelta`; time *outputs* are seconds.
- `<REST>` is the diff sentinel for "every other value of this segment".
- Data processors are verb-first (`filter_events`), widgets are nouns (`funnel`), headless twins are `<widget>_data`.

## Requirements

5.x requires **Python 3.10 or later** (3.3.0 supported 3.8–3.11). Widgets render in Jupyter, JupyterLab, VS Code, Cursor, and Google Colab — see [Installation](/docs/installation) for the one JupyterLab caveat.

## Next steps

- [Quick Start](/docs/quick-start) — the 5.x pipeline end to end in five minutes.
- [Path Analysis](/docs/path-analysis) — what each visualization actually computes, and when to trust it.
- [Eventstream](/docs/eventstream) — schema, path grain, and the methods that aren't processors or widgets.
