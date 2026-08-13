# Eventstream

`Eventstream` is the central object in retentioneering. It wraps your event data and exposes all widgets and data processors as methods.

> Throughout this documentation, `stream` refers to an `Eventstream` instance. All code examples assume you have created one as shown below.

## Key concepts

**Path** is the unit of analysis — an ordered sequence of events that all analysis tools (transition graph, step matrix, funnel, path metrics) operate on. What counts as a path is defined by the `path_col` you choose, not fixed by the library:

- `path_col="user_id"` — a path is the whole **user journey**. If you come from Amplitude or Mixpanel, this is the closest match to how those tools group events by user.
- `path_col="session_id"` — a path is a single **session**.

Every widget and most data processors accept a `path_col` override, so the same eventstream can be analysed at user grain and at session grain without rebuilding it — **but `path_col` must be one of the columns declared in `path_cols`** (see [schema](#schema) below); passing any other column raises an error.

`path_cols` must be listed **coarsest grain first**: every value of `path_cols[i+1]` must belong to exactly one value of `path_cols[i]` (e.g. every `session_id` belongs to exactly one `user_id`, so `path_cols=["user_id", "session_id"]` is correct). This nesting is validated against your data when the `Eventstream` is created — a schema declared the wrong way round (or a `session_id` that isn't actually unique per user) raises `SchemaConfigError` immediately, rather than producing silently-wrong analysis. See [ADR-0004](https://github.com/retentioneering/retentioneering-tools/blob/master/docs/adr/0004-schema-and-grain-neutral-paths.md) for why.

Need to group by something that *isn't* a nested grain of your path — a device type, a campaign, an arbitrary cohort? That's not a `path_col`; use a segment column instead (see below).

**Step** is a position within a path — its 1st event, its 2nd, and so on. Steps are what [Step Matrix and Step Sankey](/docs/path-analysis) count over, and they are always relative to an anchor: the start of the path by default, or any event you choose.

**Segment** is a way to split paths into meaningful groups. A split is defined by a *segment column* that maps each event to a group: for example, a `country` column assigns one of the segment levels `US`, `DE`, `FR` to every event of a path. Segments can be static (acquisition channel, user age group, etc.) or dynamic — changing along the path, like weekend/weekday or an evolving user state (new/returning/loyal). Segment columns are declared in the [schema](#schema) (`segment_cols`) or created with the [Add Segment](/docs/data-processors/add-segment) data processor, and drive all segment-aware tools. See the [Segments](/docs/segments) page for the full story.

### Coming from Amplitude, Mixpanel or GA4

Most of the vocabulary translates, with one structural difference worth knowing up front: those tools are built around *users*, while retentioneering is built around *paths* — and a path is whatever grain you declare, so the same eventstream can be analysed per user and per session without reloading it.

| There | Here | Note |
|---|---|---|
| User / user timeline | **Path** with `path_col="user_id"` | The default. `path_col="session_id"` switches the whole analysis to session grain. |
| User property, cohort, A/B arm | **Segment column** | One segment column describes a whole *split* (`US`/`DE`/`FR`), not a single group. Values live per event, so a segment can also change along a path. |
| Event property | **Custom column** | Any extra column rides along; promote it to a segment with [`add_segment`](/docs/data-processors/add-segment) when you want to compare by it. |
| Pathfinder / Journeys / Path exploration | [Transition Graph](/docs/widgets/transition-graph) | Plus [Step Sankey](/docs/widgets/step-sankey) for the step-by-step view. |
| Funnel | [Funnel](/docs/widgets/funnel) | Ordered and unique-path-counted; see its page for the exact conventions. |
| Segment comparison | [Diff mode](/docs/widgets#diff-mode) | Every core widget renders group1 − group2 directly. |
| Session (defined server-side) | [`split_sessions`](/docs/data-processors/split-sessions) | You choose the timeout or the boundary events, and can change your mind later. |

There is no dashboard state to configure and no sampling: everything is a method call on an object you hold, computed over the full log on your machine.

## Creating an Eventstream

By default, Eventstream expects columns named `user_id`, `event`, and `timestamp`. If your data uses different column names, pass a [schema](#schema).

```python
import pandas as pd
import retentioneering as rete

df = pd.read_csv("events.csv")
stream = rete.Eventstream(df)
```

You can also pass a CSV path directly:

```python
stream = rete.Eventstream("events.csv")
```

## Expected data format

Each row in your DataFrame represents a single event. At minimum, you need a path identifier column, an event name column, and a timestamp column.

| user_id | event | timestamp |
|---|---|---|
| user_1 | page_view | 2024-01-01 10:00:00 |
| user_1 | add_to_cart | 2024-01-01 10:02:00 |
| user_1 | purchase | 2024-01-01 10:05:00 |

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame \| str` | required | Event data as a pandas DataFrame or a path to a CSV file. |
| `schema` | `dict \| None` | `None` | Schema configuration. See below. |
| `preprocess` | `bool` | `True` | When `True`, parses timestamps, casts categoricals, and sorts rows. Set to `False` if your DataFrame is already preprocessed. |

## Schema

The schema tells retentioneering which columns in your DataFrame correspond to paths, events, timestamps, and segments. Pass it as a dict to the `schema` parameter.

```python
stream = rete.Eventstream(df, schema={
    "path_cols": ["user_id"],
    "event_cols": ["event"],
    "timestamp_col": "timestamp",
    "segment_cols": ["country", "plan"],
})
```

| Field | Default | Description |
|---|---|---|
| `path_cols` | `["user_id"]` | Columns that identify a path, ordered **coarsest grain first** (e.g. `["user_id", "session_id"]`). The first column is the primary/default path ID and every later column must nest inside all earlier ones — validated against your data at construction time. `path_col` overrides passed to tools must be one of these columns. |
| `event_cols` | `["event"]` | Columns that contain event names. The first column is the primary event column. |
| `timestamp_col` | `"timestamp"` | The timestamp column. |
| `segment_cols` | `[]` | Columns treated as segmentations, available in widgets and metrics. See [Key concepts](#key-concepts). |
| `custom_cols` | `None` | Extra columns you may need for working with the eventstream. Left as `None`, every column not covered by the rest of the schema is included automatically. Set to a list — even `[]` — and only those columns (plus the ones already covered by the schema) are kept; anything else is dropped. |

## Sample dataset

retentioneering ships with a synthetic e-commerce dataset you can use to try the library without your own data. It contains six months of user sessions on a consumer electronics store, with several embedded behavioral patterns designed to showcase the analysis tools.

```python
import retentioneering as rete

stream = rete.datasets.load_ecom()
```
which is equivalent to:

```python
import retentioneering as rete

df = rete.datasets.load_ecom(as_dataframe=True)
stream = rete.Eventstream(df, schema={
    "path_cols": ["user_id", "session_id"],
    "segment_cols": [
        "platform",
        "acquisition_channel",
        "user_cohort",
        "user_lifecycle",
    ],
})
```

## Inspecting your data

`stream.describe()` is a quick sanity check on what got loaded: dataset shape, schema, date range, event frequency, and path length/duration statistics.

```python
stream.describe()
```

Parameters:

- `percentiles` — percentiles (0-1) reported in `path_stats`. Default `(0.25, 0.5, 0.75, 0.9, 0.99)`.
- `top_events` — number of most frequent events to include in `event_frequency`. Default `20`; pass `None` to include every unique event, unranked and unlimited (e.g. when building a full event rename mapping, where the default cap would silently drop long-tail events).

Returns a dict:

| Key | Contents |
|---|---|
| `schema` | `event_col`, `path_col`, `path_cols`, `segment_cols`, `timestamp_col` |
| `shape` | `n_events`, `n_paths`, `n_unique_events` |
| `date_range` | `min`, `max`, `span` |
| `event_frequency` | `DataFrame` of `event`/`count`/`share`, sorted descending, limited to `top_events` rows (default 20; pass `top_events=None` for the full, unranked list). `.attrs["truncated"]` and `.attrs["n_total_events"]` say whether/how much this was cut down |
| `path_stats` | dict keyed by each entry of `path_cols`, each a `DataFrame` (`DataFrame.describe()` shape: count/mean/std/min/percentiles/max) with `length`/`duration` columns |
| `segments` | `DataFrame` of `segment_col`/`value`/`count`/`share`, one row per segment level across all segment columns |

## Other Eventstream methods

Beyond the [data processors](/docs/data-processors) and [widgets](/docs/widgets), the object exposes a handful of methods that don't fit either category.

### Getting your data back out

```python
df = stream.to_dataframe()                      # plain pandas copy
df = stream.to_dataframe(exclude_start_end=False)  # keep path_start / path_end rows
```

Processors never mutate an eventstream, so a `to_dataframe()` result is a snapshot you can hand to any other library. `stream.get_event_counts()` returns a `{event: count}` dict, and `stream.get_segment_levels()` returns `{segment_col: [levels]}` — handy before writing a `diff=` or a rename mapping.

### Per-path metrics as a feature table

`get_metrics()` runs the same [path metrics](/docs/path-metrics) registry that powers Segment Overview and clustering, and returns one row per path — a ready-to-join feature table for churn or LTV models.

```python
features = stream.get_metrics([
    {"metric": "length"},
    {"metric": "duration"},
    {"metric": "active_days"},
    {"metric": "event_count_bulk"},
    {"metric": "matches_pattern", "metric_args": {"pattern": "home->.*->purchase"}},
])
```

Metric configs here take no `agg` field — these are raw per-path values, not aggregates. `event_count_bulk` / `has_event_bulk` expand into one column per event.

`get_metric_distribution()` is the related one-off: it returns the distribution of a single metric for one segment level against another (or against everything else, with `complement=True`), which is what the Segment Overview widget draws when you click a cell.

### Conversion from one event to another

`get_conversion_rate()` answers "given that Y happened, how often does X follow — and is that different from the baseline?".

```python
stream.get_conversion_rate("add_to_cart", "purchase")
```

| start_anchor | end_anchor | paths_with_start | converted | conversion_rate | base_rate | lift |
|---|---|---|---|---|---|---|
| add_to_cart | purchase | 522 | 297 | 0.569 | 0.547 | 1.041 |

Five columns instead of one number, because the number alone is not a finding:

- **`paths_with_start`** is the denominator. 0.5 out of two paths and 0.5 out of five thousand are different claims, and nothing in a rate distinguishes them.
- **`base_rate`** is the share of *all* paths where the target happens at all, and **`lift`** is the rate divided by it. In the run above, 57% of users who add to cart go on to purchase — but 55% of *all* users purchase anyway, so adding to cart tells you almost nothing at user grain (`lift` 1.04). A lift below 1 means the start event makes the outcome **less** likely, which reads far more directly than a paragraph about confounders.

`lift` is the only column you cannot derive from the others, since it needs a frequency measured over the whole eventstream rather than over the paths that reached the start.

**A window.** `within` measures from the start anchor and includes its far edge — an int counts events, a duration string counts time:

```python
stream.get_conversion_rate("product_view", "add_to_cart", within=10)   # within 10 events
stream.get_conversion_rate("support_chat", "purchase", within="30m")   # within half an hour
```

**Several targets.** A list on either side fans out into separate questions, one row per combination — a start against a set of independent outcomes, or several starts against the same one. (This is *not* `truncate_paths`' list, where several anchors describe one window bound.)

```python
stream.get_conversion_rate("add_to_cart", ["purchase", "cart", "support_chat"], within=5)
```

**Anchors, not just event names.** Both sides take the same [anchor specs](/docs/data-processors/truncate-paths) as `truncate_paths` — a `pattern`, which of its events to anchor on (`at`), which occurrence (`occurrence`), an `offset` — and `path_start` / `path_end` are ordinary names. That covers questions a pair of event names cannot ask:

```python
# exit rate: the path ended right after the error
stream.get_conversion_rate("payment_error", "path_end", within=1)

# the sessions that *landed* on the catalog, not every session that visited it
stream.get_conversion_rate(
    {"pattern": "path_start->catalog", "at": -1}, "purchase", path_col="session_id"
)
```

**The unit is the path, not the occurrence.** A path where the start event happened three times counts once in the denominator, so "of 23,000 visits to this page, how many were entrances" is a different question this method does not answer — the per-visit figure can differ from the per-path one by several points. `path_col="session_id"` is usually the right lever: at session grain "added to cart" and "purchased" describe the same visit rather than the same person, and the lift stops being washed out by everything a user ever did.

### Reproducing an eventstream

Every processor call is recorded, so a derived eventstream can describe how it was built:

```python
prepared = stream.filter_events(drop={"event": ["checkout_bug"]}).truncate_paths(
    start_anchor="path_start", end_anchor="purchase"
)
prepared.recipe()
# [{"type": "filter_events", "drop": {...}},
#  {"type": "truncate_paths", "start_anchor": "path_start", "end_anchor": "purchase"}]
```

`Eventstream.from_recipe(df, recipe)` replays that list onto a base DataFrame, rebuilding an identical eventstream — useful for moving a prepared pipeline between notebooks, storing it next to a report, or handing it to the [MCP server](/docs/mcp-server), whose preprocessor steps use exactly this format. `stream.fingerprint` (a property) is a content hash for checking two eventstreams really are the same, and `stream.equals(other)` compares them directly.
