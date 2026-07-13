# Eventstream

`Eventstream` is the central object in retentioneering. It wraps your event data and exposes all widgets and data processors as methods.

> Throughout this documentation, `stream` refers to an `Eventstream` instance. All code examples assume you have created one as shown below.

## Key concepts

**Path** is the unit of analysis — an ordered sequence of events that all analysis tools (transition graph, step matrix, funnel, path metrics) operate on. What counts as a path is defined by the `path_col` you choose, not fixed by the library:

- `path_col="user_id"` — a path is the whole **user journey**. If you come from Amplitude or Mixpanel, this is the closest match to how those tools group events by user.
- `path_col="session_id"` — a path is a single **session**.

Every widget and most data processors accept a `path_col` override, so the same eventstream can be analysed at user grain and at session grain without rebuilding it — **but `path_col` must be one of the columns declared in `path_cols`** (see [schema](#schema) below); passing any other column raises an error.

`path_cols` must be listed **coarsest grain first**: every value of `path_cols[i+1]` must belong to exactly one value of `path_cols[i]` (e.g. every `session_id` belongs to exactly one `user_id`, so `path_cols=["user_id", "session_id"]` is correct). This nesting is validated against your data when the `Eventstream` is created — a schema declared the wrong way round (or a `session_id` that isn't actually unique per user) raises `SchemaConfigError` immediately, rather than producing silently-wrong analysis. See [ADR-0004](https://github.com/retentioneering/retentioneering-tools/blob/main/docs/adr/0004-schema-and-grain-neutral-paths.md) for why.

Need to group by something that *isn't* a nested grain of your path — a device type, a campaign, an arbitrary cohort? That's not a `path_col`; use a segment column instead (see below).

**Segment** is a way to split paths into meaningful groups. A split is defined by a *segment column* that maps each event to a group: for example, a `country` column assigns one of the segment levels `US`, `DE`, `FR` to every event of a path. Segments can be static (acquisition channel, user age group, etc.) or dynamic — changing along the path, like weekend/weekday or an evolving user state (new/returning/loyal). Segment columns are declared in the [schema](#schema) (`segment_cols`) or created with the [Add Segment](/docs/data-processors/add-segment) data processor, and drive all segment-aware tools. See the [Segments](/docs/segments) page for the full story.

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

Returns a dict:

| Key | Contents |
|---|---|
| `schema` | `event_col`, `path_col`, `path_cols`, `segment_cols`, `timestamp_col` |
| `shape` | `n_events`, `n_paths`, `n_unique_events` |
| `date_range` | `min`, `max`, `span` |
| `event_frequency` | `DataFrame` of `event`/`count`/`share`, sorted descending |
| `path_stats` | dict keyed by each entry of `path_cols`, each a `DataFrame` (`DataFrame.describe()` shape: count/mean/std/min/percentiles/max) with `length`/`duration` columns |
| `segments` | `DataFrame` of `segment_col`/`value`/`count`/`share`, one row per segment value across all segment columns |
