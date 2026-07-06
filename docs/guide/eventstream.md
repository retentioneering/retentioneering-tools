# Eventstream

`Eventstream` is the central object in retentioneering. It wraps your event data and exposes all widgets and data processors as methods.

> Throughout this documentation, `stream` refers to an `Eventstream` instance. All code examples assume you have created one as shown below.

## Key concepts

**Path** is the unit of analysis — an ordered sequence of events that all analysis tools (transition graph, step matrix, funnel, path metrics) operate on. What counts as a path is defined by the `path_col` you choose, not fixed by the library:

- `path_col="user_id"` — a path is the whole **user journey**. If you come from Amplitude or Mixpanel, this is the closest match to how those tools group events by user.
- `path_col="session_id"` — a path is a single **session**.
- Any other identifier works too: paths can be individual checkout attempts, support tickets, or arbitrary trajectory fragments, as long as each fragment has its own ID column.

Every widget and most data processors accept a `path_col` override, so the same eventstream can be analysed at user grain and at session grain without rebuilding it.

**Segment** in retentioneering is defined by a *segment column*: the column names a segmentation (a dimension), and each of its values is one segment — the group of paths/events carrying that value. Note the difference from Amplitude-style tools, where "creating a segment" means defining a single group of users; here `add_segment` adds a whole segmentation column at once.

Segments can be:

- **Static** — one value per path for its entire lifetime: `country`, `acquisition_channel`, an A/B test arm.
- **Dynamic** — the value changes along the path, because it is assigned per event row: `is_weekend`, or a `user_state` that evolves from `new` through `advanced` to `mature` as the user gains experience.

Segment columns are declared in the [schema](#schema) (`segment_cols`) or created with `add_segment` / `add_clusters`, and drive segment-aware tools: diff mode, Segment Overview, and the `in_segment` path metric.

## Creating an Eventstream

By default, Eventstream expects columns named `user_id`, `event`, and `timestamp`. If your data uses different column names, pass a [schema](#schema).

```python
import pandas as pd
from retentioneering import Eventstream

df = pd.read_csv("events.csv")
stream = Eventstream(df)
```

You can also pass a CSV path directly:

```python
stream = Eventstream("events.csv")
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
stream = Eventstream(df, schema={
    "path_cols": ["user_id"],
    "event_cols": ["event"],
    "timestamp_col": "timestamp",
    "segment_cols": ["country", "plan"],
})
```

| Field | Default | Description |
|---|---|---|
| `path_cols` | `["user_id"]` | Columns that identify a path. The first column is the primary path ID. |
| `event_cols` | `["event"]` | Columns that contain event names. The first column is the primary event column. |
| `timestamp_col` | `"timestamp"` | The timestamp column. |
| `segment_cols` | `[]` | Columns treated as segmentations, available in widgets and metrics. See [Key concepts](#key-concepts). |
| `custom_cols` | `[]` | Extra columns to carry through without special treatment. |

## Sample dataset

retentioneering ships with a synthetic e-commerce dataset you can use to try the library without your own data. It contains six months of user sessions on a consumer electronics store, with several embedded behavioral patterns designed to showcase the analysis tools.

```python
from retentioneering.datasets.ecom import load_ecom

stream = load_ecom()
```
which is equivalent to:

```python
from retentioneering.datasets.ecom import load_ecom
from retentioneering import Eventstream

df = load_ecom(as_dataframe=True)
stream = Eventstream(df, schema={
    "path_cols": ["user_id", "session_id"],
    "segment_cols": [
        "platform",
        "acquisition_channel",
        "user_cohort",
        "user_lifecycle",
    ],
})
```
