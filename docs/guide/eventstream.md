# Eventstream

`Eventstream` is the central object in retentioneering. It wraps your event data and exposes all widgets and data processors as methods.

> Throughout this documentation, `stream` refers to an `Eventstream` instance. All code examples assume you have created one as shown below.

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
| `prepare` | `bool` | `True` | When `True`, parses timestamps, casts categoricals, and sorts rows. Set to `False` if your DataFrame is already prepared. |

## Schema

The schema tells retentioneering which columns in your DataFrame correspond to paths, events, timestamps, and segments. Pass it as a dict to the `schema` parameter.

```python
stream = Eventstream(df, schema={
    "path_cols": ["user_id"],
    "event_cols": ["event"],
    "timestamp": "timestamp",
    "segment_cols": ["country", "plan"],
})
```

| Field | Default | Description |
|---|---|---|
| `path_cols` | `["user_id"]` | Columns that identify a path. The first column is the primary path ID. |
| `event_cols` | `["event"]` | Columns that contain event names. The first column is the primary event column. |
| `timestamp` | `"timestamp"` | The timestamp column. |
| `segment_cols` | `[]` | Columns treated as segment dimensions, available in widgets and metrics. |
| `custom_cols` | `[]` | Extra columns to carry through without special treatment. |

## Sample dataset

retentioneering ships with a synthetic e-commerce dataset you can use to try the library without your own data. It contains six months of user sessions on a consumer electronics store, with several embedded behavioral patterns designed to showcase the analysis tools.

```python
from retentioneering.datasets.ecom import load_ecom
from retentioneering import Eventstream

df = load_ecom()
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
