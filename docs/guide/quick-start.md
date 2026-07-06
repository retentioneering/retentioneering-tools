# Quick Start

This guide walks you through a complete example — from installation to your first interactive visualization — in under five minutes.

## 1. Install

```bash
pip install retentioneering
```

## 2. Load your data

Create an [Eventstream](/docs/eventstream) from a pandas DataFrame. By default, Eventstream expects columns named `user_id`, `event`, and `timestamp`. If your data uses different column names, pass a [schema](/docs/eventstream#schema).

```python
import pandas as pd
from retentioneering import Eventstream

df = pd.read_csv("events.csv")
stream = Eventstream(df)
```

No CSV yet? Use the built-in sample dataset to follow along:

```python
from retentioneering.datasets.ecom import load_ecom
from retentioneering import Eventstream

df = load_ecom()
stream = Eventstream(df, schema={
    "path_cols": ["user_id"],
    "segment_cols": ["platform", "acquisition_channel"],
})
```

## 3. Explore with a widget

Open an interactive Transition Graph — no arguments needed. Configure everything in the sidebar.

<DemoWidget cmd={`stream.transition_graph()`} path="/docs-demos/guide/quick-start/transition-graph.html" height={560} />

Build a conversion funnel:

<DemoWidget cmd={`stream.funnel(steps=["catalog", "add_to_cart", "cart", "purchase"])`} path="/docs-demos/guide/quick-start/funnel.html" height={480} />

Compare two user segments side by side:

<DemoWidget cmd={`stream.transition_graph(diff=["platform", "mobile", "desktop"])`} path="/docs-demos/guide/quick-start/transition-graph-diff.html" height={560} />

## 4. Prepare your data

Use [data processors](/docs/data-processors) to clean and shape the eventstream before visualizing:

```python
stream = (
    Eventstream(df)
    .filter_events(drop={"event": ["bot_ping"]})
    .rename_events({"btn_click": "button_click"})
)

stream.step_sankey()
```

## Next steps

- [Eventstream](/docs/eventstream) — schema configuration and data format
- [Widgets](/docs/widgets) — all available visualizations and how they work
- [Data Processors](/docs/data-processors) — full list of transformations
