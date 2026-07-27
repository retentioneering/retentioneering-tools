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
import retentioneering as rete

df = pd.read_csv("events.csv")
stream = rete.Eventstream(df)
```

No CSV yet? Use the built-in sample dataset to follow along:

```python
import retentioneering as rete

stream = rete.datasets.load_ecom()
```

## 3. Explore with a widget

Open an interactive [Transition Graph](/docs/widgets/transition-graph) — no arguments needed. Configure everything in the sidebar.

<DemoWidget cmd={`stream.transition_graph()`} path="/docs-demos/guide/quick-start/transition-graph.html" height={560} />

Compare two user segments side by side:

<DemoWidget cmd={`stream.transition_graph(diff=["platform", "mobile", "desktop"])`} path="/docs-demos/guide/quick-start/transition-graph-diff.html" height={560}/>

Explore user paths step by step around important events or drop-off points with [Step Sankey](/docs/widgets/step-sankey):

<DemoWidget cmd={`stream.step_sankey(path_pattern="purchase")`} path="/docs-demos/guide/quick-start/step-sankey.html" height={560} sidebarOpen={false}/>

or its equivalent [Step Matrix](/docs/widgets/step-matrix):

<DemoWidget cmd={`stream.step_matrix(path_pattern="purchase")`} path="/docs-demos/guide/quick-start/step-matrix.html" height={560} sidebarOpen={false} />

## 4. Prepare your data

Use [data processors](/docs/data-processors) to clean and shape the eventstream before visualizing:

```python
stream = (
    rete.datasets.load_ecom()
    .filter_events(drop={"event": ["checkout_bug"]})
    .rename_events({"wishlist_add": "add_to_wishlist"})
)

stream.step_sankey()
```

Every processor returns a new eventstream, so they chain into a pipeline and never modify what they were called on. Note that they also validate event names against your data: dropping or renaming an event that isn't there raises an error instead of silently doing nothing.

## Next steps

- [Path Analysis](/docs/path-analysis) — the one page that explains what all of these widgets actually compute. Read this before the rest.
- [Eventstream](/docs/eventstream) — schema configuration and data format
- [Widgets](/docs/widgets) — all available visualizations and how they work
- [Data Processors](/docs/data-processors) — full list of transformations
