[![Rete logo](https://raw.githubusercontent.com/retentioneering/pics/master/pics/logo_long_black.png)](https://github.com/retentioneering/retentioneering-tools)
[![Discord](https://img.shields.io/badge/server-on%20discord-blue)](https://discord.com/invite/hBnuQABEV2)
[![Telegram](https://img.shields.io/badge/chat-on%20telegram-blue)](https://t.me/retentioneering_support)
[![Python version](https://img.shields.io/pypi/pyversions/retentioneering)](https://pypi.org/project/retentioneering/)
[![PyPI version](https://img.shields.io/pypi/v/retentioneering)](https://pypi.org/project/retentioneering/)
[![Downloads](https://pepy.tech/badge/retentioneering)](https://pepy.tech/project/retentioneering)
[![Downloads](https://static.pepy.tech/badge/retentioneering/month)](https://pepy.tech/project/retentioneering)

## What is Retentioneering?

Retentioneering is a Python library for behavioral analytics on event data:
clickstreams, product event logs, user paths. Where funnels tell you *what*
happened, retentioneering shows *how* users actually behave — real paths to
conversion, loops and dead ends, behavioral segments, and the differences in
behavior between any two groups of users.

It runs on your raw data (a pandas DataFrame or CSV is enough), and renders interactive
widgets right in Jupyter — no SaaS, no data leaving your machine.

**Version 5.0 is a ground-up rewrite.** The pandas engine and CDN-loaded
widgets of 3.x were replaced with a DuckDB-backed `Eventstream`, a new
generation of open-source [anywidget](https://anywidget.dev)-based widgets,
and an MCP server that lets LLM agents run analyses on your eventstream. See
[CHANGELOG.md](CHANGELOG.md) for the full 3.3.0 → 5.0 delta; the legacy 3.x
engine lives on the [`3.x` branch](https://github.com/retentioneering/retentioneering-tools/tree/3.x).

## Documentation

Complete documentation is available at
**[https://retentioneering.com/docs](https://retentioneering.com/docs/)**.

## Installation

Python 3.11+ is required.

```bash
pip install retentioneering
```

Or directly from a Jupyter (Lab, Notebook, Desktop) / [Google Colab](https://colab.research.google.com/) / VS Code:

```bash
!pip install retentioneering
```

## Quick start

All you need is a DataFrame with three columns: a path identifier, an event
name, and a timestamp. (Different column names? Pass a
[schema](https://retentioneering.com/docs/eventstream).)

```python
import pandas as pd
from retentioneering import Eventstream

df = pd.read_csv("events.csv")   # columns: user_id, event, timestamp
stream = Eventstream(df)

stream.transition_graph()        # interactive behavior graph, right in the notebook
```

No data at hand? Use the bundled synthetic e-commerce dataset:

```python
from retentioneering.datasets.ecom import load_ecom
from retentioneering import Eventstream

ecom = Eventstream(load_ecom(), schema={
    "path_cols": ["user_id", "session_id"],
    "segment_cols": ["platform", "acquisition_channel"],
})

ecom.funnel(steps=["catalog", "add_to_cart", "purchase"])

# Compare two segments in one picture (diff mode)
ecom.transition_graph(diff=["platform", "mobile", "desktop"])
```

Clean and shape the data by chaining data processors — every step returns a
new `Eventstream`, the original is never modified:

```python
clean = (
    stream
    .filter_events(drop={"event": ["bot_ping"]})
    .collapse_events(consecutive=True)
    .split_sessions(timeout="30m")
)
clean.step_matrix(path_pattern=".*->add_to_cart->.*->purchase")
```

Need raw numbers instead of a widget? Every widget has a headless twin:

```python
tm = stream.transition_graph_data(edge_weight="proba_out")   # DataFrame
funnel = stream.funnel_data(steps=["catalog", "add_to_cart", "purchase"])  # dict
```

## What's inside

- **Interactive widgets** for Jupyter — [Transition Graph](https://retentioneering.com/docs/widgets/transition-graph),
  [Step Matrix](https://retentioneering.com/docs/widgets/step-matrix),
  [Step Sankey](https://retentioneering.com/docs/widgets/step-sankey),
  [Funnel](https://retentioneering.com/docs/widgets/funnel),
  [Segment Overview](https://retentioneering.com/docs/widgets/segment-overview),
  [Cluster Analysis](https://retentioneering.com/docs/widgets/cluster-analysis).
  Every parameter is also editable live in the widget sidebar, and every
  widget exports to a self-contained interactive HTML file.
- **Diff mode** in every widget — overlay two segments (`["plan", "pro", "free"]`,
  or a segment vs. everyone else via `"<REST>"`) to see *how* behavior
  differs, not just that a metric moved.
- **[Data processors](https://retentioneering.com/docs/data-processors)** —
  chainable, SQL-powered preprocessing: filtering events and paths,
  sessionization, collapsing noise, synthetic events (incl. churn markers),
  segments, URL parsing, daily lifecycle states, sampling.
- **[Path metrics](https://retentioneering.com/docs/path-metrics)** — one
  registry of per-path metrics that feeds behavioral clustering, segment
  comparison, path filtering, and your own ML feature pipelines
  (`stream.get_metrics()`).
- **[MCP server](https://retentioneering.com/docs/mcp)** —
  `retentioneering.mcp.serve(stream)` exposes the eventstream to Claude or
  any MCP client: agents explore the data, build report tabs, and export a
  validated interactive HTML report where every number links to its source.

## Contributing

This is a community-driven open source project in active development. Any
contributions — new ideas, bug reports, bug fixes, documentation
improvements — are very welcome. See **[CONTRIBUTING.md](CONTRIBUTING.md)**
for the local development setup.

Apps are better with math! :)
Retentioneering is a research laboratory, analytics methodology and
opensource tools founded by
[Maxim Godzi](https://www.linkedin.com/in/godsie/) in 2015. Please feel free
to contact us at retentioneering@gmail.com if you have any questions
regarding this repo.
