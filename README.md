[![Rete logo](https://raw.githubusercontent.com/retentioneering/pics/master/pics/logo_long_black.png)](https://github.com/retentioneering/retentioneering-tools)
[![Discord](https://img.shields.io/badge/server-on%20discord-blue)](https://discord.com/invite/hBnuQABEV2)
[![Telegram](https://img.shields.io/badge/chat-on%20telegram-blue)](https://t.me/retentioneering_support)
[![Python version](https://img.shields.io/pypi/pyversions/retentioneering)](https://pypi.org/project/retentioneering/)
[![PyPI version](https://img.shields.io/pypi/v/retentioneering)](https://pypi.org/project/retentioneering/)
[![Downloads](https://pepy.tech/badge/retentioneering)](https://pepy.tech/project/retentioneering)
[![Downloads](https://static.pepy.tech/badge/retentioneering/month)](https://pepy.tech/project/retentioneering)
[![OpenAgentSkill Trust](https://www.openagentskill.com/api/badge/retentioneering-retentioneering-tools?metric=trust&label=Trust)](https://www.openagentskill.com/skills/retentioneering-retentioneering-tools)

## What is Retentioneering?

**Retentioneering is an open-source Python toolkit, MCP server, and collection of agent skills for reproducible product analytics on clickstream and event log data.** Instead of relying on one-off scripts generated for a single question, analysts and AI agents can use tested analytical primitives and reusable workflows to inspect customer journeys, explore graph-based user flows, discover behavioral segments, evaluate experiments, and cross-check results through independent, auditable computations. By reusing domain-specific analytics components instead of generating every analysis from scratch, Retentioneering can reduce implementation effort, agent token usage, and the risk of subtle analytical errors.

Use Retentioneering when you want to turn raw sequences of user and system events into answers to questions such as: *Where do users get stuck? Which journeys lead to conversion or churn? What behavioral segments exist in the data? How do flows differ between cohorts or experiment groups?* Load a clickstream, product event log, or other timestamped event data into a Retentioneering `Eventstream` object, then explore interactive user-flow graphs and detailed step-matrix visualizations, compare conversion paths, analyze behavioral segments, evaluate A/B tests, or ask an AI agent to run and cross-check the analysis through Retentioneering MCP and agent skills. The resulting analyses, visualizations, and simulated interventions can be exported into clear, shareable HTML reports.

**Retentioneering supports multi-resolution analysis of user behavior, from individual events lasting seconds to sessions, recurring usage patterns, and customer journeys unfolding over months or years.** Funnels summarize whether users passed through predefined stages; Retentioneering reveals how they actually moved through the product: alternative routes to conversion, loops, dead ends, repeated behaviors, hidden segments, and differences between any two cohorts. The same graph and step-matrix methods can operate on raw events or on higher-level behavioral units, such as sessions, feature-use episodes, lifecycle stages, or custom product patterns. This makes it possible to zoom in on local interactions, zoom out to long-term journeys, and move between these levels without abandoning the same analytical framework.


Retentioneering is code-first and quickest way from raw data into detailed views and product insights.
With average python knowledge you can quickly identify what can be improved in the website, application or chats interactions by finding and isolating specific patterns of different users at multiple scales - session to session and within each session or its pieces.

It runs directly on your raw event data, whether provided as a pandas DataFrame, CSV, TSV, Parquet file, or a custom export from BigQuery, ClickHouse, or another event database.

Retentioneering renders interactive widgets directly in Jupyter, Google Colab, Cursor, VS Code, Codex, Claude Code, or another Python-compatible development environment, without requiring a hosted Retentioneering SaaS platform. The analysis runs in your own environment. Your raw and analysed event data never leaves your machine. Anonymous product telemetry, used to understand overall tool usage and improve the toolkit, may be enabled by default and can be disabled at any time.


**Retentioneering-tools Version 5.0 is a ground-up rewrite.** The pandas engine and CDN-loaded
widgets of 3.x were replaced with a much faster DuckDB-backed `Eventstream`, a new
generation of open-source [anywidget](https://anywidget.dev)-based widgets,
and an MCP server that lets LLM agents run analyses on your eventstream. See
[CHANGELOG.md](CHANGELOG.md) for the full 3.3.0 → 5.0 delta; the legacy 3.x
engine lives on the [`3.x` branch](https://github.com/retentioneering/retentioneering-tools/tree/3.x).

With this update we also include the opensource code for widgets front, and we highly encourage for the collaboration in its further development.

Regarding migrations of any features from 3.x to next release of 5.x please feel free to open new issues or pull requests in this repo, we will focus on faster development of useful features and ideas based on your engagement.

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
import retentioneering as rete

df = pd.read_csv("events.csv")   # columns: user_id, event, timestamp
stream = rete.Eventstream(df)

stream.transition_graph()        # interactive behavior graph, right in the notebook
```

No data at hand? Use the bundled synthetic e-commerce dataset:

```python
import retentioneering as rete

ecom = rete.datasets.load_ecom()

# Build a funnel
ecom.funnel(steps=["catalog", "add_to_cart", "purchase"])

# Compare two segments in one picture (diff mode) with transition graph
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
clean.step_matrix(path_pattern="add_to_cart->.*->purchase")
```

Need raw numbers instead of a widget? Every widget has a headless twin:

```python
tm = stream.transition_graph_data(edge_weight="proba_out")   # DataFrame
funnel = stream.funnel_data(steps=["catalog", "add_to_cart", "purchase"])  # dict
```

## What's inside

- **Interactive widgets** for in-depth analysis of user behavior:
  - [Transition Graph](https://retentioneering.com/docs/widgets/transition-graph),
  - [Step Matrix](https://retentioneering.com/docs/widgets/step-matrix),
  - [Step Sankey](https://retentioneering.com/docs/widgets/step-sankey),
  - [Funnel](https://retentioneering.com/docs/widgets/funnel),
  - [Segment Overview](https://retentioneering.com/docs/widgets/segment-overview),
  - [Cluster Analysis](https://retentioneering.com/docs/widgets/cluster-analysis).
- **Diff mode** in every widget — overlay two segments to see *how* behavior
  differs, not just that a metric moved.
- **[Data processors](https://retentioneering.com/docs/data-processors)** —
  chainable methods for filtering events and paths,
  sessionization, collapsing events, adding synthetic events (including churn markers),
  segments, URL parsing, daily lifecycle states, sampling, etc.
- **[Path metrics](https://retentioneering.com/docs/path-metrics)** — one
  registry of per-path metrics that feeds behavioral clustering, segment
  comparison, path filtering, and your own ML feature pipelines.
- **[MCP server](https://retentioneering.com/docs/mcp)** — exposes the eventstream to Claude or
  any MCP client: agents explore the data, build report tabs, and export a
  validated interactive HTML report where every number links to its source.

## Contributing

This is a community-driven open source project in active development. Any
contributions — bug reports, documentation improvements, examples, visualizations, analytical recipes, integrations, performance improvements, API proposals, widgets improvements, new agent skills and prompt libraries,  new analytical capabilities — are very welcome. See **[CONTRIBUTING.md](CONTRIBUTING.md)**
for the local development setup.
Please feel free to contact us at retentioneering@gmail.com if you have any questions
regarding this repo.

## Apps are better with math, join us! :)

## License and commercial model

Retentioneering-tools is open-source software licensed under the Apache License, Version 2.0.

Retentioneering is a community research laboratory dedicated to developing new analytics methodology and
opensource tools.

Copyright retentioneering-tools v.5.0 Maxim Godzi, Vladimir Kukushkin and Anatoly Zaytsev. Updates may include software developed by the Retentioneering community.

You are free to use, modify, distribute, and build commercial products with Retentioneering-tools, subject to the terms of the Apache-2.0 license.

Other Retentioneering libraries, packages and managed execution services, enterprise integrations, premium diagnostic workflows, hosted collaboration features are separate proprietary products and are governed by their respective commercial terms. Additional details provided in [COMMERCIAL.md](COMMERCIAL.md).

The Apache-2.0 license applies only to the source code and assets distributed in this repository. It does not grant rights to use the Retentioneering name, logo, trademarks, hosted services, proprietary cloud infrastructure, or commercial content that is not distributed in this repository.

We welcome contributions from individuals and organizations. Contributions to Retentioneering-tools are accepted under the contribution terms described in [CONTRIBUTING.md](CONTRIBUTING.md).

Our goal is to keep the core analytical language and ecosystem open, extensible, and useful for independent analysts, researchers, startups, and enterprise teams, while funding long-term maintenance through optional commercial products and services.
