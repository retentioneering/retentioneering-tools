<p align="center">
  <a href="https://github.com/retentioneering/retentioneering-tools"><img src="https://raw.githubusercontent.com/retentioneering/pics/master/pics/logo_long_black.png" alt="Retentioneering" width="420"></a>
</p>

<p align="center">
  <b>User behavior analysis in Python.</b><br>
  Think code-first Amplitude or Mixpanel, without uploading your data to a third-party service.
</p>

<p align="center">
  <a href="https://colab.research.google.com/github/retentioneering/retentioneering-tools/blob/master/notebooks/retentioneering_5_tour.ipynb"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"></a>
  <a href="https://pypi.org/project/retentioneering/"><img src="https://img.shields.io/pypi/v/retentioneering" alt="PyPI version"></a>
  <a href="https://pypi.org/project/retentioneering/"><img src="https://img.shields.io/pypi/pyversions/retentioneering" alt="Python versions"></a>
  <a href="https://github.com/retentioneering/retentioneering-tools/actions/workflows/ci.yml"><img src="https://github.com/retentioneering/retentioneering-tools/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue" alt="License: Apache 2.0"></a>
  <a href="https://pepy.tech/project/retentioneering"><img src="https://static.pepy.tech/badge/retentioneering/month" alt="Downloads"></a>
  <a href="https://discord.com/invite/hBnuQABEV2"><img src="https://img.shields.io/badge/chat-discord-5865F2" alt="Discord"></a>
</p>

<p align="center">
  <img src="https://raw.githubusercontent.com/retentioneering/retentioneering-tools/master/.github/readme/transition-graph.gif" alt="A transition graph: from all user paths at once to the path to purchase, then to what happens after the payment step" width="820">
</p>

Retentioneering takes a plain event log (who did what, and when) and shows how people actually move through your product: the routes that end in a purchase, the loops, the dead ends, and where two groups of users part ways. You work in a Jupyter notebook, the charts are interactive, and the data stays on your machine.

- **Interactive widgets built for user paths**: transition graph, step matrix, step Sankey, funnel, cluster analysis.
- **Group comparison**: put two groups of paths side by side and the widgets highlight where their behavior differs.
- **Path patterns**: a small regex-like language for finding, filtering and slicing sequences of events.
- **Behavioral clustering**: split paths into types of behavior and see what makes each type different.
- **AI agents**: Claude, Codex and other agents can run the analysis for you and hand back an interactive report.

**Try it without installing anything:** the [tour notebook](https://colab.research.google.com/github/retentioneering/retentioneering-tools/blob/master/notebooks/retentioneering_5_tour.ipynb) runs in Google Colab on a bundled demo dataset.

## Install

```bash
pip install retentioneering
```

Python 3.10 to 3.13. Widgets render in Jupyter, JupyterLab, VS Code, Cursor and Google Colab. In a notebook cell, use `%pip install retentioneering`.

## Quick start

All you need is a table with three columns: a user id, an event name and a timestamp. If your columns are named differently, pass a [schema](https://retentioneering.com/docs/eventstream).

```python
import pandas as pd
import retentioneering as rete

df = pd.read_csv("events.csv")  # columns: user_id, event, timestamp
stream = rete.Eventstream(df)

stream.transition_graph()  # interactive graph of how users move between events
```

No data at hand? There's a synthetic e-commerce dataset in the box:

```python
ecom = rete.datasets.load_ecom()
ecom.funnel(steps=["catalog", "add_to_cart", "purchase"])
```

Every data processor returns a new `Eventstream`, so cleaning steps chain and the original stays untouched:

```python
clean = (
    stream
    .filter_events(drop={"event": ["bot_ping"]})
    .collapse_events(loops=True)
    .split_sessions(timeout="30m")
)
```

And every widget has a headless twin that returns plain data, if you'd rather work with a DataFrame:

```python
matrix = stream.transition_graph_data(edge_weight="proba_out")  # DataFrame
```

## What you can do with it

The examples below run on the demo dataset, `stream = rete.datasets.load_ecom()`, so you can paste them into a notebook as is.

### Find out why a metric moved

Pick the dates when something went wrong, turn them into a segment, and compare those paths with the usual ones. Red edges happen more often during the drop, blue ones less often.

```python
drop = stream.add_segment("drop", time_range=("2024-05-19", "2024-06-07"))
drop.transition_graph(diff=["drop", "inside", "outside"])
```

<img src="https://raw.githubusercontent.com/retentioneering/retentioneering-tools/master/.github/readme/diff-graph.png" alt="Transition graph in diff mode: during the drop, users go from payment details to a payment error and then to support chat instead of completing the purchase" width="720">

Here the answer is on the screen: fewer people get from payment details to purchase, and a new route shows up, payment error followed by support chat.

The same comparison works for any two groups. Segments can be columns you already have (country, platform) or something you define on the fly: users who reached checkout and never bought vs. those who did, a user's first week vs. the rest of their life, one acquisition channel vs. all others.

### Look inside A/B test results

A test finishes and the headline metric went up, down or nowhere. The next question is always why, and it comes back after every test you run. Treat the variant as a segment and compare the two groups step by step: which funnel step changed, which detours appeared, what the treatment group does instead of converting.

```python
stream = rete.Eventstream(df, {"segment_cols": ["variant"]})

ab = ["variant", "control", "treatment"]
stream.funnel(steps=["catalog", "add_to_cart", "cart", "shipping_details", "purchase"], diff=ab)
stream.transition_graph(diff=ab)
```

<img src="https://raw.githubusercontent.com/retentioneering/retentioneering-tools/master/.github/readme/funnel-diff.png" alt="Funnel widget in diff mode comparing two groups step by step" width="620">

<sub>The demo dataset has no experiment in it, so the picture compares mobile and desktop. With a real test you'd put the variant column there.</sub>

There's no special A/B test machinery in the library. You get the same graph, funnel and step views, pointed at two groups of users, and you can rerun the same notebook every time a new test ends.

### Describe the paths you care about with patterns

Patterns look like regular expressions, with events instead of characters:

| Pattern | Matches |
|---|---|
| `cart->.*->purchase` | opened the cart and bought something later |
| `cart->[^shipping_details\|support_chat]*->path_end` | opened the cart, then left without starting checkout or asking support |
| `[search\|catalog]->product_view` | came to a product page from search or from the catalog |

The same syntax works across the library. Filter whole paths:

```python
abandoned = stream.filter_paths(
    {"op": "=", "metric": "matches_pattern", "value": True,
     "metric_args": {"pattern": "cart->[^shipping_details|support_chat]*->path_end"}}
)
```

Cut each path down to the part you're interested in, say from a failed payment to the support chat that followed it:

```python
after_error = stream.truncate_paths(
    start_anchor={"pattern": "payment_details->payment_error"},
    end_anchor={"pattern": "payment_error->[^payment_details]*->support_chat"},
)
```

Or zoom in on the neighborhood of each funnel step. Step Matrix and Step Sankey show a couple of steps before and after every anchor in the pattern and fold everything in between into a gap:

```python
stream.step_matrix(path_pattern="shipping_details->.*->payment_details", step_window=2)
```

<img src="https://raw.githubusercontent.com/retentioneering/retentioneering-tools/master/.github/readme/path-pattern-matrix.png" alt="Step matrix anchored on two funnel events, two steps before and after each" width="620">

The full syntax is on the [Path Patterns](https://retentioneering.com/docs/path-patterns) page.

### Split users into behavior types

Cluster Analysis groups paths by what people did and shows what sets each group apart. Why did users churn? Cluster their paths and look at what each group actually did before leaving.

```python
stream.cluster_analysis(features=[{"metric": "length"}, {"metric": "event_count_bulk"}])
```

<img src="https://raw.githubusercontent.com/retentioneering/retentioneering-tools/master/.github/readme/cluster-analysis.png" alt="Cluster analysis heatmap: one column per cluster, one row per event, color shows where a cluster stands out" width="520">

Once the clusters make sense, `add_clusters` turns them into a segment column, and you can compare them like any other group.

### Let an AI agent do the analysis

There are two ways to hand the work to an agent:

- **[Agent skills](https://retentioneering.com/docs/agent-skills)**. Point Claude Code, Codex or Cursor at a skill from this repo and it writes and runs Retentioneering code against your files, following a tested workflow: inspect the log, pick a recipe, run it, check the result.
- **[MCP server](https://retentioneering.com/docs/mcp-server)** (beta). Start it from a notebook with `rete.mcp.serve(stream)`, connect your agent, and ask questions in plain words. The agent builds an interactive HTML report where every number links to the chart it came from, so you can check it instead of taking it on trust.

<img src="https://raw.githubusercontent.com/retentioneering/retentioneering-tools/master/.github/readme/agent-report.png" alt="An HTML report written by an agent: the text on the left, charts in tabs on the right, numbers in the text link to the charts" width="720">

Agents that write code with the library can also read the docs directly: they're available as [`llms.txt`](https://retentioneering.com/llms.txt) and through a [documentation MCP server](https://retentioneering.com/docs/mcp-server#documentation-mcp-server).

## Documentation

Everything is at **[retentioneering.com/docs](https://retentioneering.com/docs/)**: guides, the full API reference, and live widget demos.

Coming from 3.x? Version 5 is a rewrite with a new API. See the [migration guide](https://retentioneering.com/docs/migration-from-3x) and the [changelog](CHANGELOG.md). The old engine is on the [`3.x` branch](https://github.com/retentioneering/retentioneering-tools/tree/3.x).

## Your data stays with you

The analysis runs where your notebook runs. There's no hosted service, and your event data never leaves your machine. The library does send anonymous usage statistics (which methods get called, never the data), and you can switch them off with one setting. Details are on the [tracking page](https://retentioneering.com/docs/tracking).

## Community and contributing

Questions and ideas are welcome on [Discord](https://discord.com/invite/hBnuQABEV2) and in [GitHub issues](https://github.com/retentioneering/retentioneering-tools/issues). There's also a [Telegram chat](https://t.me/retentioneering_support), mostly in Russian.

We'd love help with anything: bug reports, docs, examples, new widgets, new agent skills, analysis recipes. [CONTRIBUTING.md](CONTRIBUTING.md) explains how to set up the project locally. If you're not sure where to start, look for issues labelled [good first issue](https://github.com/retentioneering/retentioneering-tools/labels/good%20first%20issue). You can also reach us at retentioneering@gmail.com.

## License

Retentioneering-tools is licensed under [Apache 2.0](LICENSE), so you can use it, change it and build commercial products on top of it. Copyright Maxim Godzi, [Vladimir Kukushkin](https://www.linkedin.com/in/vladimir-kukushkin/) and Anatoly Zaytsev, with contributions from the Retentioneering community.

Retentioneering is also a community research lab working on analytics methods. Some other tools and services we build are commercial; they're described in [COMMERCIAL.md](COMMERCIAL.md). The Apache license covers the code in this repository and doesn't extend to the Retentioneering name and logo.
