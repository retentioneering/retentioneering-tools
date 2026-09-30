[![Retentioneering logo](https://raw.githubusercontent.com/retentioneering/pics/master/pics/logo_long_black.png)](https://github.com/retentioneering/retentioneering-tools)

### See why your metrics moved, not just that they did.

**Retentioneering is an open-source Python library for user journey and path analysis on raw event data.** Point it at a clickstream or any event log and it maps the routes people really take, finds the steps where they stall, loop or leave, and puts any two groups side by side on one picture so you can see what changed. It runs in your notebook on your own data, and your AI coding agent can run it for you.

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/retentioneering/retentioneering-tools/blob/master/notebooks/retentioneering_5_tour.ipynb)
[![PyPI version](https://img.shields.io/pypi/v/retentioneering)](https://pypi.org/project/retentioneering/)
[![Python version](https://img.shields.io/pypi/pyversions/retentioneering)](https://pypi.org/project/retentioneering/)
[![Downloads](https://static.pepy.tech/badge/retentioneering/month)](https://pepy.tech/project/retentioneering)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Discord](https://img.shields.io/badge/chat-on%20discord-blue)](https://discord.com/invite/hBnuQABEV2)

**[Docs](https://retentioneering.com/docs/)** · **[Quick start](https://retentioneering.com/docs/quick-start)** · **[Recipes](https://retentioneering.com/docs/recipes)** · **[Use it with an AI agent](#let-an-ai-agent-run-it)** · **[Is it the right tool?](#when-to-use-it-and-when-to-use-something-else)** · **[Upgrading from 3.x?](#coming-from-3x)**

---

## From "conversion dropped" to "here's where it broke"

The bundled demo store is synthetic data with a planted incident: for three weeks in late May, checkout conversion collapses. A dashboard would show you the drop. Retentioneering shows you what people did differently:

```python
import retentioneering as rete

stream = rete.datasets.load_ecom()   # synthetic demo store: 600 users, 31k events over six months

# Mark the bad weeks as a segment...
stream = stream.add_segment("period", time_range=("2024-05-19", "2024-06-07"))

# ...keep only the checkout steps, and compare those weeks with the rest of the time
checkout = stream.filter_events(keep={"event": [
    "cart", "shipping_details", "payment_details", "payment_error", "support_chat", "purchase",
]})
checkout.transition_graph(diff=("period", "inside", "outside"))
```

![Transition graph in diff mode with payment_details selected: the edge to payment_error is red (+0.19) and the edge to purchase is blue (-0.18)](docs/img/readme-hero-diff.png)

Red transitions became more likely during those weeks and blue ones less likely. Each label is the change in the probability of that next step. Click `payment_details`, as in the picture, and the story is clear. Inside the window, 32 of 163 `payment_details` steps were followed by a `payment_error` (20%), against 3 of 546 the rest of the time (0.5%). Meanwhile `payment_details → purchase` fell from 25% to 7%. A funnel would only have told you that the last step got worse. The graph shows where it broke, which is where to look for the cause.

The graph is interactive: drag nodes, click an event to focus on it, and switch between probabilities, counts and times. It also [exports](#share-what-you-found) to a single HTML file that anyone can open.

## Is it for you?

You'll get the most out of Retentioneering if all three are true:

1. **You have event-level data.** That means one row per action, with a user, session or case ID, an event name and a timestamp. It usually lives in the GA4 BigQuery export, Segment, RudderStack or Snowplow tables, a raw Amplitude or Mixpanel export, an events table in your warehouse, or a trace export from an LLM agent.
2. **Your question is *why*, *where* or *how*.** Why did conversion drop, where do people leave, what did churned users do differently, how do two groups' journeys differ.
3. **You can run Python, or you work with an AI agent that can.** Jupyter, VS Code, Cursor and Colab all work, and so do Claude Code, Codex and other coding agents.

If all you have is totals from a dashboard, export the raw events first, because Retentioneering needs the individual journeys. If you need a cohort table or a p-value, [other tools fit better](#when-to-use-it-and-when-to-use-something-else).

## Try it on your data

```bash
pip install retentioneering        # Python 3.10+
```

```python
import pandas as pd
import retentioneering as rete

df = pd.read_csv("events.csv")       # columns: user_id, event, timestamp
stream = rete.Eventstream(df)

stream.transition_graph()            # every route people take, loops and dead ends included
stream.funnel(steps=["signup", "onboarding_done", "first_payment"])
```

If your columns have other names, map them once with a [schema](https://retentioneering.com/docs/eventstream#schema), and list the columns you want to compare by as `segment_cols`:

```python
stream = rete.Eventstream(df, schema={
    "path_cols": ["client_id"], "event_col": "action", "timestamp_col": "ts",
    "segment_cols": ["platform", "plan"],
})
```

<details>
<summary><b>Loading from GA4, Amplitude, Mixpanel or Segment</b></summary>

Anything pandas can read works: CSV, Parquet, or the result of a SQL query. These are the usual column mappings:

| Source | Path ID | Event | Timestamp |
|---|---|---|---|
| GA4 BigQuery export | `user_pseudo_id` | `event_name` | `TIMESTAMP_MICROS(event_timestamp)` |
| Amplitude export | `user_id` or `amplitude_id` | `event_type` | `event_time` |
| Mixpanel export | `distinct_id` | `event` | `time` (Unix seconds) |
| Segment / RudderStack `tracks` table | `user_id` or `anonymous_id` | `event` | `timestamp` |

For example, from the GA4 export:

```python
df = bigquery_client.query("""
    SELECT user_pseudo_id AS user_id, event_name AS event,
           TIMESTAMP_MICROS(event_timestamp) AS timestamp, device.category AS platform
    FROM `my-project.analytics_123456.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '20240501' AND '20240531'
""").to_dataframe()
stream = rete.Eventstream(df, schema={"segment_cols": ["platform"]})
```
</details>

No data at hand? Run the demo above, or click **Open in Colab** for a guided tour that needs no install. Widgets render in Jupyter, VS Code, Cursor and Colab; for JupyterLab, see the [installation notes](https://retentioneering.com/docs/installation).

**Or hand it to your AI agent.** Install the [agent skill](#let-an-ai-agent-run-it) and ask in plain words: *"Why do users drop off between cart and payment in events.csv? Use retentioneering."*

## Questions it answers

Each question opens to the code that answers it. Every snippet runs as-is on the demo dataset (`stream = rete.datasets.load_ecom()`), and the linked recipe explains the method in depth.

<details>
<summary><b>"Conversion dropped last week. What changed?"</b> Diff the bad window against normal days.</summary>

```python
stream = stream.add_segment("incident", time_range=("2024-05-19", "2024-06-07"))
stream.transition_graph(diff=("incident", "inside", "outside"))   # which transitions changed

# narrow it down: did it happen on every platform?
stream.filter_events(keep={"platform": ["mobile"]}).transition_graph(diff=("incident", "inside", "outside"))
```

The window is a *dynamic* segment: each event is labelled by when it happened, so someone active before and during the incident counts on both sides, and you compare behavior rather than people. Paths also look shorter inside a window because the window cuts them off, so read edges into `path_end` with care. Recipe: [Root cause of an anomaly](https://retentioneering.com/docs/recipes#find-a-root-cause-in-an-anomalous-period).

</details>

<details>
<summary><b>"Where do we lose users, and what do they do instead?"</b> Open up the funnel.</summary>

```python
stream.funnel(steps=["add_to_cart", "shipping_details", "purchase"])

# label each user by the deepest step they reached in order, then compare drop-offs with buyers
labelled = stream.add_segment("funnel", funnel_events=["add_to_cart", "shipping_details", "purchase"])
labelled.transition_graph(diff=("funnel", "shipping_details", "purchase"))

# zoom in on what happens after shipping_details, for buyers and non-buyers alike
stream.truncate_paths(start_anchor="shipping_details", end_anchor=["purchase", "path_end"]).transition_graph()
```

`truncate_paths` drops paths that never reach an anchor, so the `"path_end"` fallback keeps the people who never bought. Recipe: [Open up the funnel](https://retentioneering.com/docs/recipes#open-up-the-funnel).

</details>

<details>
<summary><b>"Why do users churn? What's our aha moment?"</b> Compare the first week of users who stayed with users who left.</summary>

```python
import pandas as pd

# who left: no activity in the last 30 days of data (only users observed for at least 37 days)
seen = stream.to_dataframe().groupby("user_id", observed=True)["timestamp"].agg(["min", "max"])
end = seen["max"].max()
seen = seen[seen["min"] < end - pd.Timedelta(days=37)]
left = seen.index[seen["max"] < end - pd.Timedelta(days=30)].tolist()
stayed = seen.index[seen["max"] >= end - pd.Timedelta(days=30)].tolist()

# what they did in their first seven days
first_week = stream.truncate_paths(
    start_anchor="path_start", end_anchor={"pattern": "path_start", "offset": "7D"},
)
first_week.step_matrix(diff=(left, stayed))
first_week.transition_graph(diff=(left, stayed))
```

The label comes from the whole history, but the comparison uses only each user's first week. Differences you see there happened *before* the outcome, so they are candidate causes rather than symptoms of leaving. Diff mode also accepts two lists of user IDs, as here. Recipe: [What leads to churn](https://retentioneering.com/docs/recipes#find-what-leads-to-churn).

</details>

<details>
<summary><b>"What do users actually do?"</b> Map the real routes to conversion.</summary>

```python
stream.transition_graph()                                  # the whole map
stream.truncate_paths(start_anchor="path_start", end_anchor="purchase").step_sankey()   # routes that end in a purchase
```

You see the detours, back-and-forth loops and dead ends that nobody designed. Recipe: [Paths to conversion](https://retentioneering.com/docs/recipes#see-which-paths-lead-to-conversion).

</details>

<details>
<summary><b>"How do mobile and desktop users differ?"</b> (or paid and organic, test and control) Scan all segments, then diff a pair.</summary>

```python
stream.segment_overview(segment_col="acquisition_channel", metrics=[
    {"metric": "length", "agg": "mean"},
    {"metric": "active_days", "agg": "median"},
    {"metric": "has_event", "metric_args": {"event": "purchase"}, "agg": "mean"},
])                                                                        # many metrics, all channels
stream.step_matrix(diff=("acquisition_channel", "paid_search", "<REST>")) # one channel vs the rest
stream.transition_graph(diff=("platform", "mobile", "desktop"))
```

For an A/B test, declare the arm as a segment column and diff test against control the same way, then use your usual stats for the verdict. Recipes: [Compare channels](https://retentioneering.com/docs/recipes#compare-acquisition-channels), [A/B beyond the headline](https://retentioneering.com/docs/recipes#read-an-ab-test-beyond-the-headline-metric).

</details>

<details>
<summary><b>"What kinds of users do we have?"</b> Cluster users by behavior.</summary>

```python
stream.cluster_analysis()     # interactive: pick a split and see each cluster's typical journey

stream = stream.add_clusters("behavior", features=[
    {"metric": "length"},
    {"metric": "active_days"},
    {"metric": "has_event", "metric_args": {"event": "purchase"}},
], method_args={"n_clusters": 4})   # the clusters become a segment you can diff
```

Recipe: [Behavior types](https://retentioneering.com/docs/recipes#discover-your-behavior-types).

</details>

<details>
<summary><b>"I need behavioral features for a churn or LTV model."</b> One row per user, ready to join.</summary>

```python
features = stream.get_metrics([
    {"metric": "length"},
    {"metric": "active_days"},
    {"metric": "time_between", "metric_args": {"start_event": "path_start", "end_event": "purchase"}},
    {"metric": "matches_pattern", "metric_args": {"pattern": "add_to_cart->.*->purchase"}},
])   # a pandas DataFrame indexed by user; times are in seconds, NaN where the event never happened
```

To avoid leaking the future into a model, compute features only from events before the prediction date: truncate or filter the stream first. Recipe: [Features for ML](https://retentioneering.com/docs/recipes#extract-behavioral-features-for-ml).

</details>

The first three are the analyses people most often get subtly wrong by hand: comparing a time window by users instead of by behavior, counting funnel steps reached out of order, and letting the outcome leak into the comparison. The diffs are descriptive, so check the counts behind any edge that matters (`edge_weight="count"` or the `*_data` twin) before you act on it.

## Not just clickstream

Anything that happens as **a sequence of steps per case** is a path, and the same tools work on it:

- **AI agent traces:** which tool calls come before a failed run, and where do agents get stuck in a retry loop?
- **Chatbot conversations:** which intents and flows end in a human handoff or an abandoned chat?
- **Support tickets:** which status changes and team handoffs stretch resolution time?
- **Learning platforms:** what separates learners who finish a course from those who drop out?
- **IVR menus, onboarding wizards, multi-step forms:** where do people get stuck or start over?

For example, to see where failed agent runs loop, export your traces (from Langfuse, LangSmith, Phoenix or OpenTelemetry) with one row per tool call or LLM generation, leaving out wrapper spans. Put the step's status into the event name, so that a failed call and a successful one become different nodes:

```python
spans = pd.read_csv("agent_steps.csv")   # columns: trace_id, tool_name, status, started_at, outcome
spans["step"] = spans["tool_name"].where(spans["status"] == "ok", spans["tool_name"] + ":error")

stream = rete.Eventstream(spans, schema={
    "path_cols": ["trace_id"], "event_col": "step",
    "timestamp_col": "started_at", "segment_cols": ["outcome"],
})
stream.transition_graph(diff=("outcome", "failed", "succeeded"), edge_weight="avg_per_path")
```

A retry loop shows up as a thick red cycle, such as `llm_call ⇄ apply_patch:error`, with its average count per run. Leave out the steps that define the outcome (like `max_steps_exceeded` or `final_answer`), or they will dominate the diff. Parallel calls often share a timestamp: tied events keep the order of your input rows, so sort by a sequence column before loading. For latency, use `edge_weight="time_median"` or `"time_q95"`.

## Let an AI agent run it

Ask a coding agent to analyse an event log and it will usually write a few hundred lines of one-off pandas that are hard to check and easy to get subtly wrong. With Retentioneering it uses tested building blocks and a proven workflow instead.

**Agent skill.** `retentioneering-product-analytics` gives Claude Code, Codex and other tools that support [Agent Skills](https://retentioneering.com/docs/agent-skills) a five-step workflow: inspect the log, pick a recipe, run it, validate the numbers, and interpret the result. To install it for Claude Code:

```bash
git clone --depth 1 https://github.com/retentioneering/retentioneering-tools /tmp/rete
mkdir -p ~/.claude/skills && cp -r /tmp/rete/.claude/skills/retentioneering-product-analytics ~/.claude/skills/
```

For Codex and other tools, copy the same folder from [`.agents/skills/`](.agents/skills/) into your project's `.agents/skills/`. Then ask in plain words, for example *"Why do users drop off between cart and payment in events.csv? Use retentioneering."*

**[MCP server over your data](https://retentioneering.com/docs/mcp-server).** Your agent explores an eventstream held in your local Jupyter kernel and builds an interactive HTML report in which every number links back to its source. It needs a local kernel, so it doesn't run in Colab.

```python
rete.mcp.serve(stream, port=8765)
# then, in a terminal:
# claude mcp add --scope user --transport sse retentioneering http://localhost:8765/sse
```

**Docs your assistant can read.** The [documentation MCP server](https://retentioneering.com/docs/mcp-server#documentation-mcp-server) needs no install (`claude mcp add --scope user --transport http retentioneering-docs https://retentioneering.com/docs/mcp`), and [`llms.txt`](https://retentioneering.com/llms.txt) and [`llms-full.txt`](https://retentioneering.com/llms-full.txt) give any model the current API in plain text.

> [!NOTE]
> **For AI assistants writing Retentioneering code:** version 5 is a ground-up rewrite, and code from 2.x and 3.x tutorials, blog posts and old answers will not run. Use `import retentioneering as rete`, then `stream = rete.Eventstream(df, schema={...})` with `path_cols`, `event_col`, `timestamp_col` and `segment_cols`. Every analysis is a method on `stream`: `transition_graph`, `step_matrix`, `step_sankey`, `funnel`, `segment_overview`, `cluster_analysis`. Each has a `*_data` twin that returns plain data; with `diff=` it returns a tuple of (difference, group A, group B). Data processors (`add_segment`, `filter_events`, `truncate_paths`, ...) return a new `Eventstream`. Diff mode takes `diff=("segment_col", "a", "b")` or two lists of path IDs. Before writing code, read `https://retentioneering.com/llms.txt` or connect the docs MCP server.

## When to use it, and when to use something else

**Retentioneering fits** when you have event-level data and your question is about behavior over time: why people drop off, what churned users did differently, how two groups' journeys differ, what happened inside an anomaly.

**Something else fits better** for these jobs:

| You need | Better choice | Where Retentioneering still helps |
|---|---|---|
| A path chart inside the analytics tool you already use | Amplitude Journeys, Mixpanel Flows, GA4 Path exploration | When you need your own export or non-product events, segments defined in SQL or Python, a diff of any two groups, or code an agent can rerun |
| A cohort retention table, or D1/D7/D30 curves | A few lines of pandas or SQL | Once two cohorts differ, diff their behavior to see why |
| A p-value or sample size for an A/B test | statsmodels, scipy | Seeing *what* changed in behavior between the arms |
| Marketing attribution, ROAS or media mix modelling | Attribution and MMM tools | Not this job |
| Conformance checking against a BPMN process model | [pm4py](https://github.com/pm4py/pm4py) | Exploring the routes cases really take, and comparing failed with successful ones |
| Real-time dashboards and alerting | Your BI or product analytics tool | Digging into an anomaly a dashboard has flagged |
| Aggregated numbers only (dashboard totals, weekly counts) | Export the raw events first | Everything above, once you have them |

## What's inside

- **Six interactive widgets:** [Transition Graph](https://retentioneering.com/docs/widgets/transition-graph), [Step Matrix](https://retentioneering.com/docs/widgets/step-matrix), [Step Sankey](https://retentioneering.com/docs/widgets/step-sankey), [Funnel](https://retentioneering.com/docs/widgets/funnel), [Segment Overview](https://retentioneering.com/docs/widgets/segment-overview) and [Cluster Analysis](https://retentioneering.com/docs/widgets/cluster-analysis).
- **Diff mode** in Transition Graph, Step Matrix, Step Sankey and Funnel, showing group A minus group B on one picture. Groups can be segments, time windows, funnel stages, clusters or explicit lists of users.
- **A table behind every chart.** Each widget has a `*_data` twin, such as `stream.funnel_data(...)` or `stream.transition_graph_data(...)`, that returns the numbers for pipelines, tests and custom charts.
- **[Segments](https://retentioneering.com/docs/segments)** defined by rules, SQL, a Python function, a time window or the deepest funnel step reached. They can be static (channel, plan) or change along the path (new, then returning, then loyal).
- **[Path patterns](https://retentioneering.com/docs/path-patterns)** such as `add_to_cart->.*->purchase`, for anchoring, filtering and measuring any sequence.
- **[Data processors](https://retentioneering.com/docs/data-processors)** you chain to prepare the data: sessions, filters, collapsed loops, URLs turned into events, churn markers, daily lifecycle states and sampling. Each step returns a new `Eventstream`, and `stream.recipe()` records the whole chain so it can be replayed.
- **[Path metrics](https://retentioneering.com/docs/path-metrics):** one registry of per-user metrics that feeds clustering, segment comparison, filtering and your own ML features.
- **A [DuckDB](https://duckdb.org) engine** that computes over the full log, not a sample. On a 4-core machine with 16 GB of RAM, 10 million synthetic events from 500,000 users loaded in about 20 seconds, a transition graph took about 30 more, and the Python process peaked at about 5 GB. Data arrives as a pandas DataFrame, so it has to fit in memory.

## Share what you found

Every widget exports to one self-contained interactive HTML file. Add your conclusions to it, and the product manager you send it to can open it without Python:

```python
graph = checkout.transition_graph(diff=("period", "inside", "outside"))
graph.export_html("checkout-incident.html", analysis="Payment errors started on May 19 ...")
```

## Your data stays with you

The library runs in your own environment and never uploads your event data. If you connect an AI agent through the skill or the MCP server, the agent's model provider sees whatever the agent reads, so choose the agent to suit your data.

The library does send anonymous usage telemetry by default. It records which methods were called, the names (never values) of the parameters you changed, whether the call succeeded, the size of the table, a hashed device ID and basic environment details such as the library, Python and OS versions. It never sends event names, user IDs, segment values, parameter values or error messages. [Here is exactly what is sent](https://retentioneering.com/docs/tracking). To turn it off, set `RETENTIONEERING_NO_TRACK=1` in your shell or at the top of your notebook; in Colab, add it as a secret.

## Coming from 3.x?

Version 5 is a ground-up rewrite, with a DuckDB-backed `Eventstream`, new open-source [anywidget](https://anywidget.dev)-based widgets that also work in VS Code and Cursor, diff mode, a `*_data` twin for every widget, an MCP server and agent skills. Most 3.x code won't run unchanged. The Preprocessing Graph, `Cohorts`, `StatTests` and `Sequences` are not in 5.x yet. If your work depends on them, stay on 3.x with `pip install "retentioneering<4"`.

[Migrating from 3.x](https://retentioneering.com/docs/migration-from-3x) maps the old API onto the new one, [CHANGELOG.md](CHANGELOG.md) lists every change, and the 3.x engine lives on the [`3.x` branch](https://github.com/retentioneering/retentioneering-tools/tree/3.x). Missing a 3.x feature? [Open an issue](https://github.com/retentioneering/retentioneering-tools/issues). We prioritise what people ask for.

## Documentation

Full documentation is at **[retentioneering.com/docs](https://retentioneering.com/docs/)**. Good places to start:

- [Quick start](https://retentioneering.com/docs/quick-start): from `pip install` to your first graph in five minutes.
- [Path analysis](https://retentioneering.com/docs/path-analysis): what each visualization shows, what it leaves out, and when to trust it.
- [Recipes](https://retentioneering.com/docs/recipes): common product questions mapped to working code.

## Contributing

Retentioneering is a community project in active development. Bug reports, recipes, examples from new domains, widget improvements, agent skills, performance work and API proposals are all welcome. See **[CONTRIBUTING.md](CONTRIBUTING.md)** to set up a development environment. Questions go to [Discord](https://discord.com/invite/hBnuQABEV2), [Telegram](https://t.me/retentioneering_support) or retentioneering@gmail.com.

*Apps are better with math. Join us!*

## License and commercial model

Retentioneering-tools is open-source software licensed under the Apache License, Version 2.0.

Retentioneering is a community research laboratory dedicated to developing new analytics methodology and
opensource tools.

Copyright retentioneering-tools v.5.0 Maxim Godzi, [Vladimir Kukushkin](https://www.linkedin.com/in/vladimir-kukushkin/) and Anatoly Zaytsev. Updates may include software developed by the Retentioneering community.

You are free to use, modify, distribute, and build commercial products with Retentioneering-tools, subject to the terms of the Apache-2.0 license.

Other Retentioneering libraries, packages and managed execution services, enterprise integrations, premium diagnostic workflows, hosted collaboration features are separate proprietary products and are governed by their respective commercial terms. Additional details provided in [COMMERCIAL.md](COMMERCIAL.md).

The Apache-2.0 license applies only to the source code and assets distributed in this repository. It does not grant rights to use the Retentioneering name, logo, trademarks, hosted services, proprietary cloud infrastructure, or commercial content that is not distributed in this repository.

We welcome contributions from individuals and organizations. Contributions to Retentioneering-tools are accepted under the contribution terms described in [CONTRIBUTING.md](CONTRIBUTING.md).

Our goal is to keep the core analytical language and ecosystem open, extensible, and useful for independent analysts, researchers, startups, and enterprise teams, while funding long-term maintenance through optional commercial products and services.
