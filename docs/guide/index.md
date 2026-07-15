# Retentioneering

**See why your metrics moved — not just that they did.**

Retentioneering is an open-source Python library for user behavior analysis. It turns raw event logs into interactive maps of how users actually move through your product: the paths they take, the loops they get stuck in, the step where they silently leave. All inside your notebook, on your own data, in a few lines of code.

```python
import retentioneering as rete

stream = rete.Eventstream(df)
stream.transition_graph()
```

## Dashboards tell you *what*. Retentioneering shows you *why*.

Amplitude, Mixpanel, and BI dashboards are great at reporting that conversion dropped or retention dipped. They are much worse at answering the question that follows: *what did users actually do differently?* Aggregate metrics flatten thousands of individual journeys into a single number — and the explanation lives in the journeys.

Retentioneering works at the level of behavior. Instead of a predefined funnel, it reconstructs the real paths users take — including the detours, back-and-forth loops, and dead ends no one designed — and gives you interactive tools to explore, segment, and compare them.

## What you can do

### Map real user journeys

Visualize how users actually navigate your product with an interactive [Transition Graph](/docs/widgets/transition-graph), or unfold paths step by step with [Step Sankey](/docs/widgets/step-sankey) and [Step Matrix](/docs/widgets/step-matrix). Discover which routes really lead to purchase, where users circle back, and which screen is most often the last one before they leave — without deciding in advance what the "correct" path looks like.

### Find where journeys break

Build [funnels](/docs/widgets/funnel) when you know the steps — and go beyond them when you don't. Anchor the analysis on any event, look at what happens right before drop-off, and trace an abstract "conversion fell" down to the concrete screen where the scenario falls apart.

### Compare groups, explain the difference

Any split becomes a [segment](/docs/segments) — an A/B arm, an acquisition channel, a behavioral cluster discovered by [Cluster Analysis](/docs/widgets/cluster-analysis) — and every core widget can render the behavioral *difference* between two groups. Two examples of what that unlocks:

- **Explain an anomaly.** Segment events into "anomalous period vs. normal", diff the two groups, and see at the level of raw events what actually caused the dip.
- **Open up the funnel.** Path patterns like `add_to_cart->.*->purchase` show what really happens *between* funnel levels, and a funnel segment — each path labelled with the last step it reached — reveals where exactly the users who never finished got lost.

More recipes like these live on the [Recipes](/docs/recipes) page.

## Built for real data

- **Fast on millions of rows.** The engine is backed by [DuckDB](https://duckdb.org), so exploration stays interactive on production-scale event logs — on your laptop.
- **Your data, your rules.** Load events from pandas, CSV, or Parquet — whatever comes out of your warehouse. Custom event grouping, sessionization ([`split_sessions`](/docs/data-processors)), and cleanup are a chained method call away, with no SaaS interface limits.
- **Notebook-native, shareable anywhere.** Widgets run in Jupyter, VS Code, and Google Colab, and every one of them exports to a standalone interactive HTML file you can drop into a message to a PM — no Python required to open it.
- **AI-ready.** The built-in [MCP server](/docs/mcp) lets an LLM agent explore your eventstream with the full toolkit — ask "why did retention dip last week?" in plain language. [Agent Skills](/docs/agent-skills) teach coding agents like Claude Code or Codex how to run and contribute analyses in this codebase.

## Who is it for

- **Product analysts** who hit the ceiling of funnel dashboards and need to see behavior, not just rates.
- **Quantitative UX researchers** connecting the "something feels broken here" intuition to quantitative evidence in event logs.
- **Growth and lifecycle teams** looking for what separates retained users from churned ones — and who to target next.
- **Experimenters** hunting for the segment where the A/B effect hides when the aggregate metric reads flat.
- **Data scientists** extracting behavioral features and interpretable patterns from raw trajectories for churn and LTV models.

## Where to start

Go from `pip install` to your first interactive transition graph in five minutes with the [Quick Start](/docs/quick-start) — a bundled e-commerce dataset is included, so you can explore before touching your own data. Then dive into [Eventstream](/docs/eventstream) to connect your logs, and [Widgets](/docs/widgets) for the full tour of visualizations.
