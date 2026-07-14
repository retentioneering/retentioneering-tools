# Analysis recipes

Ten field-tested patterns, each distilled from real product investigations (e-commerce
checkout, game telemetry at 1.6M events, catalog browsing, navigation-game paths).
Each recipe: when to use → skeleton → how to read → pitfalls. Pick the SMALLEST recipe
that answers the question; combine only when each addition resolves a distinct uncertainty.

Skeletons assume `stream` is an `Eventstream` (see `api-map.md`).

---

## R1 · First picture of a product ("what is going on here?")

```python
d = stream.describe(top_events=None)          # full frequency table, no truncation
f = stream.funnel_data(steps=[...])           # your best guess of the core flow
tg = stream.transition_graph(edge_weight="proba_out")
tg.export_html("artifacts/overview_graph.html", title="...", analysis="...")
```

Read: `step_conversion_rate` locates the worst cliff; the graph shows where traffic
actually goes. Pitfalls: start the funnel at a meaningful entry (not the landing page —
many users enter mid-flow); optional steps (e.g. a review screen only 10% pass) do not
belong in `steps`.

## R2 · Converted vs dropped ("what do winners do differently?")

The single highest-yield move in the toolkit.

```python
lab = stream.add_segment("stage", funnel_events=["basket", "checkout", "purchase"])
lab.transition_graph(diff=("stage", "purchase", "basket"))     # diff = purchase − basket
lab.step_matrix(path_pattern=".*->basket->.*", diff=("stage", "purchase", "basket"))
```

Read: positive cells = over-represented among converters. Pitfalls: `funnel_events` is
closed/ordered — a path that hit `checkout` before ever hitting `basket` is labeled
`basket`, target-only paths get `out_of_funnel`; check `get_segment_levels()` and group
sizes before interpreting.

## R3 · Around an anchor event ("what happens right before/after X?")

```python
(m,) = stream.step_matrix_data(max_steps=6, path_pattern=".*->basket->.*")
m[[0, 1, 2]]        # column 0 = the anchor itself, 1..n = steps after
```

Read: rising `path_end` share right after the anchor = the anchor is where paths die.
Pitfalls: with `path_pattern` the headless return is a tuple of per-anchor blocks —
unpack; with `diff` it is `(blocks, g1_blocks, g2_blocks)`.

## R4 · Micro-journey between two funnel levels

```python
micro = stream.truncate_paths(start_event="basket", end_event="checkout")
micro.transition_graph(edge_weight="proba_out")
micro.get_metrics([{"metric": "length"}, {"metric": "duration"}]).describe()
```

Read: what successful walkers actually traverse (detours, loops, help pages) and how
long the crossing takes (median steps/minutes → timing for recovery nudges).
Pitfalls: paths lacking either anchor are DROPPED — population = completers only. To
compare with non-completers, split first (`filter_paths` on `has_event`), truncate each
population by its own rules, analyze separately.

## R5 · Behavioral segmentation without target leakage

```python
KEY_FAMILIES = ["listing", "product", "add_to_cart", "search", "promo"]  # NO outcome events!
FEATURES = (
    [{"metric": "length"}, {"metric": "duration"}, {"metric": "active_days"}]
    + [{"metric": "event_count", "metric_args": {"event": e}} for e in KEY_FAMILIES]
)
res = stream.cluster_analysis_data(
    features=FEATURES, n_clusters="3-8",
    overview_metrics=FEATURES + [
        {"metric": "has_event", "metric_args": {"event": "purchase"}, "agg": "mean"},
    ],   # outcome goes HERE, for validation only
)
labeled = stream.add_clusters(name="behavior", features=FEATURES,
                              n_clusters=res["best_params"]["n_clusters"])
```

Read: conversion spread ACROSS clusters (from overview) is the finding; cluster profiles
name the personas. Pitfalls: outcome events in `features` produce silhouette≈0.9
"clusters" that merely restate the funnel — impressive and useless. Inspect the
silhouette curve yourself: a best-K at the range boundary or a >90/10 split means the
clustering is weak regardless of `best_params`; on degenerate data `best_params` may be
absent entirely. Cluster labels are strings (`"cluster_0"`); interpret via top routes and
sizes, and treat clusters as centroids, not rules.

## R6 · Loops and repeated behavior ("where do users churn in place?")

Self-loops live on the transition-matrix diagonal; A→B→A patterns via `matches_pattern`:

```python
tm = stream.transition_graph_data(edge_weight="proba_out")
selfloops = {e: tm.loc[e, e] for e in tm.index}
pogo = stream.get_metrics([{"metric": "matches_pattern",
                            "metric_args": {"pattern": "listing->product->listing"}}])
```

Pitfalls (both are result-killers): (1) count loops BEFORE `collapse_events(consecutive=)`
— collapsing erases them; (2) naive "conversion of users with loop vs without" is
confounded by exposure — longer paths have more loops AND more chances to convert;
stratify by path-length quantiles before comparing.

## R7 · Time-to-outcome and intervention windows

```python
tb = stream.get_metrics([{"metric": "time_between",
                          "metric_args": {"start_event": "basket",
                                          "end_event": "checkout"}}]).dropna()
# completers' timing; for a survival view add censored paths (reached basket, no checkout)
```

Read: median/quantiles of completion → after which delay organic completion is <5% —
that is when a nudge fires. Pitfalls: `time_between` = first A to first B globally in the
path, not "first B after A" — for strict semantics compute from `to_dataframe()`; account
for right-censoring near the end of the log window.

## R8 · Compare segments on many metrics at once

```python
stream.segment_overview(segment_col="device", metrics=[
    {"metric": "length", "agg": "median"},
    {"metric": "has_event", "metric_args": {"event": "purchase"}, "agg": "mean"},
])
```

Read: scan for the metric with the largest cross-segment gap, then drill with R2/R3.
Pitfalls: report `n` per segment value alongside every share — a 100% on 5 paths is
noise; flag small cells explicitly (reviewers will ask).

## R9 · What-if on a Markov chain (advanced; custom math on top)

Estimate the value of removing/rerouting a friction step (e.g., guest checkout):

```python
counts = sub.transition_graph_data(edge_weight="count").astype(float).fillna(0)
# build absorbing chain on counts; edit edges (reroute basket->login mass to basket->checkout);
# recompute absorption; bootstrap paths for CIs
```

Non-negotiables learned in the field: (1) build the chain on the RELEVANT SUB-POPULATION
(paths containing the anchor, `truncate_paths` to first anchor→outcome) — a chain over
the full log averages transition rows over unrelated users and badly distorts absorption;
(2) validation gate — base-chain absorption must reproduce the observed conversion before
any scenario is trusted; (3) plug-in absorption equals the training-set rate by
construction, so the model's value is in scenario DELTAS, not level forecasts;
(4) rerouted users converting like organic ones is an upper-bound assumption — present a
sensitivity grid (25/50/100% of the empirical rate).

## R10 · Package results for stakeholders

```python
w = lab.transition_graph(diff=("stage", "purchase", "basket"))
w.export_html("artifacts/conv_vs_drop.html", title="...",
              analysis="Findings in markdown; [basket] and [checkout] become clickable.")
meta = {"recipe": processed.recipe(), "version": retentioneering.__version__,
        "filters": "...", "params": {...}}
```

Rules that survived stakeholder review: write the `analysis=` text AFTER conclusions are
final (a stale caption contradicting the report is a credibility killer); caption numbers
must come from the headless twin, not from memory; ship `recipe()` + versions in
`run-metadata.json` so any artifact is regenerable from raw data.

---

## Choosing quickly

| Question shape | Recipe |
|---|---|
| "What's going on / where do we lose people?" | R1 → R2 |
| "What happens around event X?" | R3 |
| "What do successful users do between A and B?" | R4 |
| "What kinds of users do we have?" | R5 |
| "Why do users go in circles?" | R6 |
| "When should we intervene?" | R7 |
| "Which segment behaves differently?" | R8 |
| "What is fixing X worth?" | R9 |
| "How do I hand this to a PM?" | R10 |
