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
micro = stream.truncate_paths(start_anchor="basket", end_anchor="checkout")
micro.transition_graph(edge_weight="proba_out")
micro.get_metrics([{"metric": "length"}, {"metric": "duration"}]).describe()
```

Read: what successful walkers actually traverse (detours, loops, help pages) and how
long the crossing takes among observed completers. These quantiles alone cannot set
a recovery-nudge threshold (see R7).
Pitfalls: paths lacking either anchor are DROPPED — population = completers only. To
compare with non-completers, split first (`filter_paths` on `has_event`), truncate each
population by its own rules, analyze separately.

## R5 · Behavioral segmentation without target leakage

For targeting at a decision time, first define eligibility using information available
at that time. Example: a calendar landmark, with features strictly before it and an
outcome window after it. Adapt the dates and timezone to the observation contract;
`stream` here contains only the eligible population, with follow-up through the horizon.
Do not choose each user's cutoff from their future purchase time.

```python
import pandas as pd

cutoff = pd.Timestamp("2026-01-08")
horizon = pd.Timestamp("2026-01-15")
time_col = stream.schema.timestamp_col
path_col = stream.schema.path_col
event_col = stream.schema.event_col
feature_stream = stream.filter_events(func=lambda df: df[time_col] < cutoff)
if feature_stream.is_empty():
    raise ValueError("No pre-decision history for clustering")
outcome_stream = stream.filter_events(
    func=lambda df: (df[time_col] >= cutoff) & (df[time_col] < horizon)
)
KEY_FAMILIES = ["listing", "product", "add_to_cart", "search", "promo"]
# Fit the feature vocabulary on pre-decision data only; absent events are rejected
# by event_count. Keep this vocabulary fixed when evaluating another cohort.
available = set(feature_stream.to_dataframe()[event_col])
KEY_FAMILIES = [e for e in KEY_FAMILIES if e in available]
FEATURES = (
    [{"metric": "length"}, {"metric": "duration"}, {"metric": "active_days"}]
    + [{"metric": "event_count", "metric_args": {"event": e}} for e in KEY_FAMILIES]
)
res = feature_stream.cluster_analysis_data(
    features=FEATURES, method_args={"n_clusters": "3-8", "random_state": 0},
    overview_metrics=FEATURES,
)
if not res.get("best_params"):
    raise ValueError("No valid clustering; inspect sample size and feature variation")
labeled = feature_stream.add_clusters(name="behavior", features=FEATURES,
                                      **res["best_params"])

labels = labeled.to_dataframe().groupby(path_col, observed=True)["behavior"].first()
future = outcome_stream.to_dataframe()
purchased = future[event_col].eq("purchase").groupby(future[path_col], observed=True).any()
# Valid only with complete follow-up: no future events then means no purchase.
evaluation = labels.to_frame().assign(
    purchased=purchased.reindex(labels.index, fill_value=False)
)
overview = evaluation.groupby("behavior", observed=True).agg(
    paths=("purchased", "size"), conversion_rate=("purchased", "mean")
)
```

`overview` keeps every eligible feature-path ID as the denominator. An ID absent from
the outcome log means no observed outcome only when follow-up is complete, not when
tracking stopped. Report eligible IDs lost because they had no pre-cutoff events.
For first-purchase prediction, exclude prior purchasers at eligibility time. For new-user
targeting use a fixed elapsed-time landmark and equal follow-up instead of these dates.

Read: profiles describe behavior in the feature window; later conversion differences
are associations. Inspect the silhouette curve, cluster sizes and stability; neither a
high silhouette nor conversion separation validates targeting. Select features/K without
optimizing on evaluation outcomes, and assess targeting on held-out users or a later cohort.
All features (including length, duration, active days and patterns) must use the same
pre-decision window. Cutting at the future outcome, or removing its event name from an
otherwise complete path, still leaks future information. A full-path clustering is useful
for retrospective description but must be labeled as such. Cluster labels are strings
(`"cluster_0"`); interpret via routes and sizes, and treat clusters as centroids, not rules.

## R6 · Loops and repeated behavior ("where do users churn in place?")

Self-loops live on the transition-matrix diagonal; A→B→A patterns via `matches_pattern`:

```python
tm = stream.transition_graph_data(edge_weight="proba_out")
selfloops = {e: tm.loc[e, e] for e in tm.index}
pogo = stream.get_metrics([{"metric": "matches_pattern",
                            "metric_args": {"pattern": "listing->product->listing"}}])
```

Pitfalls (both are result-killers): (1) count loops BEFORE `collapse_events(loops=)`
— collapsing erases them; (2) naive "conversion of users with loop vs without" is
confounded by exposure — longer paths have more loops AND more chances to convert;
compare exposure measured before a common decision time (R5). Conditioning on total
path length, which includes future behavior, can itself introduce bias.

## R7 · Time-to-outcome and intervention windows

For descriptive timing, resolve the first checkout after the first basket before
computing the metric; a checkout earlier in the original path must not become the end.

```python
# Current truncate_paths bounds use index alone, not the full (index, subindex).
# Reject shared indices rather than silently including an event before entry.
schema = stream.schema
if stream.to_dataframe().duplicated([schema.path_col, schema.index]).any():
    raise ValueError("This timing recipe requires a unique index per path; "
                     "use explicit (index, subindex) slicing for enriched streams")
completed = stream.truncate_paths(start_anchor="basket", end_anchor="checkout")
if completed.is_empty():
    raise ValueError("No ordered basket-to-checkout completions")
tb = completed.get_metrics([{"metric": "time_between",
                            "metric_args": {"start_event": "basket",
                                            "end_event": "checkout"}}]).dropna()
```

Read: median/quantiles describe timing among observed ordered completers only.
`time_between` on the original stream subtracts the first timestamps globally and can
be negative when checkout precedes basket. Truncation fixes event order; elapsed-time
analysis additionally requires trustworthy, nondecreasing timestamps within each path.
Equal timestamps are allowed: event order, not a positive time difference, determines
which checkout follows basket. The guard above is a limitation of this skeleton on the
current API, not a restriction of Eventstream: enriched streams may validly share an
index with distinct subindices. Use explicit slicing by `schema.order_cols` for them;
do not drop rows or reset indices just to bypass the guard.

For an intervention question, retain ALL basket entrants, including non-completers.
Define time zero, eligibility, the completion horizon H and each entrant's actual
follow-up end. With elapsed completion time T and known coverage C, record
`duration = min(T, C, H)` and `event = (T <= min(C, H))`; missing completion means T is
infinite, not zero. State the endpoint convention (this formula includes H). A last activity is not a known censoring time: use the known
coverage/observation end. Administrative censoring near the log boundary is not failure.
If S is the completion-free survival function, the probability of completion by H
among those still waiting at t is `1 - S(H) / S(t)` for `t < H` and `S(t) > 0`.
This needs support through H and justified censoring assumptions; competing terminal
outcomes require a competing-risk analysis. Completer quantiles do not estimate this
conditional probability. Report uncertainty and test a proposed nudge experimentally;
low organic completion probability alone does not establish that a nudge helps.

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

Estimate a conditional scenario for removing/rerouting a friction step. Before running
this skeleton, define the path unit, entry, observation horizon and eligible population.
Here `stream` must already have complete follow-up to that horizon (or genuinely closed
sessions for a within-session question), with later events removed. `path_end` then means
no purchase in that observation window, not permanent abandonment. Do not silently label
right-censored users as failures; use a censoring-aware method for an incomplete cohort.

```python
import numpy as np

schema = stream.schema
frame = stream.to_dataframe()
if frame.duplicated([schema.path_col, schema.index]).any():
    raise ValueError("This Markov recipe requires a unique index per path; "
                     "use explicit (index, subindex) slicing for enriched streams")
# The graph and this model use these names for synthetic absorbing boundaries.
if frame[schema.event_col].isin(["path_start", "path_end"]).any():
    raise ValueError("This Markov recipe uses reserved boundary names; remove "
                     "synthetic boundary markers or rename genuine events first")
entrants = stream.truncate_paths(start_anchor="basket", end_anchor="path_end")
sub = entrants.truncate_paths(start_anchor="basket", end_anchor=["purchase", "path_end"])
path_col = stream.schema.path_col
event_col = stream.schema.event_col
assert set(sub.to_dataframe()[path_col]) == set(entrants.to_dataframe()[path_col])
if sub.is_empty():
    raise ValueError("No eligible basket entrants")

counts = sub.transition_graph_data(edge_weight="count").astype(float).fillna(0)
# The graph adds boundary markers, including purchase -> path_end. Override both
# terminal rows: purchase must remain success, not flow on to the failure state.
absorbing = ["purchase", "path_end"]
states = counts.index.union(counts.columns).union(absorbing)
counts = counts.reindex(index=states, columns=states, fill_value=0.0)
for state in absorbing:
    counts.loc[state] = 0.0
    counts.loc[state, state] = 1.0
P = counts.div(counts.sum(axis=1), axis=0)
transient = states.drop(absorbing)
Q = P.loc[transient, transient].to_numpy()
R = P.loc[transient, absorbing].to_numpy()
B = np.linalg.solve(np.eye(len(Q)) - Q, R)
base_conversion = B[transient.get_loc("path_start"), absorbing.index("purchase")]
observed = sub.to_dataframe().groupby(path_col, observed=True)[event_col].agg(
    lambda events: (events == "purchase").any()
).mean()
assert np.isclose(base_conversion, observed)
```

The list bound uses the earliest resolved end after the start: purchase if present,
otherwise path end. A bare purchase end drops non-converters. A purchase before basket
alone is not a conversion; a later purchase is. Verify retained IDs, not just the count.
Use transition **counts**, not `unique_paths`, to fit this chain; repeated visits matter.
This skeleton requires unmarked input without genuine events named `path_start` or
`path_end`: otherwise real states can be confused with the graph's synthetic boundaries.
The unique-index guard has the same rationale as R7; it rejects unsupported input
before fitting instead of relying on the baseline equality to detect an incorrect cut.

The baseline equality above is a construction check on this finite, fully observed,
count-fitted population. It does not validate the first-order Markov assumption or
predictive accuracy. Diagnose discrepancies in cuts, weights, endpoints and starting
population; use separate held-out cohorts to assess forecasts. A state-based absorption
model has no calendar clock, so changing transitions does not by itself forecast
conversion by a fixed time horizon; that needs an explicit time model.

For a scenario, copy P, move a stated amount of probability mass from basket→login to
basket→checkout, and recompute absorption. Require nonnegative entries, rows summing
to one and reachable absorbing states; if an edit creates a closed transient class,
`I - Q` is singular and this absorption solve is not applicable. Rerouted users following
organic checkout transitions is an unverified transport assumption, not a guaranteed upper
bound. Include no-benefit and adverse-effect cases alongside optimistic cases; describe
how each assumption changes transition probabilities. Bootstrap whole independent units
(users if sessions repeat), re-fitting each replicate, for sampling uncertainty. Such
intervals do not cover structural or causal uncertainty. Scenario deltas are conditional
hypotheses for an experiment, not causal forecasts of a shipped change.

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
