# Recipes

Short recipes that map common product questions to retentioneering tools. Each one is a starting point, not a full tutorial — follow the links for details on every tool involved. Throughout this page, `stream` is an [Eventstream](/docs/eventstream) instance.

## Find a root cause in an anomalous period

A KPI metric dipped last week and you need to find the root cause? Create a dynamic [segment](/docs/segments) that separates the anomalous window from normal operation, then [diff](/docs/widgets/diff) the two groups on any widget:

```python
stream = stream.add_segment("incident", time_range=("2024-03-10", "2024-03-17"))
stream.transition_graph(diff=("incident", "inside", "outside"))
```

The graph highlights exactly which transitions degraded during the window. [Segment Overview](/docs/widgets/segment-overview) with the same segment scans many metrics at once when you don't yet know where to look. See [Segments](/docs/segments#inside-vs-outside-an-anomalous-period) for why a *dynamic* segment is the right tool here.

## Open up the funnel

A [Funnel](/docs/widgets/funnel) `add_to_cart` → `checkout_start` → `purchase` tells you 40% of paths drop between `checkout_start` and `purchase` — but not what those users did instead. Two moves recover the lost context.

First, look *between* the levels: trim each path to the window between two funnel steps and map what actually happens there:

```python
stream.truncate_paths(start_event="checkout_start", end_event="purchase").transition_graph()
```

Second, follow the users who never made it. The `funnel_events` mode of [Add Segment](/docs/data-processors/add-segment) labels each path with the deepest funnel step it completed *in order* — reaching a step out of sequence, or skipping an earlier one, doesn't count — so drop-offs at each level become comparable groups:

```python
labelled = stream.add_segment("funnel", funnel_events=["add_to_cart", "checkout_start", "purchase"])
labelled.transition_graph(diff=("funnel", "checkout_start", "purchase"))
```

Also, you can use [Step Matrix](/docs/widgets/step-matrix) or [Step Sankey](/docs/widgets/step-sankey) along with the `path_pattern` parameter to explore paths around the funnel steps:

```python
stream.step_matrix(path_pattern="add_to_cart->.*->checkout_start->.*->purchase")
stream.step_sankey(path_pattern="add_to_cart->.*->checkout_start->.*->purchase")
```

## See which paths lead to conversion

Instead of assuming the intended route, reconstruct the routes that actually end in the target event. Trim every converting path to the window from its start to the first conversion, and drop the rest:

```python
stream.truncate_paths(start_event="path_start", end_event="purchase").step_sankey()
```

[Step Sankey](/docs/widgets/step-sankey) unfolds the converging routes step by step; the [Transition Graph](/docs/widgets/transition-graph) shows the same data as a map.

## Read an A/B test beyond the headline metric

The test metric reads flat, but you suspect behavior shifted somewhere the aggregate masks. Declare the test arm as a segment column and compare behavior, not just rates:

```python
stream.segment_overview(segment_col="ab_arm")            # scan many metrics for any difference
stream.step_matrix(diff=("ab_arm", "test", "control"))   # inspect where journeys diverge
```

To hunt for heterogeneity — an effect that exists only for part of the audience — narrow the stream first and diff again:

```python
stream\
    .filter_events(keep={"platform": ["mobile"]})\
    .step_matrix(diff=("ab_arm", "test", "control"))
```

## Compare acquisition channels

Users from different channels arrive with different intent, and their journeys show it. With the channel declared in the schema's [segment_cols](/docs/eventstream#schema), every widget can compare one channel against all the others using the `<REST>` shorthand:

```python
stream.step_matrix(diff=("acquisition_channel", "paid_search", "<REST>"))
```
Also, you can use [Segment Overview](/docs/widgets/segment-overview) to get a quick overview of the differences between the channels
comparing multiple metrics – such as path length, event count, conversion rate, and time to purchase – at once.

```python
stream.segment_overview(
    segment_col="acquisition_channel",
    metrics=[
        {"metric": "length"},
        {"metric": "event_count_bulk"},
        # has_event mean is equivalent to conversion rate
        {"metric": "has_event", "metric_args": {"event": "purchase"}},
        # median time to purchase (in seconds)
        {"metric": "time_between", "metric_args": {"start_event": "path_start", "end_event": "purchase"}, "agg": "median"},
    ]
)
```

## Discover your behavior types

How many kinds of users does the product actually have? [Cluster Analysis](/docs/widgets/cluster-analysis) clusters paths by behavioral [path metrics](/docs/path-metrics) and profiles each cluster interactively:

```python
stream.cluster_analysis()
```

Once the clusters look meaningful, persist them with [Add Clusters](/docs/data-processors/add-clusters) — the labels become an ordinary segment, usable in diff mode and every other segment-aware tool:

```python
stream = stream.add_clusters(
    "behavior",
    features=[
        {"metric": "length"},
        {"metric": "active_days"},
        {"metric": "has_event", "metric_args": {"event": "purchase"}},
    ],
)
```

## Measure time to activation

How long does it take a new user to reach the key action? The `time_between` [path metric](/docs/path-metrics) computes it per path in one call:

```python
time_to_purchase = stream.get_metrics([
    {"metric": "time_between", "metric_args": {"start_event": "path_start", "end_event": "purchase"}},
])
pd.to_timedelta(time_to_purchase, unit='s').mean()
```

## Path as a sequence of sessions

A raw eventstream often has hundreds of granular events, which makes every path
long and hard to read so the widgets become overloaded and branchy. Instead of removing or collapsing raw events,
we can collapse each session down to one event named after its behavior. Ultimately, the resulting
eventstream has as many unique events as session types, and is far easier to
explore.

First, split paths into sessions if the eventstream doesn't have them yet:

```python
stream = stream.split_sessions(timeout="30m")
```

Then cluster sessions — not paths — by passing the session column as
`path_col` to [Cluster Analysis](/docs/widgets/cluster-analysis), and use the
widget to find a splitting that makes sense and see what each cluster
actually contains.

```python
stream.cluster_analysis(
    path_col="session_id",
    features=[{"metric": "length"}, {"metric": "event_count_bulk"}],
    overview_metrics=[{"metric": "length"}, {"metric": "event_count_bulk"}],
)
```

Once the split looks right, label the clusters right in the UI and click "Save Clusters" to persist the clusters which will become a new column in the eventstream. This is equivalent to running [Add Clusters](/docs/data-processors/add-clusters) with the same `path_col`, `features`, and `n_clusters` you settled on.

Finally, collapse each session into a single event named after its type with
[Collapse Events](/docs/data-processors/collapse-events):

```python
stream = stream.collapse_events(session_col="session_id", session_type_col="session_type")
```

The new eventstream has one synthetic event per session, and the number of unique
events equals the number of session types from clustering — small enough to
read a [Step Matrix](/docs/widgets/step-matrix) or [Transition Graph](/docs/widgets/transition-graph) at a glance.

## Extract behavioral features for ML

Churn and LTV models improve when they see *behavior*, not just demographics. `get_metrics()` turns any set of [path metric](/docs/path-metrics) configs into a clean per-path feature table, ready to join with your training data:

```python
features = stream.get_metrics([
    {"metric": "length"},
    {"metric": "duration"},
    {"metric": "active_days"},
    {"metric": "event_count_bulk"},
    {"metric": "matches_pattern", "metric_args": {"pattern": "login->.*->purchase"}},
])
```
