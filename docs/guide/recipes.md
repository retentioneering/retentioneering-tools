# Recipes

Short recipes that map common product questions to retentioneering tools. Each one is a starting point, not a full tutorial — follow the links for details on every tool involved. Throughout this page, `stream` is an [Eventstream](/docs/eventstream) instance.

## Explain an anomalous period

Conversion dipped last week — was it the payment gateway incident, the new release, or a bot spike? Create a dynamic [segment](/docs/segments) that separates the anomalous window from normal operation, then diff the two groups on any widget:

```python
stream = stream.add_segment(
    "incident",
    sql="""
        SELECT CASE
            WHEN timestamp BETWEEN '2024-03-10' AND '2024-03-17' THEN 'inside'
            ELSE 'outside'
        END
        FROM eventstream
    """,
)
stream.transition_graph(diff=("incident", "inside", "outside"))
```

The graph highlights exactly which transitions degraded during the window. [Segment Overview](/docs/widgets/segment-overview) with the same segment scans many metrics at once when you don't yet know where to look. See [Segments](/docs/segments#inside-vs-outside-an-anomalous-period) for why a *dynamic* segment is the right tool here.

## Open up the funnel

A [Funnel](/docs/widgets/funnel) tells you 40% of paths drop between `add_to_cart` and `checkout_start` — but not what those users did instead. Two moves recover the lost context.

First, look *between* the levels: trim each path to the window between two funnel steps and map what actually happens there:

```python
stream.truncate_paths(start_event="add_to_cart", end_event="checkout_start").transition_graph()
```

Second, follow the users who never made it. The `funnel_events` mode of [Add Segment](/docs/data-processors/add-segment) labels each path with the last funnel step it reached, so drop-offs at each level become comparable groups:

```python
labelled = stream.add_segment("funnel", funnel_events=["add_to_cart", "checkout_start", "purchase"])
labelled.transition_graph(diff=("funnel", "add_to_cart", "purchase"))
```

For funnels where order matters more than a rigid step list, the `matches_pattern` [path metric](/docs/path-metrics) expresses a level as a sequence pattern — `"search->.*->purchase"` matches any path that searched and *later* purchased, whatever happened in between.

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
stream.filter_events(keep={"platform": ["mobile"]}).step_matrix(diff=("ab_arm", "test", "control"))
```

## Compare acquisition channels

Users from different channels arrive with different intent, and their journeys show it. With the channel declared in the schema's `segment_cols`, every widget can compare one channel against all the others using the `<REST>` shorthand:

```python
stream.step_matrix(diff=("acquisition_channel", "paid_search", "<REST>"))
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
stream.get_metrics([
    {"metric": "time_between", "metric_args": {"start_event": "path_start", "end_event": "purchase"}},
])
```

Add an `agg` and the same config becomes a [Segment Overview](/docs/widgets/segment-overview) row, so you can compare activation speed across channels, platforms, or A/B arms.

## Extract behavioral features for ML

Churn and LTV models improve when they see *behavior*, not just demographics. `get_metrics()` turns any set of [path metric](/docs/path-metrics) configs into a clean per-path feature table, ready to join with your training data:

```python
features = stream.get_metrics([
    {"metric": "length"},
    {"metric": "duration"},
    {"metric": "active_days"},
    {"metric": "event_count", "metric_args": {"event": "search"}},
    {"metric": "matches_pattern", "metric_args": {"pattern": "login->.*->purchase"}},
])
```
