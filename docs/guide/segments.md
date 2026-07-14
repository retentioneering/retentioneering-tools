# Segments

Segmentation is how you slice an eventstream into comparable groups: mobile vs desktop, paying vs free, during an incident vs before it. Almost every non-trivial analysis is a comparison, and segments are the mechanism that defines what is being compared.

In retentioneering a segment is a way to split paths, or parts of paths, into meaningful groups. A split is defined by a *segment column* that maps each event to a group: for example, a `country` column assigns a country to every event of a path, splitting the eventstream into segment levels `US`, `DE`, `FR`, and so on. Note the difference from Amplitude-style tools, where "creating a segment" means defining a single group of users; here one segment column describes a whole split at once.

```python
stream = Eventstream(df, schema={
    "segment_cols": ["country", "plan"],
})
```

Columns already present in your data become segments by listing them in the schema's `segment_cols` up front. If the `Eventstream` already exists — the column just rode along as a `custom_col` (see [Eventstream](/docs/eventstream#schema)) — call `add_segment` with no `rules`/`func`/`sql`/`funnel_events` argument to promote it in place, keeping its existing values:

```python
stream.add_segment("returned")
```

New segment columns are derived with [Add Segment](/docs/data-processors/add-segment) and [Add Clusters](/docs/data-processors/add-clusters), have their levels renamed with [Rename Segment Levels](/docs/data-processors/rename-segment-levels), and are removed with [Drop Segment](/docs/data-processors/drop-segment).

## Static and dynamic segments

Segment values are stored per event row. This gives two kinds of segments:

- **Static** — the value is constant for the whole path: `country`, `acquisition_channel`, an A/B test arm, a cluster label, the deepest funnel step reached in order. A static segment answers "*which paths* behave differently?"
- **Dynamic** — the value changes along the path, because it is assigned per event: `weekend` / `weekday` depending on each event's timestamp, `inside` / `outside` an incident window, a user state that evolves from `new` through `returning` to `loyal`. A dynamic segment answers "*which parts of a path* behave differently?"

The row-level modes of `add_segment` (`rules`, `sql`, `func`) can produce either kind — a static segment is simply one whose value happens to be constant within each path. The `funnel_events` mode and `add_clusters` always produce static, per-path segments.

Dynamic segments are handled naturally by the segment-aware tools. [Segment Overview](/docs/widgets/segment-overview) splits each path into its within-segment fragments, so the same user's behavior inside and outside a time window lands in separate columns of the heatmap. The `in_segment` [path metric](/docs/path-metrics) offers `any` / `all` / `event_share` modes to express "the path ever touched this segment", "the path stayed in it entirely", or "at least a given share of its events were in it".

## Creating segments

[Add Segment](/docs/data-processors/add-segment) supports five modes — exactly one must be used per call:

```python
# rules — ordered CASE-WHEN conditions
stream.add_segment(
    "region",
    rules=[
        ["country", "=", "US", "domestic"],
        ["country", "in", "('GB', 'DE', 'FR')", "europe"],
        ["other"],
    ],
)

# sql — a DuckDB SELECT returning one label per row
stream.add_segment(
    "device",
    sql="SELECT CASE WHEN platform = 'mobile' THEN 'mobile' ELSE 'web' END FROM eventstream",
)

# func — any Python function over the raw DataFrame
stream.add_segment("power_user", func=lambda df: df["user_id"].map(power_users_lookup))

# funnel_events — the deepest funnel step each path completed in order;
# skipping or reordering a step keeps it out of that step's group (out_of_funnel
# if even the first step was never completed in order)
stream.add_segment("funnel", funnel_events=["add_to_cart", "checkout_start", "purchase"])

# time_range — binary "inside" vs "outside" an inclusive timestamp interval
stream.add_segment("incident", time_range=("2024-03-10", "2024-03-17"))
```

[Add Clusters](/docs/data-processors/add-clusters) is a special case: it clusters paths by behavioral metrics with ML and stores the cluster labels as a new static segment, so clusters immediately work everywhere ordinary segments do.

## Common patterns

### Inside vs outside an anomalous period

When something unusual happens — a payment gateway incident, a broken release, a marketing spike, a bot attack — the first question is "how did behavior change?" A dynamic segment over the timestamp cleanly separates the anomalous window from normal operation. The `time_range` mode covers this directly — pass the `(start, end)` bounds and every event is labeled `inside` or `outside`:

```python
stream.add_segment("incident", time_range=("2024-03-10", "2024-03-17"))
```

For anything the binary inside/outside split doesn't cover — more than two buckets, or a boundary rule other than a plain inclusive interval — fall back to `sql`:

```python
stream.add_segment(
    "incident",
    sql="""
        SELECT CASE
            WHEN timestamp < '2024-03-10' THEN 'before'
            WHEN timestamp <= '2024-03-17' THEN 'during'
            ELSE 'after'
        END
        FROM eventstream
    """,
)
```

Because the segment is dynamic, a user active both during and outside the window contributes to both groups — you are comparing *behavior in the period* against *behavior outside it*, not "users who happened to be around that week" against everyone else. Pass it to a widget's diff mode (`diff=("incident", "inside", "outside")`) to see exactly which transitions or funnel steps degraded, or to [Segment Overview](/docs/widgets/segment-overview) to scan many metrics at once.

### Weekend vs weekday

Time-of-week (or time-of-day) segments reveal rhythm in product usage — different intent, different conversion, different session shape:

```python
stream.add_segment(
    "day_type",
    sql="""
        SELECT CASE WHEN isodow(timestamp) IN (6, 7) THEN 'weekend' ELSE 'weekday' END
        FROM eventstream
    """,
)
```

The same pattern with `hour(timestamp)` gives working-hours vs evening segments.

### User state

A user is not the same person in their first session and in their fiftieth. A state segment assigns each event the user's stage at that moment, so you can compare how the *same* users navigate at different stages of their lifetime:

- The bundled [e-commerce dataset](/docs/eventstream#sample-dataset) ships a `user_lifecycle` segment that evolves from `new` through `returning` to `loyal` session by session.
- [To Daily States](/docs/data-processors/to-daily-states) takes the state idea further: it converts the eventstream into one row per path per calendar day, labelled with engagement states (`new`, `current`, `reactivated`, `at_risk_wau`, `dormant`, ...), which is a natural input for retention and lifecycle analysis.
- Any custom state logic fits in `add_segment`'s `func` mode — compute the state per row in pandas and return the labels.

Combined with the `in_segment` metric, state segments answer questions like "what share of paths ever reached the `loyal` state" (`mode="any"`) or "which paths spent most of their events in `dormant`" (`mode="event_share"`).

## Using segments in analysis

Once a segment column exists, every segment-aware tool picks it up:

- **Diff mode** — [Transition Graph](/docs/widgets/transition-graph), [Step Sankey](/docs/widgets/step-sankey), [Step Matrix](/docs/widgets/step-matrix), and [Funnel](/docs/widgets/funnel) accept `diff=(segment_col, value1, value2)` to render the difference between two segments; `value2` may be `<REST>` to compare one segment against everyone else.
- **[Segment Overview](/docs/widgets/segment-overview)** — an interactive heatmap with metrics as rows and segment values as columns: the fastest way to scan where segments differ before diving into a specific widget. See its documentation page for parameters and examples.
- **`in_segment` path metric** — turns segment membership into a per-path value usable in [Path Metrics](/docs/path-metrics) contexts, including as a clustering feature in [Add Clusters](/docs/data-processors/add-clusters).
