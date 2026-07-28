# Path Analysis

A real event log contains from a hundred to a million paths. Although you may want to look at individual paths, mostly you look at them as a whole. Every visualization in this library is an aggregated representation of the same trajectories that keeps some aspects of each path. Retentioneering builds three such representations: by step, by transition, by milestone. This page demonstrates each of them and explains how to choose the right one for your question.

## A path is the unit of analysis

A raw event log has one row per event. The journey appears once you group rows by who did them and sort by time. That ordered sequence is a *path*. What counts as a path is your choice, not the library's. Typically, you want to switch between user-level and session-level paths. See [Eventstream key concepts](/docs/eventstream#key-concepts) for how to declare and switch between path definitions.

Those five paths are the running example for the rest of this page:

![A raw event log with one row per event, grouped and sorted into five ordered paths](/docs-demos/img/paths.svg "Left: a raw event log, one row per event. Right: the same rows grouped by user and sorted by time.")

Loaded into an [Eventstream](/docs/eventstream) — the object every tool on this page is called on — they look like this:

```python
import pandas as pd
import retentioneering as rete

paths = {
    "u1": ["home", "catalog", "cart", "purchase"],
    "u2": ["home", "catalog", "cart", "purchase"],
    "u3": ["home", "catalog", "catalog", "cart"],
    "u4": ["home", "search", "cart"],
    "u5": ["home", "search", "search"],
}
df = pd.DataFrame(
    [
        {"user_id": path_id, "event": event, "timestamp": f"2024-01-01 10:0{step}"}
        for path_id, events in paths.items()
        for step, event in enumerate(events)
    ]
)
stream = rete.Eventstream(df)
```

Every widget shown below is that `stream` — so each one renders exactly the five paths above, and you can check its numbers by hand. See [Eventstream](/docs/eventstream) for loading your own data, naming columns and declaring a schema.

You will also meet two events that appear in no event log: `path_start` and `path_end`. Retentioneering automatically brackets every path with them depending on the `path_col` argument that is given to the widget.

## Representation 1: by step

A natural way to visualize paths is to stack them up so they would form a **trajectory tree**.

![Five paths merged into a trajectory tree, each node carrying the number of paths passing through it](/docs-demos/img/trajectory-tree.svg "The five example paths stacked into a trajectory tree: identical beginnings share a branch, and each node carries the number of paths passing through it.")

On five paths it is the clearest view available: every route is visible, and each branch carries the number of paths on it. On real data it stops working. Branching compounds at every step, so even a few thousand paths over a few dozen event names give a tree with nearly as many leaves as there are paths. Nothing repeats often enough to be legible, and no layout rescues it.

The way out is to give up *which branch* a path came from and keep *where in the path it is*: group the paths by step and look at the distribution of events at each step. Everything at the same depth carrying the same event name collapses into one number.

![The step matrix of the five example paths: one row per event, one column per step](/docs-demos/img/step-matrix.svg "The same tree counted by step. The two cart nodes at step 3 — one reached via catalog, one via search — merge into a single cell: 2 + 1 = 3 of 5 paths.")

That table is the [Step Matrix](/docs/widgets/step-matrix): one row per event, one column per step, each cell is the share of paths sitting on that event at that step. **Every column sums to 1**, because every path is somewhere at every step — including the paths that are already over, which pile up in `path_end`. And **a cell counts paths, not events**: e.g. `cart` at step 3 is `0.6` because three of the five paths had `cart` as their third event.

<DemoWidget cmd={`stream.step_matrix()`} path="/docs-demos/guide/path-analysis/step-matrix.html" height={250} stepWindow={4} sidebarOpen={false} />

[Step Sankey](/docs/widgets/step-sankey) draws the exact same numbers as flows between adjacent columns instead of as a heatmap — the two widgets share a single headless method, `step_sankey_data()`. Use the matrix to compare one event across many steps, the Sankey to watch volume move from one step to the next.

<DemoWidget cmd={`stream.step_sankey()`} path="/docs-demos/guide/path-analysis/step-sankey.html" height={340} stepWindow={4} sidebarOpen={false} />

**Blind spot: which branch a path came from.** The two ways of reaching `cart` at step 3 — via `catalog` and via `search` — merge into a single `0.6` cell, and nothing downstream can tell them apart. In particular, a Sankey ribbon does not track individual paths across more than one hop: following three ribbons in a row does *not* give you the share of users who took that three-event route. See in the next section how to capture narrower sub-sequences with a `path_pattern`.

### Aligning on any event

Steps are counted from the beginning of each path by default. That is the right frame for onboarding questions and the wrong one for almost everything else — if the thing you care about happens at step 4 for some users and step 40 for others, aligning at the start smears it across many columns.

`path_pattern` can re-anchor the columns on an event instead. Every path is shifted until that event sits in column 0. The steps before/after it get negative/positive numbers correspondingly:

![Five paths shifted so that each path's cart event sits in column zero, with negative column numbers before it](/docs-demos/img/path-pattern.svg 'Every path shifted until its cart event sits in column 0. u5 never reaches it at all and drops out of the view.')

Now column −1 answers "what do users do immediately before the cart?" (75% `catalog`, 25% `search`) and column +1 answers "what happens right after?" (50% `purchase`, 50% the path ends). Neither question is answerable from the start-anchored view.

<DemoWidget cmd={`stream.step_matrix(path_pattern="cart")`} path="/docs-demos/guide/path-analysis/step-matrix-anchored.html" height={250} sidebarOpen={false} />

Note the denominator: `u5` never reaches `cart`, so it is not in this picture at all, and the shares are out of the four paths that do. Re-anchoring silently redefines the population to "paths that match the pattern" — which is usually what you want, and always worth knowing.

Two more anchors worth having:

- `path_pattern="path_end"` — [centers on where paths stop](/docs/widgets/step-matrix#path-pattern---drop-off-points), so the columns to its left are the last things users did before leaving.
- `path_pattern="catalog->.*->purchase"` — [several anchors in sequence](/docs/widgets/step-matrix#path-pattern---funnel-patterns), where `.*` matches any run of events. This allows you to explore steps surrounding events in a funnel pattern.

## Representation 2: by transition

Now drop position altogether and look at one transition at a time. Every path breaks down into the pairs of events that follow each other in it (bigrams, if you know the term from text analysis). For example, `u3`, `home → catalog → catalog → cart`, gives the pairs `home→catalog`, `catalog→catalog` and `catalog→cart`. Transition graph is a graph where nodes are unique events, edges are transitions between them, and the edge weight is some numerical characteristic of the transition, e.g. transition count.

![Five paths on the left; on the right their adjacent event pairs counted into a transition graph with weighted edges and self-loops](/docs-demos/img/event-aggregation.svg "Building a transition graph from paths. Edge weight is the number of transitions.")

<DemoWidget cmd={`stream.transition_graph(edge_weight="count")`} path="/docs-demos/guide/path-analysis/transition-graph.html" height={480} nodePositions={{"path_start": {"x": -460, "y": 0}, "home": {"x": -260, "y": 0}, "catalog": {"x": -40, "y": -130}, "search": {"x": -40, "y": 130}, "cart": {"x": 180, "y": 0}, "purchase": {"x": 180, "y": 130}, "path_end": {"x": 600, "y": 0}}} sidebarOpen={false} />

This buys two things the step representation cannot give you:

- **Loops become visible.** `u3`'s two consecutive `catalog` events and `u5`'s two `search` events are pairs like any other, so they turn into self-loops. Users going in circles are one of the most common findings in real event logs, and this is the representation that shows them directly.
- **The picture stays finite.** However long the paths, the graph has one node per distinct event — so it works as a map of the product, while the step representation grows a column per step.

A transition can be scored by more than a count: by how many distinct paths made it, by a transition probability (a.k.a. [Markov transition probabilities](https://en.wikipedia.org/wiki/Discrete-time_Markov_chain), the default option), by how long it takes. See the full list is on the [Transition Graph](/docs/widgets/transition-graph#edge-weights) page.

**Blind spot: when it happened.** A `cart → purchase` edge of weight 2 says two transitions happened somewhere; it does not say whether it was on step 3 or step 30, or whether they happened in the same path, or what exactly preceded them (the latter explains why Markov chains are called "memoryless" models).

## Representation 3: by milestone

A [Funnel](/docs/widgets/funnel) keeps only the events you name, and asks of each path: did it reach these, in this order? Everything in between is ignored.

<DemoWidget cmd={`stream.funnel(steps=["home", "cart", "purchase"])`} path="/docs-demos/guide/path-analysis/funnel.html" height={420} sidebarOpen={false} />

Of the five paths, all five hit `home`, four reach `cart` (`u5` never does) and two go on to `purchase` — 100% → 80% → 40%.

**Blind spot: everything that is not a milestone** — which is exactly why a funnel is easy to explain to stakeholders and unable to tell you *why* the drop happened. See [how to analyze what happens between funnel levels](/docs/recipes#open-up-the-funnel).

## Comparing groups of paths

Every representation above summarizes one population of paths, while most real questions are comparative: this cohort against that one, before against after, converted against not. Define the groups as a [segment](/docs/segments) — an A/B arm, a platform, an acquisition channel, a time window, a behavioral cluster, a rule over your own columns — and every core widget will render `group1 − group2` in place of one group's numbers ([diff mode](/docs/widgets#diff-mode)).

Three important cases that are widely used in practice:

- **An anomalous period.** A dynamic segment splits the eventstream into "inside the window" and "outside", and a diffed widget shows which steps or transitions actually degraded there — see [Find a root cause in an anomalous period](/docs/recipes#find-a-root-cause-in-an-anomalous-period).
- **How far a path got in a funnel.** `add_segment(..., funnel_events=[...])` labels every path with the deepest funnel level it completed *in order*, so "stalled at `cart`" and "converted" become two comparable groups — see [Open up the funnel](/docs/recipes#open-up-the-funnel).
- **The user's state.** A user is not the same person in their first session and in their fiftieth. `split_sessions` numbers each path's sessions, and a rule over that number turns it into named stages — first session, getting familiar, experienced — so the diff compares the *same* people at different points of their lifetime — see [Compare newcomers with experienced users](/docs/recipes#compare-newcomers-with-experienced-users).

Because both groups are still summarized from raw events, the difference lands on a concrete event, step or transition instead of on another aggregate rate — which is what makes this a root-cause tool rather than a reporting one.

**More than two levels?** A segment often has more levels than that — six acquisition channels, dozens of countries or regions — and diffing every pair is not a plan. [Segment Overview](/docs/widgets/segment-overview) puts [metrics](/docs/path-metrics) in rows and segment levels in columns, so one heatmap tells you which level is the outlier and on which metric. That is the big picture; the diff is the drill-down that follows it.

**Need behavioral segments?** Every example above starts from a rule you already had in mind. When you don't have one, [Cluster Analysis](/docs/widgets/cluster-analysis) goes the other way round — it groups paths by their [metrics](/docs/path-metrics), suggests a splitting, and lets you inspect what each cluster actually contains. Once clustered, it becomes an ordinary segment column: from there it is the same `diff` and the same three representations as everything else on this page.

## Preparing the data

Event logs rarely arrive in a shape worth analysing. Event names are whatever the tracking code happened to emit, passing-by users and bots sit next to meaningful ones, sessions are not marked, etc. [Data processors](/docs/data-processors) are small methods built for exactly this kind of work — filtering, renaming, collapsing, splitting, labelling. Each one returns a new `Eventstream`, so they chain into a readable pipeline and never modify what they were called on:

```python
stream = (
    rete.Eventstream(df)
    .filter_events(drop={"event": ["checkout_bug"]})
    .split_sessions(timeout="30m")
    .truncate_paths(start_event="path_start", end_event="purchase")
)
```

Processors validate event names against your data, so a misspelled event raises instead of silently filtering nothing — see [Data Processors](/docs/data-processors#event-names-are-validated).

### When the picture is unreadable

Retentioneering's widgets are built for high-cardinality event logs — hundreds of distinct event names — but applied head-on to a raw log they can still come out as a hairball of a graph or a plate of Sankey spaghetti. That is rather a data-preparation signal, not a rendering problem. Consider to try the following data processors:

- [`filter_events`](/docs/data-processors/filter-events) / [`drop_events`](/docs/data-processors/drop-events) — remove technical and bot events that split the paths without meaning anything.
- [`filter_paths`](/docs/data-processors/filter-paths) — remove short paths or paths that do not follow the pattern you are interested in.
- [`collapse_events`](/docs/data-processors/collapse-events) — merge related events (all the checkout steps into one `checkout`), or squash runs of repeats.
- [`truncate_paths`](/docs/data-processors/truncate-paths) — cut every path down to the window you are actually asking about.

Also, treating sessions as paths might help: [Path as a sequence of sessions](/docs/recipes#path-as-a-sequence-of-sessions).

## Which tool for which question

| Question | Tool | Why |
|---|---|---|
| How does the product connect up? Where do users loop? | [Transition Graph](/docs/widgets/transition-graph) | The only representation that shows repetition and cycles |
| What happens right before / right after event X? | [Step Matrix](/docs/widgets/step-matrix) or [Step Sankey](/docs/widgets/step-sankey) with `path_pattern="X"` | Position relative to an anchor is preserved |
| How do the first N steps unfold? | Step Sankey, no pattern | Start-anchored steps are the question |
| Where do users leave? | Step Matrix with `path_pattern="path_end"` | Anchors on the ending |
| What share reaches these milestones in order? | [Funnel](/docs/widgets/funnel) | Ignores everything between them, on purpose |
| Do two groups behave differently? | Any of the above with [`diff`](/docs/widgets#diff-mode) | Renders group1 − group2 in place |
| Which of many segment levels stands out? | [Segment Overview](/docs/widgets/segment-overview) | Scans every level on metrics at once, so you know which pair to diff |
| Which behaviors exist at all? | [Cluster Analysis](/docs/widgets/cluster-analysis) | Groups paths by metrics instead of by shape, and saves the result as a segment |

## Asking an agent instead

The same techniques introduced on this page could be used by an LLM agent with the help of the built-in [MCP server](/docs/mcp-server) or [skills](/docs/agent-skills).

## Next steps

- [Widgets](/docs/widgets) — the shared behaviour of every visualization: diff mode, headless twins, HTML export.
- [Segments](/docs/segments) — how to define the groups you compare.
- [Recipes](/docs/recipes) — these ideas applied to concrete product questions.
- [Data processors](/docs/data-processors) — how to prepare the data for analysis.
- [MCP server](/docs/mcp-server) — connecting an agent, the full tool list, and what it can and cannot do.
