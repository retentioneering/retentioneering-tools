# Widgets

Widgets are interactive visualizations that run directly in your notebook. Each widget connects to your eventstream, computes the relevant analysis, and renders an interactive UI where you can adjust parameters and explore the data without writing additional code.

## Creating a widget

Every widget is available as a method on the [Eventstream](/docs/eventstream) object. In the examples throughout this section, `stream` refers to an `Eventstream` instance.

```python
stream.transition_graph()
stream.step_sankey()
stream.step_matrix()
stream.funnel()
stream.segment_overview()
stream.cluster_analysis()
```

All parameters are optional. You can call any widget without arguments and configure it interactively using the sidebar that appears on the right side of the widget.

## Common parameters

All widgets share the following parameters:

| Parameter | Type | Description |
|---|---|---|
| `path_col` | `str \| None` | Override the path ID column from the schema. |
| `height` | `int` | Widget height in pixels. |
| `sidebar_open` | `bool` | Whether the settings sidebar starts open. Default: `True`. |
| `state_file` | `str \| None` | JSON file the widget state is bound to. See [Saving widget state](#saving-widget-state). |

Each widget's parameters split into two groups. **Data parameters** change the
computed result — they are exactly the arguments of the widget's headless
`*_data()` twin (see [Headless mode](#headless-mode)). **Display parameters**
(`height`, `sidebar_open`, ...) only affect how the widget is rendered. The
per-widget documentation pages use the same two groups.

## Interactive configuration

Every widget has a settings sidebar where you can adjust all parameters interactively. Changes take effect immediately when you click **Apply**. You do not need to re-run the notebook cell.

Passing arguments and configuring the widget in the sidebar are equivalent. For example, these two are the same:

```python
# Configure via arguments
stream.funnel(steps=["page_view", "add_to_cart", "purchase"])

# Configure interactively — open the widget, add the same steps in the sidebar
stream.funnel()
```

Arguments are a convenient way to reproduce a specific configuration without clicking through the UI. They are also useful when sharing notebooks — a reader can see the configuration at a glance without opening the widget.

## Saving widget state

Every widget accepts a `state_file` parameter — a path to a JSON file the
widget state is bound to, e.g. `stream.transition_graph(state_file="checkout.json")`.
The file is loaded if it exists and created otherwise, and every subsequent
change is auto-saved to it. It captures the full widget configuration — data
and display parameters plus extras like node layout, filters, sorting, scroll
position, and zoom (results are recomputed, not saved) — so re-running the
cell restores the widget exactly as you left it. Explicitly passed arguments
override the loaded state.

## Diff mode

Transition Graph, Step Sankey, Step Matrix, and Funnel support diff mode, which overlays two groups in the same visualization so you can compare their behavior directly.

Pass a 3-element list `[segment_col, value1, value2]` to the `diff` parameter:

```python
stream.funnel(
    steps=["page_view", "add_to_cart", "purchase"],
    diff=["plan", "pro", "free"],
)
```

Use the reserved value `<REST>` as `value2` to compare a segment against everyone else:

```python
stream.transition_graph(diff=["country", "US", "<REST>"])
```

Diff mode can also be configured interactively in the widget sidebar.

## Headless mode

Every widget has a corresponding **headless** method that runs the same computation and returns the raw data instead of rendering a widget. This is useful for building custom visualizations, exporting results, or running analysis in automated pipelines.

Headless methods follow the naming pattern `<widget>_data()`:

| Widget | Headless |
|---|---|
| `stream.transition_graph()` | [`stream.transition_graph_data()`](/docs/widgets/transition-graph#headless-mode) |
| `stream.step_sankey()` | [`stream.step_sankey_data()`](/docs/widgets/step-sankey#headless-mode) |
| `stream.step_matrix()` | [`stream.step_matrix_data()`](/docs/widgets/step-matrix#headless-mode) — alias of `step_sankey_data()`; both widgets share one computation |
| `stream.funnel()` | [`stream.funnel_data()`](/docs/widgets/funnel#headless-mode) |
| `stream.segment_overview()` | [`stream.segment_overview_data()`](/docs/widgets/segment-overview#headless-mode) |
| `stream.cluster_analysis()` | [`stream.cluster_analysis_data()`](/docs/widgets/cluster-analysis#headless-mode) |

```python
# Get transition matrix as a DataFrame
tm = stream.transition_graph_data(edge_weight="proba_out")

# Get funnel results as a dict
result = stream.funnel_data(steps=["page_view", "add_to_cart", "purchase"])
```

Headless methods accept the same parameters as their widget counterparts, excluding those that are needed for visualization only, like `height`.
