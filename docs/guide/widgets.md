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
| `path_id_col` | `str \| None` | Override the path ID column from the schema. |
| `height` | `int` | Widget height in pixels. |
| `sidebar_open` | `bool` | Whether the settings sidebar starts open. Default: `True`. |

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

Transition Graph and Step Matrix accept a `cloud_file_name` parameter for saving/restoring widget configuration (including manual layout tweaks) to the cloud:

```python
stream.transition_graph(cloud_file_name="my_graph")
stream.step_matrix(cloud_file_name="my_matrix")
```

Cloud save requires a Supabase-backed service that isn't part of the open-source library by default — it's disabled unless `RETENTIONEERING_CLOUD_ENABLED` and the related Supabase environment variables are configured for your own deployment. Without that setup, the cloud icon is hidden and `cloud_file_name` has no effect.

### Overriding saved state with arguments

Any argument passed explicitly takes precedence over the saved state. This lets you load a saved configuration and adjust one parameter without touching the rest:

```python
# Load saved configuration but override the height
stream.transition_graph(cloud_file_name="my_graph", height=600)
```

## Diff mode

Transition Graph, Step Sankey, Step Matrix, and Funnel support diff mode, which overlays two groups in the same visualization so you can compare their behavior directly.

Pass a 3-element list `[segment_col, value1, value2]` to the `diff` parameter:

```python
stream.funnel(
    steps=["page_view", "add_to_cart", "purchase"],
    diff=["plan", "pro", "free"],
)
```

Use the special value `<OUTER>` as `value2` to compare a segment against everyone else:

```python
stream.transition_graph(diff=["country", "US", "<OUTER>"])
```

Diff mode can also be configured interactively in the widget sidebar.

## Headless mode

Every widget has a corresponding **headless** method that runs the same computation and returns the raw data instead of rendering a widget. This is useful for building custom visualizations, exporting results, or running analysis in automated pipelines.

Headless methods follow the naming pattern `<widget>_data()`:

| Widget | Headless |
|---|---|
| `stream.transition_graph()` | [`stream.transition_graph_data()`](/docs/widgets/transition-graph#headless-mode) |
| `stream.step_sankey()` | [`stream.step_sankey_data()`](/docs/widgets/step-sankey#headless-mode) |
| `stream.step_matrix()` | [`stream.step_sankey_data()`](/docs/widgets/step-matrix#headless-mode) |
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
