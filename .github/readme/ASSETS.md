# README media

Every image in the README is a real Retentioneering widget rendered in a browser.
The datasets are synthetic teaching fixtures (the bundled e-commerce store and the
agent-run fixture in `notebooks/agent_path_analysis.ipynb`), and the recordings show
how the interface works rather than evidence of a real business outcome.

## Where each file comes from

| File | README section | Made by |
|---|---|---|
| `transition-graph.gif` | header | recorded by hand in JupyterLab, no script (see below) |
| `kpi-diff.png`, `funnel.png`, `error-sankey.png`, `last-error-sankey.png`, `abandoned-sankey.png`, `visit-graph.png` | Common use cases | `docs/scripts/capture_readme_screenshots.py` |
| `agent-paths.png` | Explore agent runs | `docs/scripts/capture_readme_screenshots.py` |
| `step-matrix-funnel.gif` | use case 2 | `docs/scripts/record_readme_step_matrix.py` |
| `cluster-analysis.gif` | use case 5 | `docs/scripts/record_readme_cluster_analysis.py` |
| `agent-report.gif` | Work with an AI agent | `docs/scripts/record_readme_agent_report.py` |

The screenshots run the code shown next to them in the README; the scripts hold the
same calls, so update both together.

## What the recordings show

Frames are actual browser states. No chart value is drawn or edited by hand. Frame
durations are chosen for reading, so pauses are longer than in real time, and a
drawn cursor is overlaid where an interaction is shown. Height, sidebar and frame
width are set for presentation only.

- **`transition-graph.gif`** (about 24 s). Recorded from a live widget in JupyterLab
  with retentioneering 5.2.3, since turning on comparison needs a kernel. It selects
  the saved path-to-purchase view, returns to Default so the comparison colors are
  not hidden behind the path focus, turns on comparison in the sidebar, focuses the
  payment step, opens its ego view and follows three neighbors (payment error,
  support chat, back to payment details). The
  [tour notebook](../../notebooks/retentioneering_5_tour.ipynb) opens on the same
  saved view. Cursor and click rings are drawn over the frames, and the route
  statistics badge of the path view is hidden, because its `P(route) 0.00%` for that
  exact contiguous route reads as an error out of context.
  The clip compares baseline minus incident, while the first README use case
  compares incident minus baseline, so the signs and colors are reversed between
  them. Check the group order before carrying a color reading from one to the other.
- **`kpi-diff.png`** focuses `payment_details` through the graph's search, the node
  the README tells readers to click.
- **`step-matrix-funnel.gif`**: sessions grouped by their deepest completed step of
  the ordered cart → shipping → purchase funnel, aligned on `cart` and
  `shipping_details`. It hovers cells to show each group's values, then sorts rows by
  following and by preceding steps. A shipping-stage session may contain a purchase
  outside that ordered funnel; the group is not a blanket non-buyer label. The frame
  is wide enough that hover tooltips stay inside it, and the page gets the
  sans-serif font a notebook would give the widget.
- **`cluster-analysis.gif`**: recorded in a JupyterLab that the script starts itself,
  because Apply and switching partitions recompute in Python. It starts from a bare
  `cluster_analysis()` call and goes through the sidebar, Apply, the Silhouette tab,
  renaming in the header and Save Clusters. Native `<select>` popups do not appear in
  headless screenshots, so a list with the same options is drawn over the real field
  while a value is picked; the value itself is set on the real field. The overview
  table is scrolled through its own container. The cluster names (`browsers`,
  `researchers`, `buyers`, `light_users`) describe each cluster's event profile.
- **`visit-graph.png`** names the four session clusters by their event profile
  (`browsing`, `quick_visit`, `purchase_visit`, `promo_visit`). KMeans uses a fixed
  seed, so the numbering is stable for a given scikit-learn version, here and in the
  cluster recording.
- **`agent-paths.png`**: the graph that `agent_path_analysis.ipynb` exports, average
  transitions per run, failed minus succeeded. The fixture has six failed and ten
  successful runs; complete retry patterns are checked separately in the notebook.
- **`agent-report.gif`**: a three-tab report (transition graph, step matrix, segment
  overview) built with the MCP server's own tool functions and exported to HTML.
  Links in its text focus graph edges, step matrix cells and segment overview cells.
  The analysis text is written for this demo; every number in it is read from the
  tabs, and the script fails unless `check_analysis` passes. The links locate the
  evidence for a statement; they do not establish its cause. The GIF palette is
  sampled from every frame plus a band of the focus-ring color, otherwise the thin
  amber outline is merged into the cell colors.

## Reproduce

Use Python 3.10–3.13 and the repository's [development setup](../../CONTRIBUTING.md),
then build the widget JavaScript:

```bash
make install-dev
make build
```

The scripts drive a browser through Playwright for Python, which `uv run --with
playwright` provides. Set `CHROME_PATH` to use an installed Chrome instead of
Playwright's Chromium (otherwise install it once with
`uv run --with playwright python -m playwright install chromium`).

```bash
RETENTIONEERING_NO_TRACK=1 uv run --with playwright python docs/scripts/capture_readme_screenshots.py
RETENTIONEERING_NO_TRACK=1 uv run --with playwright python docs/scripts/record_readme_step_matrix.py
RETENTIONEERING_NO_TRACK=1 uv run --with playwright python docs/scripts/record_readme_cluster_analysis.py
RETENTIONEERING_NO_TRACK=1 uv run --with playwright python docs/scripts/record_readme_agent_report.py
```

Widget exports go to `docs/build/readme/` (gitignored); final media is written here.
The step matrix and report recorders fail on browser errors.

## Notebooks and Colab links

The two public notebooks are self-contained and committed without outputs; run their
cells in order to reproduce the analyses. The tour runs the README use-case code.
Colab links in the README and in the tour pin a tested repository revision, so the
examples keep working when a PR branch is deleted. After a merge, repin them to a
commit on `master`; a squash merge drops the pinned commit from history. The
relative notebook links lead to the current checkout.
