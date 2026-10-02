# README demonstrations

The README uses real Retentioneering widgets. The datasets are synthetic teaching
fixtures, and the recordings are examples of interaction rather than evidence of
a real business outcome.

## Preserved recordings

`transition-graph.gif` and `agent-report.gif` are copied unchanged from
[`docs/readme-refresh` at e0c572b](https://github.com/retentioneering/retentioneering-tools/tree/e0c572b800312884de9b9a243d0f1663a1a37c56/.github/readme).
Their PNG companions are the final frames, provided for readers who prefer a
static view. The graph recording is 13.38 seconds long. It demonstrates a saved
path view, comparison settings and node focus; the
[existing tour](../../notebooks/retentioneering_5_tour.ipynb) covers this workflow.

The preserved graph clip uses baseline minus incident in its sidebar, while the
new README comparison example uses incident minus baseline. The sign and color
therefore reverse. The README describes each recording as an interface tour and
states the comparison direction beside the executable example; do not transfer
a color interpretation between them without checking the group order.

The report clip demonstrates links from a finding to a chart element. Its
authored causal interpretation is not independently established by those links.
Use the links to inspect evidence, then validate the analytical conclusion.

## New widget recordings and images

- `step-matrix.gif` / `.png`: sessions grouped by their deepest completed step in
  the ordered cart → shipping → purchase funnel, aligned on shipping. The GIF
  hovers purchase and path-end cells to expose group values. A shipping-stage
  session may contain a purchase outside that ordered funnel; it is not a
  blanket non-buyer label.
- `cluster-analysis.gif` / `.png`: event-count features, candidate cluster counts
  3–5, session grain. The GIF switches between the overview and silhouette tabs.
  Recomputing features, selecting another partition and saving clusters require
  the live notebook. The metric column is widened through the widget's resize
  handle so labels remain readable.
- `agent-paths.png`: the graph exported by
  [agent_path_analysis.ipynb](../../notebooks/agent_path_analysis.ipynb). It uses
  average transitions per run, failed minus succeeded. The data includes six
  failed and ten successful runs; complete retry patterns are checked separately.

The matrix and cluster animations hold actual browser states for two seconds per
frame. No chart values are drawn or edited by hand. Their static PNGs show the
initial state. Height and sidebar settings are adjusted only for presentation.

## Reproduce

Use Python 3.10–3.13 and the repository's [development setup](../../CONTRIBUTING.md).
Build the widget JavaScript before exporting from a source checkout:

```bash
make install-dev
make build
RETENTIONEERING_NO_TRACK=1 uv run python docs/scripts/generate_readme_demos.py
```

Then install Playwright and its browser in your chosen Node environment, and make
`ffmpeg` available. From the repository root:

```bash
node docs/scripts/record_readme_demos.cjs
```

The recorder also accepts `PLAYWRIGHT_PACKAGE` (module path), `CHROME_PATH`
(browser executable) and `FFMPEG` (executable path). Exports, captured frames and
browser-check results are written to `docs/build/readme/`; final media goes here.
The recorder fails on browser errors or unexpected external requests.

To regenerate the two preserved PNG posters with Pillow:

```python
from pathlib import Path
from PIL import Image

assets = Path(".github/readme")
for name in ("transition-graph", "agent-report"):
    with Image.open(assets / f"{name}.gif") as animation:
        animation.seek(animation.n_frames - 1)
        animation.convert("RGB").save(assets / f"{name}.png", optimize=True)
```

The two public notebooks are self-contained and intentionally committed without
executed outputs. Run the cells in order to reproduce the analyses. Colab links
in the README pin a tested repository revision so the examples survive deletion
of a PR branch. The relative notebook links lead to the current checkout.
