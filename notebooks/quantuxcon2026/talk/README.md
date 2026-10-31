# Where User Experience Diverges

*Visualizing Behavioral Segments Across Events, Sessions, and Product Journeys*,
the Quant UX Con 2026 talk by Maxim Godzi and Vladimir Kukushkin (2026-10-28,
about 7 minutes).

The question: why did people who reached the payment screen never order? A
funnel shows where they leave. A path pattern, a window after the payment
screen and clustering show what they did instead. A diff step matrix points at
one Android build.

## Files

- `slides.ipynb`: the slides, a Quarto revealjs notebook, styled like the
  workshop
- `deck.html`: the deck script (click steps, graph stacks, table frames),
  which Quarto includes after the slides; `custom.scss`: the theme
- `talk.py`: the case pipeline (select, cut a window, cluster) and the slide
  figures
- `analysis.ipynb`: the same analysis step by step, with the checks behind it
  (window sizes 5/10/20). Stored without outputs: run it to see them
- `render.sh`, `serve.py`, `export.py`, `bundle.py`: the build (below)

## Data

The talk reads `data/lumeo_events.csv.gz`, which is not tracked: the
synthetic Lumeo v8.7 event log by Maxim Godzi (seed 5044, 40,000 users,
2026-03-02 to 2026-05-25). Put the file there before rendering. Its generator
is Maxim's part and will be added separately.

## Rendering with Quarto

Do the setup from [the folder README](../README.md) first: `uv sync` and
`make build` in the repository root, and Quarto installed. Then, in this
folder:

```bash
./render.sh            # slides.html
./render.sh preview    # live preview at http://localhost:4400 (PORT=… to change)
./render.sh export     # export/where_ux_diverges.pdf and export/where_ux_diverges.html
```

- **`slides.html`** is the live deck. It needs the `_build_slides_files/`
  folder next to it, so open it where it was rendered.
- **`export/where_ux_diverges.pdf`** has one page per click (screenshots
  taken by `export.py`). `export/slides.pdf` and `export/slides.html` are the
  same frames.
- **`export/where_ux_diverges.html`** is the live deck in one file, the pen
  included: `C` toggles the pen, `B` the board. Quarto's `embed-resources`
  refuses the chalkboard plugin, so `bundle.py` inlines the rendered deck's
  scripts, styles and images itself.

`render.sh` never renders `slides.ipynb` itself. It renders a copy,
`_build_slides.ipynb`, because Quarto writes the executed notebook back into
the file it renders. It uses the repository's Python (`.venv`); set
`QUARTO_PYTHON` to use another one, or `QUARTO` to use another Quarto binary.

Plain Quarto works too, but it writes the outputs into `slides.ipynb`. Clear
them before you commit:

```bash
QUARTO_PYTHON=../../../.venv/bin/python quarto render slides.ipynb
```

In the preview, the page reloads by itself after each render.
`render.sh preview` re-renders when `slides.ipynb`, `deck.html`,
`custom.scss` or `talk.py` change.

## Results

These are the numbers on the slides. `analysis.ipynb` gives the same ones.

- Funnel, by user: 2,878 start checkout → 2,259 reach payment → 1,979 order.
- 280 people reached payment and never ordered. 212 of them reached it
  before 2026-05-11, so they had at least two weeks left to come back.
- Six clusters of the 10 events after the payment screen: payment · gone 43,
  review · gone 37, error · gone 27, payment · back 35, review · back 38,
  error · back 32.
- On Android 4.12.0, `payment_error` follows the payment screen far more
  often than on the other builds: +0.29 at step 1 in the diff step matrix.
