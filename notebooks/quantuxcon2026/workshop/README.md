# From Complex User Flows to Clear Behavioral Patterns

The Quant UX Con 2026 hands-on workshop on the Retentioneering library
(2026-10-27). It teaches the tools (transition graph, step matrix, clustering,
segment comparison, the MCP report) on toy paths and on Taskboard, a synthetic
task tracker for teams.

## Files

- `slides.ipynb`: the slides, a Quarto revealjs notebook
- `deck.html`: the deck script (click steps, widget frames, highlights), which
  Quarto includes after the slides; `custom.scss`: the theme
- `toy.py`: the toy examples and slide figures
- `taskboard.py`: the Taskboard generator; `LEGEND.md`: the story it encodes
- `events.csv`: the four-row example log shown on the slides
- `lumeo_examples/`: three static Lumeo widget pages for the "What
  Retentioneering is" slide, taken from the Lumeo v8.7 workshop package
  (`generated/`). The widget JS bundle is cut out of them to keep them small;
  `build_deck.py` puts the bundle from `make build` back and adds the pages to
  the deck script
- `img/`: portraits, screenshots and logos
- `render.sh`, `serve.py`, `export.py`, `bundle.py`: the build (below)

## Data

`taskboard.csv.gz` (5,000 workspaces, seed 1) is not tracked. `render.sh`
makes it when it's missing. To make it by hand:

```bash
../../../.venv/bin/python taskboard.py
```

The generator is deterministic: the same seed gives the same file.

## Rendering with Quarto

Do the setup from [the folder README](../README.md) first: `uv sync` and
`make build` in the repository root, and Quarto installed. Then, in this
folder:

```bash
./render.sh            # slides.html
./render.sh preview    # live preview at http://localhost:4400 (PORT=… to change)
./render.sh export     # export/slides.pdf, export/slides.html, export/workshop.html
```

- **`slides.html`** is the live deck. It needs the `_build_slides_files/`
  folder next to it, so open it where it was rendered.
- **`export/slides.pdf`** has one page per click. `export/slides.html` shows
  the same frames as images.
- **`export/workshop.html`** is the live deck in one file (`bundle.py`
  inlines its scripts, styles and images), so you can send it to someone.

`render.sh` never renders `slides.ipynb` itself. It renders a copy,
`_build_slides.ipynb`, because Quarto writes the executed notebook back into
the file it renders. It uses the repository's Python (`.venv`); set
`QUARTO_PYTHON` to use another one, or `QUARTO` to use another Quarto binary.

Plain Quarto works too, but it writes the outputs (tens of MB) into
`slides.ipynb`. Clear them before you commit:

```bash
../../../.venv/bin/python taskboard.py    # if taskboard.csv.gz is missing
../../../.venv/bin/python build_deck.py   # _deck.html, the script the slides include
QUARTO_PYTHON=../../../.venv/bin/python quarto render slides.ipynb
```

In the preview, the page reloads by itself after each render.
`render.sh preview` re-renders when `slides.ipynb`, `deck.html`,
`custom.scss`, `toy.py`, `taskboard.py` or the Lumeo examples change.
