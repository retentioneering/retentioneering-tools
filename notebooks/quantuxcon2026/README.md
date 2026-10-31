# Quant UX Con 2026

Materials from Retentioneering's part of Quant UX Con 2026 (Maxim Godzi,
Vladimir Kukushkin).

| Folder | What it is |
| --- | --- |
| [`workshop/`](workshop/) | **From Complex User Flows to Clear Behavioral Patterns**, the hands-on workshop (2026-10-27), on the synthetic Taskboard dataset |
| [`talk/`](talk/) | **Where User Experience Diverges**, the 7-minute talk (2026-10-28), on the Lumeo v8.7 dataset |
| [`carousel/`](carousel/) | The LinkedIn carousel that announced the workshop |

Both decks are Quarto revealjs presentations made from a Jupyter notebook
(`slides.ipynb`). Each folder has its own README on how to build it. Generated
files (rendered slides, exports, datasets) are not tracked: the scripts in each
folder make them again.

## Setup (once, from the repository root)

```bash
uv sync      # Python: the library from this checkout, Jupyter
make build   # the widget JS bundle the slides embed
```

You also need [Quarto](https://quarto.org/docs/get-started/). The decks were
built with Quarto 1.10.19.

To export PDFs you also need Chromium for Playwright:

```bash
uv run --with playwright python -m playwright install chromium
```

The decks run against the library in this checkout, not against a PyPI
release. To get the exact code they were built with, check out the commit
that last changed this folder.

## Build

```bash
cd notebooks/quantuxcon2026/workshop   # or talk
./render.sh                             # slides.html
./render.sh preview                     # live preview, http://localhost:4400
./render.sh export                      # the PDF and single-file HTML in export/
```
