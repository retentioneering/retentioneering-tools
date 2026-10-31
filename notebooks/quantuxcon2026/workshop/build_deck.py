"""Build _deck.html, the deck script Quarto includes (include-after-body), from
deck.html and the Lumeo examples on the "What Retentioneering is" slide.

The three examples (lumeo_examples/*.html) are static widget pages. Each one
needs the widget bundle, which is over 1 MB, so the stored pages hold a
placeholder instead and the bundle is taken from the repository's build
(src/retentioneering/static/widget-static.js; `make build`). The finished
pages go into deck.html's EXAMPLES, so slides.html stays one file.

    python build_deck.py      (render.sh runs it before every render)
"""

from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
BUNDLE = HERE.parents[2] / "src" / "retentioneering" / "static" / "widget-static.js"
BUNDLE_MARK = "/*WIDGET_STATIC_JS*/"
EXAMPLES_MARK = "/*LUMEO_EXAMPLES*/{}"
# The order of EXAMPLES in deck.html.
EXAMPLES = ["03_graph_all", "04_first_open_matrix", "02_user_profiles"]


def main() -> None:
    if not BUNDLE.exists():
        raise SystemExit(f"build_deck.py: no {BUNDLE}; run `make build` first")
    bundle = BUNDLE.read_text()
    pages = {}
    for name in EXAMPLES:
        page = (HERE / "lumeo_examples" / f"{name}.html").read_text()
        assert page.count(BUNDLE_MARK) == 1, name
        pages[name] = page.replace(BUNDLE_MARK, bundle)
    # A JS string inside <script>: "</" must not close the tag.
    examples = json.dumps(pages).replace("</", "<\\/")
    deck = (HERE / "deck.html").read_text()
    assert deck.count(EXAMPLES_MARK) == 1
    (HERE / "_deck.html").write_text(deck.replace(EXAMPLES_MARK, examples))


if __name__ == "__main__":
    main()
