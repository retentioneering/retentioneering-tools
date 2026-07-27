#!/usr/bin/env python3
"""Generate static HTML demos for every <DemoWidget> tag in docs/templates
and docs/guide.

Each <DemoWidget cmd={`...`} path="..." height={N} /> found in
docs/templates/widgets/*.md.jinja or docs/guide/*.md is executed against the
bundled ecom dataset (or, for the pages listed in STREAM_BUILDERS, against that
page's own example data), and the resulting widget is exported as a standalone
HTML file via the widget's own `export_html()` (the same static-export
mechanism used for full report exports). Output paths mirror the `path`
attribute, rooted at docs/build/demos/ — retentioneering-web serves that
directory through a `public/docs-demos` symlink and iframes it from
<DemoWidget>.

Usage (from repo root):
    uv run python docs/scripts/generate_widget_demos.py
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

from retentioneering.datasets.ecom import load_ecom
from retentioneering.eventstream import Eventstream

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
TEMPLATES_DIR = REPO_ROOT / "docs" / "templates"
GUIDE_DIR = REPO_ROOT / "docs" / "guide"
DEMOS_ROOT = REPO_ROOT / "docs" / "build" / "demos"

# Matches: <DemoWidget cmd={`...`} path="..." height={N} stepWindow={N} />
# cmd is a JS template literal so the Python call inside can freely use
# double quotes without escaping. Attributes have no ">" in them, so the tail
# can be matched greedily up to the tag's own ">".
_DEMO_RE = re.compile(
    r"<DemoWidget\s+cmd=\{`(?P<cmd>.*?)`\}\s+path=\"(?P<path>[^\"]+)\"(?P<attrs>[^>]*)>",
    re.S,
)

# Tag attributes that set a widget trait instead of appearing in `cmd`. They
# are presentation, not analysis: spelling them out in the snippet would teach
# a reader to type something they don't need. `height` is deliberately the
# same number as the iframe's, so a page author has one knob rather than two.
_TRAIT_ATTRS = {
    "height": "height",
    "stepWindow": "step_window",
    "nodePositions": "node_positions",
}
# Traits carrying JSON text rather than a number. The tag writes them as a JSX
# object literal — `nodePositions={{"home": {"x": -260, "y": 0}}}` — whose
# inner expression is also valid JSON, so it round-trips through json.loads.
_JSON_TRAITS = {"node_positions"}

# The exported page centers the widget inside a 24px margin (see the template
# in widgets/_html_export.py), so an iframe fits a widget 48px shorter.
_EXPORT_CHROME_PX = 48


@dataclass(frozen=True)
class DemoTag:
    cmd: str
    route: str
    traits: dict[str, int | str]
    source_file: Path


def _jsx_expression(attrs: str, name: str) -> str | None:
    """Return the text inside ``name={...}``, matching braces so that an object
    literal (which contains braces of its own) comes back whole."""
    match = re.search(rf"\b{name}=\{{", attrs)
    if match is None:
        return None
    start = match.end() - 1
    depth = 0
    for i in range(start, len(attrs)):
        if attrs[i] == "{":
            depth += 1
        elif attrs[i] == "}":
            depth -= 1
            if depth == 0:
                return attrs[start + 1 : i]
    raise ValueError(f"unbalanced braces in <DemoWidget {name}=...>")


def _traits_from_attrs(attrs: str) -> dict[str, int | str]:
    traits: dict[str, int | str] = {}
    for attr, trait in _TRAIT_ATTRS.items():
        expr = _jsx_expression(attrs, attr)
        if expr is None:
            continue
        if trait in _JSON_TRAITS:
            traits[trait] = json.dumps(json.loads(expr))
        elif trait == "height":
            traits[trait] = max(int(expr) - _EXPORT_CHROME_PX, 120)
        else:
            traits[trait] = int(expr)
    return traits


def find_demo_tags() -> list[DemoTag]:
    """Return one DemoTag per <DemoWidget> tag found."""
    tags = []
    source_files = sorted(TEMPLATES_DIR.rglob("*.md.jinja")) + sorted(
        GUIDE_DIR.glob("*.md")
    )
    for source_file in source_files:
        text = source_file.read_text(encoding="utf-8")
        for match in _DEMO_RE.finditer(text):
            tags.append(
                DemoTag(
                    match.group("cmd"),
                    match.group("path"),
                    _traits_from_attrs(match.group("attrs")),
                    source_file,
                )
            )
    return tags


def build_stream() -> Eventstream:
    return load_ecom()


# The five hand-written paths that docs/guide/path-analysis.md explains every
# number on. This mirrors the snippet printed on that page, line for line —
# a reader has to be able to paste it and get back the same widgets, so the two
# must stay identical (and match docs/img/paths.svg and friends).
TOY_PATHS = {
    "u1": ["home", "catalog", "cart", "purchase"],
    "u2": ["home", "catalog", "cart", "purchase"],
    "u3": ["home", "catalog", "catalog", "cart"],
    "u4": ["home", "search", "cart"],
    "u5": ["home", "search", "search"],
}


def build_toy_stream() -> Eventstream:
    import pandas as pd

    df = pd.DataFrame(
        [
            {
                "user_id": path_id,
                "event": event,
                "timestamp": f"2024-01-01 10:0{step}",
            }
            for path_id, events in TOY_PATHS.items()
            for step, event in enumerate(events)
        ]
    )
    return Eventstream(df)


# Pages whose <DemoWidget> tags run against something other than the bundled
# ecom dataset. Inside a page, `stream` always means whatever that page's own
# text means by it.
STREAM_BUILDERS = {GUIDE_DIR / "path-analysis.md": build_toy_stream}


def main() -> None:
    streams: dict[Path, Eventstream] = {}

    def stream_for(source_file: Path) -> Eventstream:
        if source_file not in streams:
            builder = STREAM_BUILDERS.get(source_file, build_stream)
            streams[source_file] = builder()
        return streams[source_file]

    tags = find_demo_tags()
    if not tags:
        print("No <DemoWidget> tags found under docs/templates/.")
        return

    for tag in tags:
        # route looks like "/docs-demos/widgets/transition-graph/default.html";
        # everything after "/docs-demos/" is the path under docs/build/demos/.
        relative = tag.route.removeprefix("/docs-demos/")
        out_path = DEMOS_ROOT / relative
        out_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            widget = eval(tag.cmd, {"stream": stream_for(tag.source_file)})  # noqa: S307 -- trusted, hand-authored template content
            for trait, value in tag.traits.items():
                # Display-only traits: none of them triggers a recompute, so
                # setting them after construction is equivalent to passing them
                # to the call — without putting them in the printed snippet.
                if widget.has_trait(trait):
                    setattr(widget, trait, value)
            widget.export_html(str(out_path))
        except Exception as exc:
            raise RuntimeError(
                f"Failed to render demo from {tag.source_file.relative_to(REPO_ROOT)}: {tag.cmd!r}"
            ) from exc

        print(f"wrote {out_path.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
