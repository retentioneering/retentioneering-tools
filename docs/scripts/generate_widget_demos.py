#!/usr/bin/env python3
"""Generate static HTML demos for every <DemoWidget> tag in docs/templates.

Each <DemoWidget cmd={`...`} path="..." height={N} /> found in
docs/templates/widgets/*.md.jinja is executed against the bundled ecom
dataset, and the resulting widget is exported as a standalone HTML file
via the widget's own `export_html()` (the same static-export mechanism
used for full report exports). Output paths mirror the `path` attribute,
rooted at docs/build/demos/ — retentioneering-web serves that directory
through a `public/docs-demos` symlink and iframes it from `<DemoWidget>`.

Usage (from repo root):
    uv run python docs/scripts/generate_widget_demos.py
"""

from __future__ import annotations

import re
from pathlib import Path

from retentioneering.datasets.ecom import load_ecom
from retentioneering.eventstream import Eventstream

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
TEMPLATES_DIR = REPO_ROOT / "docs" / "templates"
DEMOS_ROOT = REPO_ROOT / "docs" / "build" / "demos"

# Matches: <DemoWidget cmd={`...`} path="..." height={N} />
# cmd is a JS template literal so the Python call inside can freely use
# double quotes without escaping (mirrors the hopscotch-fe DemoWidget syntax).
_DEMO_RE = re.compile(
    r"<DemoWidget\s+cmd=\{`(?P<cmd>.*?)`\}\s+path=\"(?P<path>[^\"]+)\"",
    re.S,
)


def find_demo_tags() -> list[tuple[str, str, Path]]:
    """Return (cmd, path, source_file) for every <DemoWidget> tag found."""
    tags = []
    for template_file in sorted(TEMPLATES_DIR.rglob("*.md.jinja")):
        text = template_file.read_text(encoding="utf-8")
        for match in _DEMO_RE.finditer(text):
            tags.append((match.group("cmd"), match.group("path"), template_file))
    return tags


def build_stream() -> Eventstream:
    df = load_ecom()
    return Eventstream(
        df,
        schema={
            "path_cols": ["user_id", "session_id"],
            "segment_cols": [
                "platform",
                "acquisition_channel",
                "user_cohort",
                "user_lifecycle",
            ],
        },
    )


def main() -> None:
    stream = build_stream()
    tags = find_demo_tags()
    if not tags:
        print("No <DemoWidget> tags found under docs/templates/.")
        return

    for cmd, route, source_file in tags:
        # route looks like "/docs-demos/widgets/transition-graph/default.html";
        # everything after "/docs-demos/" is the path under docs/build/demos/.
        relative = route.removeprefix("/docs-demos/")
        out_path = DEMOS_ROOT / relative
        out_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            widget = eval(cmd, {"stream": stream})  # noqa: S307 -- trusted, hand-authored template content
            widget.export_html(str(out_path))
        except Exception as exc:
            raise RuntimeError(
                f"Failed to render demo from {source_file.relative_to(REPO_ROOT)}: {cmd!r}"
            ) from exc

        print(f"wrote {out_path.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
