#!/usr/bin/env python3
"""Render docs pages from Jinja2 templates + Eventstream docstrings.

Every output page has a matching template under docs/templates/ — open it to
see exactly what's on that page, top to bottom. A heading like "### Edge
weights" is a literal line in a template file (see
docs/templates/widgets/transition-graph.md.jinja), not something conjured by
conditional logic in this script. Templates pull in docstring content through
a couple of small helpers (`param_table`, `bullets`, both from
docstring_utils.py); everything else about a page's shape belongs in its
template.

Common structure lives in the two base templates
(docs/templates/_widget_base.md.jinja and _data_processor_base.md.jinja);
per-page templates `{% extends %}` a base and override a `{% block %}` only
where that page actually diverges (e.g. an extra section).

Usage (from repo root):
    uv run python docs/scripts/render_pages.py
"""

from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from pathlib import Path

import jinja2
from retentioneering.eventstream.eventstream import Eventstream
from docstring_utils import bullets, get_doc, render_param_table, split_by_headless

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
TEMPLATES_DIR = REPO_ROOT / "docs" / "templates"
GUIDE_DIR = REPO_ROOT / "docs" / "guide"
IMG_DIR = REPO_ROOT / "docs" / "img"
BUILD_DIR = REPO_ROOT / "docs" / "build"

SITE_URL = "https://retentioneering.com"

TITLE_OVERRIDES = {"urls_to_events": "URLs to Events"}

# Guide pages in reading order, with the label the docs site shows in its
# sidebar. Copying a guide page needs no such list (that's a plain glob), but
# llms.txt does: it is an ordered table of contents, and alphabetical order
# would put "Tracking" between "Segments" and "Widgets". Mirrors the NAV
# constant in retentioneering-web's app/docs/layout.tsx; write_llms_files()
# fails loudly if a file in docs/guide/ is missing here.
GUIDES = [
    ("index", "Introduction"),
    ("quick-start", "Quick Start"),
    ("installation", "Installation"),
    ("path-analysis", "Path Analysis"),
    ("eventstream", "Eventstream"),
    ("widgets", "Widgets"),
    ("data-processors", "Data Processors"),
    ("segments", "Segments"),
    ("path-metrics", "Path Metrics"),
    ("mcp-server", "MCP Server"),
    ("agent-skills", "Agent Skills"),
    ("recipes", "Recipes"),
    ("migration-from-3x", "Migrating from 3.x"),
    ("tracking", "Tracking"),
]

# Descriptions in llms.txt are the page's own opening sentence. These pages
# don't have a usable one — installation.md opens straight into a heading,
# and the other two open on a hook rather than a definition.
GUIDE_DESCRIPTIONS = {
    "index": (
        "What retentioneering is, what it is for, and how its pieces fit together."
    ),
    "installation": (
        "Requirements, pip/uv install, optional dependencies, and Jupyter setup."
    ),
    "path-analysis": (
        "The three aggregated representations of user paths — by step, by transition, "
        "by milestone — and how to choose between them."
    ),
}

WIDGETS = [
    ("widgets/transition-graph.md.jinja", "transition_graph", "transition_graph_data"),
    ("widgets/step-matrix.md.jinja", "step_matrix", "step_matrix_data"),
    ("widgets/step-sankey.md.jinja", "step_sankey", "step_sankey_data"),
    ("widgets/funnel.md.jinja", "funnel", "funnel_data"),
    ("widgets/segment-overview.md.jinja", "segment_overview", "segment_overview_data"),
    ("widgets/cluster-analysis.md.jinja", "cluster_analysis", "cluster_analysis_data"),
]

DATA_PROCESSORS = [
    "filter_events",
    "filter_paths",
    "add_events",
    "add_segment",
    "add_clusters",
    "add_start_end_events",
    "collapse_events",
    "to_daily_states",
    "drop_segment",
    "drop_events",
    "edit_events",
    "rename_events",
    "rename_segment_levels",
    "sample_paths",
    "split_sessions",
    "truncate_paths",
    "urls_to_events",
]


def slugify(name: str) -> str:
    return name.replace("_", "-")


def title_of(name: str) -> str:
    if name in TITLE_OVERRIDES:
        return TITLE_OVERRIDES[name]
    return " ".join(word.capitalize() for word in name.split("_"))


def is_missing(doc) -> bool:
    return not doc.summary and not doc.parameters and not doc.examples


def load_doc(method_name: str):
    doc = get_doc(Eventstream, method_name)
    if is_missing(doc):
        print(f"WARNING: Eventstream.{method_name} has no docstring")
        doc.summary = f"> **No docstring yet.** Add one to `Eventstream.{method_name}`."
    return doc


def build_env() -> jinja2.Environment:
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(TEMPLATES_DIR),
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
        undefined=jinja2.StrictUndefined,
    )
    env.globals["param_table"] = render_param_table
    env.globals["bullets"] = bullets
    env.globals["split_by_headless"] = split_by_headless
    return env


def render_widget(
    env: jinja2.Environment, template_rel: str, method_name: str, headless_name: str
) -> str:
    template = env.get_template(template_rel)
    return template.render(
        title=title_of(method_name),
        doc=load_doc(method_name),
        headless=load_doc(headless_name),
        headless_name=headless_name,
        default_call=f"stream.{method_name}()",
    )


def render_data_processor(env: jinja2.Environment, method_name: str) -> str:
    template = env.get_template(f"data-processors/{slugify(method_name)}.md.jinja")
    return template.render(
        title=title_of(method_name),
        doc=load_doc(method_name),
        default_call=f"stream.{method_name}()",
    )


def copy_guide_pages() -> None:
    """Copy hand-written conceptual pages (quick-start, installation, ...) as-is.

    Unlike widgets/data-processors, these have no docstring to render from —
    docs/guide/*.md IS the source, so this step is a plain copy, not a
    template render.

    The output directory is wiped first: a page that was renamed or deleted
    in docs/guide/ must disappear from the build too, or retentioneering-web
    keeps serving the stale URL from its symlinked content/docs.
    """
    out_dir = BUILD_DIR / "guide"
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    for src in sorted(GUIDE_DIR.glob("*.md")):
        dest = out_dir / src.name
        shutil.copy2(src, dest)
        print(f"wrote {dest.relative_to(REPO_ROOT)}")


def copy_figures() -> None:
    """Copy hand-drawn SVG figures into the demos tree so the site can serve them.

    retentioneering-web exposes docs/build/demos/ (and only that directory) as
    public/docs-demos, so anything a docs page needs to load over HTTP has to
    live under it — hence figures land in demos/img/ rather than a sibling of
    it, and pages reference them as `/docs-demos/img/<name>.svg`.

    The figures themselves are hand-written SVG under docs/img/, theme-aware
    through a `prefers-color-scheme` block inside each file (they are loaded
    via <img>, which isolates them from the page's CSS but still honours their
    own media queries).
    """
    if not IMG_DIR.exists():
        return
    out_dir = BUILD_DIR / "demos" / "img"
    out_dir.mkdir(parents=True, exist_ok=True)
    for src in sorted(IMG_DIR.glob("*.svg")):
        dest = out_dir / src.name
        shutil.copy2(src, dest)
        print(f"wrote {dest.relative_to(REPO_ROOT)}")


@dataclass
class LlmsPage:
    """One docs page as it appears in llms.txt / llms-full.txt."""

    title: str
    url: str
    description: str
    build_path: Path


# <DemoWidget cmd={`stream.funnel(...)`} path="..." height={480} /> — a live
# widget on the site, and nothing an LLM can use. The cmd is the interesting
# part, so it becomes a plain python block; the attributes are dropped.
DEMO_WIDGET_RE = re.compile(r"<DemoWidget\s+cmd=\{`(?P<cmd>.*?)`\}[^>]*/>", re.DOTALL)

# Site-relative targets: markdown links (/docs/...) and figures (/docs-demos/...).
# The trailing [^)\s]* stops before an image's optional " Caption" title.
SITE_LINK_RE = re.compile(r"\]\((/docs[^)\s]*)")

# First sentence: a period that ends a word, not one inside "3.3.0" or "5.0".
FIRST_SENTENCE_RE = re.compile(r"^(.+?[.!?])(?:\s|$)")


def _plain_text(text: str) -> str:
    """Flatten inline markdown so a sentence can be used as a description."""
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    text = text.replace("**", "").replace("`", "")
    return " ".join(text.split())


def _lede(markdown: str) -> str:
    """The first sentence of a page's first real paragraph.

    Skips the H1, headings, blockquotes, lists, tables and JSX tags — a page
    that opens on any of those has no lede and needs a GUIDE_DESCRIPTIONS entry.
    """
    paragraph: list[str] = []
    for line in markdown.splitlines()[1:]:
        stripped = line.strip()
        if not stripped:
            if paragraph:
                break
            continue
        if stripped.startswith(("#", ">", "-", "*", "|", "```", "<", "!")):
            break
        paragraph.append(stripped)
    sentence = _plain_text(" ".join(paragraph))
    match = FIRST_SENTENCE_RE.match(sentence)
    return match.group(1) if match else sentence


def _collect_pages() -> dict[str, list[LlmsPage]]:
    """Group every built page under its llms.txt section heading."""
    on_disk = {path.stem for path in GUIDE_DIR.glob("*.md")}
    listed = {stem for stem, _ in GUIDES}
    if on_disk != listed:
        raise SystemExit(
            f"docs/guide/ and the GUIDES list disagree: "
            f"missing from GUIDES {sorted(on_disk - listed)}, "
            f"missing from docs/guide/ {sorted(listed - on_disk)}"
        )

    guides = []
    for stem, label in GUIDES:
        source = GUIDE_DIR / f"{stem}.md"
        description = GUIDE_DESCRIPTIONS.get(stem) or _lede(
            source.read_text(encoding="utf-8")
        )
        if not description:
            raise SystemExit(
                f"docs/guide/{stem}.md has no usable opening sentence — "
                f"add a GUIDE_DESCRIPTIONS entry for it"
            )
        url = "/docs" if stem == "index" else f"/docs/{stem}"
        guides.append(
            LlmsPage(label, url, description, BUILD_DIR / "guide" / f"{stem}.md")
        )

    def from_method(method_name: str, section: str) -> LlmsPage:
        slug = slugify(method_name)
        return LlmsPage(
            title=title_of(method_name),
            url=f"/docs/{section}/{slug}",
            # get_doc, not load_doc: a missing docstring is already reported
            # once by the render pass, no need to warn about it twice.
            description=_plain_text(
                _lede(f"#\n\n{get_doc(Eventstream, method_name).summary}")
            ),
            build_path=BUILD_DIR / section / f"{slug}.md",
        )

    return {
        "Guides": guides,
        "Widgets": [from_method(name, "widgets") for _, name, _ in WIDGETS],
        "Data processors": [
            from_method(name, "data-processors") for name in DATA_PROCESSORS
        ],
    }


def write_llms_files() -> None:
    """Write llms.txt (a table of contents) and llms-full.txt (the whole corpus).

    Both follow https://llmstxt.org: llms.txt is what an agent reads to find
    the right page, llms-full.txt is the entire documentation as one file, for
    agents that would rather hold all of it at once. The docs site serves them
    from its root (/llms.txt, /llms-full.txt), so every link here is absolute.
    """
    sections = _collect_pages()

    index = [
        "# Retentioneering",
        "",
        "> Open-source Python library for user-behaviour analysis: it turns raw event "
        "logs into interactive maps of how users move through a product — the paths "
        "they take, the loops they get stuck in, the step where they leave. Everything "
        "runs locally in a notebook, on a DuckDB-backed Eventstream.",
        "",
        f"The complete documentation as a single file: {SITE_URL}/llms-full.txt",
        "",
    ]
    for heading, pages in sections.items():
        index.append(f"## {heading}")
        index.append("")
        index += [f"- [{p.title}]({SITE_URL}{p.url}): {p.description}" for p in pages]
        index.append("")
    index += [
        "## Optional",
        "",
        "- [Legacy documentation (v2.0, v3.3)](https://doc.retentioneering.com/3.3/doc/): "
        "reference for the older, no-longer-developed releases. Only useful for reading "
        "code written against one of those versions — the 5.x API differs throughout.",
        "",
    ]

    blocks = []
    for pages in sections.values():
        for page in pages:
            body = page.build_path.read_text(encoding="utf-8").strip()
            body = DEMO_WIDGET_RE.sub(
                lambda m: f"```python\n{m.group('cmd').strip()}\n```", body
            )
            body = SITE_LINK_RE.sub(lambda m: f"]({SITE_URL}{m.group(1)}", body)
            lines = body.splitlines()
            heading, rest = (
                (lines[0], "\n".join(lines[1:]).lstrip("\n"))
                if lines and lines[0].startswith("# ")
                else (f"# {page.title}", body)
            )
            blocks.append(f"{heading}\n\nSource: {SITE_URL}{page.url}\n\n{rest}")

    for name, text in (
        ("llms.txt", "\n".join(index)),
        ("llms-full.txt", "\n\n---\n\n".join(blocks) + "\n"),
    ):
        out_path = BUILD_DIR / name
        out_path.write_text(text, encoding="utf-8")
        print(
            f"wrote {out_path.relative_to(REPO_ROOT)} ({out_path.stat().st_size / 1024:.0f} KB)"
        )


def main() -> None:
    (BUILD_DIR / "widgets").mkdir(parents=True, exist_ok=True)
    (BUILD_DIR / "data-processors").mkdir(parents=True, exist_ok=True)
    env = build_env()

    copy_guide_pages()
    copy_figures()

    for template_rel, method_name, headless_name in WIDGETS:
        rendered = render_widget(env, template_rel, method_name, headless_name)
        out_path = BUILD_DIR / "widgets" / f"{slugify(method_name)}.md"
        out_path.write_text(rendered, encoding="utf-8")
        print(f"wrote {out_path.relative_to(REPO_ROOT)}")

    for method_name in DATA_PROCESSORS:
        rendered = render_data_processor(env, method_name)
        out_path = BUILD_DIR / "data-processors" / f"{slugify(method_name)}.md"
        out_path.write_text(rendered, encoding="utf-8")
        print(f"wrote {out_path.relative_to(REPO_ROOT)}")

    # Last: llms-full.txt concatenates the pages written above.
    write_llms_files()


if __name__ == "__main__":
    main()
