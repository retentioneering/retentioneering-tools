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

import shutil
from pathlib import Path

import jinja2
from retentioneering.eventstream.eventstream import Eventstream
from docstring_utils import bullets, get_doc, render_param_table, split_by_headless

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
TEMPLATES_DIR = REPO_ROOT / "docs" / "templates"
GUIDE_DIR = REPO_ROOT / "docs" / "guide"
BUILD_DIR = REPO_ROOT / "docs" / "build"

TITLE_OVERRIDES = {"url_events": "URL Events"}

# (template, method_name, headless_method_name or None). step_matrix has no
# data method of its own — it shares step_sankey_data with the Step Sankey
# widget, so it's spelled out here rather than guessed from a "<name>_data"
# naming convention.
WIDGETS = [
    ("widgets/transition-graph.md.jinja", "transition_graph", "transition_graph_data"),
    ("widgets/step-matrix.md.jinja", "step_matrix", "step_sankey_data"),
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
    "daily_states",
    "drop_segment",
    "edit_events",
    "rename_events",
    "sample_paths",
    "split_sessions",
    "truncate_paths",
    "url_events",
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
    """
    out_dir = BUILD_DIR / "guide"
    out_dir.mkdir(parents=True, exist_ok=True)
    for src in sorted(GUIDE_DIR.glob("*.md")):
        dest = out_dir / src.name
        shutil.copy2(src, dest)
        print(f"wrote {dest.relative_to(REPO_ROOT)}")


def main() -> None:
    (BUILD_DIR / "widgets").mkdir(parents=True, exist_ok=True)
    (BUILD_DIR / "data-processors").mkdir(parents=True, exist_ok=True)
    env = build_env()

    copy_guide_pages()

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


if __name__ == "__main__":
    main()
