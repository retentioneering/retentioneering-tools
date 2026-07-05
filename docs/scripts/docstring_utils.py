"""Shared numpydoc parsing helpers for the docs generation scripts.

Turns a numpy-style docstring into structured pieces (summary, parameters,
examples, returns) so page-composition scripts can pick and arrange them,
instead of dumping the docstring as one undifferentiated blob of text.
"""

from __future__ import annotations

import inspect
import re
import textwrap
from dataclasses import dataclass, field

_SECTION_UNDERLINE_RE = re.compile(r"^-{3,}$")
_PARAM_HEADER_RE = re.compile(r"^(\S.*?)\s*:\s*(.+)$")


@dataclass
class Param:
    name: str
    type: str
    lead: str
    bullets: list[str] = field(default_factory=list)

    @property
    def description(self) -> str:
        """Flat one-line description, ignoring any bulleted sub-list."""
        return self.lead


@dataclass
class ParsedDoc:
    summary: str = ""
    parameters: list[Param] = field(default_factory=list)
    examples: str = ""
    returns: str = ""


def get_doc(cls, method_name: str) -> ParsedDoc:
    """Fetch and parse the docstring of `cls.<method_name>`."""
    method = getattr(cls, method_name)
    raw = inspect.getdoc(method)
    return parse_numpydoc(raw or "")


def _split_sections(doc: str) -> dict[str, list[str]]:
    lines = doc.splitlines()
    sections: dict[str, list[str]] = {"summary": []}
    current = "summary"
    i = 0
    while i < len(lines):
        line = lines[i]
        next_line = lines[i + 1] if i + 1 < len(lines) else ""
        if line.strip() and _SECTION_UNDERLINE_RE.match(next_line.strip()):
            current = line.strip().lower()
            sections[current] = []
            i += 2
            continue
        sections.setdefault(current, []).append(line)
        i += 1
    return sections


def _build_param(name: str, type_: str, raw_lines: list[str]) -> Param:
    """Split a parameter's description lines into a lead paragraph and any
    `- ...` bullet items, so a bulleted breakdown (e.g. one line per enum
    value) can be rendered as its own list instead of being flattened into a
    single table cell.

    Uses indentation (preserved in `raw_lines`) to tell a wrapped bullet line
    apart from a trailing paragraph that follows the list back at the base
    indent level — both are plain text once whitespace is stripped, so indent
    is the only signal that distinguishes "belongs to the previous bullet"
    from "the list is over".
    """
    lead: list[str] = []
    bullets: list[str] = []
    base_indent: int | None = None
    bullet_indent: int | None = None
    in_bullet = False
    for raw in raw_lines:
        if not raw.strip():
            continue
        indent = len(raw) - len(raw.lstrip())
        text = raw.strip()
        if text.startswith("- "):
            bullets.append(text[2:].strip())
            bullet_indent = indent
            in_bullet = True
            continue
        if in_bullet and bullet_indent is not None and indent > bullet_indent:
            bullets[-1] = (bullets[-1] + " " + text).strip()
            continue
        in_bullet = False
        if base_indent is None:
            base_indent = indent
        lead.append(text)
    return Param(name=name, type=type_, lead=" ".join(lead).strip(), bullets=bullets)


def _parse_parameters(lines: list[str]) -> list[Param]:
    params: list[Param] = []
    current_name: str | None = None
    current_type: str = ""
    current_lines: list[str] = []

    def flush() -> None:
        if current_name is not None:
            params.append(_build_param(current_name, current_type, current_lines))

    for line in lines:
        if not line.strip():
            continue
        if not line.startswith((" ", "\t")):
            match = _PARAM_HEADER_RE.match(line.strip())
            if match:
                flush()
                current_name = match.group(1).strip()
                current_type = match.group(2).strip()
                current_lines = []
                continue
        current_lines.append(line)
    flush()
    return params


def _dedent_block(lines: list[str]) -> str:
    text = "\n".join(lines)
    return textwrap.dedent(text).strip("\n")


def parse_numpydoc(doc: str) -> ParsedDoc:
    sections = _split_sections(doc)
    parsed = ParsedDoc()
    parsed.summary = "\n".join(sections.get("summary", [])).strip()
    if "parameters" in sections:
        parsed.parameters = _parse_parameters(sections["parameters"])
    if "examples" in sections:
        parsed.examples = _dedent_block(sections["examples"])
    if "returns" in sections:
        parsed.returns = _dedent_block(sections["returns"])
    return parsed


def render_param_table(params: list[Param]) -> str:
    """Plain Parameter/Type/Description table. Deliberately dumb — it always
    shows just the lead sentence, never the bulleted breakdown. Templates that
    want to surface a parameter's bullets (e.g. one line per enum value) call
    `bullets()` explicitly and write their own heading for it; this function
    doesn't guess where that heading should go or what it should be called."""
    if not params:
        return "_No parameters._"
    rows = ["| Parameter | Type | Description |", "|---|---|---|"]
    for p in params:
        # Backtick-wrapped so a literal enum type like {"a", "b"} renders as
        # code instead of raw braces — which the MDX renderer on the docs site
        # would otherwise try to parse as a JS expression and fail on.
        type_cell = p.type.replace("|", "\\|")
        desc_cell = p.lead.replace("|", "\\|")
        rows.append(f"| `{p.name}` | `{type_cell}` | {desc_cell} |")
    return "\n".join(rows)


def find_param(params: list[Param], name: str) -> Param | None:
    return next((p for p in params if p.name == name), None)


def split_by_headless(
    widget_params: list[Param], headless_params: list[Param]
) -> tuple[list[Param], list[Param]]:
    """Partition a widget's parameters into "data" (shared with its headless
    counterpart, e.g. `edge_weight`, `diff`) and "display" (widget-only, e.g.
    `height`, `sidebar_open`) groups.

    Relies on a naming convention rather than any explicit tag: a data
    processor/computation param is documented under the *same name* on both
    the widget method and its headless counterpart (see `Eventstream.step_matrix`
    vs. `Eventstream.step_sankey_data`), so membership in `headless_params` is
    what distinguishes the two groups.
    """
    headless_names = {p.name for p in headless_params}
    data_params = [p for p in widget_params if p.name in headless_names]
    display_params = [p for p in widget_params if p.name not in headless_names]
    return data_params, display_params


def bullets(params: list[Param], name: str) -> str:
    """Markdown bullet list for one named parameter's `- ...` breakdown.

    For a template to call `bullets(headless.parameters, "edge_weight")`, the
    docstring's `edge_weight` parameter must document its values as a `- `
    list — see `Eventstream.transition_graph_data` for an example.
    """
    p = find_param(params, name)
    if p is None:
        print(f"WARNING: template references unknown parameter {name!r}")
        return ""
    if not p.bullets:
        print(f"WARNING: parameter {name!r} has no bulleted breakdown to render")
        return ""
    return "\n".join(f"- {item}" for item in p.bullets)
