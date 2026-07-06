# ADR-0011: Self-contained static HTML export

Status: Accepted (5.0; recorded 2026-07)

## Context

Analysis results must be shareable with people who have no Jupyter kernel,
no Python, and possibly no network: stakeholders opening a file from Slack,
and MCP-generated reports (ADR-0010).

## Decision

- Every widget has `export_html(path, title, analysis, ...)` producing a
  standalone interactive HTML file; the MCP server's `export_report` writes
  a multi-tab variant with an analysis panel and clickable
  `[tab:element]` anchors.
- Exports embed `widget-static.js` — a separate IIFE build of the same JS
  source (vs. `widget.js` ESM for live anywidget use). Unlike `widget.js`,
  it is shipped inside the wheel unconditionally, because exported HTML must
  work with no Python behind it.
- The exported file carries the widget's full data payload (results,
  catalogues, display prefs) serialized into the page; static mode disables
  controls that would require recomputation.

## Consequences

- The static bundle and live bundle are built from one source tree, so
  widget features must degrade gracefully when there is no kernel
  (`isStatic` paths in JS).
- Docs demo pages (ADR-0014) reuse exactly this mechanism, which keeps the
  static path continuously exercised.
