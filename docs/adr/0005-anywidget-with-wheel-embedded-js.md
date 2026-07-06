# ADR-0005: anywidget widgets with wheel-embedded JS (no CDN)

Status: Accepted (5.0 rewrite; recorded 2026-07)

## Context

3.x widgets were iframes that downloaded a closed-source JS bundle from a CDN
at runtime, bridged to the kernel via a custom postMessage backend. That
meant: broken offline/air-gapped installs, version skew between Python and
JS, an opaque JS layer, and a bespoke comm protocol to maintain.

## Decision

- Widgets are `anywidget.AnyWidget` subclasses; Python ↔ JS state sync goes
  through anywidget traitlets (the model keys are part of the internal
  protocol and follow the same naming conventions as the Python API).
- The JS lives in this repository as an npm workspace: `js/viz-core` (shared
  React/MobX/Cytoscape components) and `js/widget` (the anywidget entry
  point). It is open source.
- `make build` (and CI) builds `widget.js` (ESM, for live anywidget use) and
  `widget-static.js` (IIFE, for static export — ADR-0011) into
  `src/retentioneering/static/`. The bundles are **gitignored** but
  force-included into the wheel by hatchling; the release workflow verifies
  `widget.js` actually landed in the wheel.
- There is **no runtime download and no CDN fallback**: `widgets/_esm.py`
  raises `FileNotFoundError` with a "run `make build`" hint if the bundle is
  missing.
- `widget.css` is a hand-maintained source file (tracked in git), not build
  output.

## Consequences

- Python and JS versions can never skew; offline installs work.
- Contributors need Node for widget work; pure-Python contributions still
  need `make build` once to render widgets locally.
- Widget rendering has no automated tests; `vite build` success is the only
  JS correctness signal, plus the generated docs demos exercising real
  widget construction (ADR-0014).
- Renaming a traitlet is a cross-language change: Python widget class, JS
  `model.get/set` keys, static-export data dicts, and the MCP server's data
  dicts must move together.
