"""Shared base class for the six ``anywidget``-based Retentioneering widgets.

See ADR-0006 (tools vs. widgets) and ADR-0005/0008 (Python traitlets and JS
``model.get/set`` keys are one protocol — names/shapes here must not change)
before editing this file.

Factors out what's genuinely identical across ``transition_graph``,
``step_matrix``, ``step_sankey``, ``cluster_analysis``, ``funnel`` and
``segment_overview``, previously hand-rolled per widget:

- ``_esm``/``_css`` — the JS bundle location.
- ``is_loading``/``error`` — the display-state traitlets every widget's
  ``_recompute`` sets around its computation.
- ``compute_request``/``compute_response`` — the generic request/response
  RPC pair used for on-demand server-side calls from JS (JSON-encoded
  ``{"id", "tool", "params"}`` in, ``{"id", "result"}`` or ``{"id", "error"}``
  out — see ``js/viz-core/src/WidgetHost.ts``'s ``compute()`` and
  ``js/widget/src/AnywidgetHost.ts``), plus the async observer/dispatch
  plumbing around it.
- ``compute_tools`` — an explicit ``{tool_name: handler}`` table each
  subclass declares as a class attribute. Each ``handler`` is a plain
  function/method reference taken straight from the class body (i.e.
  unbound), called as ``handler(self, params)``. Tool names mirror the
  matching ``Eventstream`` method name exactly, the same convention the MCP
  server's preprocessor dispatch (``mcp/server.py::_apply_preprocessors``)
  and headless ``*_data`` twins already use (ADR-0006, ADR-0009). This table
  is also what a platform/REST backend can introspect and call directly
  (via ``dispatch_compute``) instead of hand-rolling its own tool ->
  Eventstream-method map.
"""

from __future__ import annotations

import json
import pathlib
from typing import Any, Callable

import anywidget
import traitlets

from retentioneering.widgets._esm import _get_esm
from retentioneering.widgets._state_file import StateFileMixin

_STATIC = pathlib.Path(__file__).parent.parent / "static"

#: Sentinel distinguishing "argument not passed" from "argument explicitly
#: passed as None" in widget constructors (``None`` is itself a meaningful
#: value for e.g. ``path_col``/``diff``).
_UNSET = object()

#: Traits `sync=True`-tagged by anywidget/ipywidgets' own base classes
#: (`layout`, `tabbable`, `tooltip`, `viewport`, ...) — framework DOM-widget
#: plumbing, not our state, and some hold non-JSON-serializable objects
#: (e.g. `layout` is a `Layout` instance). Excluded from `sync_state()`.
_FRAMEWORK_SYNC_TRAIT_NAMES = frozenset(anywidget.AnyWidget.class_traits(sync=True))


class RetentioneeringWidget(StateFileMixin, anywidget.AnyWidget):
    """Common base for the six widget classes in this package.

    Subclasses still own their own params/catalogs/result traitlets and
    ``__init__``/``_recompute``/``export_html`` — this only holds what's
    identical across all six.
    """

    _esm = _get_esm()
    _css = _STATIC / "widget.css"

    # ── display state ───────────────────────────────────────────────────────
    is_loading = traitlets.Bool(False).tag(sync=True)
    error = traitlets.Unicode("").tag(sync=True)

    # ── generic compute protocol ────────────────────────────────────────────
    compute_request = traitlets.Unicode("").tag(sync=True)
    compute_response = traitlets.Unicode("").tag(sync=True)

    #: {tool_name: handler}; handler is called as ``handler(self, params)``.
    #: Every subclass overrides this with its own table (empty here so a
    #: subclass that forgets to declare one fails with "Unknown tool" rather
    #: than an AttributeError).
    compute_tools: dict[str, Callable[["RetentioneeringWidget", dict], Any]] = {}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.observe(self._on_compute_request, names=["compute_request"])

    # ── compute RPC plumbing ────────────────────────────────────────────────

    def _on_compute_request(self, change) -> None:
        raw = change["new"]
        if not raw:
            return
        try:
            req = json.loads(raw)
        except Exception:
            return
        req_id = req.get("id", "")
        tool = req.get("tool", "")
        params = req.get("params") or {}
        try:
            result = self.dispatch_compute(tool, params)
            self.compute_response = json.dumps({"id": req_id, "result": result})
        except Exception as exc:
            self.compute_response = json.dumps({"id": req_id, "error": str(exc)})

    def sync_state(self) -> dict[str, Any]:
        """Every ``sync=True`` traitlet's current value, by name.

        This is what anywidget/ipywidgets sends to JS for free when a widget
        is first displayed in Jupyter (the comm-open handshake syncs every
        tagged trait). A platform backend constructing a fresh widget
        instance per HTTP request has no such handshake, so without this a
        REST client only ever sees whichever one tool's return value it
        asked for — missing traits set once at construction (e.g.
        ``event_counts``, ``segment_levels``) and never touched by
        ``compute_tools`` handlers, which are deliberately side-effect-free
        (see ``dispatch_compute``). Call this once after constructing a
        widget to seed a REST-backed ``WidgetHost`` with the same state a
        live Jupyter session would have.
        """
        return {
            name: getattr(self, name)
            for name in self.traits(sync=True)
            if not name.startswith("_")  # anywidget internals, e.g. `_esm`/`_css`
            and name not in _FRAMEWORK_SYNC_TRAIT_NAMES  # `layout`, `tooltip`, ...
        }

    def dispatch_compute(self, tool: str, params: dict) -> Any:
        """Look up ``tool`` in ``compute_tools`` and invoke it.

        Split out from ``_on_compute_request`` so a caller that already
        holds a widget instance but isn't going through the JS traitlet RPC
        (e.g. a platform backend servicing an HTTP request) can reuse the
        exact same dispatch table without round-tripping through
        JSON-encoded ``compute_request``/``compute_response`` traitlets.
        """
        handler = self.compute_tools.get(tool)
        if handler is None:
            raise ValueError(f"Unknown tool: {tool!r}")
        return handler(self, params)
