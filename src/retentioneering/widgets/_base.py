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
  Eventstream-method map. One deliberate exception: ``transition_graph``'s
  ``route_stats`` tool has no ``Eventstream`` counterpart — it's a
  widget-only helper (the route badge's data) backed directly by
  ``utils/route_stats.py``, not exposed as public API.
"""

from __future__ import annotations

import json
import pathlib
import uuid
from typing import Any, Callable

import anywidget
import traitlets

from retentioneering.exceptions import WidgetExportError
from retentioneering.widgets._esm import _get_esm
from retentioneering.widgets._html_export import render_static_display, write_html
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

    #: Unique id of this widget *instance* — a fresh uuid per Python object,
    #: deliberately NOT persisted to state files. JS uses it to namespace
    #: per-widget browser state (e.g. node positions in localStorage) so one
    #: widget's manual arrangement can never leak into another widget or a
    #: re-created one. Subclasses may override it with a stable, data-derived
    #: identity when that state SHOULD survive re-creation for the same data
    #: (the transition graph does).
    widget_id = traitlets.Unicode().tag(sync=True)

    @traitlets.default("widget_id")
    def _default_widget_id(self) -> str:
        return uuid.uuid4().hex

    # ── generic compute protocol ────────────────────────────────────────────
    compute_request = traitlets.Unicode("").tag(sync=True)
    compute_response = traitlets.Unicode("").tag(sync=True)

    #: {tool_name: handler}; handler is called as ``handler(self, params)``.
    #: Every subclass overrides this with its own table (empty here so a
    #: subclass that forgets to declare one fails with "Unknown tool" rather
    #: than an AttributeError).
    compute_tools: dict[str, Callable[["RetentioneeringWidget", dict], Any]] = {}

    #: Display name used as the browser-tab/tab-label default for
    #: ``export_html()``/``render_static()`` (e.g. ``"Transition Graph"``).
    #: Every subclass overrides this.
    _export_label: str = ""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.observe(self._on_compute_request, names=["compute_request"])

    def _raise_if_error(self) -> None:
        """Guard for ``export_html``: refuse to export a stale/failed result.

        ``_recompute`` swallows non-``RetentioneeringError`` exceptions into
        the ``error`` trait (so the live widget can show an inline banner
        instead of crashing the notebook cell) and resets ``result`` to
        ``"{}"``. ``export_html`` reads ``result`` straight off the
        traitlets, so without this check it would silently write out a file
        with an empty result instead of surfacing the failure.
        """
        if self.error:
            raise WidgetExportError(
                f"Cannot export: the widget's last computation failed: {self.error}"
            )

    def _export_data(self, sidebar_open: bool | None = None) -> dict[str, Any]:
        """Data payload for ``export_html()``/``render_static()``.

        Every subclass overrides this with its own traitlet -> dict mapping;
        this base implementation only exists so a subclass that forgets to
        override it fails with a clear ``NotImplementedError`` here rather
        than an ``AttributeError`` inside ``export_html``.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement _export_data()"
        )

    def export_html(
        self,
        path: str,
        title: str | None = None,
        analysis: str | None = None,
        sidebar_open: bool | None = None,
    ) -> None:
        """
        Export this widget as a standalone interactive HTML file.

        Parameters
        ----------
        path:
            Destination file path.
        title:
            Title shown in the browser tab. Defaults to the widget's name,
            e.g. ``"Transition Graph"``.
        analysis:
            Optional analysis text. Wrap event names in square brackets to
            make them clickable, e.g.
            ``"Drop-off at [basket]: 78% of users leave here."``. Supports
            basic markdown (bold, italic, bullet lists, tables, headings).
        sidebar_open:
            Whether the settings sidebar starts open in the exported file.
            Defaults to the widget's current ``sidebar_open`` value.
        """
        self._raise_if_error()
        data = self._export_data(sidebar_open)
        write_html(
            path, title or self._export_label, self._export_label, data, analysis
        )

    def render_static(
        self,
        title: str | None = None,
        height: int | str | None = None,
        sidebar_open: bool | None = None,
    ):
        """
        Render this widget for embedding in a notebook's own HTML export.

        Unlike just leaving the widget as a cell's output, this doesn't need
        a live kernel to render: it bakes the same self-contained page
        ``export_html()`` writes to disk into the cell's output (as an
        ``<iframe srcdoc>``), so it keeps showing correctly after
        ``jupyter nbconvert`` / "Save and Export as HTML" — those replay
        stored cell outputs without a kernel, and a live widget needs one.
        Use it in place of the widget in any cell you plan to keep visible
        after exporting the notebook.

        It reads whatever is currently in this widget's own traitlets — it
        has no way to see manual, in-browser interactions (e.g. dragged node
        positions) made on a *different* call/instance, even one that looks
        identical (same data, same params). If you want the export to
        reflect a layout you arranged by hand, construct the widget with
        ``state_file=`` first: manual changes then auto-save to that file as
        you make them, and any later widget instance built with the same
        ``state_file`` — including one you're about to call ``render_static()``
        on — loads it back synchronously, before anything else runs. Without
        ``state_file``, a fresh call always starts from the widget's default
        (or algorithmic, e.g. semantic graph layout) state, regardless of
        what you previously did to another instance in the notebook.

        Parameters
        ----------
        title:
            Title shown in the browser tab if the iframe is opened on its
            own. Defaults to the widget's name, e.g. ``"Transition Graph"``.
        height:
            iframe height in pixels, or any CSS length (e.g. ``"80vh"``).
            Defaults to the widget's current ``height``.
        sidebar_open:
            Whether the settings sidebar starts open.
            Defaults to the widget's current ``sidebar_open`` value.
        """
        self._raise_if_error()
        data = self._export_data(sidebar_open)
        resolved_height = height if height is not None else data["height"]
        return render_static_display(title or self._export_label, data, resolved_height)

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
