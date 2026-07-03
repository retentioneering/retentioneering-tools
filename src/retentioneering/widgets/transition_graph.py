import json
import pathlib

import anywidget
import traitlets

from retentioneering.widgets._esm import _get_esm
from retentioneering.widgets.cloud_mixin import CloudMixin
from retentioneering.widgets._utils import parse_diff as _parse_diff
from retentioneering.widgets._html_export import write_html

_STATIC = pathlib.Path(__file__).parent.parent / "static"
_UNSET = object()


class TransitionGraphWidget(CloudMixin, anywidget.AnyWidget):
    _esm = _get_esm()
    _css = _STATIC / "widget.css"

    widget_type  = traitlets.Unicode("transition_graph").tag(sync=True)

    # ── recompute triggers ─────────────────────────────────────────────────────
    edge_weight = traitlets.Unicode("proba_out").tag(sync=True)
    diff        = traitlets.Unicode("").tag(sync=True)
    path_id_col = traitlets.Unicode("").tag(sync=True)

    # ── catalogues ─────────────────────────────────────────────────────────────
    path_cols       = traitlets.Unicode("[]").tag(sync=True)
    event_counts    = traitlets.Unicode("{}").tag(sync=True)
    event_counts_g1 = traitlets.Unicode("{}").tag(sync=True)
    event_counts_g2 = traitlets.Unicode("{}").tag(sync=True)
    segment_levels  = traitlets.Unicode("{}").tag(sync=True)

    # ── result ─────────────────────────────────────────────────────────────────
    result     = traitlets.Unicode("{}").tag(sync=True)
    is_loading = traitlets.Bool(False).tag(sync=True)
    error      = traitlets.Unicode("").tag(sync=True)

    # ── display / persistent ───────────────────────────────────────────────────
    height           = traitlets.Int(500).tag(sync=True)
    sidebar_open     = traitlets.Bool(True).tag(sync=True)
    node_positions   = traitlets.Unicode("{}").tag(sync=True)
    event_visibility = traitlets.Unicode("{}").tag(sync=True)

    # ── generic compute protocol ───────────────────────────────────────────────
    compute_request  = traitlets.Unicode("").tag(sync=True)
    compute_response = traitlets.Unicode("").tag(sync=True)

    def __init__(
        self,
        eventstream,
        cloud_file_name: str | None = None,
        edge_weight=_UNSET,
        diff=_UNSET,
        path_id_col=_UNSET,
        height=_UNSET,
        sidebar_open=_UNSET,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._eventstream = eventstream
        self._initialized = False

        try:
            self.segment_levels = json.dumps(eventstream.get_all_segment_levels())
        except Exception:
            self.segment_levels = "{}"
        self.path_cols = json.dumps(eventstream.schema.path_cols)
        try:
            self.event_counts = json.dumps(eventstream.get_event_counts())
        except Exception:
            self.event_counts = "{}"

        self.edge_weight  = edge_weight if edge_weight is not _UNSET else "proba_out"
        _diff_val         = diff         if diff         is not _UNSET else None
        self.diff         = json.dumps(list(_diff_val)) if _diff_val else ""
        self.path_id_col  = path_id_col  if path_id_col  is not _UNSET else ""
        self.height       = height       if height       is not _UNSET else 500
        self.sidebar_open = sidebar_open if sidebar_open is not _UNSET else True
        self.node_positions = "{}"

        self._init_cloud(cloud_file_name)

        if not self._cloud_file_name:
            self._recompute()

        self._initialized = True
        self.observe(self._on_params_change,           names=["edge_weight", "diff", "path_id_col"])
        self.observe(self._on_positions_change,        names=["node_positions"])
        self.observe(self._on_event_visibility_change, names=["event_visibility"])
        self.observe(self._on_compute_request,         names=["compute_request"])

    # ── widget-specific observers ──────────────────────────────────────────────

    def _on_params_change(self, _change):
        if not self._initialized or self._loading_from_cloud:
            return
        self._recompute()
        if self._guard_auto_save():
            self._schedule_cloud_save()

    def _on_positions_change(self, _change):
        if not self._initialized or self._loading_from_cloud:
            return
        if self._guard_auto_save():
            self._schedule_cloud_save()

    def _on_event_visibility_change(self, _change):
        if not self._initialized or self._loading_from_cloud:
            return
        if self._guard_auto_save():
            self._schedule_cloud_save()

    def _on_compute_request(self, change):
        raw = change["new"]
        if not raw:
            return
        try:
            req = json.loads(raw)
        except Exception:
            return
        req_id = req.get("id", "")
        tool   = req.get("tool", "")
        params = req.get("params", {})
        try:
            result = self._dispatch(tool, params)
            self.compute_response = json.dumps({"id": req_id, "result": result})
        except Exception as exc:
            self.compute_response = json.dumps({"id": req_id, "error": str(exc)})

    # ── cloud state ────────────────────────────────────────────────────────────

    def _current_state(self) -> dict:
        return {
            **self._base_state(),
            "params": {
                "edge_weight": self.edge_weight,
                "diff":        self.diff,
                "path_id_col": self.path_id_col,
            },
            "display": {
                "height":       self.height,
                "sidebar_open": self.sidebar_open,
            },
            "node_positions":   json.loads(self.node_positions or "{}"),
            "event_visibility": json.loads(self.event_visibility or "{}"),
        }

    def _apply_state(self, state: dict) -> None:
        p = state.get("params", {})
        d = state.get("display", {})

        self.edge_weight  = p.get("edge_weight", "proba_out")
        self.height       = d.get("height", 500)
        self.sidebar_open = d.get("sidebar_open", True)
        pos = state.get("node_positions", {})
        self.node_positions = json.dumps(pos) if pos else "{}"
        ev = state.get("event_visibility", {})
        self.event_visibility = json.dumps(ev) if ev else "{}"

        _diff, _pid, _mismatch = self._apply_base_state(state)
        self.diff        = json.dumps(list(_diff)) if _diff else ""
        self.path_id_col = _pid

        self._recompute()

    # ── dispatch ───────────────────────────────────────────────────────────────

    def _dispatch(self, tool: str, params: dict):
        if tool == "transition_graph_data":
            return self._compute_tm_raw(
                edge_weight=params.get("edge_weight", self.edge_weight),
                path_id_col=params.get("path_id_col") or self.path_id_col or None,
                diff=_parse_diff(params.get("diff")),
            )
        if tool == "graph_layout":
            return self._compute_graph_layout(params)
        raise ValueError(f"Unknown tool: {tool!r}")

    # ── computations ───────────────────────────────────────────────────────────

    def _recompute(self):
        self.is_loading = True
        self.error = ""
        try:
            diff_list = _parse_diff(self.diff)
            result = self._compute_tm_raw(
                edge_weight=self.edge_weight,
                path_id_col=self.path_id_col or None,
                diff=diff_list,
            )
            self.result = json.dumps(result)

            if diff_list:
                try:
                    s1, s2 = self._eventstream.split_two(diff_list, path_id_col=self.path_id_col or None)
                    c1 = s1.get_event_counts()
                    c2 = s2.get_event_counts()
                    pid = self.path_id_col or s1.schema.path_cols[0]
                    for counts, s in ((c1, s1), (c2, s2)):
                        n = int(s.df[pid].nunique())
                        counts.setdefault("path_start", n)
                        counts.setdefault("path_end", n)
                    self.event_counts_g1 = json.dumps(c1)
                    self.event_counts_g2 = json.dumps(c2)
                except Exception:
                    self.event_counts_g1 = "{}"
                    self.event_counts_g2 = "{}"
            else:
                self.event_counts_g1 = "{}"
                self.event_counts_g2 = "{}"
        except Exception as exc:
            self.error = str(exc)
            self.result = "{}"
        finally:
            self.is_loading = False

    def _compute_tm_raw(self, edge_weight: str, path_id_col=None, diff=None) -> dict:
        tm = self._eventstream.transition_graph_data(
            edge_weight=edge_weight, path_id_col=path_id_col, diff=diff,
        )
        if diff is not None:
            tm, tm1, tm2 = tm
            return {
                "events": tm.index.tolist(),
                "values": _df_to_list(tm),
                "group1": {"events": tm1.index.tolist(), "values": _df_to_list(tm1)},
                "group2": {"events": tm2.index.tolist(), "values": _df_to_list(tm2)},
            }
        return {"events": tm.index.tolist(), "values": _df_to_list(tm)}

    def _compute_graph_layout(self, params: dict) -> dict:
        try:
            from retentioneering.tools.graph_layout import GraphLayout  # type: ignore
            result = GraphLayout(self._eventstream).fit(
                sample_size=params.get("sample_size", 1000),
                embedding_dim=params.get("embedding_dim", 32),
                n_clusters=params.get("n_clusters", 5),
                random_state=params.get("random_state", 42),
            )
            return {"result": result}
        except Exception:
            return {"result": {}}


    # ── HTML export ───────────────────────────────────────────────────────────

    def export_html(
        self,
        path: str,
        title: str = "Transition Graph",
        analysis: str | None = None,
    ) -> None:
        """
        Export the current graph as a standalone interactive HTML file.

        Parameters
        ----------
        path:
            Destination file path.
        title:
            Title shown in the browser tab.
        analysis:
            Optional analysis text. Wrap event names in square brackets to make
            them clickable, e.g. ``"Drop-off at [basket]: 78% of users leave here."``.
            Supports basic markdown (bold, italic, bullet lists, tables, headings).
        """
        data = {
            "widget_type":      "transition_graph",
            "result":           json.loads(self.result or "{}"),
            "edge_weight":      self.edge_weight,
            "diff":             json.loads(self.diff) if self.diff else None,
            "event_counts":     json.loads(self.event_counts or "{}"),
            "event_counts_g1":  json.loads(self.event_counts_g1 or "{}"),
            "event_counts_g2":  json.loads(self.event_counts_g2 or "{}"),
            "node_positions":   json.loads(self.node_positions or "{}"),
            "event_visibility": json.loads(self.event_visibility or "{}"),
            "segment_levels":   json.loads(self.segment_levels or "{}"),
            "path_cols":        json.loads(self.path_cols or "[]"),
            "path_id_col":      self.path_id_col or "",
            "height":           self.height,
            "sidebar_open":     False,
        }
        write_html(path, title, "Transition Graph", data, analysis)


def _render_analysis(text: str) -> str:  # noqa: F401 — backward compat shim
    """Convert markdown text to HTML. [event] → clickable node focus link."""
    import re, html as _html

    def _inline(s: str) -> str:
        s = re.sub(
            r"\[([^\]]+)\]",
            r'<a href="javascript:void(0)" class="node-link"'
            r' onclick="return focusNode(this)" data-node="\1">\1</a>',
            s,
        )
        s = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", s)
        s = re.sub(r"\*(.+?)\*",     r"<em>\1</em>", s)
        s = re.sub(r"`(.+?)`",       r"<code>\1</code>", s)
        return s

    def esc(s: str) -> str:
        return _html.escape(s)

    lines = text.split("\n")
    out: list[str] = []
    i = 0

    while i < len(lines):
        raw = lines[i]
        stripped = raw.strip()

        if not stripped:
            i += 1
            continue

        # Horizontal rule
        if re.match(r"^[-*_]{3,}\s*$", stripped):
            out.append("<hr>")
            i += 1
            continue

        # ATX heading  # H1  ## H2 …
        m = re.match(r"^(#{1,6})\s+(.*)", stripped)
        if m:
            lvl = len(m.group(1))
            out.append(f"<h{lvl}>{_inline(esc(m.group(2).strip()))}</h{lvl}>")
            i += 1
            continue

        # Table — header row followed by |---|---| separator
        if "|" in stripped:
            nxt = lines[i + 1].strip() if i + 1 < len(lines) else ""
            if re.match(r"^\|?[\s\-:|]+\|[\s\-:|]*$", nxt):
                headers = [c.strip() for c in stripped.strip("|").split("|")]
                i += 2  # skip separator row
                rows = []
                while i < len(lines) and "|" in lines[i] and lines[i].strip():
                    rows.append([c.strip() for c in lines[i].strip("|").split("|")])
                    i += 1
                th = "".join(f"<th>{_inline(esc(h))}</th>" for h in headers)
                tbody = "".join(
                    "<tr>" + "".join(f"<td>{_inline(esc(c))}</td>" for c in row) + "</tr>"
                    for row in rows
                )
                out.append(f"<table><thead><tr>{th}</tr></thead><tbody>{tbody}</tbody></table>")
                continue

        # Unordered list
        if re.match(r"^[-*+]\s", stripped):
            items = []
            while i < len(lines) and re.match(r"^[-*+]\s", lines[i].strip()):
                items.append(_inline(esc(re.sub(r"^[-*+]\s+", "", lines[i].strip()))))
                i += 1
            out.append("<ul>" + "".join(f"<li>{item}</li>" for item in items) + "</ul>")
            continue

        # Ordered list
        if re.match(r"^\d+[.)]\s", stripped):
            items = []
            while i < len(lines) and re.match(r"^\d+[.)]\s", lines[i].strip()):
                items.append(_inline(esc(re.sub(r"^\d+[.)]\s+", "", lines[i].strip()))))
                i += 1
            out.append("<ol>" + "".join(f"<li>{item}</li>" for item in items) + "</ol>")
            continue

        # Paragraph — collect until blank line or block element
        para: list[str] = []
        while i < len(lines):
            s = lines[i].strip()
            if not s:
                i += 1
                break
            if (re.match(r"^#{1,6}\s", s) or
                    re.match(r"^[-*_]{3,}\s*$", s) or
                    re.match(r"^[-*+]\s", s) or
                    re.match(r"^\d+[.)]\s", s)):
                break
            if "|" in s:
                nxt = lines[i + 1].strip() if i + 1 < len(lines) else ""
                if re.match(r"^\|?[\s\-:|]+\|", nxt):
                    break
            para.append(_inline(esc(lines[i])))
            i += 1
        if para:
            out.append("<p>" + "<br>".join(para) + "</p>")

    return "\n".join(out)


_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{TITLE}}</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: #f8fafc; display: flex; align-items: center;
           justify-content: center; min-height: 100vh; }
    #retentioneering-root { width: 100%; max-width: 1400px; margin: 24px; }
  </style>
</head>
<body>
  <div id="retentioneering-root"></div>
  <script>window.__RETENTIONEERING_DATA__ = {{DATA_JSON}};</script>
  <script>{{BUNDLE_JS}}</script>
  <script>
    RetentioneeringWidget.renderStatic(
      window.__RETENTIONEERING_DATA__,
      document.getElementById('retentioneering-root')
    );
  </script>
</body>
</html>"""

_HTML_TEMPLATE_ANALYSIS = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{TITLE}}</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    html, body { height: 100%; overflow: hidden;
                 font-family: system-ui, -apple-system, sans-serif; }
    #layout { display: flex; height: 100vh; }

    /* left pane */
    #left-pane { display: flex; flex-direction: column; width: 50%;
                 min-width: 240px; }
    .tab-bar { display: flex; align-items: center; padding: 0 4px; height: 40px;
               border-bottom: 1px solid #e5e7eb; background: #fff;
               flex-shrink: 0; gap: 2px; }
    .tab { padding: 0 14px; height: 100%; font-size: 12px; font-weight: 500;
           color: #6b7280; border: none; background: none; cursor: pointer;
           border-bottom: 2px solid transparent;
           display: flex; align-items: center; white-space: nowrap; }
    .tab.active { color: #111827; border-bottom-color: #2563eb; }
    .tab:hover:not(.active) { color: #374151; background: #f9fafb; }
    .tab-content { flex: 1; min-height: 0; overflow: hidden; position: relative; }
    #retentioneering-root { position: absolute; inset: 0; }
    /* override widget fixed-pixel height to fill the pane */
    #retentioneering-root > div { height: 100% !important;
                            border-radius: 0 !important; border: none !important; }

    /* splitter */
    #splitter { width: 5px; flex-shrink: 0; background: #e5e7eb; cursor: col-resize;
                transition: background 0.15s; }
    #splitter:hover { background: #93c5fd; }
    body.hs-resizing * { user-select: none; }
    body.hs-resizing #splitter { background: #2563eb; }

    /* right pane */
    #right-pane { flex: 1; min-width: 240px; display: flex; flex-direction: column;
                  overflow: hidden; background: #fff; border-left: 1px solid #e5e7eb; }
    #analysis-title { height: 40px; display: flex; align-items: center;
                      padding: 0 20px; font-size: 15px; font-weight: 600;
                      color: #111827; border-bottom: 1px solid #e5e7eb;
                      flex-shrink: 0; }
    #analysis-body { flex: 1; overflow-y: auto; padding: 20px 24px;
                     font-size: 14px; line-height: 1.75; color: #374151; }
    /* markdown */
    #analysis-body h1 { font-size: 18px; font-weight: 700; color: #111827;
                        margin: 0 0 14px; }
    #analysis-body h2 { font-size: 15px; font-weight: 600; color: #111827;
                        margin: 20px 0 8px; }
    #analysis-body h3 { font-size: 13px; font-weight: 600; color: #374151;
                        margin: 16px 0 6px; }
    #analysis-body h1:first-child,
    #analysis-body h2:first-child { margin-top: 0; }
    #analysis-body p  { margin: 0 0 12px; }
    #analysis-body ul,
    #analysis-body ol { margin: 4px 0 12px 20px; }
    #analysis-body li { margin-bottom: 4px; }
    #analysis-body strong { color: #111827; font-weight: 600; }
    #analysis-body code { font-family: ui-monospace, monospace; font-size: 12px;
                          background: #f3f4f6; padding: 1px 5px; border-radius: 3px; }
    #analysis-body hr { border: none; border-top: 1px solid #e5e7eb; margin: 16px 0; }
    #analysis-body table { border-collapse: collapse; width: 100%;
                           margin: 8px 0 16px; font-size: 13px; }
    #analysis-body th { background: #f9fafb; font-weight: 600; color: #374151;
                        padding: 7px 10px; text-align: left;
                        border: 1px solid #e5e7eb; }
    #analysis-body td { padding: 6px 10px; border: 1px solid #e5e7eb; }
    #analysis-body tr:nth-child(even) td { background: #f9fafb; }
    a.node-link { color: #2563eb; text-decoration: none;
                  border-bottom: 1px solid #bfdbfe; cursor: pointer; }
    a.node-link:hover { color: #1d4ed8; border-bottom-color: #1d4ed8; }
  </style>
</head>
<body>
  <div id="layout">
    <div id="left-pane">
      <div class="tab-bar">
        <button class="tab active" data-tab="graph">Transition Graph</button>
      </div>
      <div class="tab-content">
        <div id="retentioneering-root"></div>
      </div>
    </div>
    <div id="splitter"></div>
    <div id="right-pane">
      <div id="analysis-title">{{TITLE}}</div>
      <div id="analysis-body">{{ANALYSIS_HTML}}</div>
    </div>
  </div>
  <script>window.__RETENTIONEERING_DATA__ = {{DATA_JSON}};</script>
  <script>{{BUNDLE_JS}}</script>
  <script>
    RetentioneeringWidget.renderStatic(
      window.__RETENTIONEERING_DATA__,
      document.getElementById('retentioneering-root')
    );

    function focusNode(link) {
      RetentioneeringWidget.focusNode(link.dataset.node, document.getElementById('retentioneering-root'));
      return false;
    }

    // Resizable splitter — overlay prevents cytoscape canvas from eating mouse events
    (function() {
      var splitter = document.getElementById('splitter');
      var leftPane = document.getElementById('left-pane');
      var overlay  = null;

      splitter.addEventListener('mousedown', function(e) {
        document.body.classList.add('hs-resizing');
        overlay = document.createElement('div');
        overlay.style.cssText = 'position:fixed;inset:0;z-index:9999;cursor:col-resize';
        document.body.appendChild(overlay);
        e.preventDefault();
      });
      document.addEventListener('mousemove', function(e) {
        if (!overlay) return;
        var w = Math.max(240, Math.min(window.innerWidth - 240, e.clientX));
        leftPane.style.width = w + 'px';
        leftPane.style.flex  = 'none';
      });
      document.addEventListener('mouseup', function() {
        if (!overlay) return;
        overlay.remove(); overlay = null;
        document.body.classList.remove('hs-resizing');
      });
    })();
  </script>
</body>
</html>"""


# ── helpers ────────────────────────────────────────────────────────────────────

def _df_to_list(df) -> list:
    import pandas as pd
    rows = []
    for _, row in df.iterrows():
        cells = []
        for v in row:
            if pd.isna(v):
                cells.append(None)
            elif isinstance(v, pd.Timedelta):
                cells.append(v.total_seconds())
            elif hasattr(v, "__float__"):
                cells.append(float(v))
            else:
                cells.append(v)
        rows.append(cells)
    return rows
