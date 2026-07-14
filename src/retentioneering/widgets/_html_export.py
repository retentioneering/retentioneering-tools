"""Shared HTML export utilities for widget static reports."""

from __future__ import annotations

import json
import pathlib
import re
import html as _html_mod

_BUNDLE_PATH = pathlib.Path(__file__).parent.parent / "static" / "widget-static.js"


def _json_for_script(obj: object) -> str:
    """Serialize ``obj`` to JSON safe for embedding inside a ``<script>`` block.

    A literal ``</script>`` inside a JSON string would terminate the script
    element (breaking the page and enabling HTML injection), so escape every
    ``</`` as ``<\\/`` — a no-op inside JS string literals.
    """
    return json.dumps(obj, ensure_ascii=False).replace("</", "<\\/")


# ── Public API ─────────────────────────────────────────────────────────────────


def write_html(
    path: str,
    title: str,
    widget_label: str,
    data: dict,
    analysis: str | None = None,
) -> None:
    """Export a single widget as a standalone HTML file."""
    if analysis is None:
        _write_bare(path, title, data)
    else:
        write_report_html(
            path, title, [{"label": widget_label, "data": data}], analysis
        )


def write_report_html(
    path: str,
    title: str,
    widgets: list[dict],
    analysis: str | None = None,
    data_sources_html: str = "",
) -> None:
    """Export a multi-widget report HTML file.

    Parameters
    ----------
    widgets:
        List of ``{"label": str, "data": dict}`` entries — one per tab.
    analysis:
        Markdown text. Use ``[tab_label:event_name]`` to link to a specific tab
        and focus an event; use ``[event_name]`` to focus in the active tab.
    """
    if not _BUNDLE_PATH.exists():
        raise FileNotFoundError(
            f"Static bundle not found at {_BUNDLE_PATH}. "
            "Run `npm run build` in js/widget/ to generate it."
        )
    bundle_js = _BUNDLE_PATH.read_text(encoding="utf-8")
    # Build enriched map: label → {tab_id, widget_type, segment_col}
    # segment_col is used by render_analysis to format segment_overview links
    label_map = {
        w["label"]: {
            "tab_id": f"tab-{i}",
            "widget_type": w["data"].get("widget_type", ""),
            "segment_col": w["data"].get("segment_col", ""),
        }
        for i, w in enumerate(widgets)
    }
    widgets_json = _json_for_script(widgets)
    analysis_html = render_analysis(analysis, label_map=label_map) if analysis else ""
    if data_sources_html:
        analysis_html = data_sources_html + "\n" + analysis_html
    html = (
        _HTML_TEMPLATE_REPORT.replace("{{TITLE}}", _html_mod.escape(title))
        .replace("{{WIDGETS_JSON}}", widgets_json)
        .replace("{{BUNDLE_JS}}", bundle_js)
        .replace("{{ANALYSIS_HTML}}", analysis_html)
    )
    pathlib.Path(path).write_text(html, encoding="utf-8")


def render_analysis(text: str, label_map: dict | None = None) -> str:
    """Convert markdown text to HTML.

    ``[tab_label:ref]``  → link that activates the tab and focuses ref.
    ``[ref]``            → link that focuses ref in the active tab.

    For segment_overview tabs, ref is displayed as "segment_col: value".
    """

    def _inline(s: str) -> str:
        # [label:ref] — tab-specific link (process first, more specific)
        if label_map:

            def _tab_link(m: re.Match) -> str:
                label, ref = m.group(1), m.group(2)
                entry = label_map.get(label)
                if not entry:
                    return m.group(0)
                # entry is either a plain tab_id string (legacy) or a metadata dict
                if isinstance(entry, dict):
                    tab_id = entry["tab_id"]
                    widget_type = entry.get("widget_type", "")
                    segment_col = entry.get("segment_col", "")
                else:
                    tab_id = entry
                    widget_type = ""
                    segment_col = ""
                # Segment overview links
                if not ref:
                    # Empty ref: just activate the tab, no focus
                    return (
                        f'<a href="javascript:void(0)" class="node-link"'
                        f' onclick="return focusLink(this)"'
                        f' data-tab="{tab_id}"'
                        f' title="Open: {_html_mod.escape(label)}">'
                        f"{_html_mod.escape(label)}</a>"
                    )
                if widget_type == "segment_overview" and segment_col:
                    at = ref.find("@")
                    if at != -1:
                        metric = ref[:at]
                        seg_val = ref[at + 1 :]
                        display = (
                            f"{metric}({_html_mod.escape(segment_col)}: {seg_val})"
                        )
                    else:
                        display = f"{_html_mod.escape(segment_col)}: {ref}"
                else:
                    display = ref
                return (
                    f'<a href="javascript:void(0)" class="node-link"'
                    f' onclick="return focusLink(this)"'
                    f' data-tab="{tab_id}" data-node="{ref}"'
                    f' title="Open in: {_html_mod.escape(label)}">'
                    f"{display}</a>"
                )

            # Allow empty ref after colon: [Tab Name:] — just activate tab
            s = re.sub(r"\[([^:\]]+):(.*?)\]", _tab_link, s)

        # [text] — if text matches a tab label, activate that tab;
        #          otherwise focus as event/edge in the active tab
        def _bare_link(m: re.Match) -> str:
            ref = m.group(1)
            if label_map and ref in label_map:
                entry = label_map[ref]
                tab_id = entry["tab_id"] if isinstance(entry, dict) else entry
                return (
                    f'<a href="javascript:void(0)" class="node-link"'
                    f' onclick="return focusLink(this)"'
                    f' data-tab="{tab_id}"'
                    f' title="Open: {_html_mod.escape(ref)}">'
                    f"{_html_mod.escape(ref)}</a>"
                )
            return (
                f'<a href="javascript:void(0)" class="node-link"'
                f' onclick="return focusLink(this)" data-node="{ref}">{ref}</a>'
            )

        s = re.sub(r"\[([^\]]+)\]", _bare_link, s)
        s = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", s)
        s = re.sub(r"\*(.+?)\*", r"<em>\1</em>", s)
        s = re.sub(r"`(.+?)`", r"<code>\1</code>", s)
        return s

    def esc(s: str) -> str:
        return _html_mod.escape(s)

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

        # ATX heading
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
                i += 2
                rows = []
                while i < len(lines) and "|" in lines[i] and lines[i].strip():
                    rows.append([c.strip() for c in lines[i].strip("|").split("|")])
                    i += 1
                th = "".join(f"<th>{_inline(esc(h))}</th>" for h in headers)
                tbody = "".join(
                    "<tr>"
                    + "".join(f"<td>{_inline(esc(c))}</td>" for c in row)
                    + "</tr>"
                    for row in rows
                )
                out.append(
                    f"<table><thead><tr>{th}</tr></thead><tbody>{tbody}</tbody></table>"
                )
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

        # Paragraph
        para: list[str] = []
        while i < len(lines):
            s = lines[i].strip()
            if not s:
                i += 1
                break
            if (
                re.match(r"^#{1,6}\s", s)
                or re.match(r"^[-*_]{3,}\s*$", s)
                or re.match(r"^[-*+]\s", s)
                or re.match(r"^\d+[.)]\s", s)
            ):
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


# ── Templates ──────────────────────────────────────────────────────────────────


def _write_bare(path: str, title: str, data: dict) -> None:
    """Single widget, no analysis panel."""
    if not _BUNDLE_PATH.exists():
        raise FileNotFoundError(
            f"Static bundle not found at {_BUNDLE_PATH}. "
            "Run `npm run build` in js/widget/ to generate it."
        )
    bundle_js = _BUNDLE_PATH.read_text(encoding="utf-8")
    html = (
        _HTML_TEMPLATE_BARE.replace("{{TITLE}}", _html_mod.escape(title))
        .replace("{{DATA_JSON}}", _json_for_script(data))
        .replace("{{BUNDLE_JS}}", bundle_js)
    )
    pathlib.Path(path).write_text(html, encoding="utf-8")


_HTML_TEMPLATE_BARE = """<!DOCTYPE html>
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
  <script>window.__HS_DATA__ = {{DATA_JSON}};</script>
  <script>{{BUNDLE_JS}}</script>
  <script>
    (function () {
      var override = new URLSearchParams(location.search).get('sidebar_open');
      if (override !== null) window.__HS_DATA__.sidebar_open = override === 'true';
    })();
    RetentioneeringWidget.renderStatic(window.__HS_DATA__, document.getElementById('retentioneering-root'));
  </script>
</body>
</html>"""


_HTML_TEMPLATE_REPORT = """<!DOCTYPE html>
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

    /* ── left pane ── */
    #left-pane { display: flex; flex-direction: column; width: 50%; min-width: 240px; }
    .tab-bar { display: flex; align-items: center; padding: 0 4px; height: 40px;
               border-bottom: 1px solid #e5e7eb; background: #fff;
               flex-shrink: 0; gap: 2px; overflow-x: auto; }
    .tab { padding: 0 14px; height: 100%; font-size: 12px; font-weight: 500;
           color: #6b7280; border: none; background: none; cursor: pointer;
           border-bottom: 2px solid transparent; display: flex; align-items: center;
           white-space: nowrap; flex-shrink: 0; }
    .tab.active { color: #111827; border-bottom-color: #2563eb; }
    .tab:hover:not(.active) { color: #374151; background: #f9fafb; }
    .tab-content { flex: 1; min-height: 0; position: relative; }
    /* All panels occupy the same space; only active is visible */
    .tab-panel { position: absolute; inset: 0; visibility: hidden; pointer-events: none; }
    .tab-panel.active { visibility: visible; pointer-events: auto; }
    .widget-root { position: absolute; inset: 0; }
    .widget-root > div { height: 100% !important;
                         border-radius: 0 !important; border: none !important; }

    /* ── splitter ── */
    #splitter { width: 5px; flex-shrink: 0; background: #e5e7eb; cursor: col-resize;
                transition: background 0.15s; }
    #splitter:hover { background: #93c5fd; }
    body.hs-resizing * { user-select: none; }
    body.hs-resizing #splitter { background: #2563eb; }

    /* ── right pane ── */
    #right-pane { flex: 1; min-width: 240px; display: flex; flex-direction: column;
                  overflow: hidden; background: #fff; border-left: 1px solid #e5e7eb; }
    #analysis-title { height: 40px; display: flex; align-items: center;
                      padding: 0 20px; font-size: 15px; font-weight: 600;
                      color: #111827; border-bottom: 1px solid #e5e7eb; flex-shrink: 0; }
    #analysis-body { flex: 1; overflow-y: auto; padding: 20px 24px;
                     font-size: 14px; line-height: 1.75; color: #374151; }
    /* markdown */
    #analysis-body h1 { font-size: 18px; font-weight: 700; color: #111827; margin: 0 0 14px; }
    #analysis-body h2 { font-size: 15px; font-weight: 600; color: #111827; margin: 20px 0 8px; }
    #analysis-body h3 { font-size: 13px; font-weight: 600; color: #374151; margin: 16px 0 6px; }
    #analysis-body h1:first-child, #analysis-body h2:first-child { margin-top: 0; }
    #analysis-body p  { margin: 0 0 12px; }
    #analysis-body ul, #analysis-body ol { margin: 4px 0 12px 20px; }
    #analysis-body li { margin-bottom: 4px; }
    #analysis-body strong { color: #111827; font-weight: 600; }
    #analysis-body code { font-family: ui-monospace, monospace; font-size: 12px;
                          background: #f3f4f6; padding: 1px 5px; border-radius: 3px; }
    #analysis-body hr { border: none; border-top: 1px solid #e5e7eb; margin: 16px 0; }
    #analysis-body table { border-collapse: collapse; width: 100%; margin: 8px 0 16px; font-size: 13px; }
    #analysis-body th { background: #f9fafb; font-weight: 600; color: #374151;
                        padding: 7px 10px; text-align: left; border: 1px solid #e5e7eb; }
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
      <div class="tab-bar"     id="tab-bar"></div>
      <div class="tab-content" id="tab-content"></div>
    </div>
    <div id="splitter"></div>
    <div id="right-pane">
      <div id="analysis-title">{{TITLE}}</div>
      <div id="analysis-body">{{ANALYSIS_HTML}}</div>
    </div>
  </div>

  <script>window.__HS_WIDGETS__ = {{WIDGETS_JSON}};</script>
  <script>{{BUNDLE_JS}}</script>
  <script>
    (function () {
      var widgets    = window.__HS_WIDGETS__;
      var tabBar     = document.getElementById('tab-bar');
      var tabContent = document.getElementById('tab-content');

      // Build tabs and panels, render each widget
      widgets.forEach(function (w, i) {
        var tabId = 'tab-' + i;

        var btn = document.createElement('button');
        btn.className = 'tab' + (i === 0 ? ' active' : '');
        btn.dataset.tabId = tabId;
        btn.textContent = w.label;
        btn.addEventListener('click', function () { activateTab(tabId); });
        tabBar.appendChild(btn);

        var panel = document.createElement('div');
        panel.className = 'tab-panel' + (i === 0 ? ' active' : '');
        panel.id = tabId;

        var root = document.createElement('div');
        root.className = 'widget-root';
        panel.appendChild(root);
        tabContent.appendChild(panel);

        RetentioneeringWidget.renderStatic(w.data, root);
      });

      function activateTab(tabId) {
        document.querySelectorAll('.tab').forEach(function (t) {
          t.classList.toggle('active', t.dataset.tabId === tabId);
        });
        document.querySelectorAll('.tab-panel').forEach(function (p) {
          p.classList.toggle('active', p.id === tabId);
        });
      }

      // Expose for analysis links
      window.activateTab = activateTab;
      window.focusLink = function (link) {
        var tabId     = link.dataset.tab;
        var eventName = link.dataset.node;
        var root;
        if (tabId) {
          activateTab(tabId);
          var panel = document.getElementById(tabId);
          root = panel && panel.querySelector('.widget-root');
        } else {
          var activePanel = document.querySelector('.tab-panel.active');
          root = activePanel && activePanel.querySelector('.widget-root');
        }
        if (root && eventName) {
          RetentioneeringWidget.focusNode(eventName, root);
          RetentioneeringWidget.scrollToEvent(eventName, root);
        }
        return false;
      };

      // Resizable splitter
      var splitter = document.getElementById('splitter');
      var leftPane  = document.getElementById('left-pane');
      var overlay   = null;

      splitter.addEventListener('mousedown', function (e) {
        document.body.classList.add('hs-resizing');
        overlay = document.createElement('div');
        overlay.style.cssText = 'position:fixed;inset:0;z-index:9999;cursor:col-resize';
        document.body.appendChild(overlay);
        e.preventDefault();
      });
      document.addEventListener('mousemove', function (e) {
        if (!overlay) return;
        var w = Math.max(240, Math.min(window.innerWidth - 240, e.clientX));
        leftPane.style.width = w + 'px';
        leftPane.style.flex  = 'none';
      });
      document.addEventListener('mouseup', function () {
        if (!overlay) return;
        overlay.remove(); overlay = null;
        document.body.classList.remove('hs-resizing');
      });
    })();
  </script>
</body>
</html>"""
