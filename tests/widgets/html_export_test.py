"""Tests for script/HTML injection hardening in widgets._html_export."""

import json
import re

import pytest

from retentioneering.widgets import _html_export

XSS_VALUE = "</script><img src=x onerror=alert(1)>"
XSS_TITLE = "</title><script>x</script>"


@pytest.fixture(autouse=True)
def fake_bundle(tmp_path, monkeypatch):
    """Point _BUNDLE_PATH at a tiny stub so tests don't need the JS build."""
    bundle = tmp_path / "widget-static.js"
    bundle.write_text("/* stub bundle */", encoding="utf-8")
    monkeypatch.setattr(_html_export, "_BUNDLE_PATH", bundle)


def _extract_json(html: str, var_name: str) -> str:
    m = re.search(
        rf"<script>window\.{var_name} = (.*?);</script>",
        html,
        flags=re.DOTALL,
    )
    assert m, f"window.{var_name} assignment not found in output"
    return m.group(1)


def test_report_html_escapes_script_breakout_in_widget_data(tmp_path):
    widgets = [
        {"label": "Tab 1", "data": {"widget_type": "step_matrix", "note": XSS_VALUE}}
    ]
    out = tmp_path / "report.html"

    _html_export.write_report_html(
        str(out), "Report", widgets, analysis="Some *analysis*."
    )
    html = out.read_text(encoding="utf-8")

    # The raw breakout sequence must never appear in the document.
    assert "</script><img" not in html
    # The JSON payload survives with the safe <\/ escape instead.
    assert "<\\/script><img src=x onerror=alert(1)>" in html

    # The embedded JSON still round-trips to the original data.
    payload = _extract_json(html, "__HS_WIDGETS__")
    assert json.loads(payload) == widgets


def test_report_html_escapes_title(tmp_path):
    widgets = [{"label": "Tab 1", "data": {"widget_type": "step_matrix"}}]
    out = tmp_path / "report.html"

    _html_export.write_report_html(str(out), XSS_TITLE, widgets, analysis="text")
    html = out.read_text(encoding="utf-8")

    assert XSS_TITLE not in html
    escaped = "&lt;/title&gt;&lt;script&gt;x&lt;/script&gt;"
    # Both title sinks (<title> and the analysis header div) are escaped.
    assert f"<title>{escaped}</title>" in html
    assert f'<div id="analysis-title">{escaped}</div>' in html


def test_bare_html_escapes_script_breakout_and_title(tmp_path):
    data = {"widget_type": "step_matrix", "note": XSS_VALUE}
    out = tmp_path / "widget.html"

    # analysis=None routes write_html through the bare template (_write_bare).
    _html_export.write_html(str(out), XSS_TITLE, "Tab 1", data, analysis=None)
    html = out.read_text(encoding="utf-8")

    assert "</script><img" not in html
    assert "<\\/script><img src=x onerror=alert(1)>" in html
    assert XSS_TITLE not in html
    assert "<title>&lt;/title&gt;&lt;script&gt;x&lt;/script&gt;</title>" in html

    payload = _extract_json(html, "__HS_DATA__")
    assert json.loads(payload) == data
