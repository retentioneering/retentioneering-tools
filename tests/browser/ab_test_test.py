"""Render the A/B test widget in a real browser: the static export, and a live
host whose sidebar changes go to the Python widget and come back recomputed.

Run with ``uv run --with playwright pytest tests/browser -v`` after
``uv run --with playwright python -m playwright install chromium`` and
``make build``. The ordinary Python suite skips this module when Playwright
is absent; CI runs it with Chromium on Python 3.12.
"""

import json
import os

import numpy as np
import pandas as pd
import pytest

from retentioneering import Eventstream
from retentioneering.widgets._html_export import _BUNDLE_PATH, _CSS_PATH

playwright = pytest.importorskip("playwright.sync_api")

DIFF = ("grp", "t", "c")
METRICS = [
    {"name": "conversion", "metric": "has_event", "metric_args": {"event": "buy"}},
    {"name": "events", "metric": "length"},
    {"metric": "duration"},
]


def _stream() -> Eventstream:
    rng = np.random.default_rng(1)
    rows = []
    for g, p in (("t", 0.7), ("c", 0.4)):
        for i in range(120):
            t = pd.Timestamp("2026-01-01") + pd.Timedelta(days=int(rng.integers(0, 21)))
            platform = "ios" if i % 2 else "web"
            rows.append([f"{g}{i}", "home", t, g, platform])
            if rng.random() < p:
                rows.append([f"{g}{i}", "buy", t + pd.Timedelta("7min"), g, platform])
    df = pd.DataFrame(
        rows, columns=["user_id", "event", "timestamp", "grp", "platform"]
    )
    return Eventstream(df, {"segment_cols": ["grp", "platform"]})


def _launch(runtime):
    # Optional local-browser override; CI uses Playwright's pinned Chromium.
    return runtime.chromium.launch(
        executable_path=os.environ.get("RETENTIONEERING_TEST_CHROME")
    )


def test_static_export_renders_every_view_read_only(tmp_path):
    export = tmp_path / "ab.html"
    _stream().ab_test(
        DIFF, METRICS, strata_col="platform", strata_pre_exposure=True, period="W"
    ).export_html(str(export))

    with playwright.sync_playwright() as runtime:
        browser = _launch(runtime)
        try:
            page = browser.new_page(viewport={"width": 1300, "height": 900})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(export.as_uri())
            page.locator("[data-testid=scorecard]").wait_for(state="visible")
            assert page.locator("[data-testid=scorecard-row]").count() == 3
            assert page.locator("[data-testid=forest-point]").count() == 3
            # conversion is a 0/1 metric: its delta is in percentage points
            row = page.locator("tr[data-event=conversion]")
            assert " pp" in row.locator("[data-testid=delta-cell]").inner_text()
            # no kernel behind a static export: the sidebar is locked
            assert page.get_by_label("Confidence level").is_disabled()
            assert page.get_by_label("Strata column").is_disabled()

            page.locator("[data-testid=ab-tab-strata]").click()
            assert page.locator("[data-testid=strata-row]").count() == 6
            assert page.locator("[data-testid=strata-exploratory]").count() == 0

            page.locator("[data-testid=ab-tab-periods]").click()
            points = page.locator("[data-testid=period-point]")
            assert points.count() > 3
            # the last week ends after the data: drawn hollow
            assert (
                page.locator("[data-testid=period-point][data-hollow=true]").count()
                >= 1
            )
            assert "NaN" not in page.locator("body").inner_text()
            assert not errors, errors
        finally:
            browser.close()


_LIVE_PAGE = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><style>{css}</style></head>
<body><div id="root" style="margin:16px"></div>
<script>{bundle}</script>
<script>
  const state = {state};
  const listeners = {{}};
  const fire = (key) => (listeners[key] || []).forEach((cb) => cb());
  // A stand-in for the anywidget model: every write goes to the Python widget
  // (window.pySet), which recomputes and returns the traitlets that changed.
  const host = {{
    get: (key) => (key in state ? state[key] : null),
    set(key, value) {{
      state[key] = value; fire(key);
      window.pySet(key, value).then((changed) => {{
        for (const [k, v] of Object.entries(changed)) {{ state[k] = v; fire(k); }}
      }});
    }},
    setMany(values) {{ for (const [k, v] of Object.entries(values)) host.set(k, v); }},
    onChange(key, cb) {{
      (listeners[key] = listeners[key] || []).push(cb);
      return () => {{ listeners[key] = listeners[key].filter((c) => c !== cb); }};
    }},
    compute: () => Promise.reject(new Error("not used")),
  }};
  RetentioneeringWidget.renderWithHost(host, document.getElementById("root"));
</script></body></html>"""


def test_sidebar_changes_recompute_through_python(tmp_path):
    widget = _stream().ab_test(DIFF, METRICS, sidebar_open=True)
    page_file = tmp_path / "live.html"
    page_file.write_text(
        _LIVE_PAGE.format(
            css=_CSS_PATH.read_text(),
            bundle=_BUNDLE_PATH.read_text(),
            state=json.dumps(widget.sync_state()),
        )
    )
    calls = []

    def py_set(key, value):
        before = widget.sync_state()
        calls.append((key, value))
        setattr(widget, key, value)
        after = widget.sync_state()
        return {k: v for k, v in after.items() if before.get(k) != v}

    with playwright.sync_playwright() as runtime:
        browser = _launch(runtime)
        try:
            page = browser.new_page(viewport={"width": 1300, "height": 900})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.expose_function("pySet", py_set)
            page.goto(page_file.as_uri())
            page.locator("[data-testid=scorecard]").wait_for(state="visible")
            assert page.locator("[data-testid=ab-tab-strata]").count() == 0

            # strata: a new tab, computed by Python
            page.get_by_label("Strata column").select_option("platform")
            page.locator("[data-testid=ab-tab-strata]").wait_for(state="visible")
            assert widget.strata_col == "platform"
            page.locator("[data-testid=ab-tab-strata]").click()
            assert page.locator("[data-testid=strata-row]").count() == 6
            # not declared pre-exposure: exploratory, and the library says so
            assert page.locator("[data-testid=strata-exploratory]").count() == 1
            assert (
                "strata_pre_exposure"
                in page.locator("[data-testid=ab-warnings]").inner_text()
            )
            page.get_by_label("Strata fixed before exposure").select_option("true")
            page.locator("[data-testid=strata-exploratory]").wait_for(state="detached")
            assert widget.strata_pre_exposure is True

            # confidence level and correction: the scorecard's note follows
            page.locator("[data-testid=ab-tab-scorecard]").click()
            page.get_by_label("Confidence level").select_option("0.99")
            page.get_by_text("99% intervals").first.wait_for(state="visible")
            assert json.loads(widget.result)["ci_level"] == 0.99
            page.get_by_label("Correction").select_option("bh")
            page.get_by_text("adjusted with bh").first.wait_for(state="visible")

            # periods
            page.get_by_label("Period").select_option("W")
            page.locator("[data-testid=ab-tab-periods]").wait_for(state="visible")
            assert widget.period == "W"

            # a horizon without a unit is refused before it reaches Python
            n_calls = len(calls)
            horizon = page.get_by_label("Horizon")
            assert horizon.is_disabled()  # no assignment table with assigned_at
            assert len(calls) == n_calls

            # the metric editor: drop the duration row, apply, one recompute
            page.locator("[data-testid=ab-metrics-button]").click()
            rows = page.locator("[data-testid=ab-metric-row]")
            assert rows.count() == 3
            rows.nth(2).locator("button").first.click()
            page.locator("[data-testid=ab-metrics-apply]").click()
            page.wait_for_function(
                "document.querySelectorAll('[data-testid=scorecard-row]').length === 2"
            )
            assert [m.get("name") for m in json.loads(widget.metrics)] == [
                "conversion",
                "events",
            ]
            assert [key for key, _ in calls].count("metrics") == 1
            assert widget.error == ""
            assert not errors, errors
        finally:
            browser.close()


def test_horizon_is_validated_as_a_duration(tmp_path):
    from retentioneering.datasets.ab_test import load_ab_test, load_ab_test_assignment

    stream = load_ab_test()
    widget = stream.ab_test(
        ("exp_group", "treatment", "control"),
        [
            {
                "name": "conversion",
                "metric": "has_event",
                "metric_args": {"event": "order_success"},
            }
        ],
        assignment=load_ab_test_assignment(),
    )
    page_file = tmp_path / "live.html"
    page_file.write_text(
        _LIVE_PAGE.format(
            css=_CSS_PATH.read_text(),
            bundle=_BUNDLE_PATH.read_text(),
            state=json.dumps(widget.sync_state()),
        )
    )
    calls = []

    def py_set(key, value):
        before = widget.sync_state()
        calls.append(key)
        setattr(widget, key, value)
        return {k: v for k, v in widget.sync_state().items() if before.get(k) != v}

    with playwright.sync_playwright() as runtime:
        browser = _launch(runtime)
        try:
            page = browser.new_page(viewport={"width": 1300, "height": 900})
            page.expose_function("pySet", py_set)
            page.goto(page_file.as_uri())
            page.locator("[data-testid=scorecard]").wait_for(state="visible")
            horizon = page.get_by_label("Horizon")
            horizon.fill("14")
            horizon.press("Enter")
            page.locator("[data-testid=horizon-error]").wait_for(state="visible")
            assert "horizon" not in calls and widget.horizon == ""
            horizon.fill("28D")
            horizon.press("Enter")
            page.wait_for_function(
                "!document.querySelector('[data-testid=horizon-error]')"
            )
            page.wait_for_timeout(100)
            assert widget.horizon == "28D"
            assert json.loads(widget.result)["overall"][0]["group1_paths"] < 4960
        finally:
            browser.close()


def test_report_link_rings_the_metric_row(tmp_path):
    from retentioneering.mcp import tools
    from retentioneering.mcp._report_session import ReportSession

    session = ReportSession(_stream())
    tools.add_ab_test(session, "Experiment", list(DIFF), METRICS)
    path = tmp_path / "report.html"
    session.export("Report", "Conversion moved: [Experiment:conversion].", str(path))

    with playwright.sync_playwright() as runtime:
        browser = _launch(runtime)
        try:
            page = browser.new_page(viewport={"width": 1300, "height": 800})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(path.as_uri())
            page.locator("[data-testid=scorecard]").wait_for(state="visible")
            page.locator("a.node-link", has_text="conversion").click()
            page.wait_for_timeout(400)
            first_cell = page.locator('tr[data-event="conversion"] td').first
            assert first_cell.evaluate(
                "el => getComputedStyle(el).outlineColor === 'rgb(245, 158, 11)'"
            )
            assert not errors, errors
        finally:
            browser.close()
