"""Report links mark their target cell in table widgets until something else does.

Run with ``uv run --with playwright pytest tests/browser -v`` after
``uv run --with playwright python -m playwright install chromium`` and
``make build``. The ordinary Python suite skips this module when Playwright
is absent; CI runs it with Chromium on Python 3.12.
"""

import os

import pandas as pd
import pytest

from retentioneering import Eventstream
from retentioneering.mcp import tools
from retentioneering.mcp._report_session import ReportSession

playwright = pytest.importorskip("playwright.sync_api")

# The link-focus ring specifically: a clicked overview cell gets the widget's
# own selection outline, which is not what these assertions are about.
FOCUSED = """el => getComputedStyle(el).outlineStyle !== 'none'
    && getComputedStyle(el).outlineColor === 'rgb(245, 158, 11)'"""


def _stream() -> Eventstream:
    rows = []
    ts = pd.Timestamp("2026-01-01")
    paths = {
        "u1": (["home", "cart", "error", "support"], "a"),
        "u2": (["home", "cart", "purchase"], "b"),
        "u3": (["home", "error", "cart", "purchase"], "a"),
    }
    for user, (events, group) in paths.items():
        for i, event in enumerate(events):
            rows.append(
                {
                    "user_id": user,
                    "event": event,
                    "timestamp": ts + pd.Timedelta(minutes=i),
                    "group": group,
                }
            )
    return Eventstream(pd.DataFrame(rows), schema={"segment_cols": ["group"]})


def _report(tmp_path) -> str:
    session = ReportSession(_stream())
    tools.add_step_matrix(session, "Matrix", max_steps=3, path_pattern="error")
    tools.add_segment_overview(
        session,
        "Groups",
        segment_col="group",
        metrics=[{"metric": "has_event", "metric_args": {"event": "purchase"}}],
    )
    path = tmp_path / "report.html"
    session.export(
        "Report",
        "After [Matrix:support@1] and [Matrix:cart@1]; "
        "[Groups:has_event_purchase@b] in group b.",
        str(path),
    )
    return path.as_uri()


def test_report_links_ring_cells_until_the_next_focus(tmp_path):
    url = _report(tmp_path)
    with playwright.sync_playwright() as runtime:
        executable = os.environ.get("RETENTIONEERING_TEST_CHROME")
        browser = runtime.chromium.launch(executable_path=executable)
        try:
            page = browser.new_page(viewport={"width": 1100, "height": 700})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(url)
            page.wait_for_timeout(500)

            def cell(tab, selector):
                return page.locator(f"#{tab} {selector}")

            matrix_tab = page.locator(".tab-panel").nth(0).get_attribute("id")
            groups_tab = page.locator(".tab-panel").nth(1).get_attribute("id")
            support = cell(matrix_tab, 'tr[data-event="support"] td[data-step="1"]')
            cart = cell(matrix_tab, 'tr[data-event="cart"] td[data-step="1"]')
            purchase_b = cell(
                groups_tab, 'tr[data-metric^="has_event_purchase"] td[data-segment="b"]'
            )

            page.locator("a.node-link", has_text="support@1").click()
            page.wait_for_timeout(400)
            assert support.evaluate(FOCUSED)

            # The ring outlasts the old one-second flash ...
            page.wait_for_timeout(1500)
            assert support.evaluate(FOCUSED)

            # ... and moves to the next linked cell of the same widget.
            page.locator("a.node-link", has_text="cart@1").click()
            page.wait_for_timeout(400)
            assert cart.evaluate(FOCUSED)
            assert not support.evaluate(FOCUSED)

            page.locator("a.node-link", has_text="has_event_purchase").click()
            page.wait_for_timeout(400)
            assert purchase_b.evaluate(FOCUSED)

            # A click inside the widget dismisses its ring.
            purchase_b.click()
            assert not purchase_b.evaluate(FOCUSED)
            assert not errors, f"Report links raised browser errors: {errors}"
        finally:
            browser.close()
