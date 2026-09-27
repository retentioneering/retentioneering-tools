"""Render the shipped static bundle, rather than only constructing its widget.

Run with ``uv run --with playwright pytest tests/browser -v`` after
``uv run --with playwright python -m playwright install chromium`` and
``make build``. The ordinary Python suite skips this module when Playwright
is absent; CI runs it with Chromium on Python 3.12.
"""

import os

import pandas as pd
import pytest

from retentioneering import Eventstream

playwright = pytest.importorskip("playwright.sync_api")


def _drain_frames(page):
    """Let mount/rebuild/ResizeObserver callbacks reach their scheduled frames."""
    page.evaluate(
        """() => new Promise(resolve => {
            let remaining = 8;
            function tick() {
                if (--remaining === 0) resolve();
                else requestAnimationFrame(tick);
            }
            requestAnimationFrame(tick);
        })"""
    )


def test_static_graph_mount_and_resize_have_no_stale_frame_errors(tmp_path):
    stream = Eventstream(
        pd.DataFrame(
            {
                "user_id": ["u1", "u1", "u1"],
                "event": ["home", "cart", "purchase"],
                "timestamp": pd.date_range("2026-01-01", periods=3, freq="min"),
            }
        )
    )
    export = tmp_path / "graph.html"
    stream.transition_graph(height=400, sidebar_open=False).export_html(str(export))

    with playwright.sync_playwright() as runtime:
        # Optional local-browser override; CI uses Playwright's pinned Chromium.
        executable = os.environ.get("RETENTIONEERING_TEST_CHROME")
        browser = runtime.chromium.launch(executable_path=executable)
        try:
            page = browser.new_page(viewport={"width": 1000, "height": 700})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(export.as_uri())
            page.locator("#retentioneering-root canvas").first.wait_for(state="visible")
            _drain_frames(page)
            assert not errors, f"Graph mount raised browser errors: {errors}"

            page.set_viewport_size({"width": 700, "height": 550})
            _drain_frames(page)
            assert not errors, f"Graph resize raised browser errors: {errors}"
            assert page.get_by_role(
                "button", name="Search event", exact=True
            ).is_visible()
        finally:
            browser.close()
