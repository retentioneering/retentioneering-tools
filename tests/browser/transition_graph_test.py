"""Render the shipped static bundle, rather than only constructing its widget.

Run with ``uv run --with playwright pytest tests/browser -v`` after
``uv run --with playwright python -m playwright install chromium`` and
``make build``. The ordinary Python suite skips this module when Playwright
is absent; CI runs it with Chromium on Python 3.12.
"""

import os
import re

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


@pytest.mark.parametrize("path_groups", [False, True])
def test_millisecond_diff_hover_and_ego_keep_units(tmp_path, path_groups):
    origin = pd.Timestamp("2026-01-01")
    stream = Eventstream(
        pd.DataFrame(
            [
                (1, "request", origin, "fast"),
                (1, "ready", origin + pd.Timedelta(milliseconds=125), "fast"),
                (2, "request", origin, "slow"),
                (2, "ready", origin + pd.Timedelta(milliseconds=375), "slow"),
            ],
            columns=["user_id", "event", "timestamp", "cohort"],
        ),
        schema={"segment_cols": ["cohort"]},
    )
    export = tmp_path / "diff.html"
    stream.transition_graph(
        edge_weight="time_median",
        time_unit="ms",
        diff=([1], [2]) if path_groups else ("cohort", "fast", "slow"),
        height=500,
    ).export_html(str(export))
    with playwright.sync_playwright() as runtime:
        browser = runtime.chromium.launch(
            executable_path=os.environ.get("RETENTIONEERING_TEST_CHROME")
        )
        try:
            page = browser.new_page(viewport={"width": 1280, "height": 850})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(export.as_uri())
            page.locator("#retentioneering-root canvas").first.wait_for(state="visible")
            _drain_frames(page)
            assert (
                page.locator(
                    '[title="Compare two groups: group 1 minus group 2. Positive differences are red; negative differences are blue."]'
                ).count()
                == 1
            )
            if path_groups:
                assert (
                    page.locator("option:checked")
                    .filter(has_text="Path ID groups")
                    .count()
                    == 1
                )
                assert page.get_by_text(
                    "Comparing Group 1 − Group 2 from supplied path IDs."
                ).is_visible()
                assert (
                    page.get_by_text("Not enough values in this segment.").count() == 0
                )
            # Read canvas geometry, then use real pointer events for both tooltips.
            point = page.evaluate(
                """() => {
                    const cy = [...document.querySelectorAll('*')].find(e => e.__cy).__cy;
                    const edge = cy.edges().filter(e => e.data('source') === 'request' && e.data('target') === 'ready');
                    const p = edge.renderedMidpoint(), r = cy.container().getBoundingClientRect();
                    return {x: r.x + p.x, y: r.y + p.y};
                }"""
            )
            page.mouse.move(point["x"], point["y"])
            for value in ("125ms", "375ms", "-250ms"):
                # The Ego ribbon also has a tspan with the delta; require the
                # tooltip's HTML span so a missing tooltip cannot pass this check.
                playwright.expect(
                    page.locator("span").filter(
                        has_text=re.compile(r"^" + re.escape(value) + r"$")
                    )
                ).to_be_visible()
            point = page.evaluate(
                """() => {
                    const cy = [...document.querySelectorAll('*')].find(e => e.__cy).__cy;
                    const p = cy.getElementById('request').renderedPosition(), r = cy.container().getBoundingClientRect();
                    return {x: r.x + p.x, y: r.y + p.y};
                }"""
            )
            page.mouse.click(point["x"], point["y"])
            page.get_by_role("button", name="Ego view: request", exact=True).click()
            page.locator("svg g path").first.hover()
            for value in ("125ms", "375ms", "-250ms"):
                # The Ego ribbon also has a tspan with the delta; require the
                # tooltip's HTML span so a missing tooltip cannot pass this check.
                playwright.expect(
                    page.locator("span").filter(
                        has_text=re.compile(r"^" + re.escape(value) + r"$")
                    )
                ).to_be_visible()
            assert not errors
        finally:
            browser.close()
