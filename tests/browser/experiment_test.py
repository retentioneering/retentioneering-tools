"""Render the shipped experiment bundle without a Python kernel."""

import os

import pandas as pd
import pytest

from retentioneering import Eventstream

playwright = pytest.importorskip("playwright.sync_api")


def test_experiment_static_methods_tabs_and_resize(tmp_path):
    events = pd.DataFrame(
        {
            "user_id": ["c1", "c1", "c2", "t1", "t1", "t2"],
            "event": ["open", "buy", "open", "open", "buy", "open"],
            "timestamp": pd.Timestamp("2026-01-03"),
        }
    )
    roster = pd.DataFrame(
        {
            "user_id": ["c1", "c2", "c3", "t1", "t2", "t3"],
            "variant": ["c"] * 3 + ["t"] * 3,
            "assigned_at": pd.Timestamp("2026-01-01"),
        }
    )
    stream = Eventstream(events)
    widget = stream.experiment(
        assignments=roster,
        variant_col="variant",
        control="c",
        treatment="t",
        metrics=[
            {
                "name": "converted",
                "metric": "has_event",
                "metric_args": {"event": "buy"},
                "agg": "mean",
                "display": {
                    "label": "Purchase conversion",
                    "scale": 100,
                    "unit": "proportion",
                },
            }
        ],
        observation={
            "start": "2026-01-02",
            "end": "2026-01-05",
            "assignment_time_col": "assigned_at",
            "complete": True,
        },
        funnel_steps=["open", "buy"],
        binary_intervals={"method": "newcombe"},
    )
    output = tmp_path / "experiment.html"
    widget.export_html(str(output))
    with playwright.sync_playwright() as runtime:
        browser = runtime.chromium.launch(
            executable_path=os.environ.get("RETENTIONEERING_TEST_CHROME"),
            args=[
                "--disable-background-timer-throttling",
                "--disable-renderer-backgrounding",
                "--disable-backgrounding-occluded-windows",
            ],
        )
        try:
            page = browser.new_page(viewport={"width": 1280, "height": 900})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(output.as_uri())
            page.locator('tr[data-metric="converted"]').wait_for(state="visible")
            assert page.get_by_text("Newcombe", exact=False).count() > 0
            assert page.get_by_text("Wilson", exact=False).count() > 0
            assert page.locator("svg[role=img]").count() > 0
            # Independent assignment rows must not be present in offline data.
            assert '"c3"' not in output.read_text()
            page.get_by_role("button", name="Cumulative funnel", exact=True).click(
                force=True
            )
            assert page.get_by_text(
                "Ordered reach divided by all assigned units, including silent assignments."
            ).is_visible()
            page.get_by_role("button", name="Conditional funnel", exact=True).click(
                force=True
            )
            assert page.get_by_text(
                "These contrasts do not isolate a causal mechanism.", exact=False
            ).is_visible()
            page.get_by_role("button", name="Outcomes", exact=True).click(force=True)
            page.set_viewport_size({"width": 700, "height": 650})
            assert page.locator('tr[data-metric="converted"]').is_visible()
            assert not errors
        finally:
            browser.close()
