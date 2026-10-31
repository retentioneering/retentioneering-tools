"""Render the ``ab_compression`` widget in a real browser: the static export's path
chart, step slider, mapping table and before / after counts, all of which work
without a kernel, against the twin's plan at every step.

Run with ``uv run --with playwright pytest tests/browser -v`` after
``uv run --with playwright python -m playwright install chromium`` and
``make build``. The ordinary Python suite skips this module when Playwright is
absent.
"""

import os

import pytest

from retentioneering import datasets

playwright = pytest.importorskip("playwright.sync_api")

DIFF = ("arm", "treatment", "control")
ARGS = {"keep": ["quick_add", "cart"], "max_events": 10}


def _launch(runtime):
    # Optional local-browser override; CI uses Playwright's pinned Chromium.
    return runtime.chromium.launch(
        executable_path=os.environ.get("RETENTIONEERING_TEST_CHROME")
    )


def _rows(page) -> dict:
    return dict(
        page.eval_on_selector_all(
            "[data-testid=abc-mapping-row]",
            "rows => rows.map(r => [r.dataset.event, r.dataset.action])",
        )
    )


def test_static_compression_follows_the_plan_at_every_step(tmp_path):
    stream = datasets.load_ab_test()
    table = datasets.load_ab_test_assignment()
    widget = stream.ab_compression(DIFF, assignment=table, **ARGS)
    plan = stream.get_ab_compression(DIFF, assignment=table, **ARGS)
    path = plan.attrs["path"]
    chosen = plan.attrs["chosen_step"]
    export = tmp_path / "compression.html"
    widget.export_html(str(export))
    with playwright.sync_playwright() as runtime:
        browser = _launch(runtime)
        try:
            page = browser.new_page(viewport={"width": 1400, "height": 900})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(export.as_uri())
            page.locator("[data-testid=abc-chart]").wait_for(state="visible")
            # four panels, one point per step each (a step without an MDE has none)
            for measure in ("testable", "untestable_share", "mass_retained"):
                points = page.locator(
                    f"[data-testid=abc-panel-{measure}] [data-testid=abc-point]"
                )
                assert points.count() == len(path)
            assert page.locator("[data-testid=abc-chosen]").count() == 4
            # no target_delta: no users-needed panel
            assert page.locator("[data-testid=abc-panel-needed]").count() == 0
            header = page.locator("[data-testid=abc-header]").inner_text()
            assert f"chosen step {chosen} of {len(path) - 1}" in header
            assert "Arm-blind" in header
            assert page.locator("[data-testid=abc-step]").inner_text() == (
                f"{chosen} / {len(path) - 1}"
            )
            # the slider: at every step, the table is the plan at that step
            slider = page.get_by_label("Step")
            for step in range(len(path)):
                slider.fill(str(step))
                at = stream.get_ab_compression(
                    DIFF, assignment=table, step=step, **ARGS
                )
                changed = at[at["action"] != "keep"]
                assert _rows(page) == dict(
                    zip(changed["event"], changed["action"])
                ), step
                row = path.set_index("step").loc[step]
                counts = page.locator("[data-testid=abc-before-after]").inner_text()
                assert f"{row['vocabulary']}" in counts
                value = page.locator("[data-testid=abc-value-testable]").inner_text()
                assert value == f"{row['testable']} of {row['cells']}"
            # every event, kept ones included
            page.get_by_label("Changed events only").uncheck()
            assert page.locator("[data-testid=abc-mapping-row]").count() == len(plan)
            # a click on a point selects its step; the button goes back to the chosen
            page.locator(
                "[data-testid=abc-panel-testable] [data-testid=abc-point][data-step='1']"
            ).click()
            assert page.locator("[data-testid=abc-step]").inner_text().startswith("1 /")
            page.locator("[data-testid=abc-to-chosen]").click()
            assert (
                page.locator("[data-testid=abc-step]")
                .inner_text()
                .startswith(f"{chosen} /")
            )
            # a static export locks the data controls
            assert page.get_by_label("Screen").is_disabled()
            assert not page.get_by_label("Changed events only").is_disabled()
            assert "NaN" not in page.locator("body").inner_text()
            assert not errors, errors
        finally:
            browser.close()


def _scale(value: float) -> str:
    """``fmtScale`` of ab_compression_encoding.ts."""
    if value >= 100:
        return f"×{round(value)}"
    if value >= 10:
        return f"×{value:.1f}"
    return f"×{value:.2f}"


def test_static_compression_draws_the_users_needed(tmp_path):
    stream = datasets.load_ab_test()
    table = datasets.load_ab_test_assignment()
    args = {**ARGS, "target_delta": 0.02}
    widget = stream.ab_compression(DIFF, assignment=table, **args)
    path = stream.get_ab_compression(DIFF, assignment=table, **args).attrs["path"]
    export = tmp_path / "needed.html"
    widget.export_html(str(export))
    with playwright.sync_playwright() as runtime:
        browser = _launch(runtime)
        try:
            page = browser.new_page(viewport={"width": 1400, "height": 1000})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(export.as_uri())
            panel = page.locator("[data-testid=abc-panel-needed]")
            panel.wait_for(state="visible")
            for name in ("median_scale_needed", "scale_to_test_90"):
                drawn = panel.locator(
                    f"[data-testid=abc-need-point][data-series={name}]"
                )
                assert drawn.count() == int(path[name].gt(0).sum()), name
            # the legend reads the selected step's numbers
            slider = page.get_by_label("Step")
            for step in (0, len(path) - 1):
                slider.fill(str(step))
                row = path.set_index("step").loc[step]
                text = page.locator(
                    "[data-testid=abc-value-median_scale_needed]"
                ).inner_text()
                assert text.startswith(_scale(row["median_scale_needed"])), text
                assert "2.00 pp" in text
            counts = page.locator("[data-testid=abc-before-after]").inner_text()
            assert "users for half the cells to detect the target" in counts
            assert "NaN" not in page.locator("body").inner_text()
            assert not errors, errors
        finally:
            browser.close()


def test_static_compression_counts_the_segment_paths(tmp_path):
    stream = datasets.load_ab_test()
    diff = ("exp_group", "treatment", "control")
    widget = stream.ab_compression(diff, by="platform", by_pre_exposure=True)
    plan = stream.get_ab_compression(diff, by="platform", by_pre_exposure=True)
    first = plan.attrs["homogeneity"][0]
    export = tmp_path / "segments.html"
    widget.export_html(str(export))
    with playwright.sync_playwright() as runtime:
        browser = _launch(runtime)
        try:
            page = browser.new_page(viewport={"width": 1400, "height": 900})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(export.as_uri())
            header = page.locator("[data-testid=abc-header]")
            header.wait_for(state="visible")
            text = header.inner_text()
            # a segment's page pools the segment's paths, not the experiment's
            assert f"{first['paths']} paths of this segment" in text, text
            assert (
                f"arm ratio, {plan.attrs['group1_paths']} : "
                f"{plan.attrs['group2_paths']}" in text
            ), text
            assert f"platform = {first['segment']}: " in text
            assert not errors, errors
        finally:
            browser.close()
