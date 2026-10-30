"""Render a static funnel export with confidence intervals in a real browser.

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


def _ab_stream(n: int = 10, buys: tuple = (6, 3)) -> Eventstream:
    rows = []
    for group, n_buy in zip(("t", "c"), buys):
        for i in range(n):
            rows.append((f"{group}{i}", "home", "2026-01-01 00:00", group))
            if i < n_buy:
                rows.append((f"{group}{i}", "buy", "2026-01-01 00:01", group))
    return Eventstream(
        pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "grp"]),
        {"segment_cols": ["grp"]},
    )


def test_static_diff_funnel_draws_whiskers_and_locks_the_ci_selector(tmp_path):
    export = tmp_path / "funnel.html"
    _ab_stream().funnel(
        steps=["home", "buy"], diff=("grp", "t", "c"), ci_level=0.95
    ).export_html(str(export))

    with playwright.sync_playwright() as runtime:
        # Optional local-browser override; CI uses Playwright's pinned Chromium.
        executable = os.environ.get("RETENTIONEERING_TEST_CHROME")
        browser = runtime.chromium.launch(executable_path=executable)
        try:
            page = browser.new_page(viewport={"width": 1100, "height": 700})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(export.as_uri())
            page.locator("[data-testid=ci-whisker]").first.wait_for(state="visible")
            # two bars per step, one whisker per bar
            assert page.locator("[data-testid=ci-whisker]").count() == 4
            # overall and step delta cells for each of the two steps
            assert page.locator("[data-testid=ci-cell]").count() == 4
            # step 2 is conditional: never coloured, marked with a dagger
            assert page.get_by_text("†").count() >= 1
            selector = page.get_by_label("Confidence interval")
            assert selector.input_value() == "0.95"
            assert selector.is_disabled()  # no kernel behind a static export
            assert "NaN" not in page.locator("body").inner_text()
            assert not errors, errors
        finally:
            browser.close()


def test_conditional_step_deltas_are_never_coloured(tmp_path):
    # 60 % vs 20 % buyers out of 100: the step-to-step delta's interval excludes 0,
    # so only the conditional rule keeps it grey
    export = tmp_path / "funnel_conditional.html"
    _ab_stream(n=100, buys=(60, 20)).funnel(
        steps=["home", "buy"], diff=("grp", "t", "c"), ci_level=0.95
    ).export_html(str(export))

    with playwright.sync_playwright() as runtime:
        executable = os.environ.get("RETENTIONEERING_TEST_CHROME")
        browser = runtime.chromium.launch(executable_path=executable)
        try:
            page = browser.new_page(viewport={"width": 1100, "height": 700})
            page.goto(export.as_uri())
            page.locator("[data-testid=ci-cell]").first.wait_for(state="visible")
            conditional = page.locator("td", has_text="†")
            assert conditional.count() >= 1
            for i in range(conditional.count()):
                assert conditional.nth(i).get_attribute("data-testid") == "delta-grey"
            # the unconditional delta of the same step is coloured: the rule, not
            # the data, keeps the conditional cell grey
            assert page.locator("[data-testid=delta-coloured]").count() >= 1
        finally:
            browser.close()
