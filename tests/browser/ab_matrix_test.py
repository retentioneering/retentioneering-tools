"""Render the A/B difference matrices in a real browser: the static export (every
cell's outline against the frame, a known significant and a known not significant
cell, the tooltips, the A/A dataset, 80 event types), the view controls that work
without a kernel, and a live host whose sidebar changes go to the Python widget and
come back recomputed.

Run with ``uv run --with playwright pytest tests/browser -v`` after
``uv run --with playwright python -m playwright install chromium`` and
``make build``. The ordinary Python suite skips this module when Playwright is
absent.
"""

import json
import os

import pytest

from retentioneering import datasets
from retentioneering.tools.ab_tables import cell_status
from retentioneering.widgets._html_export import _BUNDLE_PATH, _CSS_PATH

playwright = pytest.importorskip("playwright.sync_api")

DIFF = ("exp_group", "treatment", "control")


def _launch(runtime):
    # Optional local-browser override; CI uses Playwright's pinned Chromium.
    return runtime.chromium.launch(
        executable_path=os.environ.get("RETENTIONEERING_TEST_CHROME")
    )


@pytest.fixture(scope="module")
def stream():
    return datasets.load_ab_test()


def _cells(page) -> list[dict]:
    return page.eval_on_selector_all(
        "[data-testid=ab-cell]",
        """cells => cells.map(td => ({
            row: td.dataset.row, col: td.dataset.col, status: td.dataset.status,
            sign: td.dataset.sign, cross: td.dataset.cross,
            outline: getComputedStyle(td).outlineStyle,
            outlineColor: getComputedStyle(td).outlineColor,
            outlineWidth: parseFloat(getComputedStyle(td).outlineWidth),
            background: getComputedStyle(td).backgroundColor,
            image: getComputedStyle(td).backgroundImage,
            color: getComputedStyle(td).color,
        }))""",
    )


def _expected(frame, keys) -> dict:
    alpha = frame.attrs["alpha"]
    return {
        (str(r[keys[0]]), str(r[keys[1]])): (cell_status(r, alpha), r["delta"])
        for r in frame.to_dict("records")
    }


def _check_cells(cells: list[dict], expected: dict) -> dict:
    """Every drawn cell against its row of the frame; the counts per status."""
    counts: dict[str, int] = {}
    for cell in cells:
        status, delta = expected.get((cell["row"], cell["col"]), ("empty", None))
        assert cell["status"] == status, cell
        counts[status] = counts.get(status, 0) + 1
        if status == "significant":
            assert cell["outline"] == "solid" and cell["outlineWidth"] >= 2
            assert cell["outlineColor"] == "rgb(17, 24, 39)"  # #111827
            assert cell["cross"] == "false" and cell["image"] == "none"
        elif status == "not_significant":
            assert cell["outline"] == "solid" and cell["outlineWidth"] >= 2
            assert cell["outlineColor"] == "rgb(209, 213, 219)"  # #d1d5db
            assert cell["cross"] == "true" and "linear-gradient" in cell["image"]
        elif status == "untested":
            assert cell["outline"] == "dashed"
            assert cell["cross"] == "true" and "linear-gradient" in cell["image"]
            assert cell["color"] == "rgb(156, 163, 175)"  # muted text
        else:
            assert cell["outline"] == "none" and cell["cross"] == "false"
        if status != "empty" and delta and delta == delta:
            # the background's hue is the delta's sign: red above, blue below
            hue = "rgba(239, 68, 68" if delta > 0 else "rgba(59, 130, 246"
            assert cell["background"].startswith(hue), cell
            assert cell["sign"] == ("positive" if delta > 0 else "negative")
    return counts


def _export(widget, tmp_path, name):
    path = tmp_path / f"{name}.html"
    widget.export_html(str(path))
    return path


def test_static_transitions_every_cell_is_its_row(stream, tmp_path):
    frame = stream.get_ab_transitions(DIFF)
    export = _export(stream.ab_transitions(DIFF), tmp_path, "transitions")
    with playwright.sync_playwright() as runtime:
        browser = _launch(runtime)
        try:
            page = browser.new_page(viewport={"width": 1500, "height": 900})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(export.as_uri())
            page.locator("[data-testid=ab-matrix]").wait_for(state="visible")
            counts = _check_cells(_cells(page), _expected(frame, ("source", "target")))
            p = frame["p_value_adjusted"]
            assert counts["significant"] == int((p < 0.05).sum()) > 0
            assert counts["not_significant"] == int((p >= 0.05).sum())
            assert counts.get("untested", 0) == int(p.isna().sum())

            # a known significant cell: add_to_cart -> cart falls (planted)
            known = page.locator(
                "[data-testid=ab-cell][data-row=add_to_cart][data-col=cart]"
            )
            assert known.get_attribute("data-status") == "significant"
            assert known.get_attribute("data-sign") == "negative"
            # a known not significant one: checkout_start -> payment
            flat = page.locator(
                "[data-testid=ab-cell][data-row=checkout_start][data-col=payment]"
            )
            assert flat.get_attribute("data-status") == "not_significant"
            assert flat.get_attribute("data-cross") == "true"

            # the tooltip carries the row
            known.hover()
            tip = page.locator("[data-testid=ab-tooltip]")
            tip.wait_for(state="visible")
            text = tip.inner_text()
            for piece in (
                "add_to_cart → cart",
                "treatment",
                "control",
                "adjusted p",
                "decrease",
                "MDE",
            ):
                assert piece in text, piece
            row = frame[
                (frame["source"] == "add_to_cart") & (frame["target"] == "cart")
            ]
            assert row["reading"].iloc[0][:40] in text
            # an untested cell says why
            untested = frame[frame["p_value_adjusted"].isna()].iloc[0]
            page.locator(
                f"[data-testid=ab-cell][data-row='{untested['source']}']"
                f"[data-col='{untested['target']}']"
            ).hover()
            page.get_by_text("why not tested").wait_for(state="visible")
            assert (
                "not tested" in page.locator("[data-testid=ab-tip-status]").inner_text()
            )

            # the header names the arms, the weight, the population, the family
            header = page.locator("[data-testid=ab-header]").inner_text()
            assert "unique_paths" in header and "the stream's paths" in header
            assert f"bh over {frame.attrs['n_tested']} of {len(frame)}" in header
            # a static export locks the data controls, keeps the view controls
            assert page.get_by_label("Edge weight").is_disabled()
            assert page.get_by_label("Correction").is_disabled()
            assert not page.get_by_label("Significance filter").is_disabled()

            # view: hide rows and columns without a significant cell
            page.get_by_label("Significance filter").select_option("hide")
            rows = page.locator("[data-testid=ab-row-label]").count()
            sig_rows = frame.loc[p < 0.05, "source"].nunique()
            assert rows == sig_rows
            # view: intervals off
            assert page.locator("[data-testid=ab-cell-interval]").count() > 0
            page.get_by_label("Show intervals").uncheck()
            assert page.locator("[data-testid=ab-cell-interval]").count() == 0
            assert "NaN" not in page.locator("body").inner_text()
            assert not errors, errors
        finally:
            browser.close()


def test_static_steps_every_cell_is_its_row(stream, tmp_path):
    frame = stream.get_ab_steps(DIFF)
    export = _export(stream.ab_steps(DIFF), tmp_path, "steps")
    with playwright.sync_playwright() as runtime:
        browser = _launch(runtime)
        try:
            page = browser.new_page(viewport={"width": 1500, "height": 900})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(export.as_uri())
            page.locator("[data-testid=ab-matrix]").wait_for(state="visible")
            counts = _check_cells(_cells(page), _expected(frame, ("event", "step")))
            assert counts["significant"] == int(
                (frame["p_value_adjusted"] < 0.05).sum()
            )
            # quick_add is up at every step from 3 (planted)
            cell = page.locator(
                "[data-testid=ab-cell][data-row=quick_add][data-col='5']"
            )
            assert cell.get_attribute("data-status") == "significant"
            assert cell.get_attribute("data-sign") == "positive"
            # the tooltip carries the row, without a kernel
            cell.hover()
            tip = page.locator("[data-testid=ab-tooltip]")
            tip.wait_for(state="visible")
            text = tip.inner_text()
            row = frame[(frame["event"] == "quick_add") & (frame["step"] == 5)]
            assert "quick_add @ step 5" in text and "increase" in text
            assert row["reading"].iloc[0][:40] in text
            # report links scroll to a cell: scrollToEvent("event@step")
            page.evaluate(
                "RetentioneeringWidget.scrollToEvent('path_end@10', "
                "document.querySelector('[data-testid=ab-matrix-widget-steps]').parentElement)"
            )
            assert not errors, errors
        finally:
            browser.close()


def test_static_80_event_types(ab_markov80, tmp_path):
    # 6,500+ cells travel column by column; the export decodes them without a
    # kernel: every cell its row, and the tooltip its reading
    frame = ab_markov80.get_ab_transitions(DIFF)
    export = _export(ab_markov80.ab_transitions(DIFF), tmp_path, "transitions80")
    with playwright.sync_playwright() as runtime:
        browser = _launch(runtime)
        try:
            page = browser.new_page(viewport={"width": 1500, "height": 900})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(export.as_uri())
            page.locator("[data-testid=ab-matrix]").wait_for(state="visible")
            cells = _cells(page)
            assert len(cells) == 81 * 81
            counts = _check_cells(cells, _expected(frame, ("source", "target")))
            assert sum(n for s, n in counts.items() if s != "empty") == len(frame)
            last = frame.iloc[-1]
            page.locator(
                f"[data-testid=ab-cell][data-row='{last['source']}']"
                f"[data-col='{last['target']}']"
            ).hover()
            reading = page.locator("[data-testid=ab-tip-reading]")
            reading.wait_for(state="visible")
            assert reading.inner_text() == last["reading"]
            assert not errors, errors
        finally:
            browser.close()


def test_static_aa_draws_no_black_outline(tmp_path):
    aa = datasets.load_ab_test("aa_calibration")
    with playwright.sync_playwright() as runtime:
        browser = _launch(runtime)
        try:
            page = browser.new_page(viewport={"width": 1500, "height": 900})
            for widget, frame, keys in (
                (
                    aa.ab_transitions(DIFF),
                    aa.get_ab_transitions(DIFF),
                    ("source", "target"),
                ),
                (aa.ab_steps(DIFF), aa.get_ab_steps(DIFF), ("event", "step")),
            ):
                page.goto(_export(widget, tmp_path, widget.widget_type).as_uri())
                page.locator("[data-testid=ab-matrix]").wait_for(state="visible")
                counts = _check_cells(_cells(page), _expected(frame, keys))
                assert counts.get("significant", 0) == 0, counts
                assert counts["not_significant"] > 0
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


def test_sidebar_changes_recompute_through_python(stream, tmp_path):
    widget = stream.ab_transitions(DIFF)
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
            page = browser.new_page(viewport={"width": 1500, "height": 900})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.expose_function("pySet", py_set)
            page.goto(page_file.as_uri())
            page.locator("[data-testid=ab-matrix]").wait_for(state="visible")
            assert page.locator("[data-testid=ab-reading-hint]").count() == 0

            page.get_by_label("Edge weight").select_option("proba_out")
            page.locator("[data-testid=ab-reading-hint]").wait_for(state="visible")
            assert widget.edge_weight == "proba_out"
            frame = stream.get_ab_transitions(DIFF, "proba_out")
            page.wait_for_function(
                "document.querySelector('[data-testid=ab-header]').innerText"
                ".includes('proba_out')"
            )
            _check_cells(_cells(page), _expected(frame, ("source", "target")))

            page.get_by_label("Correction").select_option("bonferroni")
            page.wait_for_function(
                "!document.querySelector('[aria-label=\"Interval kind\"]').disabled"
            )
            page.get_by_label("Interval kind").select_option("simultaneous")
            assert widget.interval_kind == "simultaneous"
            page.get_by_text("Bonferroni simultaneous").first.wait_for(state="visible")

            # view changes are saved in the traitlets and never recompute
            n_results = sum(1 for k, _ in calls if k in ("edge_weight", "correction"))
            page.get_by_label("Sort rows").select_option("n_significant")
            assert widget.row_sort == "n_significant"
            page.locator("[aria-label='Pin cart']").click()
            assert json.loads(widget.event_visibility)["cart"]["isPinned"] is True
            assert (
                page.locator("[data-testid=ab-row-label]").first.get_attribute(
                    "data-row"
                )
                == "cart"
            )
            page.locator("[aria-label='Hide home']").first.click()
            assert json.loads(widget.event_visibility)["home"]["isHidden"] is True
            assert page.locator("[data-testid=ab-cell][data-row=home]").count() == 0
            assert page.locator("[data-testid=ab-cell][data-col=home]").count() == 0
            assert n_results == 2
            assert widget.error == ""
            assert not errors, errors
        finally:
            browser.close()
