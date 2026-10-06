"""Static screenshots for the README: the use-case blocks and the agent-runs graph.

Exports each use-case widget from the bundled ecom dataset, and the graph that
notebooks/agent_path_analysis.ipynb exports, then captures them in headless
Chromium. Run from the repository root after `make build`:
    RETENTIONEERING_NO_TRACK=1 uv run --with playwright python docs/scripts/capture_readme_screenshots.py
Set CHROME_PATH to use an installed Chrome instead of Playwright's Chromium.
Exports go to docs/build/readme/ (gitignored); PNGs go to .github/readme/.
"""

import json
import os
from pathlib import Path

from playwright.sync_api import sync_playwright

import retentioneering as rete

REPO = Path(__file__).resolve().parents[2]
BUILD = REPO / "docs" / "build" / "readme"
ASSETS = REPO / ".github" / "readme"


def export_widgets():
    stream = rete.datasets.load_ecom()
    periods = stream.add_segment("period", time_range=("2024-05-19", "2024-06-07"))
    periods.transition_graph(
        diff=("period", "inside", "outside"), sidebar_open=False, height=560
    ).export_html(BUILD / "kpi-diff.html", title="Inside vs outside the window")
    steps = ["cart", "shipping_details", "purchase"]
    stream.funnel(
        steps=steps, path_col="session_id", sidebar_open=False, height=380
    ).export_html(BUILD / "funnel.html", title="Cart to purchase")
    stream.step_sankey(
        anchor="payment_error",
        step_window=2,
        path_col="session_id",
        sidebar_open=False,
        height=480,
    ).export_html(BUILD / "error-sankey.html", title="Around payment_error")
    stream.step_sankey(
        anchor={"pattern": "payment_error", "occurrence": "last"},
        step_window=2,
        path_col="session_id",
        sidebar_open=False,
        height=480,
    ).export_html(
        BUILD / "last-error-sankey.html", title="Around the last payment_error"
    )
    abandoned = stream.filter_paths(
        {
            "metric": "matches_pattern",
            "op": "=",
            "value": True,
            "metric_args": {
                "pattern": "cart->[^shipping_details|support_chat]*->path_end"
            },
        },
        path_col="session_id",
    )
    abandoned.step_sankey(
        path_pattern="cart",
        step_window=3,
        path_col="session_id",
        sidebar_open=False,
        height=480,
    ).export_html(BUILD / "abandoned-sankey.html", title="Abandoned-cart sessions")
    typed = stream.add_clusters(
        "session_type",
        features=[{"metric": "event_count_bulk"}],
        method_args={"n_clusters": 4},
        path_col="session_id",
    )
    visits = typed.collapse_events(
        group_col="session_id", name={"col": "session_type"}
    ).rename_events(
        {
            "cluster_0": "browsing",
            "cluster_1": "quick_visit",
            "cluster_2": "purchase_visit",
            "cluster_3": "promo_visit",
        }
    )
    visits.transition_graph(sidebar_open=False, height=560).export_html(
        BUILD / "visit-graph.html", title="Session types across visits"
    )
    export_agent_runs()


def export_agent_runs():
    """Run the agent-run notebook rather than duplicating its fixture; it
    exports its comparison graph as agent-runs.html into the working directory."""
    notebook = REPO / "notebooks" / "agent_path_analysis.ipynb"
    namespace = {}
    previous_dir = Path.cwd()
    try:
        os.chdir(BUILD)
        for cell in json.loads(notebook.read_text())["cells"]:
            source = "".join(cell["source"])
            if cell["cell_type"] == "code" and not source.startswith("%pip"):
                exec(compile(source, str(notebook), "exec"), namespace)
    finally:
        os.chdir(previous_dir)


def capture():
    launch = (
        {"executable_path": os.environ["CHROME_PATH"]}
        if os.environ.get("CHROME_PATH")
        else {}
    )
    with sync_playwright() as p:
        browser = p.chromium.launch(**launch)
        for name in (
            "kpi-diff",
            "funnel",
            "error-sankey",
            "last-error-sankey",
            "abandoned-sankey",
            "visit-graph",
            "agent-paths",
        ):
            # A narrower frame enlarges text in the README, but graphs fit their
            # whole layout into it (shrinking labels) and wide Sankeys overflow.
            narrow = name in ("funnel", "error-sankey", "last-error-sankey")
            width = 600 if narrow else 960 if name == "agent-paths" else 820
            page = browser.new_page(
                viewport={"width": width, "height": 760},
                device_scale_factor=3 if narrow else 2,
            )
            html = "agent-runs" if name == "agent-paths" else name
            page.goto((BUILD / f"{html}.html").as_uri())
            page.wait_for_function(
                "document.querySelector('#retentioneering-root')?.textContent.length > 10"
            )
            page.wait_for_timeout(1500)  # the graph animates its initial layout
            if name == "kpi-diff":
                # Focus the node the README tells readers to click.
                page.get_by_text("Search event").click()
                page.keyboard.type("payment_details")
                page.keyboard.press("Enter")
                page.wait_for_timeout(1200)
            page.locator("#retentioneering-root").screenshot(
                path=str(ASSETS / f"{name}.png")
            )
            page.close()
        browser.close()


if __name__ == "__main__":
    BUILD.mkdir(parents=True, exist_ok=True)
    export_widgets()
    capture()
    print(f"Wrote README screenshots to {ASSETS}")
