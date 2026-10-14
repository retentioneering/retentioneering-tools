"""LinkedIn carousel for the Quant UX Con 2026 announcement: 9 slides, 1080x1350.

Exports widgets from the bundled ecom dataset, captures them in Chrome and lays
them out on slides, then prints carousel.pdf (upload it to LinkedIn as a document)
and a PNG per slide for previewing. From the repo root, after `make build`:
    RETENTIONEERING_NO_TRACK=1 uv run --with playwright python notebooks/quantuxcon2026/carousel/build_carousel.py
Set CHROME_PATH to use another Chrome binary.
"""

import html
import importlib.util
import json
import os
from pathlib import Path

from playwright.sync_api import sync_playwright

import retentioneering as rete

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
BUILD = HERE / "_build"
IMG = HERE.parent / "img"
CHROME = os.environ.get(
    "CHROME_PATH", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
)
FONT = '-apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif'

# name -> zoom: how much the capture is enlarged to fill its frame on the slide.
# Widgets are exported at frame size / zoom, so their text grows by the zoom.
# A {"width": px} entry sets the widget's CSS width instead, for tables whose
# columns do not stretch.
SHOTS = {
    "raw": 0.85,
    "visits": 0.85,
    "funnel-diff": {"width": 545},
    "abandoned": 1.25,
    "exits": 1.55,
    "clusters": {"width": 710},
    "diff": 1.5,
    "agent": 1.3,
}
CLUSTER_NAMES = {
    "cluster_0": "browsing",
    "cluster_1": "quick_visit",
    "cluster_2": "purchase_visit",
    "cluster_3": "promo_visit",
}
ABANDONED = "cart->[^shipping_details|payment_details|purchase]*->path_end"
FRAME_PAD = 12  # border + padding of a figure frame, per side
PAGE_MARGIN = 24  # the export page's margin around the widget, per side


def widget_sizes(frames):
    """CSS size of each widget so that, zoomed, it fills its frame on the slide."""
    sizes = {}
    for n, (w, h) in frames.items():
        w, h = w - 2 * FRAME_PAD, h - 2 * FRAME_PAD
        zoom = w / SHOTS[n]["width"] if isinstance(SHOTS[n], dict) else SHOTS[n]
        sizes[n] = (round(w / zoom), round(h / zoom))
    return sizes


def export_widgets(sizes):
    stream = rete.datasets.load_ecom()
    out = {}

    out["raw"] = stream.transition_graph(sidebar_open=False, height=sizes["raw"][1])
    visits = (
        stream.add_clusters(
            "session_type",
            features=[{"metric": "event_count_bulk"}],
            method_args={"n_clusters": 4},
            path_col="session_id",
        )
        .collapse_events(group_col="session_id", name={"col": "session_type"})
        .rename_events(CLUSTER_NAMES)
    )
    out["visits"] = visits.transition_graph(
        sidebar_open=False, height=sizes["visits"][1]
    )
    # Manual edge filter over the full weight range: every edge, not the top 3
    # per node, on both graphs.
    for name in ("raw", "visits"):
        out[name].edge_filter = json.dumps([0, 1])

    steps = ["cart", "shipping_details", "purchase"]
    out["funnel-diff"] = stream.add_segment("stage", funnel_events=steps).step_matrix(
        path_pattern="cart",
        diff=("stage", "cart", "purchase"),
        sidebar_open=False,
        height=sizes["funnel-diff"][1],
    )
    # The matrix's own row filter: hide events whose |diff| stays below 0.05.
    out["funnel-diff"].matrix_value_filter = "0.05"

    out["abandoned"] = stream.step_sankey(
        anchor={"pattern": ABANDONED, "at": "start"},
        sidebar_open=False,
        height=sizes["abandoned"][1],
    )
    out["exits"] = stream.step_sankey(
        path_pattern="path_end",
        sidebar_open=False,
        height=sizes["exits"][1],
    )

    out["clusters"] = stream.cluster_analysis(
        features=[{"metric": "event_count_bulk"}],
        method="kmeans",
        method_args={"n_clusters": "4-8"},
        select={"n_clusters": 4},
        path_col="session_id",
        overview_metrics=[
            {"metric": "length"},
            {"metric": "event_count", "metric_args": {"event": "catalog"}},
            {"metric": "event_count", "metric_args": {"event": "product_view"}},
            {"metric": "event_count", "metric_args": {"event": "promo_page"}},
            {"metric": "has_event", "metric_args": {"event": "cart"}},
            {"metric": "has_event", "metric_args": {"event": "purchase"}},
            {"metric": "has_event", "metric_args": {"event": "payment_error"}},
            {"metric": "has_event", "metric_args": {"event": "support_chat"}},
            {"metric": "event_count", "metric_args": {"event": "search"}},
        ],
        sidebar_open=False,
        height=sizes["clusters"][1],
    )
    out["clusters"].cluster_renames = json.dumps(CLUSTER_NAMES)

    out["diff"] = stream.add_segment(
        "period", time_range=("2024-05-19", "2024-06-07")
    ).transition_graph(
        diff=("period", "inside", "outside"),
        sidebar_open=False,
        height=sizes["diff"][1],
    )

    for name, widget in out.items():
        widget.export_html(BUILD / f"{name}.html", title=name)

    # The agent report is built the way the MCP server builds it; reuse the README
    # recorder's report so the analysis text stays checked against the tabs.
    spec = importlib.util.spec_from_file_location(
        "agent_report", REPO / "docs" / "scripts" / "record_readme_agent_report.py"
    )
    agent = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(agent)
    agent.HTML = BUILD / "agent.html"
    agent.build_report()


# The graph widget keeps its Cytoscape instance on the canvas container's parent.
FIND_CY = "[...document.querySelectorAll('div')].find(e => e.__cy).__cy"
COMPACT_GRAPH = f"""
([density, stretch, moves, pad]) => {{
  const cy = {FIND_CY};
  // Reshape the layout to the frame's aspect ratio and pull the nodes closer
  // together: labels keep their size, so after fit() they come out larger.
  const bb = cy.nodes().boundingBox();
  const k = Math.sqrt((cy.width() / cy.height()) / (bb.w / bb.h));
  const cx = bb.x1 + bb.w / 2, cy0 = bb.y1 + bb.h / 2;
  cy.nodes().positions(n => ({{
    x: cx + (n.position('x') - cx) * k * density * stretch,
    y: cy0 + (n.position('y') - cy0) / k * density,
  }}));
  // Then nudge single nodes, in units of the layout's width and height, the
  // way a user would drag them off crowded edges.
  const b2 = cy.nodes().boundingBox();
  for (const [id, dx, dy] of moves) {{
    const n = cy.getElementById(id);
    n.position({{x: n.position('x') + dx * b2.w, y: n.position('y') + dy * b2.h}});
  }}
  cy.fit(undefined, pad);
}}
"""
# Zoom the report's graph onto the edge a link points at.
FOCUS_EDGE = """
([source, target, pad]) => {
  const cys = [...document.querySelectorAll('div')].filter(e => e.__cy && e.offsetParent).map(e => e.__cy);
  for (const cy of cys) {
    const edge = cy.edges().filter(e => e.source().id() === source && e.target().id() === target);
    if (edge.length) { cy.stop(); cy.fit(edge.union(edge.connectedNodes()), pad); return true; }
  }
  return false;
}
"""
# A drawn cursor clicking at (x, y): headless screenshots have no pointer.
CURSOR = """
([x, y]) => {
  const ring = document.createElement('div');
  Object.assign(ring.style, {position: 'fixed', left: (x - 16) + 'px', top: (y - 16) + 'px',
    width: '32px', height: '32px', borderRadius: '50%', zIndex: 2147483646, pointerEvents: 'none',
    background: 'hsla(45, 93%, 58%, .35)', border: '2px solid hsl(45, 93%, 50%)'});
  const c = document.createElement('div');
  c.innerHTML = '<svg width="26" height="31" viewBox="0 0 22 26"><path d="M2 2 L2 20 L7 15.5 L10.5 23 L13.5 21.6 L10 14.3 L16.5 14.3 Z" fill="black" stroke="white" stroke-width="1.6" stroke-linejoin="round"/></svg>';
  Object.assign(c.style, {position: 'fixed', left: (x - 2) + 'px', top: (y - 2) + 'px',
    zIndex: 2147483647, pointerEvents: 'none'});
  document.body.append(ring, c);
}
"""
# A hand arrangement of the visit graph, as a user would drag it: entry on the
# left, exit on the right, the four session types in between.
VISIT_LAYOUT = (
    """
() => {
  const cy = %s;
  const pos = {path_start: [0, 160], purchase_visit: [100, 0], quick_visit: [300, 0],
    promo_visit: [100, 320], browsing: [300, 320], path_end: [400, 160]};
  for (const [id, [x, y]] of Object.entries(pos)) cy.getElementById(id).position({x, y});
  cy.fit(undefined, 30);
}
"""
    % FIND_CY
)


def capture(browser, sizes):
    for name, (width, height) in sizes.items():
        page = browser.new_page(
            viewport={
                "width": width + 2 * PAGE_MARGIN,
                "height": height + 2 * PAGE_MARGIN,
            },
            device_scale_factor=3,
        )
        errors = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.goto((BUILD / f"{name}.html").as_uri())
        page.add_style_tag(content=f"body {{ font-family: {FONT}; }}")
        page.wait_for_timeout(2500)  # graphs animate their initial layout
        if name == "visits":
            page.evaluate(VISIT_LAYOUT)
            page.wait_for_timeout(500)
        if name == "diff":
            page.get_by_text("Search event").click()
            page.keyboard.type("payment_details")
            page.keyboard.press("Enter")
            page.wait_for_timeout(1500)
        if name == "agent":
            split = page.locator("#splitter").bounding_box()
            y = split["y"] + split["height"] / 2
            page.mouse.move(split["x"] + split["width"] / 2, y)
            page.mouse.down()
            page.mouse.move(width * 0.6, y, steps=10)
            page.mouse.up()
            page.wait_for_timeout(1500)
            page.set_viewport_size({"width": width, "height": height})
            page.wait_for_timeout(1500)
            link = page.locator(
                "a.node-link", has_text="payment_details->payment_error"
            )
            link.first.evaluate("e => e.scrollIntoView({block: 'center'})")
            link.first.click()
            page.wait_for_timeout(1500)  # the link animates the camera to the edge
            if not page.evaluate(FOCUS_EDGE, ["payment_details", "payment_error", 70]):
                raise RuntimeError(
                    "agent: edge payment_details->payment_error not found"
                )
            page.wait_for_timeout(500)
            box = link.first.bounding_box()
            page.mouse.move(0, 0)
            page.evaluate(
                CURSOR,
                [
                    box["x"] + min(box["width"] * 0.5, 70),
                    box["y"] + box["height"] * 0.6,
                ],
            )
            page.screenshot(path=str(BUILD / f"{name}.png"))
        else:
            page.locator("#retentioneering-root").screenshot(
                path=str(BUILD / f"{name}.png")
            )
        if errors:
            raise RuntimeError(f"{name}: page errors: {errors}")
        page.close()


def code(text):
    return f'<pre class="code">{html.escape(text.strip())}</pre>'


# Each slide: title, lead, code, figures as (capture, caption) pairs and an
# optional note under them. Two figures are stacked, or side by side with "row".
SLIDES = [
    {
        "title": "Explain a metric drop",
        "lead": "Mark the incident window as a segment and diff the transition "
        'graph <span class="red">inside</span> it against '
        '<span class="blue">the rest of the log</span>. The diff works on raw '
        "events, the finest resolution in the log, so it points at the exact "
        "transitions that changed.",
        "code": """
stream.add_segment("period", time_range=("2024-05-19", "2024-06-07")) \\
      .transition_graph(diff=("period", "inside", "outside"))""",
        "figs": [("diff", "")],
    },
    {
        "title": "Analyze funnel leakage",
        "lead": "add_segment labels each user by the deepest funnel step they "
        "reached. The step matrix aligns paths on the cart and highlights the "
        'difference between those who <span class="blue">reached purchase</span> '
        'and those who <span class="red">reached cart only</span>.',
        "code": """
staged = stream.add_segment(
    "stage", funnel_events=["cart", "shipping_details", "purchase"])
staged.step_matrix(path_pattern="cart",
                   diff=("stage", "cart", "purchase"))""",
        "figs": [("funnel-diff", "")],
    },
    {
        "title": "Query paths like a regex",
        "lead": "Path patterns use a regex-like syntax with events as tokens. This "
        "one anchors the Sankey on a cart visit after which the path ends with "
        "no shipping, payment or purchase.",
        "code": f"""
pattern = "{ABANDONED}"
stream.step_sankey(anchor={{"pattern": pattern, "at": "start"}})""",
        "figs": [("abandoned", "")],
    },
    {
        "title": "Inspect why users drop off",
        "lead": "Align paths on path_end to read them backwards from the moment "
        "users leave: which events come last, and which routes lead into them.",
        "code": """
stream.step_sankey(path_pattern="path_end")""",
        "figs": [("exits", "")],
    },
    {
        "title": "Explore behavioral clusters",
        "lead": "Run k-means for every k in a range, choose a split with the help "
        "of silhouette scores, and describe the clusters through any path "
        "metrics. Name them and save them as a segment.",
        "code": """
stream.cluster_analysis(
    features=[{"metric": "event_count_bulk"}],
    method="kmeans", method_args={"n_clusters": "4-8"},
    select={"n_clusters": 4}, path_col="session_id",
)""",
        "figs": [("clusters", "")],
    },
    {
        "title": "Slice and dice event logs",
        "lead": "A graph of raw events has a node per event type: dozens, often "
        "hundreds. Cluster sessions by behavior and collapse each one into its "
        "type, and a handful of nodes show how users move between kinds of visits.",
        "code": """
visits = (stream
    .add_clusters("session_type", path_col="session_id",
                  features=[{"metric": "event_count_bulk"}])
    .collapse_events(group_col="session_id",
                     name={"col": "session_type"}))
visits.transition_graph()""",
        "figs": [("raw", "Before: raw events"), ("visits", "After: session types")],
        "row": True,
    },
    {
        "title": "Delegate analysis to an AI agent",
        "lead": "A local MCP server (beta) lets your agent run the analysis and "
        "build an interactive report. Every number in its text links to the "
        "chart cell or graph edge it comes from.",
        "code": "",
        "figs": [("agent", "")],
    },
]

CSS = """
@page { size: 1080px 1350px; margin: 0; }
* { box-sizing: border-box; }
html, body { margin: 0; background: #e5e7eb; }
body { font-family: FONT; color: #111827; -webkit-font-smoothing: antialiased; }
.slide { width: 1080px; height: 1350px; position: relative; overflow: hidden;
  background: #fafaf9; display: flex; flex-direction: column; padding: 56px 64px 56px;
  page-break-after: always; break-after: page; }
.top { display: flex; justify-content: flex-end; align-items: center;
  font-size: 22px; letter-spacing: .06em; color: #6b7280; font-weight: 600; }
.top .pill { background: hsl(45, 93%, 58%); color: #111827; padding: 4px 14px;
  border-radius: 999px; font-weight: 700; margin-right: 6px; }
h1 { font-size: 70px; line-height: 1.05; margin: 30px 0 0; letter-spacing: -.02em; }
.lead { font-size: 31px; line-height: 1.35; color: #374151; margin: 22px 0 0; }
.code { font: 20px/1.4 "SF Mono", Menlo, monospace; background: #111827; color: #f9fafb;
  border-radius: 14px; padding: 20px 24px; margin: 26px 0 0; white-space: pre; overflow: hidden; }
.figs { flex: 1; min-height: 0; margin-top: 28px; display: flex; flex-direction: column; gap: 18px; }
.fig { flex: 1; min-height: 0; min-width: 0; display: flex; flex-direction: column; }
.figs.row { flex-direction: row; }
.fig .cap { font-size: 24px; font-weight: 700; color: #374151; margin-bottom: 10px; }
.fig img { flex: 1; min-height: 0; width: 100%; object-fit: contain;
  background: #fff; border: 2px solid #e5e7eb; border-radius: 16px; padding: 10px; }
.note { font-size: 22px; color: #6b7280; margin-top: 16px; }
.red, .blue { font-weight: 700; }
.red { color: #ef4444; }
.blue { color: #3b82f6; }
.cover, .end { background: #111827; color: #f9fafb; }
.cover .top, .end .top { color: #9ca3af; }
.cover h1, .end h1 { font-size: 96px; margin-top: 120px; }
.cover h1::after, .end h1::after { content: ""; display: block; width: 180px; height: 12px;
  background: hsl(45, 93%, 58%); margin-top: 36px; }
.cover .lead, .end .lead { color: #d1d5db; font-size: 38px; margin-top: 40px; }
.when { margin-top: 44px; font-size: 40px; font-weight: 700; color: hsl(45, 93%, 58%); }
.people { margin-top: auto; display: flex; gap: 48px; }
.person { display: flex; align-items: center; gap: 22px; font-size: 30px; font-weight: 600; }
.person img { width: 120px; height: 120px; border-radius: 50%; object-fit: cover; border: 4px solid hsl(45, 93%, 58%); }
.swipe { position: absolute; right: 64px; bottom: 64px; font-size: 28px; color: #9ca3af; }
.cover .people { margin-bottom: 70px; }
.links { margin-top: 56px; display: flex; flex-direction: column; gap: 30px; font-size: 34px; }
.links b { display: block; font-size: 22px; letter-spacing: .08em; text-transform: uppercase; color: #9ca3af; margin-bottom: 6px; }
.links code { font: 32px "SF Mono", Menlo, monospace; color: hsl(45, 93%, 58%); }
.end .fine { margin-top: auto; font-size: 22px; color: #9ca3af; line-height: 1.4; }
""".replace("FONT", FONT)


def top(n, total):
    return (
        '<div class="top"><span><span class="pill">retentioneering</span> @ Quant UX Con 2026'
        f" · {n}/{total}</span></div>"
    )


def figure(name, caption):
    cap = f'<div class="cap">{caption}</div>' if caption else ""
    src = (BUILD / f"{name}.png").as_uri()
    return f'<div class="fig">{cap}<img data-name="{name}" src="{src}"></div>'


def render_html():
    total = len(SLIDES) + 2
    people = "".join(
        f'<div class="person"><img src="{(IMG / file).as_uri()}">{name}</div>'
        for name, file in [
            ("Vladimir Kukushkin", "vladimir-kukushkin.jpg"),
            ("Maxim Godzi", "maxim-godzi.jpg"),
        ]
    )
    parts = [
        f'<section class="slide cover">{top(1, total)}'
        "<h1>Retentioneering at Quant UX Con 2026</h1>"
        '<p class="lead">A talk and a hands-on workshop on getting insights from '
        "event-log data with an open-source Python library.</p>"
        '<div class="when">October 27–29 · Online</div>'
        f'<div class="people">{people}</div>'
        '<div class="swipe">Swipe →</div></section>'
    ]
    for i, slide in enumerate(SLIDES, start=2):
        figs = "".join(figure(n, c) for n, c in slide["figs"])
        note = f'<div class="note">{slide["note"]}</div>' if slide.get("note") else ""
        parts.append(
            f'<section class="slide">{top(i, total)}'
            f"<h1>{slide['title']}</h1>"
            f'<p class="lead">{slide["lead"]}</p>'
            f"{code(slide['code']) if slide['code'] else ''}"
            f'<div class="figs{" row" if slide.get("row") else ""}">{figs}</div>'
            f"{note}</section>"
        )
    parts.append(
        f'<section class="slide end">{top(total, total)}'
        "<h1>Join us online, October 27–29</h1>"
        '<p class="lead">We’ll walk through all of this live, on real code.</p>'
        '<div class="links">'
        "<div><b>Install</b><code>pip install retentioneering</code></div>"
        "<div><b>Code</b>github.com/retentioneering/retentioneering-tools</div>"
        "<div><b>Docs</b>retentioneering.com/docs</div>"
        "</div>"
        '<p class="fine">Screens are real Retentioneering widgets on the synthetic '
        "e-commerce dataset bundled with the library.</p></section>"
    )
    doc = (
        '<!doctype html><html><head><meta charset="utf-8">'
        f"<style>{CSS}</style></head><body>{''.join(parts)}</body></html>"
    )
    (BUILD / "carousel.html").write_text(doc)


def measure_frames(browser):
    """Lay the slides out once and read the size of every figure frame."""
    render_html()
    page = browser.new_page(viewport={"width": 1080, "height": 1350})
    page.goto((BUILD / "carousel.html").as_uri())
    frames = page.evaluate(
        """() => Object.fromEntries([...document.querySelectorAll('.fig img')].map(
            i => [i.dataset.name, [i.offsetWidth, i.offsetHeight]]))"""
    )
    page.close()
    return frames


def print_slides(browser):
    page = browser.new_page(
        viewport={"width": 1080, "height": 1350}, device_scale_factor=2
    )
    page.goto((BUILD / "carousel.html").as_uri())
    page.wait_for_timeout(500)
    page.pdf(
        path=str(HERE / "carousel.pdf"),
        width="1080px",
        height="1350px",
        print_background=True,
    )
    for k, slide in enumerate(page.locator("section.slide").all(), start=1):
        slide.screenshot(path=str(BUILD / f"slide-{k:02d}.png"))
    page.close()


if __name__ == "__main__":
    BUILD.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(executable_path=CHROME)
        sizes = widget_sizes(measure_frames(browser))
        export_widgets(sizes)
        capture(browser, sizes)
        render_html()
        print_slides(browser)
        browser.close()
    print(f"Wrote {HERE / 'carousel.pdf'}")
