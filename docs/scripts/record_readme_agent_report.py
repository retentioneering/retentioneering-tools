"""Record the README MCP report GIF: links in the analysis text focus elements of three tools.

Builds the report the way the MCP server does (`ReportSession` plus the
`add_transition_graph` / `add_step_matrix` / `add_segment_overview` tool functions) on
the bundled ecom dataset, exports it to standalone HTML and clicks the links in its
text: Segment Overview cells, transition graph edges and Step Matrix cells. The
analysis text is written for this demo; every number in it is read from the tabs.
From the repo root, after `make build`:
    RETENTIONEERING_NO_TRACK=1 uv run --with playwright python docs/scripts/record_readme_agent_report.py
Set CHROME_PATH to use an installed Chrome instead of Playwright's Chromium.
"""

import io
import math
import os
from pathlib import Path

from PIL import Image
from playwright.sync_api import sync_playwright

import retentioneering as rete
from retentioneering.mcp import tools
from retentioneering.mcp._report_session import ReportSession

REPO = Path(__file__).resolve().parents[2]
HTML = REPO / "docs" / "build" / "readme" / "agent-report.html"
OUT = REPO / ".github" / "readme" / "agent-report.gif"
VIEWPORT = {"width": 1100, "height": 820}
# Width of the tools pane: wide enough for the matrix and overview to stay legible.
LEFT_PANE = 620
OUT_W = 1100
# The widgets' link-focus outline colour (focusCells in js/widget/src/widget-utils.tsx).
FOCUS_RING = (245, 158, 11)
CURSOR = """
(() => {
  const c = document.createElement('div');
  c.id = '__cursor';
  c.innerHTML = '<svg width="22" height="26" viewBox="0 0 22 26"><path d="M2 2 L2 20 L7 15.5 L10.5 23 L13.5 21.6 L10 14.3 L16.5 14.3 Z" fill="black" stroke="white" stroke-width="1.6" stroke-linejoin="round"/></svg>';
  Object.assign(c.style, {position:'fixed', left:'0px', top:'0px', zIndex: 2147483647, pointerEvents:'none',
    transformOrigin:'2px 2px', transition:'transform 80ms'});
  document.body.appendChild(c);
  document.addEventListener('mousemove', e => { c.style.left = (e.clientX - 2) + 'px'; c.style.top = (e.clientY - 2) + 'px'; }, true);
  document.addEventListener('mousedown', () => c.style.transform = 'scale(0.82)', true);
  document.addEventListener('mouseup', () => c.style.transform = 'scale(1)', true);
})();
"""

ANALYSIS = """# Purchase conversion dropped during the payment incident

Between 19 May and 7 June, sessions ended in a purchase half as often: **7.0%** [Periods:has_event_purchase@inside] against **14.2%** [Periods:has_event_purchase@outside] in the rest of the log.

## Where the paths changed

- The step from payment details to a purchase is **13 p.p.** less likely inside the window [Payment flow:payment_details->purchase].
- Instead, payment details lead to a payment error: **12%** of transitions inside the window against `0.5%` outside [Payment flow:payment_details->payment_error].
- **14.9%** of sessions in the window hit a payment error [Periods:has_event_payment_error@inside], and **16.7%** open a support chat [Periods:has_event_support_chat@inside].

## What follows a payment error

- Right after the error, **18%** of sessions open a support chat [After payment error:support_chat@1].
- Two steps later, **26%** of sessions have already ended [After payment error:path_end@2].

The comparison locates where behavior changed; confirming the cause needs the payment logs for the same period.
"""

# Links clicked in the recording, by their text, in order.
CLICKS = [
    "has_event_purchase(period: inside)",
    "has_event_purchase(period: outside)",
    "payment_details->purchase",
    "payment_details->payment_error",
    "support_chat@1",
    "path_end@2",
]


def build_report():
    HTML.parent.mkdir(parents=True, exist_ok=True)
    session = ReportSession(
        rete.datasets.load_ecom(),
        context={"description": "E-commerce store. Main KPI: purchase conversion."},
    )
    tools.update_base_stream(
        session,
        [
            {
                "type": "add_segment",
                "name": "period",
                "time_range": ["2024-05-19", "2024-06-07"],
            }
        ],
    )
    tools.add_transition_graph(
        session,
        "Payment flow",
        diff=["period", "inside", "outside"],
        path_col="session_id",
    )
    tools.add_step_matrix(
        session,
        "After payment error",
        max_steps=3,
        path_pattern="payment_error",
        path_col="session_id",
    )
    tools.add_segment_overview(
        session,
        "Periods",
        segment_col="period",
        path_col="session_id",
        metrics=[
            {"metric": "has_event", "metric_args": {"event": "purchase"}},
            {"metric": "has_event", "metric_args": {"event": "payment_error"}},
            {"metric": "has_event", "metric_args": {"event": "support_chat"}},
        ],
    )
    check = tools.check_analysis(ANALYSIS)
    if check["status"] != "ok":
        raise RuntimeError(f"analysis has unlinked numbers: {check}")
    session.export("Payment incident: purchase conversion", ANALYSIS, str(HTML))


def record():
    frames = []  # (PIL image, duration ms)
    with sync_playwright() as p:
        launch = (
            {"executable_path": os.environ["CHROME_PATH"]}
            if os.environ.get("CHROME_PATH")
            else {}
        )
        b = p.chromium.launch(**launch)
        pg = b.new_page(viewport=VIEWPORT, device_scale_factor=2)
        errs = []
        pg.on("pageerror", lambda e: errs.append(str(e)))
        pg.goto(HTML.as_uri())
        pg.wait_for_timeout(2500)  # the graph animates its initial layout
        split = pg.locator("#splitter").bounding_box()
        pg.mouse.move(split["x"] + split["width"] / 2, split["y"] + split["height"] / 2)
        pg.mouse.down()
        pg.mouse.move(LEFT_PANE, split["y"] + split["height"] / 2, steps=10)
        pg.mouse.up()
        pg.wait_for_timeout(1500)  # widgets refit to the wider pane
        pg.evaluate(CURSOR)
        pos = [VIEWPORT["width"] * 0.8, VIEWPORT["height"] * 0.9]

        def shot(ms):
            pg.evaluate(
                "() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)))"
            )
            frames.append((Image.open(io.BytesIO(pg.screenshot())).convert("RGB"), ms))

        def move(x, y, n=10):
            x0, y0 = pos
            for i in range(1, n + 1):
                e = 0.5 - 0.5 * math.cos(math.pi * i / n)
                pg.mouse.move(x0 + (x - x0) * e, y0 + (y - y0) * e)
                shot(40)
            pos[:] = [x, y]

        pg.mouse.move(*pos)
        shot(1500)
        for text in CLICKS:
            link = pg.locator("a.node-link", has_text=text).first
            bx = link.bounding_box()
            move(bx["x"] + min(bx["width"] / 2, 60), bx["y"] + bx["height"] / 2)
            shot(250)
            pg.mouse.down()
            shot(60)
            pg.mouse.up()
            if "@" in (link.get_attribute("data-node") or ""):
                # Matrix and overview cells flash, then keep an outline.
                pg.wait_for_timeout(350)
                shot(600)
                pg.wait_for_timeout(700)
                shot(1500)
            else:
                # Graph links animate the camera onto the edge.
                for _ in range(4):
                    pg.wait_for_timeout(120)
                    shot(120)
                pg.wait_for_timeout(500)
                shot(1800)
        if errs:
            raise RuntimeError(f"page errors: {errs}")
        b.close()

    h = round(frames[0][0].height * OUT_W / frames[0][0].width)
    imgs = [im.resize((OUT_W, h), Image.LANCZOS) for im, _ in frames]
    # Build the palette from every frame (at half size): a colour that shows up
    # in a few frames only, like the amber focus ring, would otherwise be mapped
    # onto the cell colour under it and vanish.
    # The ring is also a thin line, so median cut can still merge it into the
    # cell colours; a solid band of it in the sample guarantees a palette slot.
    small = [im.resize((OUT_W // 2, h // 2)) for im in imgs]
    band = h // 2
    mosaic = Image.new("RGB", (OUT_W // 2, (h // 2) * len(small) + band), FOCUS_RING)
    for k, im in enumerate(small):
        mosaic.paste(im, (0, (h // 2) * k))
    pal = mosaic.quantize(colors=256, method=Image.Quantize.MEDIANCUT)
    q = [im.quantize(palette=pal, dither=Image.Dither.NONE) for im in imgs]
    q[0].save(
        OUT,
        save_all=True,
        append_images=q[1:],
        duration=[d for _, d in frames],
        loop=0,
        optimize=True,
    )
    print(
        len(frames),
        "frames",
        sum(d for _, d in frames),
        "ms",
        OUT.stat().st_size // 1024,
        "KB",
    )


if __name__ == "__main__":
    build_report()
    record()
