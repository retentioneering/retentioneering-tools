"""Record the README step matrix GIF: funnel-anchored diff, cell hovers and row sorting.

Exports the widget from the bundled ecom dataset, then drives it in headless Chromium
with a drawn cursor and assembles the frames into a GIF with Pillow. From the repo root,
after `make build`:
    RETENTIONEERING_NO_TRACK=1 uv run --with playwright python docs/scripts/record_readme_step_matrix.py
Set CHROME_PATH to use an installed Chrome instead of Playwright's Chromium.
"""

import io
import math
import os
from pathlib import Path

from PIL import Image
from playwright.sync_api import sync_playwright

import retentioneering as rete

REPO = Path(__file__).resolve().parents[2]
HTML = REPO / "docs" / "build" / "readme" / "step-matrix-funnel.html"
# Wide enough that the hover tooltip of the right-hand block stays inside the frame.
VIEWPORT = {"width": 1000, "height": 540}
OUT_W = 1180
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


def export_widget():
    HTML.parent.mkdir(parents=True, exist_ok=True)
    stream = rete.datasets.load_ecom()
    steps = ["cart", "shipping_details", "purchase"]
    widget = stream.add_segment(
        "stage", funnel_events=steps, path_col="session_id"
    ).step_matrix(
        path_pattern="cart->.*->shipping_details",
        step_window=2,
        path_col="session_id",
        diff=("stage", "shipping_details", "purchase"),
        sidebar_open=False,
        height=470,
    )
    widget.export_html(HTML, title="Funnel step matrix")


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
        pg.wait_for_function(
            "document.querySelector('#retentioneering-root')?.textContent.length > 30"
        )
        # Jupyter gives widgets a sans-serif page font; the bare export page does not.
        pg.add_style_tag(
            content='body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; }'
        )
        pg.evaluate(CURSOR)
        root = pg.locator("#retentioneering-root").bounding_box()
        clip = {
            "x": root["x"],
            "y": root["y"],
            "width": root["width"],
            "height": root["height"],
        }
        pos = [root["x"] + root["width"] * 0.62, root["y"] + root["height"] * 0.86]

        def shot(ms):
            pg.evaluate(
                "() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)))"
            )
            frames.append(
                (Image.open(io.BytesIO(pg.screenshot(clip=clip))).convert("RGB"), ms)
            )

        def move(x, y, n=12):
            x0, y0 = pos
            for i in range(1, n + 1):
                t = i / n
                e = 0.5 - 0.5 * math.cos(math.pi * t)  # ease in-out
                pg.mouse.move(x0 + (x - x0) * e, y0 + (y - y0) * e)
                shot(50)
            pos[:] = [x, y]

        def center(loc):
            bx = loc.bounding_box()
            return bx["x"] + bx["width"] / 2, bx["y"] + bx["height"] / 2

        def cell(event, block, step=1):
            # The matrix has one block per pattern anchor: 0 = cart, 1 = shipping_details.
            return pg.locator(f'tr[data-event="{event}"] td[data-step="{step}"]').nth(
                block
            )

        def sort_btn(title, block):
            return pg.locator(f'button[title="{title}"]').nth(block)

        pg.mouse.move(*pos)
        shot(1300)
        move(*center(cell("purchase", 1)))
        shot(1900)
        move(*center(cell("path_end", 1)), n=6)
        shot(1900)

        def click(loc, n=12):
            move(*center(loc), n=n)
            shot(400)
            pg.mouse.down()
            shot(80)
            pg.mouse.up()
            pg.wait_for_timeout(250)  # let the rows re-order
            shot(1900)

        click(sort_btn("Sort by following steps", 1))
        click(sort_btn("Sort by preceding steps", 1), n=8)
        move(*center(cell("purchase", 1, step=-1)))
        shot(2600)
        if errs:
            raise RuntimeError(f"page errors: {errs}")
        b.close()

    # downscale, shared palette, no dithering (flat UI colours)
    h = round(frames[0][0].height * OUT_W / frames[0][0].width)
    imgs = [im.resize((OUT_W, h), Image.LANCZOS) for im, _ in frames]
    mosaic = Image.new("RGB", (OUT_W, h * len(imgs[::4])))
    for k, im in enumerate(imgs[::4]):
        mosaic.paste(im, (0, h * k))
    pal = mosaic.quantize(colors=256, method=Image.Quantize.MEDIANCUT)
    q = [im.quantize(palette=pal, dither=Image.Dither.NONE) for im in imgs]
    out = REPO / ".github" / "readme" / "step-matrix-funnel.gif"
    q[0].save(
        out,
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
        out.stat().st_size // 1024,
        "KB",
        (OUT_W, h),
    )


if __name__ == "__main__":
    export_widget()
    record()
