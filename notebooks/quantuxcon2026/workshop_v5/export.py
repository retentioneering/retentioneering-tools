"""Export the rendered deck (slides.html) as still frames: export/slides.pdf
and export/slides.html, a self-contained page of the same frames.

    ./render.sh export     render, then export
    uv run --with playwright --with img2pdf python export.py   export only

A slide with clicks becomes a frame per click, as on screen, unless FRAMES
below says otherwise. The deck runs in its export mode (slides.html?export,
deck.html and custom.scss): no controls, no transitions, and a slide marked
`export-all` shows all its highlights at once.

Frames are screenshots (Chromium, via Playwright), so the widgets come out
exactly as on screen; the text in the PDF is an image, not selectable.
"""

from __future__ import annotations

import base64
import functools
import http.server
import sys
import threading
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "export"
WIDTH, HEIGHT = 1280, 760
SCALE = 1.5  # screenshot pixels per slide pixel
JPEG_QUALITY = 85

# Slide id → which frames to take. "last": the final click only (the data
# processors: before and after at once); "all": one frame with every
# highlight shown (export-all); a list: those click states (-1 = no click).
# Anything not listed: a frame per click.
PROCESSORS = [
    "collapse_events-collapse-loops",
    "collapse_events-one-event-per-session",
    "truncate_paths-cut-a-window-between-two-events",
    "truncate_paths-expand-your-horizons",
    "truncate_paths-set-borders-with-regex",
    "add_events-mark-a-position",
]
FRAMES: dict[str, str | list[int]] = {
    **{sid: "last" for sid in PROCESSORS},
    # All the column groups framed, the schema code without a highlight.
    "sequence-log-data": "all",
    "eventstream": "all",
    # No highlight; proba_out = 3 / 5 = 0.6; proba_in = 3 / 9 ≈ 0.33.
    "edge-weights": [-1, 4, 7],
    # The three step columns at once, each in its own color.
    "step-matrix-count-events-at-each-step": "all",
    "tree-vs.-step-matrix": "all",
    "step-matrix-centered-on-an-event": "all",
}


def serve(root: Path) -> tuple[http.server.ThreadingHTTPServer, int]:
    """Serve `root` on a free local port (the widgets are same-origin iframes,
    which a file:// page can't reach into)."""

    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *args):
            pass

    handler = functools.partial(Quiet, directory=str(root))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, server.server_address[1]


def capture() -> list[bytes]:
    from playwright.sync_api import sync_playwright

    server, port = serve(HERE)
    frames: list[bytes] = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page(
                viewport={"width": WIDTH, "height": HEIGHT}, device_scale_factor=SCALE
            )
            page.goto(f"http://127.0.0.1:{port}/slides.html?export")
            page.wait_for_function("window.Reveal && Reveal.isReady()")
            page.wait_for_timeout(3000)
            slides = page.evaluate(
                """Reveal.getSlides().map(function (s) {
                  var idx = Array.prototype.map.call(s.querySelectorAll('.fragment[data-fragment-index]'),
                    function (f) { return +f.dataset.fragmentIndex; });
                  return { id: s.id, frags: idx.length ? Math.max.apply(null, idx) + 1 : 0,
                           widgets: !!s.querySelector('iframe') };
                })"""
            )
            unknown = set(FRAMES) - {s["id"] for s in slides}
            if unknown:
                print(
                    "export.py: no slides with ids",
                    sorted(unknown),
                    "(renamed?)",
                    file=sys.stderr,
                )
            for n, slide in enumerate(slides):
                rule = FRAMES.get(slide["id"])
                if rule == "all":
                    page.evaluate(
                        "id => document.getElementById(id).classList.add('export-all')",
                        slide["id"],
                    )
                    steps = [-1]
                elif rule == "last":
                    steps = [slide["frags"] - 1]
                elif isinstance(rule, list):
                    steps = rule
                else:
                    steps = list(range(-1, slide["frags"]))
                for k, step in enumerate(steps):
                    page.evaluate(
                        """([n, f]) => { var s = Reveal.getSlides()[n]; var i = Reveal.getIndices(s);
                                         Reveal.slide(i.h, i.v, f); }""",
                        [n, step],
                    )
                    # Widgets reload once their slide shows (deck.html refitWidgets).
                    wait = (4500 if k == 0 else 2500) if slide["widgets"] else 900
                    page.wait_for_timeout(wait)
                    frames.append(page.screenshot(type="jpeg", quality=JPEG_QUALITY))
                    print(
                        f"\r{n + 1}/{len(slides)} slides, {len(frames)} frames",
                        end="",
                        flush=True,
                    )
            print()
            browser.close()
    finally:
        server.shutdown()
    return frames


def write_pdf(frames: list[bytes], path: Path) -> None:
    import img2pdf

    # One page per frame, the slide's proportions, 13.33 in wide (16:9-ish).
    page = (img2pdf.in_to_pt(13.333), img2pdf.in_to_pt(13.333 * HEIGHT / WIDTH))
    path.write_bytes(img2pdf.convert(frames, layout_fun=img2pdf.get_layout_fun(page)))


VIEWER = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>From Complex User Flows to Clear Behavioral Patterns</title>
<style>
  html, body { margin: 0; height: 100%; background: #111827; }
  body { display: flex; align-items: center; justify-content: center; overflow: hidden; }
  img { max-width: 100vw; max-height: 100vh; display: block; }
  #n { position: fixed; right: 12px; bottom: 8px; color: #9ca3af; font: 12px sans-serif; }
</style></head><body>
<img id="frame" alt="slide"><div id="n"></div>
<script>
  var FRAMES = __FRAMES__;
  var i = Math.max(0, Math.min(FRAMES.length - 1, (parseInt(location.hash.slice(1), 10) || 1) - 1));
  function show() {
    document.getElementById("frame").src = "data:image/jpeg;base64," + FRAMES[i];
    document.getElementById("n").textContent = (i + 1) + " / " + FRAMES.length;
    history.replaceState(null, "", "#" + (i + 1));
  }
  function go(d) { i = Math.max(0, Math.min(FRAMES.length - 1, i + d)); show(); }
  document.addEventListener("keydown", function (e) {
    if (["ArrowRight", "ArrowDown", "PageDown", " ", "Enter"].indexOf(e.key) >= 0) go(1);
    if (["ArrowLeft", "ArrowUp", "PageUp", "Backspace"].indexOf(e.key) >= 0) go(-1);
    if (e.key === "Home") { i = 0; show(); }
    if (e.key === "End") { i = FRAMES.length - 1; show(); }
  });
  document.addEventListener("click", function (e) { go(e.clientX < innerWidth / 3 ? -1 : 1); });
  show();
</script></body></html>
"""


def write_html(frames: list[bytes], path: Path) -> None:
    data = ",".join('"' + base64.b64encode(f).decode() + '"' for f in frames)
    path.write_text(VIEWER.replace("__FRAMES__", "[" + data + "]"))


def main() -> None:
    if not (HERE / "slides.html").exists():
        sys.exit("export.py: no slides.html; run ./render.sh first")
    frames = capture()
    OUT.mkdir(exist_ok=True)
    write_pdf(frames, OUT / "slides.pdf")
    write_html(frames, OUT / "slides.html")
    size = sum(len(f) for f in frames) / 1e6
    print(
        f"{len(frames)} frames ({size:.0f} MB) → {OUT / 'slides.pdf'}, {OUT / 'slides.html'}"
    )


if __name__ == "__main__":
    main()
