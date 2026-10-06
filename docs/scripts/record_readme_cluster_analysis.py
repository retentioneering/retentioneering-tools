"""Record the README cluster analysis GIF in a live JupyterLab.

Starts from a bare `stream.cluster_analysis()` and configures it through the UI:
event_count_bulk features, 3-8 clusters, three overview metrics, Apply, another
partition on the Silhouette tab, renamed clusters and Save Clusters. Apply and the
partition switch recompute in Python, so this drives a real kernel rather than a
static export. Needs `make build`; from the repo root:
    RETENTIONEERING_NO_TRACK=1 uv run --with playwright python docs/scripts/record_readme_cluster_analysis.py
Set CHROME_PATH to use an installed Chrome instead of Playwright's Chromium.
"""

import io
import json
import math
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

from PIL import Image
from playwright.sync_api import sync_playwright

REPO = Path(__file__).resolve().parents[2]
OUT = REPO / ".github" / "readme" / "cluster-analysis.gif"
OUT_W = 1000
TOKEN = "readme"
NOTEBOOK = {
    "cells": [
        {
            "cell_type": "code",
            "execution_count": None,
            "id": "c1",
            "metadata": {},
            "outputs": [],
            "source": [
                "import os\n",
                "os.environ['RETENTIONEERING_NO_TRACK'] = '1'\n",
                "import retentioneering as rete\n",
                "stream = rete.datasets.load_ecom()\n",
                "stream.cluster_analysis()",
            ],
        }
    ],
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        }
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}
CURSOR = """
(() => {
  const c = document.createElement('div'); c.id = '__cursor';
  c.innerHTML = '<svg width="22" height="26" viewBox="0 0 22 26"><path d="M2 2 L2 20 L7 15.5 L10.5 23 L13.5 21.6 L10 14.3 L16.5 14.3 Z" fill="black" stroke="white" stroke-width="1.6" stroke-linejoin="round"/></svg>';
  Object.assign(c.style, {position:'fixed', left:'-50px', top:'-50px', zIndex: 2147483647, pointerEvents:'none', transformOrigin:'2px 2px', transition:'transform 80ms'});
  document.body.appendChild(c);
  document.addEventListener('mousemove', e => { c.style.left = (e.clientX - 2) + 'px'; c.style.top = (e.clientY - 2) + 'px'; }, true);
  document.addEventListener('mousedown', () => c.style.transform = 'scale(0.82)', true);
  document.addEventListener('mouseup', () => c.style.transform = 'scale(1)', true);
  const st = document.createElement('style');
  st.textContent = '.jp-cell-toolbar, .jp-Toolbar-item.jp-CellToolbar, .jp-InputArea-editor .cm-cursor {display:none !important}';
  document.head.appendChild(st);
})();
"""
# Stand-in for the native <select> popup, which headless screenshots do not capture.
DROPDOWN = """
([sel, value, top, bottom]) => {
  const r = sel.getBoundingClientRect();
  const d = document.createElement('div'); d.id = '__dropdown';
  Object.assign(d.style, {position:'fixed', left: r.left + 'px', top: (r.bottom + 2) + 'px', minWidth: r.width + 'px',
    background:'#fff', border:'1px solid #d1d5db', borderRadius:'6px', boxShadow:'0 6px 16px rgba(0,0,0,.15)',
    zIndex: 2147483646, padding:'4px 0', font:'12px -apple-system, BlinkMacSystemFont, sans-serif', color:'#111827'});
  for (const o of sel.options) {
    if (!o.value && !o.textContent.trim()) continue;
    const it = document.createElement('div'); it.textContent = o.textContent; it.dataset.value = o.value;
    Object.assign(it.style, {padding:'3px 10px', whiteSpace:'nowrap'});
    d.appendChild(it);
  }
  document.body.appendChild(d);
  // Like a browser: open on the side with more room, cap the list to it and
  // scroll the list so the option about to be picked is in view.
  const below = bottom - r.bottom - 2, above = r.top - top - 2;
  const room = Math.max(below, above);
  if (d.offsetHeight > room) Object.assign(d.style, {maxHeight: room + 'px', overflowY: 'auto'});
  if (below < d.offsetHeight && above > below) d.style.top = (r.top - 2 - d.offsetHeight) + 'px';
  const target = [...d.children].find(c => c.dataset.value === value);
  d.scrollTop = target.offsetTop - d.clientHeight / 2;
}
"""


def start_lab(root):
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "jupyter",
            "lab",
            "--no-browser",
            f"--port={port}",
            f"--IdentityProvider.token={TOKEN}",
            f"--ServerApp.root_dir={root}",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={**os.environ, "RETENTIONEERING_NO_TRACK": "1"},
    )
    for _ in range(120):
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/api/status?token={TOKEN}")
            return proc, port
        except OSError:
            time.sleep(0.5)
    proc.terminate()
    raise RuntimeError("JupyterLab did not start")


def record(port):
    PORT = port
    frames = []  # (PIL image, duration ms)
    with sync_playwright() as p:
        launch = (
            {"executable_path": os.environ["CHROME_PATH"]}
            if os.environ.get("CHROME_PATH")
            else {}
        )
        b = p.chromium.launch(**launch)
        pg = b.new_page(viewport={"width": 1060, "height": 900}, device_scale_factor=2)
        pg.goto(f"http://localhost:{PORT}/lab/tree/clusters.ipynb?token={TOKEN}&reset")
        pg.wait_for_selector(".jp-Notebook .jp-Cell", timeout=60000)
        pg.wait_for_timeout(2500)
        pg.keyboard.press("ControlOrMeta+b")
        pg.wait_for_timeout(500)
        pg.locator(".jp-Cell").first.click()
        pg.keyboard.press("Shift+Enter")
        pg.get_by_role("button", name="Apply").wait_for(timeout=60000)
        pg.wait_for_timeout(1500)
        pg.locator(".jp-Cell").nth(1).click()
        pg.keyboard.press("Escape")  # leave edit mode, park focus
        pg.evaluate(CURSOR)
        # Frame the widget alone: the notebook around it is not part of the demo.
        out = (
            pg.locator(".jp-Cell")
            .first.locator(".jp-OutputArea-output")
            .first.bounding_box()
        )
        clip = {
            "x": out["x"] - 1,
            "y": out["y"] - 1,
            "width": out["width"] + 2,
            "height": out["height"] + 2,
        }
        pos = [clip["x"] + clip["width"] * 0.45, clip["y"] + clip["height"] * 0.55]

        def shot(ms):
            pg.evaluate(
                "() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)))"
            )
            frames.append(
                (Image.open(io.BytesIO(pg.screenshot(clip=clip))).convert("RGB"), ms)
            )

        def move(x, y, n=8):
            x0, y0 = pos
            for i in range(1, n + 1):
                e = 0.5 - 0.5 * math.cos(math.pi * i / n)
                pg.mouse.move(x0 + (x - x0) * e, y0 + (y - y0) * e)
                shot(40)
            pos[:] = [x, y]

        def center(loc):
            bx = loc.bounding_box()
            return bx["x"] + bx["width"] / 2, bx["y"] + bx["height"] / 2

        def click(loc, hold=500, n=8):
            move(*center(loc), n=n)
            shot(200)
            pg.mouse.down()
            shot(60)
            pg.mouse.up()
            pg.wait_for_timeout(150)
            shot(hold)

        def type_text(text, every=2):
            for i, ch in enumerate(text):
                pg.keyboard.type(ch, delay=20)
                if i % every == every - 1 or i == len(text) - 1:
                    shot(60)

        def pick(sel, value):
            click(sel, hold=150, n=8)
            pg.evaluate(
                DROPDOWN,
                [
                    sel.element_handle(),
                    value,
                    clip["y"] + 14,
                    clip["y"] + clip["height"] - 14,
                ],
            )
            item = pg.locator(f'#__dropdown div[data-value="{value}"]')
            shot(250)
            move(*center(item), n=6)
            item.evaluate(
                "e => { e.style.background = '#2563eb'; e.style.color = '#fff'; }"
            )
            shot(300)
            pg.mouse.down()
            pg.mouse.up()
            pg.evaluate("() => document.getElementById('__dropdown').remove()")
            sel.select_option(value)
            pg.wait_for_timeout(150)
            shot(300)

        def btn(name):
            return pg.get_by_role("button", name=name, exact=True)

        pg.mouse.move(*pos)
        shot(1000)

        # 1. features: event_count_bulk
        click(pg.get_by_role("button", name="Configure Features"), hold=1100)
        click(btn("Done"), hold=500)
        # 2. 3-8 clusters
        field = pg.locator('input[placeholder^="e.g. 3-8"]')
        click(field, hold=150)
        pg.keyboard.press("ControlOrMeta+a")
        shot(200)
        type_text("3-8", every=1)
        shot(500)
        # 3. overview metrics
        click(pg.get_by_role("button", name="Configure Metrics"), hold=700)
        metric_sel = pg.locator("select").filter(has=pg.locator("option[value=length]"))
        click(btn("Add Metric"), hold=300)
        pick(metric_sel.nth(1), "length")
        click(btn("Add Metric"), hold=300)
        pick(metric_sel.nth(2), "in_segment_bulk")
        pick(
            pg.locator("select")
            .filter(has=pg.locator("option[value=acquisition_channel]"))
            .first,
            "acquisition_channel",
        )
        shot(700)
        click(btn("Done"), hold=400)
        # 4. apply
        click(btn("Apply"), hold=300)
        for _ in range(3):
            pg.wait_for_timeout(500)
            shot(300)
        btn("Silhouette").wait_for(timeout=120000)
        pg.wait_for_timeout(800)
        shot(1500)
        # show the overview metrics at the bottom of the table, then back
        move(
            *center(
                pg.locator(".jp-OutputArea-output td")
                .filter(has_text="catalog · event_count_bulk")
                .first
            ),
            n=8,
        )
        # Scroll the table's own container: wheel events past its end would
        # chain to the notebook and scroll the whole cell out of the frame.
        TABLE = """(f) => { const el = [...document.querySelectorAll('.jp-OutputArea-output div')]
            .find(e => e.scrollHeight > e.clientHeight + 5 && getComputedStyle(e).overflowY === 'auto' && e.querySelector('table'));
            el.scrollTop = f * (el.scrollHeight - el.clientHeight); }"""
        for i in range(1, 9):
            pg.evaluate(TABLE, i / 8)
            shot(60)
        shot(1600)
        for i in range(7, -1, -1):
            pg.evaluate(TABLE, i / 8)
            shot(40)
        # 5-6. silhouette, another partition
        click(btn("Silhouette"), hold=1100)
        click(pg.locator("svg g").filter(has_text="k=4").first, hold=200)
        pg.wait_for_function(
            "() => document.body.innerText.includes('k=4') && !document.body.innerText.includes('Computing')",
            timeout=120000,
        )
        pg.wait_for_timeout(1500)
        shot(1100)
        click(btn("Overview"), hold=800)
        # 7. rename clusters in the header
        for old, new in [
            ("cluster_0", "browsers"),
            ("cluster_1", "researchers"),
            ("cluster_2", "buyers"),
            ("cluster_3", "light_users"),
        ]:
            click(pg.get_by_text(old, exact=True).first, hold=150, n=8)
            pg.keyboard.press("ControlOrMeta+a")
            type_text(new, every=3)
            pg.keyboard.press("Enter")
            pg.wait_for_timeout(150)
            shot(250)
        shot(1000)
        # 8. save clusters
        # the Save Clusters section sits below the fold of the sidebar
        save = btn("Save Clusters…")
        move(*center(pg.get_by_role("button", name="Configure Metrics")), n=10)
        for _ in range(8):
            if save.bounding_box()["y"] + 40 < out["y"] + out["height"]:
                break
            pg.mouse.wheel(0, 120)
            pg.wait_for_timeout(80)
            shot(80)
        shot(300)
        click(save, hold=900)
        name = pg.locator(
            "xpath=//*[normalize-space(text())='Segment column name']/following::input[1]"
        )
        click(name, hold=100)
        pg.keyboard.press("ControlOrMeta+a")
        type_text("user_type", every=3)
        shot(600)
        click(btn("Apply in-place"), hold=400)
        pg.wait_for_timeout(1500)
        shot(2600)
        b.close()

    h = round(frames[0][0].height * OUT_W / frames[0][0].width)
    imgs = [im.resize((OUT_W, h), Image.LANCZOS) for im, _ in frames]
    mosaic = Image.new("RGB", (OUT_W, h * len(imgs[::6])))
    for k, im in enumerate(imgs[::6]):
        mosaic.paste(im, (0, h * k))
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
    with tempfile.TemporaryDirectory() as root:
        (Path(root) / "clusters.ipynb").write_text(json.dumps(NOTEBOOK))
        lab_proc, lab_port = start_lab(root)
        try:
            record(lab_port)
        finally:
            lab_proc.terminate()
