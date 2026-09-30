"""Generate the README figure from the exact quick-start data and native widget.

From the repository root:
    python docs/scripts/generate_readme_figure.py
    node docs/scripts/capture_readme_figure.cjs

The screenshot uses a saved view to arrange the nodes and hide events outside
the retry loop. It does not modify the event data or redraw the graph.
"""

import os
import runpy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "artifacts/readme"
OUT.mkdir(parents=True, exist_ok=True)
os.chdir(OUT)
demo = runpy.run_path(str(ROOT / "examples/readme_quickstart.py"))
stream = demo["stream"]
raw = demo["events"]

scores = []
for group in ("A", "B"):
    rows = raw[raw.variant.eq(group)]
    users = rows.user_id.nunique()
    buyers = rows[rows.event.eq("purchase")].user_id.nunique()
    repeaters = rows[rows.event.eq("shipping")].groupby("user_id").size().gt(1).sum()
    scores.append(
        f'<div class="score"><b>GROUP {group}</b><strong>{buyers} / {users} buyers</strong>'
        f"<span>{repeaters} / {users} repeat shipping</span></div>"
    )

view = {
    "name": "Retry loop",
    "nodePositions": {
        "shipping": {"x": 0, "y": 100},
        "address_error": {"x": 200, "y": 0},
        "cart": {"x": 400, "y": 100},
    },
    "hiddenEvents": ["checkout", "purchase", "path_start", "path_end"],
    "viewport": "fit",
}
stream.transition_graph(
    diff=("variant", "B", "A"),
    edge_weight="unique_paths",
    height=270,
    sidebar_open=False,
    views=[view],
    view="Retry loop",
).export_html(str(OUT / "figure-widget.html"), sidebar_open=False)

css = """
*{box-sizing:border-box}body{margin:0;background:white;color:#122e38;
font:16px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
.figure{width:1000px;padding:26px 30px 24px;background:#f1f6f5;border:1px solid #dce6e3;border-radius:14px}
.eyebrow{font-size:11px;font-weight:700;letter-spacing:1.5px;color:#45685f;margin-bottom:7px}
h1{font-size:32px;line-height:1.15;letter-spacing:-.8px;margin:0 0 19px}
.scores{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px}
.score{display:grid;grid-template-columns:1fr auto;align-items:center;gap:4px 16px;background:#14343e;color:white;padding:12px 18px;border-radius:8px}
.score b{font-size:11px;letter-spacing:1px;color:#a4e6d3}.score strong{grid-row:1/3;grid-column:2;font-size:26px;letter-spacing:-.6px}
.score span{font-size:13px;color:#d4e3e6}.score:last-child b{color:#ffb0a7}
.frame{background:white;border:1px solid #dce6e3;border-radius:8px;overflow:hidden}
iframe{display:block;width:100%;height:318px;border:0}
.caption{font-size:13px;color:#385762;margin:12px 0 0}.caption b{color:#cc3545}
.foot{font-size:10px;letter-spacing:.6px;color:#60796f;margin-top:10px}
"""
html = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<title>Same conversion. Different paths.</title><style>{css}</style></head>
<body><article class="figure"><div class="eyebrow">RETENTIONEERING · EXPLORE USER JOURNEYS</div>
<h1>Same conversion. Different paths.</h1>
<div class="scores">{''.join(scores)}</div>
<div class="frame"><iframe src="figure-widget.html" title="Native Retentioneering difference graph, focused on the retry loop"></iframe></div>
<p class="caption"><b>+3 users on each red transition in B.</b> Edge values compare distinct users, B minus A.</p>
<div class="foot">SIX SYNTHETIC USERS · NATIVE WIDGET WITH A SAVED VIEW · REPRODUCE WITH THE QUICK START</div>
</article></body></html>"""
(OUT / "figure.html").write_text(html)
print(f"Figure composition: {OUT / 'figure.html'}")
