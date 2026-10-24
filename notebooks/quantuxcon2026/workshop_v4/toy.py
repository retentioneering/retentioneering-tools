"""Toy data and figures for the workshop slides.

Everything here is deliberately tiny, so a path can be read at a glance and
the same handful of paths can be shown in every view: sunburst, transition
graph, step matrix, step Sankey.
"""

from __future__ import annotations

import math
from collections import Counter

import pandas as pd

# Variations of the paths from the "Why event-log analysis is challenging"
# slide: group 1 goes A→B→C (plus a drop-off, noisy variants, a loop and a
# shortcut, so path lengths run from 2 to 5), group 2 goes D→B→E. The drop-off
# A→B keeps the A→B→C funnel realistic: not everyone who reaches B gets to C.
TOY_PATHS = [
    ("group_1", "A B C"),
    ("group_1", "A B"),
    ("group_1", "A X B C"),
    ("group_1", "A B B C"),
    ("group_1", "B A C"),
    ("group_1", "A B A B C"),
    ("group_1", "A C"),
    ("group_2", "D B E"),
    ("group_2", "D B E"),
    ("group_2", "D B E"),
]


def toy_events() -> pd.DataFrame:
    """TOY_PATHS as an event log: one row per event, a minute apart."""
    rows = []
    for i, (group, path) in enumerate(TOY_PATHS, start=1):
        start = pd.Timestamp("2026-01-01 10:00") + pd.Timedelta(hours=i)
        for step, event in enumerate(path.split()):
            rows.append(
                {
                    "user_id": f"user_{i:02d}",
                    "event": event,
                    "timestamp": start + pd.Timedelta(minutes=step),
                    "group": group,
                }
            )
    return pd.DataFrame(rows)


# ── Sunburst ────────────────────────────────────────────────────────────────

PALETTE = [
    "#f7c630",  # retentioneering yellow
    "#3b82f6",  # diff blue
    "#f97316",  # heat orange
    "#ef4444",  # diff red
    "#8b5cf6",
    "#10b981",
    "#0ea5e9",
    "#ec4899",
    "#84cc16",
    "#4b5563",  # graph-edge graphite
    "#a855f7",
    "#14b8a6",
]


def _prefix_counts(paths: list[list[str]], depth: int) -> Counter:
    counts: Counter = Counter()
    for path in paths:
        for d in range(1, min(depth, len(path)) + 1):
            counts[tuple(path[:d])] += 1
    return counts


def _arc(cx, cy, r0, r1, a0, a1) -> str:
    """SVG path of an annular sector between angles a0..a1 (radians)."""
    a1 = min(a1, a0 + 2 * math.pi - 1e-4)
    large = 1 if a1 - a0 > math.pi else 0

    def pt(r, a):
        return f"{cx + r * math.sin(a):.2f},{cy - r * math.cos(a):.2f}"

    return (
        f"M{pt(r1, a0)} A{r1},{r1} 0 {large} 1 {pt(r1, a1)} "
        f"L{pt(r0, a1)} A{r0},{r0} 0 {large} 0 {pt(r0, a0)} Z"
    )


def sunburst_svg(
    paths: list[list[str]],
    depth: int = 4,
    size: int = 360,
    labels: bool = True,
    colors: dict[str, str] | None = None,
) -> str:
    """A sunburst of path prefixes: ring k is step k, each arc is a prefix, its
    angle the share of paths that start with it."""
    counts = _prefix_counts(paths, depth)
    total = len(paths)
    events = sorted({e for p in paths for e in p[:depth]})
    colors = dict(colors or {})
    for e in events:
        colors.setdefault(e, PALETTE[len(colors) % len(PALETTE)])

    c = size / 2
    hole = size * 0.12
    ring = (c - hole - 2) / depth
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {size} {size}" '
        f'width="{size}" height="{size}" font-family="Source Sans Pro, sans-serif">'
    ]

    def draw(prefix: tuple, a0: float, a1: float):
        level = len(prefix)
        children = sorted(
            (p for p in counts if len(p) == level + 1 and p[:level] == prefix),
            key=lambda p: (-counts[p], p[-1]),
        )
        a = a0
        for child in children:
            span = (a1 - a0) * counts[child] / (counts[prefix] if prefix else total)
            r0, r1 = hole + ring * level, hole + ring * (level + 1)
            event = child[-1]
            parts.append(
                f'<path d="{_arc(c, c, r0, r1, a, a + span)}" fill="{colors[event]}" '
                'stroke="#fff" stroke-width="1.5"/>'
            )
            # Label only arcs the whole word fits into (~0.6em per character).
            mid, rm = a + span / 2, (r0 + r1) / 2
            font = min(16, ring * 0.45)
            if labels and span * rm > len(event) * font * 0.6 + 6:
                x, y = c + rm * math.sin(mid), c - rm * math.cos(mid)
                parts.append(
                    f'<text x="{x:.1f}" y="{y:.1f}" text-anchor="middle" '
                    f'dominant-baseline="central" font-size="{font:.0f}" '
                    f'font-weight="600" fill="#111827">{event}</text>'
                )
            if level + 1 < depth:
                draw(child, a, a + span)
            a += span

    draw((), 0.0, 2 * math.pi)
    parts.append("</svg>")
    return "".join(parts)


# ── Path beads ──────────────────────────────────────────────────────────────


def event_colors(events) -> dict[str, str]:
    """Event → color, assigned the way sunburst_svg does, so A, B, C, … keep
    their colors from one figure to the next."""
    return {e: PALETTE[i % len(PALETTE)] for i, e in enumerate(sorted(set(events)))}


def _bead_row(events, colors, dropped=()) -> str:
    """One path as beads; events at positions in `dropped` get the dropped class."""
    beads = []
    for i, event in enumerate(events):
        cls = "bead dropped" if i in dropped else "bead"
        beads.append(
            f'<span class="{cls}" style="--bead: {colors[event]}">{event}</span>'
        )
    return "".join(beads)


def paths_figure(before, after=None, strike=False) -> str:
    """Every path of `before` (an Eventstream) as a row of colored beads.

    With `after` (the result of a data processor applied to `before`), whatever
    the processor removed stays in place and is marked: a dropped event fades,
    and a dropped path fades too, or, with `strike=True`, gets crossed out to
    show it failed the condition. Rows are matched on the path id and events on
    the eventstream's `index` column, which processors keep.
    """
    schema = before.schema
    path_col, event_col, index_col = schema.path_col, schema.event_col, schema.index
    df = before.df.sort_values([path_col, index_col])
    colors = event_colors(df[event_col].astype(str))
    kept = kept_paths = None
    if after is not None:
        kept = set(zip(after.df[path_col], after.df[index_col]))
        kept_paths = set(after.df[path_col])

    rows = []
    for path, events in df.groupby(path_col, sort=True):
        names = list(events[event_col].astype(str))
        dropped_path = kept is not None and path not in kept_paths
        if dropped_path:
            dropped = () if strike else range(len(names))
            row_cls = "path-row struck" if strike else "path-row"
        else:
            dropped = {
                i
                for i, index in enumerate(events[index_col])
                if kept is not None and (path, index) not in kept
            }
            row_cls = "path-row"
        rows.append(f'<div class="{row_cls}">{_bead_row(names, colors, dropped)}</div>')
    return f'<div class="path-beads">{"".join(rows)}</div>'


def metrics_figure(stream, metrics: pd.DataFrame) -> str:
    """The `get_metrics` table with each path drawn as beads in the first
    column, so a path and its metric values sit on the same row."""
    schema = stream.schema
    df = stream.df.sort_values([schema.path_col, schema.index])
    colors = event_colors(df[schema.event_col].astype(str))
    paths = {
        path: list(events[schema.event_col].astype(str))
        for path, events in df.groupby(schema.path_col, sort=True)
    }
    head = "".join(f"<th>{col}</th>" for col in metrics.columns)
    body = []
    for path, values in metrics.iterrows():
        cells = "".join(f"<td>{v}</td>" for v in values)
        body.append(
            f'<tr><td class="path-cell">{_bead_row(paths[path], colors)}</td>{cells}</tr>'
        )
    return (
        '<table class="metrics-figure">'
        f"<thead><tr><th>path</th>{head}</tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table>"
    )


def chain_steps_figure(source, result) -> str:
    """The paths after each step of a processor chain, one column per step.

    The steps are read from `result`'s recipe and replayed on `source` one at a
    time, so the figure always matches the chain in the code. Rows stay aligned
    by path; a renamed event is outlined, an event removed by the step stays in
    place crossed out, a dropped path is crossed out (shown as it was before the
    step that dropped it). Events keep their colors from
    the source column.
    """
    from retentioneering.ops import apply_op

    states, labels = [source], ["source"]
    for op in result.recipe()[len(source.recipe()) :]:
        states.append(apply_op(states[-1], op))
        labels.append(op["type"])

    schema = source.schema
    path_col, event_col, index_col = schema.path_col, schema.event_col, schema.index

    def paths(stream):
        df = stream.df.sort_values([path_col, index_col])
        return {
            path: list(zip(events[index_col], events[event_col].astype(str)))
            for path, events in df.groupby(path_col, sort=True)
        }

    tables = [paths(s) for s in states]
    order = list(tables[0])
    colors = event_colors(e for path in tables[0].values() for _, e in path)
    columns = []
    for k, (label, table) in enumerate(zip(labels, tables)):
        rows = []
        for path in order:
            if path in table:
                # Walk the path as it was before this step, so an event the step
                # removed stays in place as a crossed-out ghost; a renamed event
                # gets the "changed" ring.
                now = dict(table[path])
                prev = tables[k - 1][path] if k else table[path]
                beads = []
                for idx, old in prev:
                    if idx not in now:
                        cls, name = "bead removed", old
                    elif now[idx] != old:
                        cls, name = "bead changed", now[idx]
                    else:
                        cls, name = "bead", old
                    colors.setdefault(name, PALETTE[len(colors) % len(PALETTE)])
                    beads.append(
                        f'<span class="{cls}" style="--bead: {colors[name]}">{name}</span>'
                    )
                rows.append(f'<div class="path-row">{"".join(beads)}</div>')
            else:
                last = next(t[path] for t in reversed(tables[:k]) if path in t)
                rows.append(
                    f'<div class="path-row struck">{_bead_row([e for _, e in last], colors)}</div>'
                )
        columns.append(
            f'<div class="chain-col"><div class="chain-label">{label}</div>{"".join(rows)}</div>'
        )
    return f'<div class="chain-steps">{"".join(columns)}</div>'


def grouped_paths_figure(stream, group_col: str) -> str:
    """All paths as beads, one column per value of `group_col`, labelled."""
    schema = stream.schema
    path_col, event_col, index_col = schema.path_col, schema.event_col, schema.index
    df = stream.df[stream.df[event_col].notna()].sort_values([path_col, index_col])
    colors = event_colors(df[event_col].astype(str))
    columns = []
    for group, rows in df.groupby(group_col, sort=True, observed=True):
        paths = [
            f'<div class="path-row">{_bead_row(list(events[event_col].astype(str)), colors)}</div>'
            for _, events in rows.groupby(path_col, sort=True)
        ]
        columns.append(
            f'<div class="group-col"><div class="group-label">{group}</div>{"".join(paths)}</div>'
        )
    return f'<div class="grouped-paths">{"".join(columns)}</div>'


def pairs_figure(stream, spotlight: int = 3) -> str:
    """How a transition graph is built: every adjacent pair of events in every
    path, counted. Only real events are paired (no path_start / path_end).

    Two columns: the paths as beads, and the pairs (bead pairs with a count and
    a bar). The `spotlight` most frequent pairs each get a click (reveal.js
    fragment k): a black frame around the pair in the list and around every
    place it occurs in the paths. Bead geometry is fixed (1.7em beads, 1em
    connectors), so a frame's position is computed here, in em.
    """
    schema = stream.schema
    path_col, event_col, index_col = schema.path_col, schema.event_col, schema.index
    df = stream.df.sort_values([path_col, index_col])
    colors = event_colors(df[event_col].astype(str))
    paths = [
        list(events[event_col].astype(str))
        for _, events in df.groupby(path_col, sort=True)
    ]
    pairs: Counter = Counter()
    for names in paths:
        pairs.update(zip(names, names[1:]))
    ranked = sorted(pairs.items(), key=lambda p: (-p[1], p[0]))
    spot = {pair: k for k, (pair, _) in enumerate(ranked[:spotlight])}

    def frame(k, left_em=None):
        pos = (
            f' style="left: calc({left_em} * 2.7em - 0.3em)"'
            if left_em is not None
            else ""
        )
        return f'<span class="spot-frame fragment current-visible" data-fragment-index="{k}"{pos}></span>'

    path_rows = []
    for names in paths:
        frames = "".join(
            frame(spot[pair], i)
            for i, pair in enumerate(zip(names, names[1:]))
            if pair in spot
        )
        path_rows.append(
            f'<div class="path-row">{_bead_row(names, colors)}{frames}</div>'
        )

    top = ranked[0][1]
    pair_rows = []
    for (a, b), n in ranked:
        mark = frame(spot[(a, b)]) if (a, b) in spot else ""
        pair_rows.append(
            '<div class="pair-row">'
            f'<div class="pair">{_bead_row([a, b], colors)}{mark}</div>'
            f'<div class="pair-count">{n}</div>'
            f'<div class="pair-bar" style="--share: {n / top:.3f}"></div>'
            "</div>"
        )
    return (
        '<div class="pairs-layout">'
        f'<div class="pairs-paths">{"".join(path_rows)}</div>'
        f'<div class="pairs">{"".join(pair_rows)}</div>'
        "</div>"
    )


def edge_weights_figure(
    stream, edges: list[tuple[str, str]], weights: list[str]
) -> str:
    """Edge weights, explained on the first edge of `edges`, one click at a time.

    Left: the paths as beads, with small path_start / path_end dots (the graph
    counts those transitions too). Right: `edges` × `weights` (count,
    unique_paths, proba_out, proba_in), read from `transition_graph_data`, and
    the proba formulas. The clicks walk through the first row:

      count        → frames around every occurrence of the edge
      unique_paths → rings around the paths that contain it
      proba_out    → "n / out" appears; then n (the edge again), then out
                     (every transition out of the source)
      proba_in     → "n / in"; then n, then in (every transition into the target)

    Every highlight is a reveal.js fragment with its index set here; frame
    positions are computed in em from the fixed bead geometry.
    """
    schema = stream.schema
    path_col, event_col, index_col = schema.path_col, schema.event_col, schema.index
    df = stream.df.sort_values([path_col, index_col])
    colors = event_colors(df[event_col].astype(str))
    paths = [
        list(events[event_col].astype(str))
        for _, events in df.groupby(path_col, sort=True)
    ]
    data = {w: stream.transition_graph_data(edge_weight=w) for w in weights}
    counts = stream.transition_graph_data(edge_weight="count")
    src, dst = edges[0]

    # Clicks, in order.
    step = {
        "count": 0,
        "unique_paths": 1,
        "proba_out": 2,
        "out_n": 3,
        "out_total": 4,
        "proba_in": 5,
        "in_n": 6,
        "in_total": 7,
    }

    def frag(k, cls, style="", inner=""):
        style = f' style="{style}"' if style else ""
        return f'<span class="{cls} fragment current-visible" data-fragment-index="{k}"{style}>{inner}</span>'

    # Path tokens: start dot, beads, end dot. Widths in em; 1em connectors.
    START, END = "path_start", "path_end"
    width = {START: 1.0, END: 1.0}
    rows = []
    for names in paths:
        tokens = [START] + names + [END]
        x, lefts = 0.0, []
        for t in tokens:
            lefts.append(x)
            x += width.get(t, 1.7) + 1.0

        def pair_frame(i, k):
            w = width.get(tokens[i], 1.7) + 1.0 + width.get(tokens[i + 1], 1.7) + 0.6
            return frag(
                k, "spot-frame", f"left: {lefts[i] - 0.3:.2f}em; width: {w:.2f}em"
            )

        frames = []
        for i, (a, b) in enumerate(zip(tokens, tokens[1:])):
            if (a, b) == (src, dst):
                for k in (step["count"], step["out_n"], step["in_n"]):
                    frames.append(pair_frame(i, k))
            if a == src:
                frames.append(pair_frame(i, step["out_total"]))
            if b == dst:
                frames.append(pair_frame(i, step["in_total"]))
        if (src, dst) in zip(names, names[1:]):
            frames.append(frag(step["unique_paths"], "row-ring"))
        beads = "".join(
            f'<span class="bead terminal {TERMINAL_CLASS[t]}" title="{t}"></span>'
            if t in (START, END)
            else f'<span class="bead" style="--bead: {colors[t]}">{t}</span>'
            for t in tokens
        )
        rows.append(f'<div class="path-row">{beads}{"".join(frames)}</div>')

    # The table; the first row's cells light up on their clicks.
    cell_steps = {
        "count": [step["count"]],
        "unique_paths": [step["unique_paths"]],
        "proba_out": [step["proba_out"], step["out_n"], step["out_total"]],
        "proba_in": [step["proba_in"], step["in_n"], step["in_total"]],
    }
    head = "".join(f"<th>{w}</th>" for w in weights)
    body = []
    for r, (a, b) in enumerate(edges):
        cells = []
        for w in weights:
            rings = (
                "".join(frag(k, "cell-ring") for k in cell_steps.get(w, []))
                if r == 0
                else ""
            )
            cells.append(
                f'<td><span class="cell">{data[w].loc[a, b]:.2g}{rings}</span></td>'
            )
        body.append(
            f'<tr><td class="edge-cell">{_bead_row([a, b], colors)}</td>{"".join(cells)}</tr>'
        )
    table = (
        '<table class="edge-weights">'
        f"<thead><tr><th>edge</th>{head}</tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table>"
    )

    # Formulas: n / total, each number lit up on its own click.
    n = int(counts.loc[src, dst])
    out_total = int(counts.loc[src].sum())
    in_total = int(counts[dst].sum())

    def formula(name, k, total, k_n, k_total, total_label):
        value = data[name].loc[src, dst]
        return (
            f'<div class="weight-formula fragment" data-fragment-index="{k}">'
            f"<code>{name}</code> = "
            f'<span class="num">{n}{frag(k_n, "num-ring", inner=f"<small>{src}→{dst}</small>")}</span> / '
            f'<span class="num">{total}{frag(k_total, "num-ring", inner=f"<small>{total_label}</small>")}</span>'
            f" ≈ {value:.2f}</div>"
        )

    formulas = formula(
        "proba_out",
        step["proba_out"],
        out_total,
        step["out_n"],
        step["out_total"],
        f"all out of {src}",
    ) + formula(
        "proba_in",
        step["proba_in"],
        in_total,
        step["in_n"],
        step["in_total"],
        f"all into {dst}",
    )
    return (
        '<div class="weights-layout">'
        f'<div class="weights-paths">{"".join(rows)}</div>'
        f'<div class="weights-side">{table}{formulas}</div>'
        "</div>"
    )


def funnel_figure(stream, steps: list[str]) -> str:
    """How a funnel is counted, next to the funnel itself.

    Left: every path as beads; the events the funnel counts (the steps, in
    order, each after the previous one) stay solid, the rest fade. Each row
    carries its event sequence in `data-path` (e.g. "A→B→C"), so the slide can
    point at a path. Right: the funnel bars from `funnel_data`.
    """
    schema = stream.schema
    path_col, event_col, index_col = schema.path_col, schema.event_col, schema.index
    df = stream.df.sort_values([path_col, index_col])
    colors = event_colors(df[event_col].astype(str))
    rows = []
    for _, events in df.groupby(path_col, sort=True):
        names = list(events[event_col].astype(str))
        counted, k = set(), 0
        for i, e in enumerate(names):
            if k < len(steps) and e == steps[k]:
                counted.add(i)
                k += 1
        dim = set(range(len(names))) - counted
        beads = _bead_row(names, colors, dropped=dim)
        rows.append(
            f'<div class="path-row" data-path="{"→".join(names)}">{beads}</div>'
        )

    data = stream.funnel_data(steps=steps)["steps"]
    top = max(d["unique_paths"] for d in data) or 1
    bars = []
    for d in data:
        conv = f"{d['step_conversion_rate']:.0%}"
        bars.append(
            '<div class="funnel-step">'
            f'<span class="bead" style="--bead: {colors[d["step"]]}">{d["step"]}</span>'
            f'<div class="funnel-bar" style="--share: {d["unique_paths"] / top:.3f}"></div>'
            f'<div class="funnel-count">{d["unique_paths"]} <small>{conv}</small></div>'
            "</div>"
        )
    return (
        '<div class="funnel-figure">'
        f'<div class="path-beads">{"".join(rows)}</div>'
        f'<div class="funnel-bars">{"".join(bars)}</div>'
        "</div>"
    )


# ── Step matrix and the trajectory tree ─────────────────────────────────────

START, END = "path_start", "path_end"
# path_start / path_end: small grey circles with a white ▸ / ■ drawn by CSS
# (class "start" / "end") or, in the tree SVG, as a triangle / square.
TERMINAL_CLASS = {START: "start", END: "end"}


def _paths(stream) -> list[list[str]]:
    schema = stream.schema
    df = stream.df.sort_values([schema.path_col, schema.index])
    return [
        list(events[schema.event_col].astype(str))
        for _, events in df.groupby(schema.path_col, sort=True)
    ]


def _token(name: str, colors: dict[str, str]) -> str:
    """An event as a bead; path_start / path_end as small grey circles marked
    ▸ and ■, so the two can't be confused."""
    if name in (START, END):
        return (
            f'<span class="bead terminal {TERMINAL_CLASS[name]}" title="{name}"></span>'
        )
    return f'<span class="bead" style="--bead: {colors[name]}">{name}</span>'


def step_paths_table(
    stream, steps: int, spotlight=(), pattern: str | None = None
) -> str:
    """The paths laid out by step, the way the step matrix counts them.

    Without `pattern`: column 0 is path_start, column k the k-th event, and
    once a path is over every later column holds path_end. With `pattern` (an
    event name): every path is shifted so its first occurrence of that event
    sits in column 0, columns run from -steps to +steps, and path_start fills
    the columns before the path begins; a path without the event drops out of
    the matrix and is shown crossed out at the bottom. A cell is `linked` when
    it and the previous column both hold the real path, so CSS can draw the
    connector; repeated path_start / path_end cells are `ended` (faded)."""
    colors = event_colors(e for p in _paths(stream) for e in p)
    cols = list(range(-steps, steps + 1)) if pattern else list(range(steps + 1))
    head = "".join(f"<th>{k}</th>" for k in cols)
    rows, dropped = [], []
    for names in _paths(stream):
        if pattern and pattern not in names:
            beads = "".join(f"<td>{_token(e, colors)}</td>" for e in names)
            pad = "<td></td>" * (len(cols) - len(names))
            dropped.append(f'<tr class="dropped-row">{beads}{pad}</tr>')
            continue
        offset = names.index(pattern) if pattern else -1  # column 0 ↔ names[offset]
        cells = []
        for k in cols:
            i = offset + k  # index into names; -1 is path_start, len is path_end
            name = START if i < 0 else names[i] if i < len(names) else END
            real = -1 <= i <= len(names)
            prev_real = -1 <= i - 1 <= len(names)
            linked = " linked" if real and prev_real and k != cols[0] else ""
            ended = "" if real else " ended"
            cells.append(
                f'<td class="step-cell{linked}{ended}">{_token(name, colors)}</td>'
            )
        rows.append(f"<tr>{''.join(cells)}</tr>")
    spot = ",".join(map(str, spotlight))
    return (
        f'<table class="step-paths" data-spotlight="{spot}">'
        f"<thead><tr>{head}</tr></thead><tbody>{''.join(rows + dropped)}</tbody></table>"
    )


def step_matrix_table(
    stream, steps: int, spotlight=(), pattern: str | None = None
) -> str:
    """`step_matrix_data` as a table (centered on `pattern` if given), cells
    tinted like the widget's heatmap (its orange, alpha = the share)."""
    data = stream.step_matrix_data(max_steps=steps, path_pattern=pattern)
    if isinstance(data, tuple):  # one block per anchor event; one anchor here
        data = data[0]
    colors = event_colors(e for p in _paths(stream) for e in p)
    head = "".join(f"<th>{k}</th>" for k in data.columns)
    rows = []
    for event, values in data.iterrows():
        cells = "".join(
            f'<td style="--heat: {v:.2f}">{v:.2g}</td>' if v > 0.005 else "<td>·</td>"
            for v in values
        )
        rows.append(
            f'<tr><th class="event-cell">{_token(str(event), colors)}</th>{cells}</tr>'
        )
    spot = ",".join(map(str, spotlight))
    return (
        f'<table class="step-matrix" data-spotlight="{spot}">'
        f"<thead><tr><th></th>{head}</tr></thead><tbody>{''.join(rows)}</tbody></table>"
    )


def prefix_tree_svg(stream, steps: int, spotlight=(), size=(600, 470)) -> str:
    """The trajectory tree of the paths as SVG: identical beginnings share a
    branch, every node carries the number of paths through it. Nodes of one
    step sit in one column; `spotlight` steps get a black frame around their
    column on their own click (reveal.js fragment k). path_start and path_end
    are small grey circles marked ▸ and ■."""
    paths = _paths(stream)
    colors = event_colors(e for p in paths for e in p)
    tree: dict[tuple, int] = {}
    for names in paths:
        tokens = list(names) + [END]
        for d in range(1, len(tokens) + 1):
            prefix = tuple(tokens[:d])
            tree[prefix] = tree.get(prefix, 0) + 1

    # Leaves in depth-first order get one row each; inner nodes sit at the
    # mean row of their leaves.
    def children(prefix):
        return sorted(
            (
                p
                for p in tree
                if len(p) == len(prefix) + 1 and p[: len(prefix)] == prefix
            ),
            key=lambda p: (-tree[p], p[-1]),
        )

    rows: dict[tuple, float] = {}
    order: list[tuple] = []

    def place(prefix):
        kids = children(prefix)
        if not kids:
            rows[prefix] = len(order)
            order.append(prefix)
        else:
            for kid in kids:
                place(kid)
            rows[prefix] = sum(rows[k] for k in kids) / len(kids)

    place(())
    width, height = size
    top = 24  # room for the step numbers
    col_w = width / (steps + 1)
    row_h = (height - top) / max(len(order), 1)
    r = min(col_w, row_h) * 0.36

    def xy(prefix):
        return (len(prefix) + 0.5) * col_w, top + (rows[prefix] + 0.5) * row_h

    parts = [
        f'<svg class="prefix-tree" viewBox="0 0 {width} {height}" width="{width}" height="{height}" '
        'font-family="Source Sans Pro, sans-serif">'
    ]
    for k in range(steps + 1):
        parts.append(
            f'<text class="step" x="{(k + 0.5) * col_w:.1f}" y="12">{k}</text>'
        )
    for idx, k in enumerate(spotlight):
        x = k * col_w + 3
        parts.append(
            f'<rect class="step-col spot fragment current-visible" data-fragment-index="{idx}" '
            f'x="{x:.1f}" y="{top - 4}" width="{col_w - 6:.1f}" height="{height - top + 2}" rx="8"/>'
        )
    for prefix in tree:
        (x1, y1), (x2, y2) = xy(prefix[:-1]), xy(prefix)
        xm = (x1 + x2) / 2
        parts.append(
            f'<path class="edge" d="M{x1:.1f},{y1:.1f} H{xm:.1f} V{y2:.1f} H{x2:.1f}"/>'
        )

    def terminal(x, y, name):
        rc = r * 0.65
        if name == START:  # ▸
            a = rc * 0.45
            mark = (
                f'<polygon class="terminal-mark" points="{x - a * 0.6:.1f},{y - a:.1f} '
                f'{x - a * 0.6:.1f},{y + a:.1f} {x + a:.1f},{y:.1f}"/>'
            )
        else:  # ■
            a = rc * 0.4
            mark = f'<rect class="terminal-mark" x="{x - a:.1f}" y="{y - a:.1f}" width="{2 * a:.1f}" height="{2 * a:.1f}"/>'
        return (
            f'<circle class="terminal" cx="{x:.1f}" cy="{y:.1f}" r="{rc:.1f}"/>{mark}'
        )

    x0, y0 = xy(())
    parts.append(terminal(x0, y0, START))
    for prefix, n in tree.items():
        x, y = xy(prefix)
        name = prefix[-1]
        # The count sits above-right of the node, clear of the outgoing branch.
        if name == END:
            parts.append(terminal(x, y, END))
            parts.append(
                f'<text class="count" x="{x + r * 0.8:.1f}" y="{y - r * 0.7:.1f}">{n}</text>'
            )
            continue
        parts.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r:.1f}" fill="{colors[name]}"/>'
        )
        parts.append(f'<text class="label" x="{x:.1f}" y="{y:.1f}">{name}</text>')
        parts.append(
            f'<text class="count" x="{x + r * 0.9:.1f}" y="{y - r * 0.9:.1f}">{n}</text>'
        )
    parts.append("</svg>")
    return "".join(parts)


def step_layout(left: str, right: str) -> str:
    return f'<div class="step-layout"><div>{left}</div><div>{right}</div></div>'


# ── Trees on a real dataset ─────────────────────────────────────────────────


def _prefix_tree(paths: list[list[str]], depth: int) -> dict[tuple, int]:
    """{prefix: number of paths starting with it}, for prefixes up to `depth`."""
    tree: dict[tuple, int] = {}
    for path in paths:
        for d in range(1, min(depth, len(path)) + 1):
            tree[tuple(path[:d])] = tree.get(tuple(path[:d]), 0) + 1
    return tree


def _children(tree: dict[tuple, int], prefix: tuple) -> list[tuple]:
    return sorted(
        (p for p in tree if len(p) == len(prefix) + 1 and p[: len(prefix)] == prefix),
        key=lambda p: (-tree[p], p[-1]),
    )


def icicle_svg(paths: list[list[str]], depth: int, size=(360, 360), colors=None) -> str:
    """An icicle: the sunburst unrolled. Column k is step k, each rectangle a
    path prefix, its height the share of paths that start with it."""
    tree = _prefix_tree(paths, depth)
    colors = colors or event_colors(e for p in paths for e in p[:depth])
    width, height = size
    col_w = width / depth
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="{width}" height="{height}">'
    ]

    def draw(prefix: tuple, y0: float, y1: float):
        total = tree[prefix] if prefix else len(paths)
        y = y0
        for child in _children(tree, prefix):
            h = (y1 - y0) * tree[child] / total
            x = (len(child) - 1) * col_w
            parts.append(
                f'<rect x="{x:.1f}" y="{y:.2f}" width="{col_w - 1:.1f}" height="{max(h - 0.4, 0.1):.2f}" '
                f'fill="{colors[child[-1]]}"/>'
            )
            if len(child) < depth:
                draw(child, y, y + h)
            y += h

    draw((), 0.0, float(height))
    parts.append("</svg>")
    return "".join(parts)


def tree_svg(paths: list[list[str]], depth: int, size=(360, 360), colors=None) -> str:
    """A node-link trajectory tree: one leaf row per distinct prefix, so on a
    real dataset it fans out into a near-solid wedge."""
    tree = _prefix_tree(paths, depth)
    colors = colors or event_colors(e for p in paths for e in p[:depth])
    width, height = size
    col_w = (width - 8) / depth
    rows: dict[tuple, float] = {}
    leaves: list[tuple] = []

    def place(prefix):
        kids = _children(tree, prefix)
        if not kids:
            rows[prefix] = len(leaves)
            leaves.append(prefix)
            return
        for kid in kids:
            place(kid)
        rows[prefix] = sum(rows[k] for k in kids) / len(kids)

    place(())
    row_h = height / max(len(leaves), 1)

    def xy(prefix):
        return 4 + len(prefix) * col_w, (rows[prefix] + 0.5) * row_h

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="{width}" height="{height}">'
    ]
    for prefix in tree:
        (x1, y1), (x2, y2) = xy(prefix[:-1]), xy(prefix)
        parts.append(
            f'<line x1="{x1:.1f}" y1="{y1:.2f}" x2="{x2:.1f}" y2="{y2:.2f}" '
            f'stroke="{colors[prefix[-1]]}" stroke-width="0.6" stroke-opacity="0.8"/>'
        )
    parts.append("</svg>")
    return "".join(parts)


def branching_figure(stream, depth: int = 5, colors=None, size: int = 330) -> str:
    """Tree, sunburst and icicle of the first `depth` steps of every path in
    `stream`, side by side, with the numbers that make the point: how many
    paths are unique, and how much of a typical path the `depth` levels show.
    `colors` maps events to colors (event_colors by default)."""
    paths = _paths(stream)
    unique = len({tuple(p) for p in paths})
    shown = sorted(min(depth, len(p)) / len(p) for p in paths)[len(paths) // 2]
    colors = colors or event_colors(e for p in paths for e in p[:depth])
    size = (size, size)
    n_paths = len(paths)
    n_events = stream.df[stream.schema.event_col].nunique()
    figures = [
        ("tree", tree_svg(paths, depth, size, colors)),
        (
            "sunburst",
            sunburst_svg(paths, depth=depth, size=size[0], labels=False, colors=colors),
        ),
        ("icicle", icicle_svg(paths, depth, size, colors)),
    ]
    cells = "".join(
        f"<figure>{svg}<figcaption>{name}</figcaption></figure>"
        for name, svg in figures
    )
    return (
        f'<div class="branching">{cells}</div>'
        f'<p class="branching-note">{n_paths} paths over {n_events} event types, '
        f"<strong>{unique}</strong> of them unique. The first {depth} levels show "
        f"<strong>{shown:.0%}</strong> of a typical path.</p>"
    )


# ── Motivating example: Taskboard journeys ───────────────────────────────────
# Events are colored by where in the app they happen (taskboard.EVENT_GROUP),
# visits by their own kind.

STAGE_COLORS = {
    # Event groups: where in the app an event happens (taskboard.EVENT_GROUP).
    "getting_started": "#f7c630",  # retentioneering yellow
    "tasks": "#3b82f6",
    "discussion": "#f97316",
    "team": "#10b981",
    "power": "#8b5cf6",
    "billing": "#ec4899",
    # Visits: the kinds the clustering finds.
    "onboarding": "#f7c630",
    "focus": "#3b82f6",
    "sync": "#10b981",
    "review": "#8b5cf6",
    "planning": "#06b6d4",
    "quick": "#94a3b8",
    "checkout": "#ec4899",
    "checkout_failed": "#ec4899",
}


def taskboard_colors(events) -> dict[str, str]:
    """Event or visit name → the color of its stage; anything else is grey."""
    from taskboard import EVENT_GROUP

    return {e: STAGE_COLORS.get(EVENT_GROUP.get(e, e), "#a8a29e") for e in set(events)}


def _chip(event: str, colors: dict[str, str]) -> str:
    return f'<span class="chip" style="--chip: {colors[event]}">{event}</span>'


def _journeys_table(rows: list[tuple[str, list[str | None], str]], colors) -> str:
    def cell(event: str | None) -> str:
        return f"<td>{_chip(event, colors) if event else ''}</td>"

    body = "".join(
        f'<tr><th>{who}</th>{"".join(cell(e) for e in cells)}<td class="more">{more}</td></tr>'
        for who, cells, more in rows
    )
    return f'<table class="journeys"><tbody>{body}</tbody></table>'


def distinct_colors(events) -> dict[str, str]:
    """A color of its own for every event: hues spread around the wheel (by the
    golden angle, so neighbors in sorted order differ), lightness alternating."""
    names = sorted(set(events))
    return {
        e: f"hsl({round(i * 137.5) % 360}, 70%, {48 if i % 2 else 62}%)"
        for i, e in enumerate(names)
    }


def first_visits_figure(stream, users: list[str], colors=None) -> str:
    """The first visit of each user as event chips, followed by how much of
    the user's path is left."""
    df = stream.df
    colors = colors or taskboard_colors(df["event"])
    rows, rest = [], []
    for user in users:
        events = df[df["user_id"] == user].sort_values("index")
        first = events[events["session_id"] == events["session_id"].iloc[0]]
        rows.append(list(first["event"]))
        rest.append(
            f"+ {events['session_id'].nunique() - 1} visits, {len(events) - len(first)} events"
        )
    body = "".join(
        f'<div class="journey"><span class="who">{who}</span>'
        f'{"".join(_chip(e, colors) for e in row)}<span class="more">{more}</span></div>'
        for who, row, more in zip(users, rows, rest)
    )
    return f'<div class="journeys">{body}</div>'


def visit_paths_figure(visits, users: list[str]) -> str:
    """Each user's whole path as one chip per visit."""
    df = visits.df
    colors = taskboard_colors(df["event"])
    rows = []
    for user in users:
        events = list(df[df["user_id"] == user].sort_values("index")["event"])
        rows.append((user, events, ""))
    width = max(len(r[1]) for r in rows)
    rows = [
        (who, cells + [None] * (width - len(cells)), more) for who, cells, more in rows
    ]
    return _journeys_table(rows, colors)


# Where each event group sits on the transition graph's canvas: (column, row).
# Groups follow a user's way through the app left to right; power features sit
# under discussion, the shortest column.
STAGE_GRID = {
    "getting_started": (0, 0),
    "tasks": (1, 0),
    "discussion": (2, 0),
    "team": (3, 0),
    "billing": (4, 0),
    "power": (2, 1),
}


# Visits, one node per kind, in a compact zigzag (x, y in layout units).
VISIT_LAYOUT = {
    "path_start": (0, 0),
    "onboarding": (1, -0.7),
    "quick": (1.5, 0.9),  # short visits: notifications, a task or two
    "focus": (2, 0),
    "planning": (2.2, -1.5),
    "sync": (3, -0.7),
    "review": (4, 0),
    "checkout": (4, -1.4),
    "checkout_failed": (4.6, -0.7),
    "path_end": (5, 0.3),
}


def stage_positions(events) -> dict[str, dict[str, float]]:
    """Node positions that group events by where in the app they happen (a
    stack per group, in the order of taskboard.EVENT_GROUP), or, for visits,
    VISIT_LAYOUT."""
    from taskboard import EVENT_GROUP

    events = set(events)
    if events <= set(VISIT_LAYOUT):  # visits: one node per stage
        unit = 120
        events |= {"path_start", "path_end"}  # the graph adds them; a node
        # without a position makes the widget lay out the whole graph itself
        return {
            e: {"x": VISIT_LAYOUT[e][0] * unit, "y": VISIT_LAYOUT[e][1] * unit}
            for e in events
        }
    col_w, row_h = 300, 70
    members = {
        stage: [
            e
            for e in [*EVENT_GROUP, stage]
            if e in events and EVENT_GROUP.get(e, e) == stage
        ]
        for stage in STAGE_GRID
    }
    tallest = max(len(m) for (stage, m) in members.items() if STAGE_GRID[stage][1] == 0)
    positions = {
        "path_start": {"x": -col_w, "y": 0},
        "path_end": {"x": 5 * col_w, "y": 0},
    }
    for stage, names in members.items():
        col, row = STAGE_GRID[stage]
        if row == 0:
            ys = [(i - (len(names) - 1) / 2) * row_h for i in range(len(names))]
        else:
            ys = [
                (max((tallest + 1) / 2 + 0.6, 3) + i) * row_h for i in range(len(names))
            ]
        for name, y in zip(names, ys):
            positions[name] = {"x": col * col_w, "y": y}
    return positions


def stage_graph(stream, **kwargs):
    """`stream.transition_graph(**kwargs)` with its nodes grouped by stage, plus
    a hidden {stage: [events]} map for deck.html, which frames each group."""
    import json

    from IPython.display import HTML, display
    from taskboard import EVENT_GROUP

    events = set(stream.df[stream.schema.event_col].astype(str))
    groups: dict[str, list[str]] = {}
    for e in sorted(events):
        groups.setdefault(EVENT_GROUP.get(e, e), []).append(e)
    widget = stream.transition_graph(**kwargs)
    widget.node_positions = json.dumps(stage_positions(events))
    display(
        HTML(
            f'<script type="application/json" class="stage-groups">{json.dumps(groups)}</script>'
        )
    )
    return widget


# ── Part 4: the A/B test ─────────────────────────────────────────────────────


def ab_results_table(
    stream,
    events: dict[str, str],
    primary: str | None = None,
    group_col: str = "ab_group",
) -> str:
    """The share of users who reached each event, by A/B group, with the
    difference and a two-proportion z-test p-value. `events` maps an event to
    the metric name shown for it, in the order of the rows; the `primary`
    event's row is marked as the test's primary metric."""
    from math import erfc, sqrt

    df = stream.df
    path_col, event_col = stream.schema.path_col, stream.schema.event_col
    groups = df.groupby(path_col)[group_col].first().astype(str)
    n = groups.value_counts()
    rows = []
    for event, label in events.items():
        reached = set(df.loc[df[event_col].astype(str) == event, path_col])
        hits = groups[groups.index.isin(reached)].value_counts()
        p_c, p_t = (
            hits.get("control", 0) / n["control"],
            hits.get("test", 0) / n["test"],
        )
        p = (hits.sum()) / n.sum()
        se = sqrt(p * (1 - p) * (1 / n["control"] + 1 / n["test"]))
        pvalue = erfc(abs(p_t - p_c) / se / sqrt(2))
        rows.append(
            f'<tr class="{"primary" if event == primary else ""}">'
            f"<th>{label}</th><td>{p_c:.1%}</td><td>{p_t:.1%}</td>"
            f"<td>{(p_t - p_c) * 100:+.1f} pp</td><td>{pvalue:.2f}</td></tr>"
        )
    head = (
        f"<tr><th>Metric</th><th>Control<br><small>{n['control']:,} users</small></th>"
        f"<th>Test<br><small>{n['test']:,} users</small></th><th>Difference</th><th>p-value</th></tr>"
    )
    return f'<table class="ab-results"><thead>{head}</thead><tbody>{"".join(rows)}</tbody></table>'


def _mock_window(caption: str, active: str, body: str) -> str:
    """A pseudo-screenshot of the Taskboard project page, `active` tab open."""
    tabs = "".join(
        f'<span class="{"on" if tab == active else ""}">{tab}</span>'
        for tab in ("Board", "Timeline")
    )
    return (
        f'<figure class="mock"><div class="mock-window">'
        f'<div class="mock-bar"><i></i><i></i><i></i><b>Website redesign</b></div>'
        f'<div class="mock-tabs">{tabs}</div>{body}</div>'
        f"<figcaption>{caption}</figcaption></figure>"
    )


# (task, start day, length in days, assignee initials or "")
MOCK_TASKS = [
    ("Collect references", 0, 2, "AK"),
    ("Draft sitemap", 1, 2, ""),
    ("Homepage wireframe", 3, 3, "MG"),
    ("Pick fonts", 5, 1, ""),
]


def _mock_gantt(extra: str = "", bar_class: str = "bar") -> str:
    """The timeline: a row per task, a bar per task over a week; `extra` is
    laid over the rows (click marks, a task panel)."""
    days = "".join(
        f"<span>{d}</span>" for d in ("Mo", "Tu", "We", "Th", "Fr", "Mo", "Tu")
    )
    rows = "".join(
        f'<div class="row"><span class="task">{task}</span><span class="track">'
        f'<span class="{bar_class}" style="--start: {start}; --len: {length}">'
        + (f"<em>{who}</em>" if who else '<em class="empty">+</em>')
        + "</span></span></div>"
        for task, start, length, who in MOCK_TASKS
    )
    return (
        f'<div class="mock-gantt"><div class="row head"><span class="task"></span>'
        f'<span class="track days">{days}</span></div>{rows}{extra}</div>'
    )


def timeline_mockups() -> str:
    """Two pseudo-screenshots of a new project, side by side: control opens on
    the board, test opens on Timeline with the "Plan with your team" prompt."""
    columns = {
        "To do": ["Collect references", "Draft sitemap", "Pick fonts"],
        "In progress": ["Homepage wireframe"],
        "Done": ["Kick-off call"],
    }
    board = (
        '<div class="mock-board">'
        + "".join(
            f'<div class="col"><h6>{name}</h6>'
            + "".join(f'<div class="card">{card}</div>' for card in cards)
            + "</div>"
            for name, cards in columns.items()
        )
        + "</div>"
    )
    timeline = (
        '<div class="mock-prompt"><strong>Plan with your team</strong>'
        " Set due dates and assign tasks to teammates <u>Invite</u></div>"
        + _mock_gantt()
    )
    return (
        '<div class="mockups">'
        + _mock_window("Control: a new project opens on the board", "Board", board)
        + _mock_window(
            "Test: it opens on Timeline, with a prompt", "Timeline", timeline
        )
        + "</div>"
    )


def timeline_clicks_mockups(n_clicks: int = 160, seed: int = 4) -> str:
    """Before and after, side by side. Before: where planners click on the
    timeline, as click tracking with coordinates would show it; most clicks
    land on the ends of the task bars, which do nothing. After: a bar's ends
    drag to change its dates, and a click opens the task. The click marks are
    illustrative, not data."""
    import random

    rng = random.Random(seed)
    marks = []
    for _ in range(n_clicks):
        r = rng.random()
        if r < 0.7:
            # On a bar's left or right end: users try to grab it, to move
            # the start or stretch the deadline.
            row = rng.randrange(len(MOCK_TASKS))
            _, start, length, _ = MOCK_TASKS[row]
            end = start if rng.random() < 0.5 else start + length
            x = (end + rng.gauss(0, 0.09)) / 7
            y = row + 0.5 + rng.gauss(0, 0.08)
        elif r < 0.88:  # on a bar, along it: to open the task
            row = rng.randrange(len(MOCK_TASKS))
            _, start, length, _ = MOCK_TASKS[row]
            x = (start + rng.uniform(0.15, 0.85) * length) / 7
            y = row + 0.5 + rng.gauss(0, 0.08)
        else:  # anywhere else on the timeline
            x, y = rng.random(), rng.random() * len(MOCK_TASKS)
        marks.append(f'<i style="--x: {x:.3f}; --y: {y:.3f}"></i>')
    clicks = f'<div class="clicks">{"".join(marks)}</div>'
    task, start, length, who = MOCK_TASKS[2]
    panel = (
        '<div class="task-panel"><b>Homepage wireframe</b>'
        "<span>Assignee: MG</span><span>Th – Sa</span><span>Depends on: Draft sitemap</span></div>"
    )
    return (
        '<div class="mockups">'
        + _mock_window(
            "Before: clicks pile up on the ends of the bars, which do nothing",
            "Timeline",
            _mock_gantt(clicks),
        )
        + _mock_window(
            "After: drag an end to change the dates; click to open the task",
            "Timeline",
            _mock_gantt(panel, "bar live"),
        )
        + "</div>"
    )


def app_mockups() -> str:
    """Two pseudo-screenshots of Taskboard for the introduction: the board of
    a project two people work on, and a task open with its comments, as a
    teammate joins."""
    team = '<span class="mock-team"><em>AK</em><em>MG</em><u>Invite</u></span>'
    columns = {
        "To do": [
            ("Pick fonts", "", "Fri", 0),
            ("Draft sitemap", "AK", "Thu", 2),
            ("Write homepage copy", "", "Mon", 0),
        ],
        "In progress": [
            ("Homepage wireframe", "MG", "Wed", 3),
            ("Logo variants", "AK", "Thu", 1),
        ],
        "Done": [
            ("Kick-off call", "AK", "", 1),
            ("Collect references", "MG", "", 0),
            ("Agree on the brief", "AK", "", 4),
        ],
    }

    def card(title, who, due, comments):
        meta = (f"<span>{due}</span>" if due else "") + (
            f"<span>&#128172; {comments}</span>" if comments else ""
        )
        avatar = f"<em>{who}</em>" if who else '<em class="empty">+</em>'
        return f'<div class="card">{title}<div class="meta">{meta}{avatar}</div></div>'

    board = (
        '<div class="mock-board">'
        + "".join(
            f'<div class="col"><h6>{name}</h6>'
            + "".join(card(*c) for c in cards)
            + "</div>"
            for name, cards in columns.items()
        )
        + "</div>"
    )
    comments = [
        ("AK", "@MG can you take the wireframe? The sitemap is almost done."),
        ("MG", "On it, will share a draft on Wednesday."),
        ("AK", "Great, I'll add Jamie for the copy."),
    ]
    task = (
        '<div class="mock-task"><b>Homepage wireframe</b>'
        '<div class="fields"><span>Assignee <em>MG</em></span><span>Due Wed</span>'
        "<span>Depends on Draft sitemap</span></div>"
        + "".join(
            f'<div class="comment"><em>{w}</em><p>{t}</p></div>' for w, t in comments
        )
        + '<div class="toast"><em>JS</em> Jamie joined Website redesign</div></div>'
    )
    return (
        '<div class="mockups">'
        + _mock_window("The board: tasks, assignees, due dates", "Board", team + board)
        + _mock_window(
            "A task: comments and mentions as the team grows", "Board", team + task
        )
        + "</div>"
    )
