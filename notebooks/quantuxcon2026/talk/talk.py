"""The checkout drop-off case behind the talk slides, and the figures drawn
from it.

The pipeline is the one in analysis.ipynb: select the people who reached the
payment screen and never ordered, crop a window after the payment screen,
cluster the windows, name the clusters. The slides show the calls; this module
runs them once, so every slide reads the same results.
"""

from __future__ import annotations

import html
import json
import os
from pathlib import Path

os.environ.setdefault("RETENTIONEERING_NO_TRACK", "1")
import numpy as np
import pandas as pd
import retentioneering as rete

HERE = Path(__file__).resolve().parent
DATA = HERE / "data" / "lumeo_events.csv.gz"

A, B, C = "checkout_start", "checkout_payment", "order_success"
DROPOFF = f"{A}->.*->{B}->[^{C}]*->path_end"
WINDOW = 10
ANDROID = "android 4.12.0"
ANDROID_BUILDS = ["android 4.11.2", "android 4.12.0", "android 4.12.1"]
# The clusters left to right on the slides: everyone who left for good first,
# then everyone who came back, each in checkout order.
CLUSTER_ORDER = [
    "payment · gone",
    "review · gone",
    "error · gone",
    "payment · back",
    "review · back",
    "error · back",
]

# What the clustering sees: how many times each event is in the window, and
# the window's length.
COUNTED = [
    "payment_error",
    "checkout_review",
    "cart",
    "product_view",
    "search",
    "orders_view",
]
FEATURES = [{"metric": "event_count", "metric_args": {"event": e}} for e in COUNTED] + [
    {"metric": "length"}
]
# What a cluster is named after: the share of its paths with a payment error,
# with an order review, and with a new session after the payment screen (one
# starts on home or with an app open).
NAMING_METRICS = [
    {"metric": "has_event", "metric_args": {"event": "payment_error"}, "agg": "mean"},
    {"metric": "has_event", "metric_args": {"event": "checkout_review"}, "agg": "mean"},
    {
        "metric": "has_any_event",
        "metric_args": {"events": ["home", "app_open"]},
        "agg": "mean",
    },
]
# The profile table on the slide: bulk metrics, so the widget groups their
# rows under one header.
OVERVIEW = [
    # Window length, the payment screen included: up to 11, shorter when the
    # path ends sooner.
    {"metric": "length", "agg": "mean"},
    {
        "metric": "has_event_bulk",
        "metric_args": {
            "events": [
                "payment_error",
                "checkout_review",
                "cart",
                "product_view",
                "orders_view",
            ]
        },
        "agg": "mean",
    },
    {
        "metric": "has_any_event",
        "metric_args": {"events": ["home", "app_open"]},
        "agg": "mean",
    },
]


def load() -> rete.Eventstream:
    events = pd.read_csv(DATA, parse_dates=["timestamp"])
    # One build label per row: platform and app version; the web has no app version.
    events["build"] = np.where(
        events.platform.eq("web"),
        "web",
        events.platform.astype(str) + " " + events.app_version.astype(str),
    )
    return rete.Eventstream(
        events,
        schema={"path_cols": ["user_id", "session_id"], "segment_cols": ["build"]},
    )


def mature_before(stream: rete.Eventstream) -> pd.Timestamp:
    """Two weeks before the end of the log: a payment screen before it leaves
    at least 14 days to come back."""
    end = stream.to_dataframe().timestamp.max().normalize() + pd.Timedelta(days=1)
    return end - pd.Timedelta(days=14)


def select_dropoffs(stream: rete.Eventstream) -> rete.Eventstream:
    return stream.filter_paths(
        [{"matches_pattern": DROPOFF}, {"op": "not", "args": [{"has_event": C}]}],
        path_col="user_id",
    )


def crop(
    dropoffs: rete.Eventstream, before: pd.Timestamp, window: int = WINDOW
) -> rete.Eventstream:
    anchor = {"pattern": f"{A}->.*->{B}"}
    cropped = dropoffs.truncate_paths(
        start_anchor=anchor, end_anchor={**anchor, "offset": window}, path_col="user_id"
    )
    return cropped.filter_paths(
        {"metric": "first_event_time", "op": "<", "value": before.timestamp()},
        path_col="user_id",
    )


def cluster_name(profile: pd.Series) -> str:
    """`stage · outcome`: what happened at the payment screen (`error`, an
    order `review`, or nothing after the `payment` screen) and whether the
    person came back in a new session (`back`) or not (`gone`)."""
    if profile["has_event_payment_error_mean"] > 0.5:
        stage = "error"
    elif profile["has_event_checkout_review_mean"] > 0.5:
        stage = "review"
    else:
        stage = "payment"
    came_back = profile["has_any_event_home_or_app_open_mean"] > 0.5
    return f"{stage} · " + ("back" if came_back else "gone")


def cluster(cropped: rete.Eventstream) -> rete.Eventstream:
    clustered = cropped.add_clusters(
        "dropoff",
        features=FEATURES,
        method="kmeans",
        method_args={"n_clusters": 6, "n_init": 10, "random_state": 42},
        scaler="minmax",
        path_col="user_id",
    )
    profile = clustered.segment_overview_data(
        "dropoff", metrics=NAMING_METRICS, path_col="user_id"
    )
    return clustered.rename_segment_levels(
        "dropoff", {c: cluster_name(profile[c]) for c in profile.columns}
    )


def ordered_columns(widget, order: list[str] = CLUSTER_ORDER):
    """Put a segment overview's columns in `order` (the widget itself sorts
    them by name)."""
    result = json.loads(widget.result)
    index = [result["segments"].index(level) for level in order]
    result["segments"] = list(order)
    result["values"] = [[row[i] for i in index] for row in result["values"]]
    widget.result = json.dumps(result)
    return widget


def cluster_paths(clustered: rete.Eventstream, name: str) -> rete.Eventstream:
    """The windows of one cluster."""
    return clustered.filter_paths(
        {
            "metric": "in_segment",
            "metric_args": {"segment_name": "dropoff", "segment_level": name},
            "op": "=",
            "value": True,
        },
        path_col="user_id",
    )


def _window_positions(
    df: pd.DataFrame, events: list[str], width: int = 1100
) -> dict[str, dict[str, int]]:
    """Node positions for a window graph, in columns left to right by the
    event's mean step in the window (the payment screen first, path_end last).
    Events less than a step apart share a column (three at most), spread up
    and down; the
    columns fill a fixed width, so every graph fits at about the same scale."""
    step = df.groupby("user_id").cumcount()
    mean_step = step.groupby(df.event.astype(str)).mean()
    columns: list[list[str]] = []
    for e in sorted(events, key=lambda e: mean_step.get(e, 0)):
        if (
            columns
            and len(columns[-1]) < 3
            and mean_step.get(e, 0) - mean_step.get(columns[-1][0], 0) < 1
        ):
            columns[-1].append(e)
        else:
            columns.append([e])
    columns.append(["path_end"])
    gap = width / (len(columns) - 1)
    positions = {}
    for k, column in enumerate(columns):
        # Neighbouring columns are offset, so labels on one row don't collide.
        shift = 45 if k % 2 else -45
        for i, e in enumerate(column):
            y = (i - (len(column) - 1) / 2) * 120 + (shift if len(columns) > 3 else 0)
            positions[e] = {"x": round(k * gap), "y": round(y)}
    return positions


def _visible_events(clustered: rete.Eventstream) -> list[str]:
    """The events the window graphs show: those in at least a tenth of the
    windows, plus the payment screen."""
    df = clustered.to_dataframe()
    reach = df.groupby("event", observed=True).user_id.nunique()
    return [str(e) for e, n in reach.items() if n >= users(clustered) / 10 or e == B]


def cluster_route(clustered: rete.Eventstream, name: str, limit: int = 7) -> list[str]:
    """A cluster's typical route through its windows: from the payment screen,
    the most frequent next event among the shown ones (path_end included),
    never back to an event already on the route."""
    visible = set(_visible_events(clustered))
    df = cluster_paths(clustered, name).to_dataframe()
    pairs: dict[tuple[str, str], int] = {}
    for _, events in df.groupby("user_id").event:
        # Real neighbours only: a pair across a hidden event is no edge on the
        # graph, and the highlighted route would break there.
        seq = [str(e) for e in events] + ["path_end"]
        for a, b in zip(seq, seq[1:]):
            if a in visible and (b in visible or b == "path_end"):
                pairs[(a, b)] = pairs.get((a, b), 0) + 1
    route = [B]
    while route[-1] != "path_end" and len(route) < limit:
        options = {
            b: n for (a, b), n in pairs.items() if a == route[-1] and b not in route
        }
        if not options:
            break
        route.append(max(options, key=options.get))
    if route[-1] != "path_end":
        route.append("path_end")
    return route


def route_graph(clustered: rete.Eventstream, name: str | None = None):
    """The graph of every window, the same for every cluster (same events,
    same positions, the whole graph in view), one cluster's typical route
    highlighted (a path view); with no `name`, nothing highlighted."""
    df = clustered.to_dataframe()
    visible = _visible_events(clustered)
    hidden = [str(e) for e in df.event.astype(str).unique() if e not in visible]
    view = {
        "hiddenEvents": ["path_start", *hidden],
        "edgeFilter": {"mode": "range", "range": [0, 1]},
        "nodePositions": _window_positions(df, visible),
        "viewport": "fit",
    }
    if name:
        view["focus"] = {"type": "path", "nodes": cluster_route(clustered, name)}
    return clustered.transition_graph(
        path_col="user_id",
        edge_weight="proba_out",
        label_scale=1.6,
        view=view,
        sidebar_open=False,
    )


def whole_paths_graph(dropoffs: rete.Eventstream):
    """The graph before the cut: the whole paths of the same people."""
    # Every edge, as on the graph after the cut (Manual, the full range): the
    # two graphs differ by the cut alone.
    view = {
        "hiddenEvents": ["path_start"],
        "edgeFilter": {"mode": "range", "range": [0, 1]},
        "viewport": "fit",
    }
    return dropoffs.transition_graph(
        path_col="user_id", edge_weight="proba_out", view=view, sidebar_open=False
    )


def users(stream: rete.Eventstream) -> int:
    return stream.to_dataframe().user_id.nunique()


# ── Figures ─────────────────────────────────────────────────────────────────


def _chips(events: list[str], limit: int = 6) -> str:
    shown = events[:limit]
    chips = '<span class="path-arrow">→</span>'.join(
        f'<code class="path-chip{" chip-b" if e == B else ""}{" chip-error" if e == "payment_error" else ""}">'
        f"{html.escape(e)}</code>"
        for e in shown
    )
    more = '<span class="path-more">…</span>' if len(events) > limit else ""
    return f'<div class="path-chips">{chips}{more}</div>'


def pattern_figure() -> str:
    """Toy paths: an order is a success (a green tick), a path with no order
    after the payment screen a failure (a red cross), the paths we keep."""
    rows = [
        ([A, B, "checkout_review", C], True, "ordered"),
        ([A, B, "checkout_review"], False, "failed"),
        ([A, B, "payment_error", "home", "cart"], False, "failed"),
        ([A, B, C, B], True, "watch out when applying the regexp"),
    ]
    html_rows = ""
    for events, ordered, note in rows:
        chips = _chips(events + ["path_end"], limit=10)
        mark = (
            '<span class="pattern-mark ok">✓</span>'
            if ordered
            else '<span class="pattern-mark failed">✗</span>'
        )
        trap = " pattern-trap" if note.startswith("watch out") else ""
        html_rows += f'<div class="pattern-row{trap}">{mark}{chips}<span class="pattern-note">{note}</span></div>'
    return f'<div class="pattern-examples">{html_rows}</div>'


def window_figure(
    clustered: rete.Eventstream,
    dropoffs: rete.Eventstream,
    cluster: str = "error · back",
) -> str:
    """One person's path with both cuts marked: what comes before the payment
    screen and after the 10th event is dimmed and cut away; the frame holds
    what is kept, the payment screen and the 10 events after it (the first
    two, an ellipsis, the last two)."""
    window = clustered.to_dataframe()
    sizes = window.groupby("user_id").size()
    full_windows = sizes.index[sizes == WINDOW + 1]
    in_cluster = window.loc[
        window.dropoff.eq(cluster) & window.user_id.isin(full_windows), "user_id"
    ]
    user = sorted(in_cluster.unique())[0]
    rows = dropoffs.to_dataframe()
    path = rows.loc[rows.user_id.eq(user), "event"].astype(str).tolist()
    start_at = path.index(A)
    payment_at = path.index(B, start_at)
    end_at = payment_at + WINDOW
    after = path[payment_at + 1 : end_at + 1]

    def chip(e: str, cls: str = "") -> str:
        cls += " chip-error" if e == "payment_error" else ""
        return f'<code class="path-chip{cls}">{html.escape(e)}</code>'

    arrow = '<span class="path-arrow">→</span>'
    gap = '<span class="path-more">…</span>'
    cut = '<span class="window-cut" aria-label="cut">✂</span>'
    cut_right = '<span class="window-cut window-cut-right" aria-label="cut">✂</span>'
    before = (
        (gap + arrow if start_at else "")
        + chip(A, " chip-out")
        + arrow
        + (gap + arrow if payment_at - start_at > 1 else "")
    )
    kept = (
        chip(B, " chip-b")
        + arrow
        + arrow.join(chip(e) for e in after[:2])
        + arrow
        + gap
        + arrow
        + arrow.join(chip(e) for e in after[-2:])
    )
    rest = path[end_at + 1 :]
    tail = (
        (arrow + chip(rest[0], " chip-out") + (arrow + gap if len(rest) > 1 else ""))
        if rest
        else ""
    )
    return (
        '<div class="window-figure"><div class="path-chips">'
        f'<span class="window-dropped">{before}</span>{cut}'
        f'<span class="window-box"><span class="path-chips">{kept}</span>'
        f'<span class="window-label">kept: the payment screen + {len(after)} events</span></span>'
        f'{cut_right}<span class="window-dropped">{tail}</span>'
        "</div></div>"
    )


def shares_figure(clustered: rete.Eventstream) -> str:
    """The people who never ordered as one stacked bar: a segment per
    cluster, grouped by what happened at the payment screen, `back` lighter
    than `gone`; the stage shares above, the error group in the alert color."""
    labels = clustered.to_dataframe().groupby("user_id").dropoff.first().astype(str)
    total = len(labels)
    stages = {
        "payment": "Stopped at the payment screen",
        "review": "Left after the order review",
        "error": "Payment error",
    }
    groups = ""
    for stage, title in stages.items():
        parts = ""
        stage_share = labels.str.startswith(stage).mean()
        for outcome in ["gone", "back"]:
            n = int((labels == f"{stage} · {outcome}").sum())
            parts += f'<div class="share-part {outcome}" style="flex:{n}"><span>{outcome} <b>{n / total:.0%}</b></span></div>'
        groups += (
            f'<div class="share-group stage-{stage}" style="flex:{stage_share:.4f}">'
            f'<div class="share-head"><b>{stage_share:.0%}</b> {title}</div>'
            f'<div class="share-bar">{parts}</div></div>'
        )
    return f'<div class="shares"><div class="share-groups">{groups}</div><p class="shares-note">{total} people who had two weeks to come back</p></div>'
