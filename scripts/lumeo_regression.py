#!/usr/bin/env python3
"""Regression check of the A/B tools against the Lumeo drawer experiment.

The Lumeo data (40k users, ~1M events, a smart-cart drawer tested against the
classic cart page) is not shipped with the library: pass the folder that holds
``lumeo_events.csv.gz`` and ``lumeo_ab_groups.csv``. The script runs the
analysis an analyst would run on it and asserts the reference numbers of the
original study within the stated tolerances:

* sample ratio check (no mismatch);
* the scorecard: conversion delta -0.0055 pp +- 0.0005 pp, p 0.98 +- 0.01;
* strata on pre-exposure behaviour clusters (K-means, K=6, on each user's
  events before the first add_to_cart): the "builders" stratum (clusters that
  over-index >= 3x on both collections and wishlist_add) holds 3.5 % +- 0.1 of
  users, its conversion delta is +7.0 pp +- 0.1, p 0.0007 +- 0.0003, and
  the stratum is flagged pre-exposure;
* weekly intention-to-treat rows;
* funnel intervals (when the installed version has them) and the
  ``add_to_cart -> order_success`` conversion interval;
* the drawer loop (three opens in a row) is 0 in control (the row is
  ``insufficient_data``: a metric that cannot occur in one arm has no test).

It prints every number it checks and the wall time of each step. Exit code 1
on any failed check.

Usage:
    uv run python scripts/lumeo_regression.py /path/to/lumeo_data [--out FILE]
"""

from __future__ import annotations

import argparse
import contextlib
import inspect
import io
import os
import sys
import time
import warnings
from pathlib import Path

os.environ.setdefault("RETENTIONEERING_NO_TRACK", "1")

import pandas as pd  # noqa: E402
from retentioneering.eventstream.eventstream import Eventstream  # noqa: E402

DIFF = ("exp_group", "treatment", "control")
BEHAVIOUR = [
    "collections",
    "wishlist_add",
    "pdp_reviews_view",
    "search",
    "catalog_sale",
    "help_center",
    "product_view",
]
BUILDER_PROFILE = ("collections", "wishlist_add")
BUILDER_MIN_OVERINDEX = 3.0
FUNNEL_STEPS = [
    "home",
    "product_view",
    "add_to_cart",
    "cart",
    "checkout_start",
    "order_success",
]
LOOP_PATTERN = "smart_cart_open->smart_cart_open->smart_cart_open"
ORDERS = {"metric": "event_count", "metric_args": {"event": "order_success"}}
METRICS = [
    {
        "name": "conversion",
        "metric": "has_event",
        "metric_args": {"event": "order_success"},
    },
    {"name": "adders", "metric": "has_event", "metric_args": {"event": "add_to_cart"}},
    {"name": "orders_per_user", **ORDERS},
    {
        "name": "revenue_per_user",
        "metric": "column_sum",
        "metric_args": {"column": "order_value_gbp"},
    },
    {
        "name": "aov",
        "metric": "column_sum",
        "metric_args": {"column": "order_value_gbp"},
        "denominator": ORDERS,
    },
]
SHOW = [
    "metric",
    "group1_paths",
    "group2_paths",
    "group1_value",
    "group2_value",
    "delta",
    "delta_ci_low",
    "delta_ci_high",
    "p_value",
    "p_value_adjusted",
    "mde",
    "verdict",
]

pd.set_option("display.width", 220)
pd.set_option("display.max_columns", 40)


class Report:
    def __init__(self) -> None:
        self.failures: list[str] = []
        self.timings: list[tuple[str, float]] = []

    def check(self, label: str, value: float, expected: float, tol: float) -> None:
        ok = abs(value - expected) <= tol
        status = "ok  " if ok else "FAIL"
        print(f"  [{status}] {label}: {value:.6g} (expected {expected} +- {tol})")
        if not ok:
            self.failures.append(label)

    def check_true(self, label: str, ok: bool, detail: str = "") -> None:
        print(
            f"  [{'ok  ' if ok else 'FAIL'}] {label}{': ' + detail if detail else ''}"
        )
        if not ok:
            self.failures.append(label)

    @contextlib.contextmanager
    def step(self, name: str):
        print(f"\n== {name}")
        start = time.perf_counter()
        yield
        elapsed = time.perf_counter() - start
        self.timings.append((name, elapsed))
        print(f"  ({elapsed:.2f} s)")


def load(data_dir: Path) -> tuple[Eventstream, pd.DataFrame]:
    events = pd.read_csv(data_dir / "lumeo_events.csv.gz", parse_dates=["timestamp"])
    groups = pd.read_csv(data_dir / "lumeo_ab_groups.csv", parse_dates=["assigned_at"])
    groups = groups.set_index("user_id").rename(columns={"exp_group": "arm"})
    stream = Eventstream(
        events,
        {
            "path_cols": ["user_id", "session_id"],
            "segment_cols": ["platform", "device", "channel"],
            "custom_cols": ["order_value_gbp", "order_items"],
        },
    )
    stream = stream.add_segment("exp_group", path_values=groups["arm"])
    return stream, groups


def builders(stream: Eventstream) -> tuple[Eventstream, list[str]]:
    """Pre-exposure behaviour clusters, carried to the full stream as `behaviour`."""
    pre = stream.truncate_paths(
        "path_start", [{"pattern": "add_to_cart", "offset": -1}, "path_end"]
    )
    features = [
        {"metric": "event_count_bulk", "metric_args": {"events": BEHAVIOUR}},
        {"metric": "active_days"},
    ]
    pre = pre.add_clusters(
        "behaviour",
        features=features,
        method="kmeans",
        method_args={"n_clusters": 6, "n_init": 4},
        scaler="minmax",
    )
    labels = pre.get_path_segments("behaviour")
    counts = pre.get_metrics(features[:1])
    counts.columns = [str(c).removeprefix("event_count_bulk_") for c in counts.columns]
    counts = counts.reindex(labels.index)
    profile = counts.groupby(labels).mean() / counts.mean()
    chosen = [
        str(level)
        for level, row in profile.iterrows()
        if all(row[c] >= BUILDER_MIN_OVERINDEX for c in BUILDER_PROFILE)
    ]
    print(profile[list(BUILDER_PROFILE)].round(2).to_string())
    merged = labels.where(~labels.isin(chosen), "builders")
    merged.attrs.update(labels.attrs)
    return stream.add_segment("behaviour", path_values=merged), chosen


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--out", type=Path, help="also write the output here")
    args = parser.parse_args()

    buffer = io.StringIO()

    class Tee(io.TextIOBase):
        def write(self, text: str) -> int:
            sys.__stdout__.write(text)
            buffer.write(text)
            return len(text)

    report = Report()
    with contextlib.redirect_stdout(Tee()):
        warnings.simplefilter("default")
        run(args.data_dir, report)
        print("\n== timings")
        for name, elapsed in report.timings:
            print(f"  {name:<52} {elapsed:7.2f} s")
        print(
            "\nRESULT:",
            "all checks passed"
            if not report.failures
            else f"{len(report.failures)} failed: {', '.join(report.failures)}",
        )
    if args.out:
        args.out.write_text(buffer.getvalue(), encoding="utf-8")
    return 1 if report.failures else 0


def run(data_dir: Path, report: Report) -> None:
    with report.step("load"):
        stream, groups = load(data_dir)
        print(f"  {len(stream.df)} events, {stream.df['user_id'].nunique()} users")

    with report.step("sample ratio (assignment table)"):
        srm = stream.get_sample_ratio("exp_group", assignment=groups)
        print(srm["levels"].to_string(index=False))
        print(f"  chi2 {srm['chi2']:.3f}, p {srm['p_value']:.4f}")
        report.check_true("no sample ratio mismatch", not srm["mismatch"])

    with report.step("get_ab_test: 5 metrics, 1000 replicates"):
        card = stream.get_ab_test(DIFF, METRICS, n_bootstrap=1000)
        print(card[SHOW].to_string(index=False))
        conv = card.set_index("metric").loc["conversion"]
        report.check("conversion delta (pp)", conv["delta"] * 100, -0.0055, 0.0005)
        report.check("conversion p", conv["p_value"], 0.98, 0.01)

    with report.step("get_ab_test: 5 metrics, assignment table (ITT)"):
        itt = stream.get_ab_test(DIFF, METRICS, assignment=groups)
        print(itt[SHOW + ["group1_paths_no_events"]].to_string(index=False))

    with report.step('get_ab_test: period="W", assignment table'):
        weekly = stream.get_ab_test(DIFF, METRICS, assignment=groups, period="W")
        conv_w = weekly[weekly["metric"] == "conversion"]
        print(
            conv_w[
                ["period", "period_complete", "group1_paths", "group2_paths"] + SHOW[5:]
            ].to_string(index=False)
        )
        report.check_true(
            "weekly ITT rows cover every assigned path",
            bool(
                (conv_w["group1_paths"] + conv_w["group2_paths"]).max() == len(groups)
            ),
        )

    with report.step("pre-exposure clusters (truncate, K-means K=6)"):
        stream, chosen = builders(stream)
        print(f"  builder clusters: {chosen}")
        report.check_true("at least one builder cluster", bool(chosen), str(chosen))

    with report.step("get_ab_test: strata_col=behaviour, 5 metrics"):
        strata = stream.get_ab_test(DIFF, METRICS, strata_col="behaviour")
        conv_s = strata[strata["metric"] == "conversion"]
        print(
            conv_s[
                ["stratum", "group1_share", "group2_share"]
                + SHOW[3:]
                + ["stratum_vs_rest_p_value", "strata_pre_exposure"]
            ].to_string(index=False)
        )
        row = conv_s.set_index("stratum").loc["builders"]
        n_users = row["group1_paths"] + row["group2_paths"]
        total = conv_s["group1_paths"].sum() + conv_s["group2_paths"].sum()
        report.check("builders share (%)", 100 * n_users / total, 3.5, 0.1)
        report.check("builders conversion delta (pp)", row["delta"] * 100, 7.0, 0.1)
        report.check("builders conversion p", row["p_value"], 0.0007, 0.0003)
        flag = row["strata_pre_exposure"]
        report.check_true(
            "strata flagged pre-exposure", bool(pd.notna(flag) and flag), str(flag)
        )

    with report.step("drawer loop (matches_pattern) by arm"):
        loop = stream.get_ab_test(
            DIFF,
            [
                {
                    "name": "drawer_loop",
                    "metric": "matches_pattern",
                    "metric_args": {"pattern": LOOP_PATTERN},
                }
            ],
        )
        print(loop[SHOW].to_string(index=False))
        value = float(loop["group2_value"].iloc[0])
        report.check_true("loop is 0 in control", value == 0.0, f"{value}")

    with report.step("funnel with intervals"):
        params = inspect.signature(stream.funnel_data).parameters
        if "ci_level" in params:
            funnel = stream.funnel_data(FUNNEL_STEPS, diff=DIFF, ci_level=0.95)
            print(funnel)
        else:
            print("  funnel_data has no ci_level in this version; skipped")

    with report.step("conversion interval add_to_cart -> order_success"):
        rate = stream.get_conversion_rate(
            "add_to_cart", "order_success", diff=DIFF, ci_level=0.95
        )
        print(rate.T.to_string())
        report.check(
            "add_to_cart -> order_success delta (pp)",
            rate["delta"].iloc[0] * 100,
            0.22,
            0.01,
        )
        report.check(
            "add_to_cart -> order_success p", rate["p_value"].iloc[0], 0.847, 0.005
        )


if __name__ == "__main__":
    sys.exit(main())
