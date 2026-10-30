"""Small public-API simulation check; not a universal coverage certification.

Unlike the original core-only smoke, this includes event extraction, assignments,
binary boundaries, correlated repeated-order ratios and a session invariant.
"""

import argparse
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
from scipy.stats import binomtest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "notebooks"))
from retentioneering import Eventstream
from experiment_demo import (
    CATALOG,
    OBSERVATION,
    ORDER,
    ORDERS,
    REVENUE,
    SCHEMA,
    make_repeated_sessions,
)


def calculate(stream, **kwargs):
    """Use the public Eventstream API; the adapter only makes rows convenient."""
    payload = stream.experiment_data(**kwargs)
    return SimpleNamespace(summary=pd.DataFrame(payload["summary"]))


def event_case(rng, n, p_control=None, p_treatment=None, price_effect=None):
    """Independent simulation oracle, separate from the tutorial's generator."""
    assignments, rows = [], []
    for arm in ("control", "treatment"):
        for i in range(n):
            user = f"{arm}_{i:05d}"
            assignments.append((user, arm, pd.Timestamp("2026-01-01")))
            rows.append(
                (
                    user,
                    f"{user}_history",
                    "history_view",
                    pd.Timestamp("2026-01-01 12:00"),
                    np.nan,
                    arm,
                )
            )
            if price_effect is None:
                p = p_control if arm == "control" else p_treatment
                prices = [1.0] if rng.random() < p else []
            else:
                orders = 1 + rng.poisson(2.0)
                mean = 80.0 + (price_effect if arm == "treatment" else 0.0)
                # One latent price factor per person makes their order values correlated.
                latent = rng.gamma(4.0, mean / 4.0)
                prices = latent * rng.lognormal(-0.5 * 0.15**2, 0.15, orders)
            for order, price in enumerate(prices):
                rows.append(
                    (
                        user,
                        f"{user}_s{order // 2}",
                        "order_success",
                        pd.Timestamp("2026-01-03") + pd.Timedelta(seconds=order),
                        float(price),
                        arm,
                    )
                )
    frame = pd.DataFrame(
        rows,
        columns=[
            "user_id",
            "session_id",
            "event",
            "timestamp",
            "order_value_gbp",
            "variant",
        ],
    )
    roster = pd.DataFrame(assignments, columns=["user_id", "variant", "assigned_at"])
    return Eventstream(frame, schema=SCHEMA), roster


def binomial_interval(successes, trials):
    if trials == 0:
        return None
    ci = binomtest(successes, trials).proportion_ci()
    return [float(ci.low), float(ci.high)]


def summarize(name, truth, trials, rows, method, null_test=False):
    available = [r for r in rows if r.absolute_effect_status == "ok"]
    covered = sum(
        r.absolute_effect_ci_low <= truth <= r.absolute_effect_ci_high
        for r in available
    )
    tests = [
        float(r.p_value) for r in rows if pd.notna(r.p_value) and np.isfinite(r.p_value)
    ]
    rejected = sum(p < 0.05 for p in tests)
    return {
        "scenario": name,
        "true_effect": truth,
        "trials": trials,
        "method": method,
        "available": len(available),
        "covered": int(covered),
        "coverage_over_all_trials": covered / trials,
        "coverage_conditional_on_available": covered / len(available)
        if available
        else None,
        "coverage_MC_exact_interval": binomial_interval(covered, trials),
        "mean_estimate": float(np.mean([r.absolute_effect for r in rows])),
        "test_available": len(tests),
        "rejections": int(rejected),
        "null_rejection_rate": rejected / len(tests) if null_test and tests else None,
        "null_rejection_MC_exact_interval": binomial_interval(rejected, len(tests))
        if null_test
        else None,
    }


def session_invariance(resamples):
    results = []
    for repeats in [1, 100]:
        frame = make_repeated_sessions(repeats)
        roster = (
            frame[["user_id", "variant"]]
            .drop_duplicates()
            .assign(assigned_at=pd.Timestamp("2026-01-01"))
        )
        result = calculate(
            Eventstream(frame, schema=SCHEMA),
            assignments=roster,
            variant_col="variant",
            observation=OBSERVATION,
            event_catalog=CATALOG,
            bootstrap={"n_resamples": resamples, "random_state": 73},
            metrics=[
                {
                    "name": "visit_success",
                    "agg": "ratio_of_sums",
                    "numerator": ORDERS,
                    "denominator": {
                        "metric": "event_count",
                        "metric_args": {"event": "add_to_cart"},
                    },
                }
            ],
        )
        row = result.summary.iloc[0]
        results.append(
            {
                "repeats": repeats,
                "independent_units": 8,
                "effect": row.absolute_effect,
                "ci_low": row.absolute_effect_ci_low,
                "ci_high": row.absolute_effect_ci_high,
                "ci_status": row.absolute_effect_status,
            }
        )
    assert all(
        np.isclose(results[0][key], results[1][key])
        for key in ["effect", "ci_low", "ci_high"]
    )
    assert all(row["ci_status"] == "ok" for row in results)
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trials", type=int, default=40)
    parser.add_argument("--users-per-arm", type=int, default=250)
    parser.add_argument("--resamples", type=int, default=999)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if min(args.trials, args.users_per_arm, args.resamples) < 2:
        raise ValueError("Use at least two trials, users and resamples")
    rng = np.random.default_rng(892131)
    results = []
    binary = {"method": "newcombe", "confidence_level": 0.95}
    scenarios = [
        ("ordinary_null", 0.1, 0.1),
        ("ordinary_effect", 0.1, 0.13),
        ("rare_null", 0.001, 0.001),
    ]
    for name, pc, pt in scenarios:
        rows = []
        for _ in range(args.trials):
            stream, roster = event_case(rng, args.users_per_arm, pc, pt)
            result = calculate(
                stream,
                assignments=roster,
                variant_col="variant",
                observation=OBSERVATION,
                event_catalog=CATALOG,
                metrics=[{"name": "conversion", **ORDER, "agg": "mean"}],
                binary_intervals=binary,
                tests={"methods": {"conversion": "fisher_exact"}, "adjustment": "holm"},
            )
            rows.append(result.summary.iloc[0])
        results.append(
            summarize(name, pt - pc, args.trials, rows, "newcombe", pc == pt)
        )
        print(name, "finished", flush=True)
    # Deterministic boundary example is reported as such, not counted as Monte Carlo coverage.
    zero_stream, zero_roster = event_case(rng, args.users_per_arm, 0.0, 0.0)
    boundary = calculate(
        zero_stream,
        assignments=zero_roster,
        variant_col="variant",
        observation=OBSERVATION,
        event_catalog=CATALOG,
        metrics=[{"name": "conversion", **ORDER, "agg": "mean"}],
        binary_intervals=binary,
    )
    br = boundary.summary.iloc[0]
    assert br.absolute_effect_ci_low < 0 < br.absolute_effect_ci_high
    for j, effect in enumerate([0.0, 8.0]):
        rows = []
        for trial in range(args.trials):
            stream, roster = event_case(rng, args.users_per_arm, price_effect=effect)
            result = calculate(
                stream,
                assignments=roster,
                variant_col="variant",
                observation=OBSERVATION,
                event_catalog=CATALOG,
                metrics=[
                    {
                        "name": "aov",
                        "agg": "ratio_of_sums",
                        "numerator": REVENUE,
                        "denominator": ORDERS,
                    }
                ],
                bootstrap={
                    "n_resamples": args.resamples,
                    "random_state": 800000 + j * 10000 + trial,
                },
            )
            rows.append(result.summary.iloc[0])
        results.append(
            summarize(
                f"correlated_order_ratio_effect_{effect:g}",
                effect,
                args.trials,
                rows,
                "user_bootstrap_percentile",
            )
        )
        print("correlated_order_ratio", effect, "finished", flush=True)
    report = {
        "seed": 892131,
        "users_per_arm": args.users_per_arm,
        "resamples": args.resamples,
        "binary_design": "independent Bernoulli outcomes, balanced assignment roster; public API",
        "ratio_design": "1+Poisson(2) orders per user; user-specific gamma price factor; independent lognormal order noise with mean1",
        "results": results,
        "all_zero_boundary": {
            "effect": br.absolute_effect,
            "ci_low": br.absolute_effect_ci_low,
            "ci_high": br.absolute_effect_ci_high,
            "method": br.absolute_effect_ci_method,
        },
        "uniform_session_repetition": session_invariance(args.resamples),
        "limitation": "Small unselected smoke, wide Monte Carlo intervals. Not a universal coverage certificate, no familywise or sequential inference validation.",
    }
    target = args.output or Path.cwd() / "experiment_validation.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
