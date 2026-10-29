"""Fast deterministic regressions and public-API parity for the offline audit."""

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from scipy import stats

from retentioneering import Eventstream
from retentioneering.stats import experiments as inference

MODULE = (
    Path(__file__).resolve().parents[2] / "examples/experiments/coverage_validation.py"
)
spec = importlib.util.spec_from_file_location("coverage_validation", MODULE)
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


@pytest.mark.parametrize("n", [3, 10, 30, 50, 100, 250, 500, 1000])
@pytest.mark.parametrize("level", [0.8, 0.95, 0.99])
def test_wilson_contains_exact_population_boundaries(n, level):
    # At p=0 or p=1 these samples occur with probability ONE. A tiny rounding
    # error is therefore a coverage failure, not merely cosmetic formatting.
    assert inference._wilson(0, n, level)[0] == 0.0
    assert inference._wilson(n, n, level)[1] == 1.0


@pytest.mark.parametrize("successes,n", [(0, 250), (250, 250), (1, 250), (25, 250)])
def test_wilson_agrees_with_independent_score_test_inversion(successes, n):
    lo, hi = inference._wilson(successes, n, 0.95)
    p = successes / n
    z2 = stats.norm.ppf(0.975) ** 2
    # At non-boundary endpoints, the inverted score statistic equals z^2.
    for q in [lo, hi]:
        if 0 < q < 1:
            assert n * (p - q) ** 2 / (q * (1 - q)) == pytest.approx(z2, rel=1e-11)


@pytest.mark.parametrize("scenario", audit.scenarios(), ids=lambda s: s["name"])
def test_public_api_matches_core_with_genuinely_silent_assignments(scenario):
    values, _ = audit.generate(scenario, np.random.default_rng(991214))
    roster, events = [], []
    for arm, matrix in zip(["control", "treatment"], values):
        for i, (numerator, denominator) in enumerate(matrix):
            user = f"{arm}_{i:05}"
            roster.append((user, arm, pd.Timestamp("2026-01-01")))
            if numerator != 0 or (
                scenario["kind"] not in {"binary", "money"} and denominator != 0
            ):
                events.append(
                    (
                        user,
                        "outcome",
                        pd.Timestamp("2026-01-03"),
                        numerator,
                        denominator,
                    )
                )
    # One pre-window event keeps even the all-zero source valid. Every other
    # silent user has no source rows at all, not even a history placeholder.
    events.append(
        (roster[0][0], "history", pd.Timestamp("2025-12-01"), 999999.0, 999999.0)
    )
    stream = Eventstream(
        pd.DataFrame(
            events, columns=["user_id", "event", "timestamp", "amount", "exposure"]
        ),
        schema={"custom_cols": ["amount", "exposure"]},
    )
    assignment = pd.DataFrame(roster, columns=["user_id", "variant", "assigned_at"])
    component = {
        "metric": "event_value_sum",
        "metric_args": {"event": "outcome", "value_col": "amount"},
    }
    if scenario["kind"] == "binary":
        metric = dict(
            name="outcome",
            agg="mean",
            metric="has_event",
            metric_args={"event": "outcome"},
        )
    elif scenario["kind"] == "money":
        metric = dict(name="outcome", agg="mean", **component)
    else:
        metric = dict(
            name="outcome",
            agg="ratio_of_sums",
            numerator=component,
            denominator={
                "metric": "event_value_sum",
                "metric_args": {"event": "outcome", "value_col": "exposure"},
            },
        )
    method = (
        "fisher_exact"
        if scenario["kind"] == "binary"
        else "welch"
        if scenario["kind"] == "money"
        else None
    )
    result = stream.experiment_data(
        assignments=assignment,
        variant_col="variant",
        metrics=[metric],
        observation=dict(
            start="2026-01-02",
            end="2026-01-05",
            assignment_time_col="assigned_at",
            complete=True,
        ),
        event_catalog={"event": ["outcome", "history"]},
        bootstrap=dict(n_resamples=199, random_state=73),
        binary_intervals={"method": "newcombe"}
        if scenario["kind"] == "binary"
        else None,
        tests=dict(methods={"outcome": method}, adjustment="holm") if method else None,
    )
    expected = audit.calculate(inference, scenario, values, 73, 199)
    expected = expected[
        "wilson_newcombe" if scenario["kind"] == "binary" else "percentile"
    ]
    actual = result["summary"][0]
    for name in ["control", "treatment", "absolute_effect", "relative_effect"]:
        for suffix in ["", "_ci_low", "_ci_high"]:
            key = name + suffix
            if np.isfinite(expected[key]):
                assert actual[key] == pytest.approx(expected[key], abs=1e-12)
            else:
                assert actual[key] is None
        assert actual[name + "_status"] == expected[name + "_status"]
    for matrix, diagnostic in zip(values, result["diagnostics"]):
        assert diagnostic["n_assigned"] == len(matrix)
        active = matrix[:, 0] != 0
        if scenario["kind"] not in {"binary", "money"}:
            active |= matrix[:, 1] != 0
        assert diagnostic["n_without_events"] == int((~active).sum())
    if method:
        assert actual["p_value"] == pytest.approx(expected["p_value"])


def test_monte_carlo_accounting_does_not_drop_unavailable_intervals():
    result = audit.run_scenario(
        (
            audit.scenarios()[5],
            5,
            dict(stats_file=inference.__file__, trials=3, seed=14, resamples=19),
        )
    )
    row = next(
        r
        for r in result["results"]
        if r["method"] == "percentile" and r["estimand"] == "absolute_effect"
    )
    assert row["coverage_all"]["n"] == 3
    assert row["coverage_available"]["n"] == 0
    assert row["coverage_available"]["rate"] is None
    assert row["statuses"] == {"degenerate_distribution": 3}
