"""Public adapter regressions: unavailable inference never becomes evidence."""

import json

import pandas as pd

from retentioneering import Eventstream
from retentioneering.mcp import tools
from retentioneering.mcp._report_session import ReportSession


def stream():
    rows = []
    for arm in ["c", "t"]:
        for i in range(12):
            rows.append([f"{arm}{i}", "open", pd.Timestamp("2026-01-01"), arm])
            for j in range(i % 4):
                rows.append(
                    [
                        f"{arm}{i}",
                        "buy",
                        pd.Timestamp("2026-01-02") + pd.Timedelta(minutes=j),
                        arm,
                    ]
                )
    return Eventstream(
        pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "arm"]),
        {"segment_cols": ["arm"]},
    )


def test_srm_unavailable_is_null_and_metadata_survives_splits():
    s = stream()
    assignment = pd.DataFrame(
        {"arm": ["a", "b", "c"], "platform": ["x"] * 3}, index=["c0", "t0", "c1"]
    )
    result = tools.get_sample_ratio(
        ReportSession(s, assignment=assignment), strata_col="platform"
    )
    assert result["mismatch"] is None
    assert result["inference_status"] == "insufficient_expected_counts"
    assert result["expected_count_min"] == 1
    assert result["p_value_method"] == "pearson_chi_square_asymptotic"
    assert result["sampling_assumption"] == "independent_multinomial"
    row = result["strata"][0]
    assert row["mismatch"] is None
    for key in [
        "inference_status",
        "support_warning",
        "expected_count_min",
        "p_value_method",
        "sampling_assumption",
    ]:
        assert row[key] == result[key]
    json.dumps(result, allow_nan=False)


def test_two_arm_srm_metadata_is_exact():
    result = tools.get_sample_ratio(ReportSession(stream()), "arm")
    assert result["sampling_assumption"] == "independent_bernoulli"
    assert result["p_value_method"] == "exact_binomial_two_sided"
    assert result["inference_status"] == "ok"
    assert result["mismatch"] is False


def test_conversion_dataframe_metadata_survives_mcp():
    s = stream()
    direct = s.get_conversion_rate("open", "buy", diff=("arm", "t", "c"), ci_level=0.95)
    result = tools.get_conversion_rate(
        ReportSession(s), "open", "buy", diff=["arm", "t", "c"], ci_level=0.95
    )
    for key in ["ci_basis", "ci_level", "population_basis", "independence_assumption"]:
        assert result[key] == direct.attrs[key]
    assert result["ci_basis"]["p_value"] == "fisher_exact_two_sided"


def test_ab_metadata_survives_headless_and_report_replies():
    session = ReportSession(stream())
    metrics = [
        {"name": "converted", "metric": "has_event", "metric_args": {"event": "buy"}}
    ]
    result = tools.get_ab_test(session, ["arm", "t", "c"], metrics, period="D")
    row = result["metrics"][0]
    assert row["mde_basis"] == "normal_approximation"
    assert row["ci_basis"] == "clopper_pearson_bonferroni"
    assert row["p_basis"] == "fisher_exact_two_sided"
    assert row["rel_delta_ci_low"] is None and row["rel_delta_ci_high"] is None
    assert row["numeric_unit"] == "share" and row["display_scale"] == 100
    assert all(p["ci_basis"] == row["ci_basis"] for p in row["periods"])
    added = tools.add_ab_test(session, "Exact", ["arm", "t", "c"], metrics)
    assert added["metrics"][0]["mde_basis"] == row["mde_basis"]
    assert added["metrics"][0]["p_basis"] == row["p_basis"]


def test_bonferroni_tail_status_is_not_dropped():
    metrics = [{"name": f"length{i}", "metric": "length"} for i in range(3)]
    result = tools.get_ab_test(
        ReportSession(stream()),
        ["arm", "t", "c"],
        metrics,
        test="bootstrap",
        correction="bonferroni",
    )
    for row in result["metrics"]:
        assert row["delta_ci_adjusted_status"] == "insufficient_tail_resamples"
        assert row["delta_ci_adjusted_basis"] == "path_bootstrap_percentile_approximate"
        assert row["delta_ci_low_adj"] is None and row["delta_ci_high_adj"] is None


def test_typed_group_labels_and_describe_are_distinct():
    s = Eventstream(
        pd.DataFrame(
            {
                "user_id": ["a", "b"],
                "event": ["open", "open"],
                "timestamp": pd.Timestamp("2026-01-01"),
                "arm": pd.Series([1, "1"], dtype=object),
            }
        ),
        {"segment_cols": ["arm"]},
    )
    widget = s.ab_test(("arm", 1, "1"), [{"metric": "length"}])
    payload = json.loads(widget.result)
    assert payload["group1_label"] != payload["group2_label"]
    described = tools.describe(ReportSession(s), {})
    levels = described["segment_levels"]["arm"]
    assert any(type(x) is int and x == 1 for x in levels)
    assert any(type(x) is str and x == "1" for x in levels)
