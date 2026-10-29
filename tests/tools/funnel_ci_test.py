"""`funnel_data(ci_level=, correction=)`: intervals, p-values and totals."""

import json

import numpy as np
import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import DiffConfigError, InvalidParameterError
from retentioneering.tools._stats import (
    benjamini_hochberg,
    holm,
    newcombe_ci,
    two_proportion_z,
    wilson_ci,
)

STEPS = ["home", "cart", "buy"]
DIFF = ("grp", "t", "c")


def _stream(n1: int = 200, n2: int = 200, seed: int = 0) -> Eventstream:
    """Two arms with a constant `grp`; treatment adds to cart more often."""
    rng = np.random.default_rng(seed)
    rows = []
    for g, n, p in (("t", n1, 0.6), ("c", n2, 0.5)):
        for i in range(n):
            uid = f"{g}{i}"
            t = pd.Timestamp("2026-01-01")
            rows.append([uid, "home", t, g])
            if rng.random() < p:
                rows.append([uid, "cart", t + pd.Timedelta("1min"), g])
                if rng.random() < 0.5:
                    rows.append([uid, "buy", t + pd.Timedelta("2min"), g])
    df = pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "grp"])
    return Eventstream(df, {"segment_cols": ["grp"]})


def _overlapping_stream() -> Eventstream:
    """User `a` switches platform mid-path, so it falls in both groups."""
    df = pd.DataFrame(
        [
            ["a", "home", "2026-01-01 00:00", "mobile"],
            ["a", "buy", "2026-01-02 00:00", "desktop"],
            ["b", "home", "2026-01-01 00:00", "desktop"],
            ["c", "home", "2026-01-01 00:00", "mobile"],
        ],
        columns=["user_id", "event", "timestamp", "platform"],
    )
    return Eventstream(df, {"segment_cols": ["platform"]})


class TestWithoutCi:
    def test__per_step_keys_are_unchanged(self) -> None:
        r = _stream().funnel_data(steps=["home", "cart"])
        assert set(r["steps"][0]) == {
            "step",
            "unique_paths",
            "conversion_rate",
            "step_conversion_rate",
        }
        assert "ci_level" not in r

    def test__totals_are_returned(self) -> None:
        s = _stream(n1=150, n2=250)
        assert s.funnel_data(steps=["cart"])["total_paths"] == 400
        r = s.funnel_data(steps=["cart"], diff=DIFF)
        assert (r["funnel1_total_paths"], r["funnel2_total_paths"]) == (150, 250)
        assert "delta_conversion_rate_p_value" not in r["steps"][0]

    def test__overlapping_diff_keeps_the_event_level_split(self) -> None:
        r = _overlapping_stream().funnel_data(
            steps=["home"], diff=("platform", "mobile", "desktop")
        )
        # `a` is counted in both groups, as before.
        assert (r["funnel1_total_paths"], r["funnel2_total_paths"]) == (2, 2)

    def test__empty_steps(self) -> None:
        assert _stream().funnel_data(steps=[], ci_level=0.95) == {"steps": []}


class TestSingleCi:
    def test__wilson_on_both_denominators(self) -> None:
        r = _stream().funnel_data(steps=STEPS, ci_level=0.9)
        n = r["total_paths"]
        assert r["ci_level"] == 0.9
        assert r["ci_basis"] == {"rate": "wilson"}
        prev = n
        for s in r["steps"]:
            lo, hi = wilson_ci(s["unique_paths"], n, 0.9)
            assert s["conversion_rate_ci_low"] == pytest.approx(float(lo))
            assert s["conversion_rate_ci_high"] == pytest.approx(float(hi))
            lo, hi = wilson_ci(s["unique_paths"], prev, 0.9)
            assert s["step_conversion_rate_ci_low"] == pytest.approx(float(lo))
            assert s["step_conversion_rate_ci_high"] == pytest.approx(float(hi))
            prev = s["unique_paths"]
        assert "correction" not in r

    def test__step_rate_after_an_empty_step_stays_zero_with_a_nan_interval(
        self,
    ) -> None:
        df = pd.DataFrame(
            [
                ["a", "buy", "2026-01-01 00:00"],
                ["a", "home", "2026-01-01 00:01"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        r = Eventstream(df).funnel_data(steps=["home", "buy", "buy"], ci_level=0.95)
        assert r["steps"][1]["unique_paths"] == 0
        last = r["steps"][2]
        assert last["unique_paths"] == 0
        assert last["step_conversion_rate"] == 0.0
        assert np.isnan(last["step_conversion_rate_ci_low"])
        assert np.isnan(last["step_conversion_rate_ci_high"])
        # the overall rate is defined: 0 of 1 path
        assert last["conversion_rate_ci_low"] == 0.0


class TestDiffCi:
    def test__keys_and_values(self) -> None:
        r = _stream().funnel_data(steps=STEPS, diff=DIFF, ci_level=0.95)
        n1, n2 = r["funnel1_total_paths"], r["funnel2_total_paths"]
        assert r["ci_basis"] == {
            "rate": "wilson",
            "delta": "newcombe",
            "p_value": "pooled_z",
        }
        assert r["correction"] == "none" and r["correction_family"] == "none"
        assert "unassigned_paths" not in r  # overlapping groups raise instead
        s = r["steps"][1]
        k1, k2 = s["funnel1_unique_paths"], s["funnel2_unique_paths"]
        lo, hi = wilson_ci(k1, n1, 0.95)
        assert s["funnel1_conversion_rate_ci_low"] == pytest.approx(float(lo))
        assert s["funnel1_conversion_rate_ci_high"] == pytest.approx(float(hi))
        lo, hi = newcombe_ci(k1, n1, k2, n2, 0.95)
        assert s["delta_conversion_rate_ci_low"] == pytest.approx(float(lo))
        assert s["delta_conversion_rate_ci_high"] == pytest.approx(float(hi))
        assert s["delta_conversion_rate_ci_low"] < s["delta_conversion_rate"]
        assert s["delta_conversion_rate"] < s["delta_conversion_rate_ci_high"]
        p = float(two_proportion_z(k1, n1, k2, n2)[1])
        assert s["delta_conversion_rate_p_value"] == pytest.approx(p)
        # "none" copies the raw p-value
        assert s["delta_conversion_rate_p_value_adjusted"] == pytest.approx(p)
        # the step delta's interval uses the previous step as denominator
        prev = r["steps"][0]
        lo, hi = newcombe_ci(
            s["funnel1_unique_paths"],
            prev["funnel1_unique_paths"],
            s["funnel2_unique_paths"],
            prev["funnel2_unique_paths"],
            0.95,
        )
        assert s["delta_step_conversion_rate_ci_low"] == pytest.approx(float(lo))
        assert s["delta_step_conversion_rate_ci_high"] == pytest.approx(float(hi))

    def test__conditional_step_deltas_carry_no_p_value(self) -> None:
        r = _stream().funnel_data(
            steps=STEPS, diff=DIFF, ci_level=0.95, correction="holm"
        )
        assert [s["step_conditional"] for s in r["steps"]] == [False, True, True]
        for s in r["steps"]:
            assert not [k for k in s if k.startswith("delta_step") and "p_value" in k]
            assert "delta_step_conversion_rate_ci_low" in s

    def test__first_step_reached_by_everyone_has_an_undefined_p(self) -> None:
        r = _stream().funnel_data(
            steps=STEPS, diff=DIFF, ci_level=0.95, correction="holm"
        )
        first = r["steps"][0]
        assert np.isnan(first["delta_conversion_rate_p_value"])
        assert np.isnan(first["delta_conversion_rate_p_value_adjusted"])

    @pytest.mark.parametrize(
        "correction, adjust",
        [("holm", holm), ("bh", benjamini_hochberg), ("bonferroni", None)],
    )
    def test__correction_over_the_steps(self, correction, adjust) -> None:
        r = _stream().funnel_data(
            steps=STEPS, diff=DIFF, ci_level=0.95, correction=correction
        )
        raw = np.array([s["delta_conversion_rate_p_value"] for s in r["steps"]])
        got = np.array(
            [s["delta_conversion_rate_p_value_adjusted"] for s in r["steps"]]
        )
        finite = np.isfinite(raw)
        expected = (
            adjust(raw)
            if adjust is not None
            else np.where(finite, np.minimum(1, raw * finite.sum()), np.nan)
        )
        np.testing.assert_allclose(got, expected, rtol=1e-12)
        assert r["correction"] == correction
        assert r["correction_family"] == "metric=delta_conversion_rate/steps"

    def test__invalid_arguments(self) -> None:
        with pytest.raises(InvalidParameterError, match="ci_level"):
            _stream().funnel_data(steps=["home"], ci_level=95)
        with pytest.raises(InvalidParameterError, match="ci_level"):
            _stream().funnel_data(steps=["home"], ci_level=True)
        with pytest.raises(InvalidParameterError, match="0.5"):
            _stream().funnel_data(steps=["home"], ci_level=0.3)
        with pytest.raises(InvalidParameterError, match="correction"):
            _stream().funnel_data(
                steps=["home"], diff=DIFF, ci_level=0.95, correction="sidak"
            )

    def test__overlapping_groups_raise(self) -> None:
        with pytest.raises(DiffConfigError, match="not constant within 1"):
            _overlapping_stream().funnel_data(
                steps=["home", "buy"],
                diff=("platform", "mobile", "desktop"),
                ci_level=0.95,
            )

    def test__overlapping_id_lists_raise(self) -> None:
        with pytest.raises(DiffConfigError, match="both groups"):
            _stream().funnel_data(
                steps=["home"], diff=(["t0", "t1"], ["t1", "c0"]), ci_level=0.95
            )

    def test__id_lists_are_resolved_per_path(self) -> None:
        r = _stream().funnel_data(
            steps=["home", "cart"],
            diff=([f"t{i}" for i in range(50)], [f"c{i}" for i in range(80)]),
            ci_level=0.95,
        )
        assert (r["funnel1_total_paths"], r["funnel2_total_paths"]) == (50, 80)

    def test__totals_count_paths_that_miss_step_one(self) -> None:
        df = pd.DataFrame(
            [
                ["a", "home", "2026-01-01 00:00", "x"],
                ["a", "buy", "2026-01-01 00:01", "x"],
                ["b", "buy", "2026-01-01 00:00", "y"],
                ["c", "buy", "2026-01-01 00:00", "y"],
            ],
            columns=["user_id", "event", "timestamp", "grp"],
        )
        r = Eventstream(df, {"segment_cols": ["grp"]}).funnel_data(
            steps=["home", "buy"], diff=("grp", "x", "y"), ci_level=0.95
        )
        assert r["funnel2_total_paths"] == 2
        first = r["steps"][0]
        assert first["funnel2_conversion_rate_ci_low"] == 0.0
        assert np.isnan(r["steps"][1]["funnel2_step_conversion_rate_ci_low"])

    def test__nan_is_never_zero_and_result_is_json_serialisable(self) -> None:
        r = _stream().funnel_data(steps=STEPS, diff=DIFF, ci_level=0.99)
        json.dumps(r)  # NaN allowed in the headless dict, but nothing exotic
        assert all(
            isinstance(v, (int, float, str, bool))
            for s in r["steps"]
            for v in s.values()
        )


class TestModeSwitches:
    def test__correction_needs_ci_level(self) -> None:
        with pytest.raises(InvalidParameterError, match="without ci_level"):
            _stream().funnel_data(steps=STEPS, diff=DIFF, correction="holm")
        with pytest.raises(InvalidParameterError, match="without ci_level"):
            _stream().funnel(steps=STEPS, diff=DIFF, correction="holm")
        # the default stays valid
        _stream().funnel_data(steps=STEPS, diff=DIFF, correction="none")

    def test__sidebar_keeps_its_correction_while_intervals_are_off(self) -> None:
        widget = _stream().funnel(
            steps=STEPS, diff=DIFF, ci_level=0.95, correction="holm"
        )
        widget.ci_level = 0.0
        assert widget.error == ""
        assert "correction" not in json.loads(widget.result)

    def test__diff_against_itself_raises(self) -> None:
        with pytest.raises(DiffConfigError, match="itself"):
            _stream().funnel_data(steps=STEPS, diff=("grp", "t", "t"), ci_level=0.95)
