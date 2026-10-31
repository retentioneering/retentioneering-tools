"""``get_ab_transitions_power`` / ``get_ab_steps_power``: how much data a screen needs
(``tools/ab_power.py``).

At ``scale=1`` every projected number must be the getter's (``mde``, the tested rows,
the family size); the MDE of a z-test or Welch row must scale as ``1 / sqrt(scale)``
at a fixed level; support must not be read as inference (a constant row never becomes
testable, a row of unknown spread is NA); ``scale_needed`` must bring the MDE to
``target_delta`` with the family projected at that size, and keep it there; and the
curve must summarise the rows.
"""

from __future__ import annotations

import json
import math
import pathlib
import warnings

import numpy as np
import pandas as pd
import pytest

from retentioneering import datasets
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError
from retentioneering.tools import _stats
from retentioneering.tools.ab_power import (
    DEFAULT_SCALE,
    FAMILY_INFERENCE,
    INFERENCE,
    FamilySteps,
    check_scale,
    family_steps,
    projection_inputs,
    solve_scale_needed,
)
from retentioneering.tools.ab_test import mde_alpha_of

DIFF = ("arm", "treatment", "control")
TOY = ("arm", "a", "b")
#: The solver's numbers, shared with ``js/widget/test/ab_matrix.test.ts``.
FIXTURE = (
    pathlib.Path(__file__).resolve().parents[2]
    / "js"
    / "widget"
    / "test"
    / "fixtures"
    / "scale_needed.json"
)


@pytest.fixture(scope="module")
def stream() -> Eventstream:
    return datasets.load_ab_test()


@pytest.fixture(scope="module")
def assignment() -> pd.DataFrame:
    return datasets.load_ab_test_assignment()


def _at_scale_one(power: pd.DataFrame, frame: pd.DataFrame, keys) -> pd.DataFrame:
    one = power[power["scale"] == 1].set_index(list(keys))
    return one.join(frame.set_index(list(keys)), rsuffix="_getter")


class TestScaleOneIsTheGetter:
    @pytest.mark.parametrize(
        "weight", ["unique_paths", "avg_per_path", "proba_out", "share_of_total"]
    )
    def test_transitions(self, stream, assignment, weight):
        args = dict(assignment=assignment, n_bootstrap=200)
        if weight in ("unique_paths", "avg_per_path"):
            args.pop("n_bootstrap")
        power = stream.get_ab_transitions_power(DIFF, weight, **args)
        frame = stream.get_ab_transitions(DIFF, weight, **args)
        joined = _at_scale_one(power, frame, ("source", "target"))
        assert len(joined) == len(frame)
        np.testing.assert_allclose(joined["mde"], joined["mde_getter"], equal_nan=True)
        np.testing.assert_allclose(
            joined["mde_alpha"], joined["mde_alpha_getter"], equal_nan=True
        )
        tested = np.isfinite(joined["p_value_adjusted"].to_numpy(dtype=float))
        assert joined["testable"].dtype == "boolean"
        assert not joined["testable"].isna().any()
        assert (joined["testable"].to_numpy(dtype=bool) == tested).all()
        assert (joined.loc[tested, "inference"] == "tested").all()
        assert joined.loc[tested, "supported"].all()
        assert int(joined["family_size"].iloc[0]) == frame.attrs["n_tested"]
        assert (joined["group1_paths"] == joined["group1_paths_getter"]).all()
        support = joined["group1_edge_paths"] + joined["group2_edge_paths"]
        np.testing.assert_allclose(joined["pooled_support"], support)

    def test_steps(self, stream, assignment):
        power = stream.get_ab_steps_power(DIFF, 8, assignment=assignment)
        frame = stream.get_ab_steps(DIFF, 8, assignment=assignment)
        joined = _at_scale_one(power, frame, ("event", "step"))
        np.testing.assert_allclose(joined["mde"], joined["mde_getter"], equal_nan=True)
        tested = np.isfinite(joined["p_value_adjusted"].to_numpy(dtype=float))
        assert (joined["testable"].to_numpy(dtype=bool) == tested).all()
        assert int(joined["family_size"].iloc[0]) == frame.attrs["n_tested"]
        curve = {c["scale"]: c for c in power.attrs["curve"]}
        assert curve[1.0]["testable"] == frame.attrs["n_tested"]
        assert curve[1.0]["cells"] == len(frame)

    def test_attrs_keep_the_getters(self, stream, assignment):
        power = stream.get_ab_transitions_power(DIFF, assignment=assignment)
        assert power.attrs["tool"] == "get_ab_transitions_power"
        assert power.attrs["edge_weight"] == "unique_paths"
        assert power.attrs["scale"] == [float(s) for s in DEFAULT_SCALE]
        assert "same path composition" in power.attrs["assumption"]
        assert power.attrs["sizing_basis"] == "projected_family"
        assert "total, not additional" in power.attrs["sizing"]
        assert "unknown spread" in power.attrs["family"]
        assert power.attrs["path_col"] == "user_id"
        assert set(power["inference"]) <= set(INFERENCE)
        for entry in power.attrs["curve"]:
            for key in ("supported", "unknown_spread", "no_spread", "no_estimate"):
                assert isinstance(entry[key], int)


class TestProjection:
    def test_mde_scales_as_one_over_sqrt_scale(self, stream, assignment):
        # a fixed level: without a correction the family does not move the level
        for weight in ("unique_paths", "avg_per_path"):
            power = stream.get_ab_transitions_power(
                DIFF, weight, assignment=assignment, correction="none"
            )
            wide = power.pivot_table(
                index=["source", "target"], columns="scale", values="mde"
            )
            for factor in (0.25, 4.0, 16.0):
                ratio = (wide[factor] / wide[1.0]).dropna()
                np.testing.assert_allclose(ratio, 1 / math.sqrt(factor), rtol=1e-9)

    @pytest.mark.parametrize("weight", ["unique_paths", "proba_out"])
    def test_family_level_follows_the_family(self, stream, assignment, weight):
        # the family: supported, and tested, projectable or of unknown spread
        power = stream.get_ab_transitions_power(
            DIFF, weight, assignment=assignment, n_bootstrap=200
        )
        for factor, part in power.groupby("scale"):
            counted = part["supported"] & part["inference"].isin(FAMILY_INFERENCE)
            m = int(counted.sum())
            assert m >= int(part["testable"].sum())
            assert (part["family_size"] == m).all()
            assert part["mde_alpha"].iloc[0] == pytest.approx(0.05 / m)

    def test_support_grows_with_scale_and_testable_is_monotone(
        self, stream, assignment
    ):
        power = stream.get_ab_transitions_power(DIFF, assignment=assignment)
        for column in ("supported", "testable"):
            values = power.assign(_v=power[column].fillna(False).astype(bool))
            wide = values.pivot(
                index=["source", "target"], columns="scale", values="_v"
            )
            scales = sorted(wide.columns)
            for low, high in zip(scales, scales[1:]):
                assert (wide[high] | ~wide[low]).all()
        curve = pd.DataFrame(power.attrs["curve"])
        assert curve["testable"].is_monotonic_increasing
        assert curve["supported"].is_monotonic_increasing
        assert curve["mde_median"].is_monotonic_decreasing

    def test_scale_to_test(self, stream, assignment):
        power = stream.get_ab_transitions_power(DIFF, assignment=assignment)
        one = power[power["scale"] == 1]
        reach = one["scale_to_test"].to_numpy(dtype=float)
        testable = one["testable"].to_numpy(dtype=bool)
        assert (reach[testable] <= 1 + 1e-12).all()
        assert (~(reach[~testable] <= 1)).all()

    def test_paths_needed_reach_the_target(self, stream, assignment):
        target = 0.01
        power = stream.get_ab_transitions_power(
            DIFF, assignment=assignment, target_delta=target
        )
        at_one = power[power["scale"] == 1].set_index(["source", "target"])
        # never fewer paths than the support rule needs; none for a row that no
        # multiple makes testable
        assert (at_one["scale_needed"] >= at_one["scale_to_test"] - 1e-12).all()
        assert at_one.loc[at_one["scale_to_test"].isna(), "scale_needed"].isna().all()
        # the same totals at every scale of a row
        spread = power.groupby(["source", "target"])["scale_needed"].nunique()
        assert (spread <= 1).all()
        needed = at_one["scale_needed"].dropna()
        assert (needed > at_one.loc[needed.index, "scale_to_test"] + 1e-9).any()
        # the screen at the needed size, row by row: the projected family there
        # and an mde at most the target
        rows = projection_inputs(stream.get_ab_transitions(DIFF, assignment=assignment))
        projected = stream.get_ab_transitions_power(
            DIFF, assignment=assignment, scale=sorted(set(needed))
        ).set_index(["source", "target", "scale"])
        for key, s in needed.items():
            row = projected.loc[(*key, s)]
            assert row["mde"] <= target * (1 + 1e-9)
            assert row["family_size"] == rows.steps.size_at(s)
        paths = at_one.loc[needed.index]
        assert (
            paths["group1_paths_needed"] >= paths["group1_paths"] * needed - 1e-6
        ).all()
        curve = {c["scale"]: c for c in power.attrs["curve"]}
        testable = at_one[at_one["testable"].to_numpy(dtype=bool)]
        reached = (testable["mde"] <= target).mean()
        assert curve[1.0]["share_reaching_target"] == pytest.approx(reached)

    def test_bootstrap_rows_scale_the_bootstrap_se(self, stream, assignment):
        power = stream.get_ab_transitions_power(
            DIFF, "proba_out", assignment=assignment, n_bootstrap=200, correction="none"
        )
        frame = stream.get_ab_transitions(
            DIFF, "proba_out", assignment=assignment, n_bootstrap=200, correction="none"
        )
        four = power[power["scale"] == 4].set_index(["source", "target"])
        se = frame.set_index(["source", "target"])["delta_se"]
        expected = _stats.mde_from_se(se.loc[four.index] / 2, 0.05, 0.8)
        np.testing.assert_allclose(four["mde"], expected, equal_nan=True)


def _stream_of(templates, arms=("a", "b")) -> Eventstream:
    """A stream of the given paths ``(events, count, arm)``; an arm of None
    alternates the arms."""
    rows = []
    start = pd.Timestamp("2026-01-01")
    user = 0
    for events, count, arm in templates:
        for _ in range(count):
            for k, event in enumerate(events):
                rows.append(
                    {
                        "user_id": user,
                        "event": event,
                        "timestamp": start + pd.Timedelta(minutes=user * 100 + k),
                        "arm": arm if arm is not None else arms[user % 2],
                    }
                )
            user += 1
    return Eventstream(pd.DataFrame(rows), {"segment_cols": ["arm"]})


def _constant_ratio(copies: int = 1) -> Eventstream:
    """The review's case: 50 paths per arm, one of them ``A -> B``, the others
    ``C -> D`` (``copies`` times as many of each)."""
    return _stream_of(
        [
            (["A", "B"], copies, "a"),
            (["C", "D"], 49 * copies, "a"),
            (["A", "B"], copies, "b"),
            (["C", "D"], 49 * copies, "b"),
        ]
    )


def _branches() -> Eventstream:
    """The review's sizing case: 200 paths, a common edge ``A -> B`` (half of
    them), ``A -> C`` and ten rare branches of one path each."""
    return _stream_of(
        [(["A", "B"], 100, None), (["A", "C"], 90, None)]
        + [(["A", f"R{k}"], 1, None) for k in range(10)]
    )


def _quiet(call):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return call()


class TestInference:
    """Support is not inference (review R2): passing the support rule at a scale
    does not create a spread that the row does not have."""

    def test_a_rare_constant_ratio_never_becomes_testable(self):
        stream = _constant_ratio()
        power = _quiet(
            lambda: stream.get_ab_transitions_power(TOY, "proba_out", n_bootstrap=200)
        )
        rows = power.set_index(["source", "target", "scale"])
        for edge in (("A", "B"), ("B", "path_end")):
            # proba_out is 1 in both arms: every path's own ratio is 1
            one = rows.loc[(*edge, 1.0)]
            assert one["inference"] == "no_spread"
            for factor in (8.0, 16.0):
                row = rows.loc[(*edge, factor)]
                assert bool(row["supported"])  # the support rule passes ...
                assert (
                    row["testable"] is not pd.NA and not row["testable"]
                )  # ... no test
                assert math.isnan(row["mde"]) and math.isnan(row["scale_to_test"])
        # a rare row with a spread gains support and becomes testable
        rare = rows.loc[("path_start", "A")]
        assert set(rare["inference"]) == {"projectable"}
        assert not rare.loc[1.0, "testable"] and rare.loc[8.0, "testable"]
        assert rare.loc[1.0, "scale_to_test"] == pytest.approx(5.0)
        # the family: the tested row and the rare variable one, never the constant
        # ones (four of them: two rare, two that pass the rule now)
        curve = {c["scale"]: c for c in power.attrs["curve"]}
        assert curve[1.0]["family_size"] == 1
        assert curve[8.0]["family_size"] == curve[16.0]["family_size"] == 2
        assert curve[8.0]["supported"] == 6 and curve[8.0]["no_spread"] == 4
        assert curve[8.0]["testable"] == 2 and curve[8.0]["unknown_spread"] == 0
        assert (power.loc[power["scale"] == 8.0, "mde_alpha"] == 0.025).all()
        # and eight times the paths of the same mix: still no test of A -> B
        bigger = _quiet(
            lambda: _constant_ratio(8).get_ab_transitions(
                TOY, "proba_out", n_bootstrap=200
            )
        ).set_index(["source", "target"])
        assert bigger.loc[("A", "B"), "verdict"] == "insufficient_data"
        assert math.isnan(bigger.loc[("A", "B"), "p_value_adjusted"])
        assert not math.isnan(bigger.loc[("path_start", "A"), "p_value_adjusted"])

    def test_an_unknown_spread_is_na_and_counts_in_the_family(self):
        # A -> B / A -> C: one path of each per arm, a share of 0.5 in both; the
        # bootstrap has too many replicates without A (no spread estimated now);
        # E -> F is in arm a only: arm b has no value of proba_out(E -> F)
        stream = _stream_of(
            [
                (["A", "B"], 1, "a"),
                (["A", "C"], 1, "a"),
                (["A", "B"], 1, "b"),
                (["A", "C"], 1, "b"),
                (["E", "F"], 1, "a"),
                (["X", "Y"], 47, "a"),
                (["X", "Y"], 48, "b"),
            ]
        )
        frame = _quiet(
            lambda: stream.get_ab_transitions(TOY, "proba_out", n_bootstrap=200)
        ).set_index(["source", "target"])
        assert math.isnan(frame.loc[("A", "B"), "delta_se"])
        power = _quiet(
            lambda: stream.get_ab_transitions_power(TOY, "proba_out", n_bootstrap=200)
        )
        rows = power.set_index(["source", "target", "scale"])
        unknown = rows.loc[("A", "B")]
        assert set(unknown["inference"]) == {"unknown_spread"}
        assert unknown.loc[1.0, "scale_to_test"] == pytest.approx(5.0)
        assert unknown.loc[1.0, "testable"] is not pd.NA  # not supported yet
        assert not unknown.loc[1.0, "testable"]
        assert unknown.loc[8.0, "testable"] is pd.NA  # supported, spread unknown
        assert math.isnan(unknown.loc[8.0, "mde"])
        # it counts in the family once supported: the level stays conservative
        at8 = power[power["scale"] == 8.0]
        counted = at8["supported"] & at8["inference"].isin(FAMILY_INFERENCE)
        assert int(at8["family_size"].iloc[0]) == int(counted.sum())
        assert int(at8["family_size"].iloc[0]) > int(
            at8["testable"].fillna(False).astype(bool).sum()
        )
        curve = {c["scale"]: c for c in power.attrs["curve"]}
        assert curve[8.0]["unknown_spread"] >= 2  # A -> B and A -> C
        # an arm without a value keeps none with more paths of the same mix
        missing = rows.loc[("E", "F")]
        assert set(missing["inference"]) == {"no_estimate"}
        assert missing["scale_to_test"].isna().all()
        assert not missing["testable"].fillna(True).astype(bool).any()

    def test_a_supported_untested_row_has_no_estimate(self):
        # three paths with A -> B per arm (support 6 >= min_support 6), and no other
        # path with A: too many bootstrap replicates without A, so no p-value now;
        # not provably constant (0.6 and 0.5)
        stream = _stream_of(
            [
                (["A", "B", "A", "C"], 2, "a"),
                (["A", "B"], 1, "a"),
                (["A", "B", "A", "C"], 3, "b"),
                (["X", "Y"], 47, "a"),
                (["X", "Y"], 47, "b"),
            ]
        )
        args = dict(n_bootstrap=200, min_support=6)
        frame = _quiet(
            lambda: stream.get_ab_transitions(TOY, "proba_out", **args)
        ).set_index(["source", "target"])
        assert frame.loc[("A", "B"), "support_warning"] == "invalid_replicates"
        power = _quiet(
            lambda: stream.get_ab_transitions_power(TOY, "proba_out", **args)
        ).set_index(["source", "target", "scale"])
        row = power.loc[("A", "B")]
        assert set(row["inference"]) == {"no_estimate"}
        assert row.loc[1.0, "supported"] and not row["testable"].astype(bool).any()
        assert row["scale_to_test"].isna().all()

    @pytest.mark.parametrize("screen", ["transitions", "steps"])
    def test_a_rare_share_is_projectable(self, screen):
        stream = _branches()
        if screen == "transitions":
            power = stream.get_ab_transitions_power(TOY)
        else:
            power = stream.get_ab_steps_power(TOY, 3)
        one = power[power["scale"] == 1]
        rare = one[one["pooled_support"] < 10]
        assert len(rare) >= 10 and not rare["supported"].any()
        # a z-test row: its spread is the pooled share's, known at any size
        assert set(rare["inference"]) == {"projectable"}
        assert not power["testable"].isna().any()
        # a cell every path has is constant, at any size
        everyone = one[one["pooled_support"] == 200]
        assert len(everyone) and set(everyone["inference"]) == {"no_spread"}


class TestSizing:
    """``scale_needed`` with the family projected at that size (review R5)."""

    def test_the_review_case_meets_its_target(self):
        stream = _branches()
        target = 0.05
        power = stream.get_ab_transitions_power(TOY, scale=[1], target_delta=target)
        rows = power.set_index(["source", "target"])
        common = rows.loc[("A", "B")]
        # with the family of today (4 rows) it was 2,231 paths per arm, where the
        # family is 24 and the mde 5.869 pp
        assert common["family_size"] == 4
        assert common["group1_paths_needed"] == 3073
        assert common["scale_needed"] == pytest.approx(30.72824209434185, rel=1e-12)
        assert rows.loc[("A", "R0"), "scale_needed"] == pytest.approx(10.0)
        assert power.attrs["sizing_basis"] == "projected_family"
        steps = projection_inputs(stream.get_ab_transitions(TOY)).steps
        assert list(steps.breakpoints) == [0.0, 0.1, 10 / 90, 10.0]
        assert list(steps.sizes) == [0, 2, 4, 24]
        # every needed size, plugged back into the full projection
        needed = rows["scale_needed"].dropna()
        back = stream.get_ab_transitions_power(
            TOY, scale=sorted(set(needed)), target_delta=target
        ).set_index(["source", "target", "scale"])
        for key, s in needed.items():
            row = back.loc[(*key, s)]
            assert row["mde"] <= target * (1 + 1e-9)
            assert row["family_size"] == steps.size_at(s)
            # the totals do not move with the scale they are read at
            assert row["scale_needed"] == s
        assert back.loc[("A", "B", needed[("A", "B")]), "family_size"] == 24

    @pytest.mark.parametrize("screen", ["transitions", "steps"])
    def test_the_compression_uses_the_same_solver(self, screen):
        # get_ab_compression's median_scale_needed at step 0: the same projected
        # family, so the same totals as the power getter
        stream = _branches()
        target = 0.05
        if screen == "transitions":
            power = stream.get_ab_transitions_power(TOY, scale=[1], target_delta=target)
        else:
            power = stream.get_ab_steps_power(TOY, 10, scale=[1], target_delta=target)
        path = _quiet(
            lambda: stream.get_ab_compression(TOY, screen, target_delta=target)
        ).attrs["path"]
        tested = power[power["testable"].to_numpy(dtype=bool)]
        assert path.iloc[0]["median_scale_needed"] == pytest.approx(
            tested["scale_needed"].median(), rel=1e-12
        )
        if screen == "transitions":
            assert path.iloc[0]["median_scale_needed"] > 30  # not 22.3: m grows

    def test_from_that_size_on_the_target_holds(self, stream, assignment):
        target = 0.01
        frame = stream.get_ab_steps(DIFF, 8, assignment=assignment)
        rows = projection_inputs(frame)
        needed = solve_scale_needed(rows.base, rows.scale_to_test, rows.steps, target)
        ok = np.isfinite(needed)
        assert ok.sum() > 10
        later = np.unique(
            np.concatenate(
                [needed[ok] * 1.5, needed[ok] * 7, rows.steps.breakpoints[1:]]
            )
        )
        power = stream.get_ab_steps_power(
            DIFF, 8, assignment=assignment, scale=sorted(set(later) | set(needed[ok]))
        )
        wide = power.pivot(index=["event", "step"], columns="scale", values="mde")
        keys = list(zip(frame["event"], frame["step"]))
        for i in np.flatnonzero(ok):
            series = wide.loc[keys[i]]
            after = series[series.index >= needed[i]]
            assert (after <= target * (1 + 1e-9)).all(), keys[i]
            # just below it, the target is missed (or the row is not yet supported)
            if needed[i] > rows.scale_to_test[i] * (1 + 1e-9):
                s = needed[i] * (1 - 1e-7)
                mde = rows.steps.factor_at(s) * rows.base[i] / math.sqrt(s)
                assert mde > target

    def test_every_mde_is_separable(self, stream, assignment):
        # mde(s, m) = F(m) * base / sqrt(s): the solver relies on it
        for weight in ("unique_paths", "avg_per_path", "proba_out"):
            frame = stream.get_ab_transitions(
                DIFF, weight, assignment=assignment, n_bootstrap=200
            )
            rows = projection_inputs(frame)
            power = stream.get_ab_transitions_power(
                DIFF, weight, assignment=assignment, n_bootstrap=200
            ).set_index(["source", "target"])
            for factor in (0.25, 1.0, 4.0, 16.0):
                part = power[power["scale"] == factor].loc[
                    list(zip(frame["source"], frame["target"]))
                ]
                separable = rows.steps.factor_at(factor) * rows.base / math.sqrt(factor)
                ok = np.isfinite(rows.base)
                np.testing.assert_allclose(
                    part["mde"].to_numpy()[ok], separable[ok], rtol=1e-12
                )
            # at scale 1, base * factor_now is the getter's mde
            tested = np.isfinite(frame["p_value_adjusted"].to_numpy(dtype=float))
            np.testing.assert_allclose(
                rows.base[tested] * rows.steps.factor_at(1.0),
                frame["mde"].to_numpy(dtype=float)[tested],
                rtol=1e-12,
            )

    def test_the_factors_are_the_mde_factor(self):
        steps = family_steps(
            [0.1, 0.3, 0.3, 2, 7, np.nan], alpha=0.05, correction="bh", power=0.8
        )
        assert list(steps.breakpoints) == [0.0, 0.1, 0.3, 2.0, 7.0]
        assert list(steps.sizes) == [0, 1, 3, 4, 5]
        for m, f in zip(steps.sizes, steps.factors):
            assert f == _stats._mde_factor(mde_alpha_of(0.05, int(m), "bh"), 0.8)
        assert steps.size_at(0.05) == 0 and steps.size_at(0.3) == 3
        assert steps.size_at(100) == 5
        flat = family_steps([0.5, 2], alpha=0.05, correction="none", power=0.9)
        assert len(set(flat.factors)) == 1

    def test_the_solver_against_brute_force(self):
        rng = np.random.default_rng(7)
        for _ in range(40):
            k = int(rng.integers(1, 30))
            family = np.round(np.exp(rng.normal(0, 1.5, k)), 3)
            steps = family_steps(family, alpha=0.05, correction="bh", power=0.8)
            n = 25
            to_test = np.where(
                rng.random(n) < 0.8, rng.choice(family, n), np.exp(rng.normal(0, 1, n))
            )
            base = np.exp(rng.normal(np.log(0.05), 1, n))
            target = 0.04
            got = solve_scale_needed(base, to_test, steps, target)
            for i in range(n):
                assert got[i] == pytest.approx(
                    _brute_force(base[i], to_test[i], steps, target), rel=1e-12
                )
        assert np.isnan(solve_scale_needed([np.nan], [1.0], steps, 0.1)).all()
        assert np.isnan(solve_scale_needed([0.1], [np.nan], steps, 0.1)).all()

    def test_the_browser_numbers(self):
        # js/widget/test/ab_matrix.test.ts solves the same cases with scaleNeeded
        fixture = json.loads(FIXTURE.read_text())
        assert len(fixture["cases"]) >= 3
        for case in fixture["cases"]:
            family = [np.nan if v is None else v for v in case["family_scale_to_test"]]
            steps = family_steps(
                family,
                alpha=case["alpha"],
                correction=case["correction"],
                power=case["power"],
            )
            payload = steps.payload()
            assert payload["breakpoints"] == case["breakpoints"]
            assert payload["sizes"] == case["sizes"]
            np.testing.assert_allclose(payload["factors"], case["factors"], rtol=1e-12)
            assert payload["factor_now"] == pytest.approx(case["factor_now"], rel=1e-12)
            pinned = FamilySteps(
                np.array(case["breakpoints"]),
                np.array(case["sizes"]),
                np.array(case["factors"]),
            )
            base = [np.nan if r["base"] is None else r["base"] for r in case["rows"]]
            to_test = [
                np.nan if r["scale_to_test"] is None else r["scale_to_test"]
                for r in case["rows"]
            ]
            got = solve_scale_needed(base, to_test, pinned, case["target_delta"])
            expected = [np.nan if v is None else v for v in case["scale_needed"]]
            np.testing.assert_allclose(got, expected, rtol=1e-12, equal_nan=True)


def _brute_force(base, to_test, steps, target):
    """The smallest s >= to_test from which F(m(s)) base / sqrt(s) <= target:
    on an interval the MDE falls, so it holds from s on when it holds at s and at
    every breakpoint above s; the answer is one of to_test, a breakpoint, or a
    point where the MDE equals the target."""
    if not (np.isfinite(base) and np.isfinite(to_test)):
        return np.nan
    breaks = steps.breakpoints
    x = (base / target) ** 2
    candidates = {float(to_test)} | {float(b) for b in breaks if b >= to_test}
    candidates |= {float(f * f * x) for f in steps.factors if f * f * x >= to_test}

    def holds(s):
        return steps.factor_at(s) * base / math.sqrt(s) <= target * (1 + 1e-12)

    for s in sorted(candidates):
        if holds(s) and all(holds(b) for b in breaks if b > s):
            return s
    raise AssertionError("no size reaches the target")


class TestValidation:
    def test_scale(self, stream):
        assert check_scale([4, 0.5]) == [0.5, 1.0, 4.0]
        for bad in ([], [0], [-1], ["2"], [True], "x", [float("nan")]):
            with pytest.raises(InvalidParameterError):
                stream.get_ab_transitions_power(
                    ("exp_group", "treatment", "control"), scale=bad
                )

    def test_target_delta(self, stream):
        for bad in (0, -0.1, "0.1", True, float("inf")):
            with pytest.raises(InvalidParameterError):
                stream.get_ab_steps_power(
                    ("exp_group", "treatment", "control"), 3, target_delta=bad
                )

    def test_getter_errors_pass_through(self, stream):
        with pytest.raises(InvalidParameterError):
            stream.get_ab_transitions_power(
                ("exp_group", "treatment", "control"), "count"
            )
        with pytest.raises(InvalidParameterError):
            stream.get_ab_steps_power(
                ("exp_group", "treatment", "control"), path_pattern="cart"
            )

    def test_signatures_extend_the_getters(self):
        import inspect

        for power, getter in (
            ("get_ab_transitions_power", "get_ab_transitions"),
            ("get_ab_steps_power", "get_ab_steps"),
        ):
            p = list(inspect.signature(getattr(Eventstream, power)).parameters)
            g = list(inspect.signature(getattr(Eventstream, getter)).parameters)
            assert p[: len(g)] == g
            assert p[len(g) :] == ["scale", "target_delta"]
            doc = getattr(Eventstream, power).__doc__
            assert "target_delta" in doc and "conservative for Benjamini" in doc
