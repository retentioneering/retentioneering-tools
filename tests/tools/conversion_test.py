import numpy as np
import pandas as pd
import pytest

from retentioneering.datasets import load_ecom
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError


def _stream(rows) -> Eventstream:
    df = pd.DataFrame(rows, columns=["user_id", "event", "timestamp"])
    return Eventstream(df, {"event_cols": ["event"]})


def _row(result: pd.DataFrame, index: int = 0) -> dict:
    return result.iloc[index].to_dict()


class TestConversionRateAgainstOtherTools:
    """The numbers have to agree with the tools that already answer part of this."""

    def test_matches_funnel_two_step(self) -> None:
        # A two-step funnel asks exactly this question, so numerator and
        # denominator must be the same paths — including at session grain.
        stream = load_ecom()
        funnel = stream.funnel_data(
            steps=["add_to_cart", "purchase"], path_col="session_id"
        )["steps"]
        result = _row(
            stream.get_conversion_rate("add_to_cart", "purchase", path_col="session_id")
        )

        assert result["paths_with_start"] == funnel[0]["unique_paths"]
        assert result["converted"] == funnel[1]["unique_paths"]
        assert result["conversion_rate"] == pytest.approx(
            funnel[1]["step_conversion_rate"]
        )

    def test_within_none_matches_the_pattern_metric(self) -> None:
        # The composition this method replaces: filter to paths with Y, then
        # take the mean of matches_pattern("Y->.*->X").
        stream = load_ecom()
        metrics = stream.get_metrics(
            [
                {
                    "metric": "matches_pattern",
                    "metric_args": {"pattern": "add_to_cart"},
                },
                {
                    "metric": "matches_pattern",
                    "metric_args": {"pattern": "add_to_cart->.*->purchase"},
                },
            ]
        )
        with_start = metrics[metrics["matches_pattern_add_to_cart"] == 1]
        manual = with_start["matches_pattern_add_to_cart->.*->purchase"]

        result = _row(stream.get_conversion_rate("add_to_cart", "purchase"))

        assert result["paths_with_start"] == len(with_start)
        assert result["converted"] == int(manual.sum())
        assert result["conversion_rate"] == pytest.approx(manual.mean())

    def test_base_rate_is_the_share_of_all_paths(self) -> None:
        stream = load_ecom()
        matched = stream.get_metrics(
            [{"metric": "matches_pattern", "metric_args": {"pattern": "purchase"}}]
        )
        result = _row(stream.get_conversion_rate("add_to_cart", "purchase"))

        assert result["base_rate"] == pytest.approx(
            matched["matches_pattern_purchase"].mean()
        )
        assert result["lift"] == pytest.approx(
            result["conversion_rate"] / result["base_rate"]
        )


class TestConversionRateSemantics:
    def test_order_matters(self) -> None:
        # user_2 has both events, but X precedes Y — it counts towards the
        # denominator and not towards the numerator.
        stream = _stream(
            [
                ["user_1", "Y", "2020-01-01 00:00:00"],
                ["user_1", "X", "2020-01-01 00:01:00"],
                ["user_2", "X", "2020-01-01 00:00:00"],
                ["user_2", "Y", "2020-01-01 00:01:00"],
            ]
        )
        result = _row(stream.get_conversion_rate("Y", "X"))

        assert result["paths_with_start"] == 2
        assert result["converted"] == 1
        assert result["conversion_rate"] == 0.5
        assert result["base_rate"] == 1.0
        assert result["lift"] == 0.5

    def test_repeat_of_the_same_event(self) -> None:
        # start == end asks about a repeat, so the start occurrence itself must
        # not be counted as the conversion.
        stream = _stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:01:00"],
                ["user_1", "A", "2020-01-01 00:02:00"],
                ["user_2", "A", "2020-01-01 00:00:00"],
                ["user_2", "B", "2020-01-01 00:01:00"],
            ]
        )
        result = _row(stream.get_conversion_rate("A", "A"))

        assert result["paths_with_start"] == 2
        assert result["converted"] == 1

    def test_occurrence_selects_which_start_counts(self) -> None:
        # From the first A, B follows; from the last one, nothing does.
        stream = _stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:01:00"],
                ["user_1", "A", "2020-01-01 00:02:00"],
            ]
        )
        first = _row(stream.get_conversion_rate("A", "B"))
        last = _row(
            stream.get_conversion_rate({"pattern": "A", "occurrence": "last"}, "B")
        )

        assert first["converted"] == 1
        assert last["paths_with_start"] == 1
        assert last["converted"] == 0

    def test_end_pattern_must_fall_entirely_after_the_start(self) -> None:
        # user_3 matches "cart->.*->purchase", but only with the cart *before*
        # Y — which is not something that happened after Y.
        stream = _stream(
            [
                ["user_1", "cart", "2020-01-01 00:00:00"],
                ["user_1", "Y", "2020-01-01 00:01:00"],
                ["user_1", "cart", "2020-01-01 00:02:00"],
                ["user_1", "purchase", "2020-01-01 00:03:00"],
                ["user_2", "cart", "2020-01-01 00:00:00"],
                ["user_2", "purchase", "2020-01-01 00:01:00"],
                ["user_2", "Y", "2020-01-01 00:02:00"],
                ["user_3", "cart", "2020-01-01 00:00:00"],
                ["user_3", "Y", "2020-01-01 00:01:00"],
                ["user_3", "purchase", "2020-01-01 00:02:00"],
            ]
        )
        result = _row(stream.get_conversion_rate("Y", "cart->.*->purchase"))

        assert result["paths_with_start"] == 3
        assert result["converted"] == 1

    def test_anchor_spec_selects_landings_only(self) -> None:
        # "the catalog visits that opened the session", not every catalog visit.
        stream = _stream(
            [
                ["user_1", "catalog", "2020-01-01 00:00:00"],
                ["user_1", "purchase", "2020-01-01 00:01:00"],
                ["user_2", "home", "2020-01-01 00:00:00"],
                ["user_2", "catalog", "2020-01-01 00:01:00"],
                ["user_2", "home", "2020-01-01 00:02:00"],
            ]
        )
        landed = _row(
            stream.get_conversion_rate(
                {"pattern": "path_start->catalog", "at": -1}, "purchase"
            )
        )
        any_visit = _row(stream.get_conversion_rate("catalog", "purchase"))

        assert landed["paths_with_start"] == 1
        assert landed["conversion_rate"] == 1.0
        assert any_visit["paths_with_start"] == 2
        assert any_visit["conversion_rate"] == 0.5

    def test_path_end_within_one_event_is_the_exit_rate(self) -> None:
        # user_1 leaves right after the error, user_2 carries on, user_3 never
        # hits it.
        stream = _stream(
            [
                ["user_1", "checkout", "2020-01-01 00:00:00"],
                ["user_1", "error", "2020-01-01 00:01:00"],
                ["user_2", "error", "2020-01-01 00:00:00"],
                ["user_2", "checkout", "2020-01-01 00:01:00"],
                ["user_2", "purchase", "2020-01-01 00:02:00"],
                ["user_3", "checkout", "2020-01-01 00:00:00"],
            ]
        )
        result = _row(stream.get_conversion_rate("error", "path_end", within=1))

        assert result["paths_with_start"] == 2
        assert result["converted"] == 1
        assert result["conversion_rate"] == 0.5
        # Every path ends, so ending is no distinction on its own.
        assert result["base_rate"] == 1.0

    def test_path_col_override_changes_the_unit(self) -> None:
        # One user, two sessions: per user the purchase follows the cart, per
        # session it does not.
        df = pd.DataFrame(
            [
                ["user_1", "s1", "cart", "2020-01-01 00:00:00"],
                ["user_1", "s2", "purchase", "2020-01-02 00:00:00"],
            ],
            columns=["user_id", "session_id", "event", "timestamp"],
        )
        stream = Eventstream(
            df, {"path_cols": ["user_id", "session_id"], "event_cols": ["event"]}
        )

        assert _row(stream.get_conversion_rate("cart", "purchase"))["converted"] == 1
        per_session = _row(
            stream.get_conversion_rate("cart", "purchase", path_col="session_id")
        )
        assert per_session["paths_with_start"] == 1
        assert per_session["converted"] == 0


class TestConversionRateWindow:
    def test_within_events_includes_its_far_edge(self) -> None:
        stream = _stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:01:00"],
                ["user_2", "A", "2020-01-01 00:00:00"],
                ["user_2", "noise", "2020-01-01 00:01:00"],
                ["user_2", "B", "2020-01-01 00:02:00"],
                ["user_3", "A", "2020-01-01 00:00:00"],
                ["user_3", "noise", "2020-01-01 00:01:00"],
                ["user_3", "noise", "2020-01-01 00:02:00"],
                ["user_3", "B", "2020-01-01 00:03:00"],
            ]
        )

        assert _row(stream.get_conversion_rate("A", "B", within=1))["converted"] == 1
        # user_2's B is exactly the 2nd event after A — inside the window.
        assert _row(stream.get_conversion_rate("A", "B", within=2))["converted"] == 2
        assert _row(stream.get_conversion_rate("A", "B", within=3))["converted"] == 3
        assert _row(stream.get_conversion_rate("A", "B"))["converted"] == 3

    def test_within_duration_includes_its_far_edge(self) -> None:
        stream = _stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:20:00"],
                ["user_2", "A", "2020-01-01 00:00:00"],
                ["user_2", "B", "2020-01-01 00:30:00"],
                ["user_3", "A", "2020-01-01 00:00:00"],
                ["user_3", "B", "2020-01-01 00:30:01"],
            ]
        )

        # user_2's B lands exactly on the boundary.
        assert (
            _row(stream.get_conversion_rate("A", "B", within="30m"))["converted"] == 2
        )
        assert (
            _row(stream.get_conversion_rate("A", "B", within=pd.Timedelta("30m")))[
                "converted"
            ]
            == 2
        )
        assert _row(stream.get_conversion_rate("A", "B", within="1h"))["converted"] == 3

    def test_window_is_measured_from_the_start_anchor(self) -> None:
        # Not from the beginning of the path: A sits ten minutes in, and B is
        # five minutes after it.
        stream = _stream(
            [
                ["user_1", "home", "2020-01-01 00:00:00"],
                ["user_1", "A", "2020-01-01 00:10:00"],
                ["user_1", "B", "2020-01-01 00:15:00"],
            ]
        )

        assert _row(stream.get_conversion_rate("A", "B", within="6m"))["converted"] == 1
        assert _row(stream.get_conversion_rate("A", "B", within="4m"))["converted"] == 0


class TestConversionRateFanOut:
    def test_each_end_anchor_gets_its_own_row(self) -> None:
        stream = load_ecom()
        targets = ["purchase", "cart", "path_end"]
        fanned = stream.get_conversion_rate("add_to_cart", targets, within=5)

        assert list(fanned["end_anchor"]) == targets
        for target in targets:
            single = stream.get_conversion_rate("add_to_cart", target, within=5)
            row = fanned[fanned["end_anchor"] == target].reset_index(drop=True)
            pd.testing.assert_frame_equal(row, single)

    def test_each_start_anchor_gets_its_own_row(self) -> None:
        stream = load_ecom()
        starts = ["catalog", "search"]
        fanned = stream.get_conversion_rate(starts, ["purchase", "cart"])

        assert list(fanned["start_anchor"]) == [
            "catalog",
            "catalog",
            "search",
            "search",
        ]
        for start in starts:
            single = stream.get_conversion_rate(start, ["purchase", "cart"])
            rows = fanned[fanned["start_anchor"] == start].reset_index(drop=True)
            pd.testing.assert_frame_equal(rows, single)


class TestConversionRateEdgeCases:
    def test_start_that_never_happens_is_data_not_an_error(self) -> None:
        stream = _stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:01:00"],
            ]
        )
        result = _row(stream.get_conversion_rate("B->.*->A", "B"))

        assert result["paths_with_start"] == 0
        assert result["converted"] == 0
        assert np.isnan(result["conversion_rate"])
        assert np.isnan(result["lift"])

    def test_zero_base_rate_does_not_divide_by_zero(self) -> None:
        stream = _stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:01:00"],
            ]
        )
        result = _row(stream.get_conversion_rate("A", "B->.*->A"))

        assert result["base_rate"] == 0.0
        assert result["conversion_rate"] == 0.0
        assert np.isnan(result["lift"])

    def test_counts_are_integers(self) -> None:
        stream = _stream([["user_1", "A", "2020-01-01 00:00:00"]])
        result = stream.get_conversion_rate("A", "A")

        assert result["paths_with_start"].dtype == "int64"
        assert result["converted"].dtype == "int64"


class TestConversionRateValidation:
    def test_unknown_event_lists_the_available_ones(self) -> None:
        stream = _stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:01:00"],
            ]
        )
        with pytest.raises(InvalidParameterError) as exc:
            stream.get_conversion_rate("Purchse", "B")
        assert "start_anchor" in str(exc.value)
        assert "'A', 'B'" in str(exc.value)

        with pytest.raises(InvalidParameterError) as exc:
            stream.get_conversion_rate("A", ["B", "Purchse"])
        assert "end_anchor" in str(exc.value)

    def test_unknown_path_col(self) -> None:
        stream = _stream([["user_1", "A", "2020-01-01 00:00:00"]])
        with pytest.raises(InvalidParameterError):
            stream.get_conversion_rate("A", "A", path_col="session_id")

    @pytest.mark.parametrize("within", [0, -1, True, "30"])
    def test_invalid_within(self, within) -> None:
        stream = _stream([["user_1", "A", "2020-01-01 00:00:00"]])
        with pytest.raises((InvalidParameterError, ValueError)):
            stream.get_conversion_rate("A", "A", within=within)
