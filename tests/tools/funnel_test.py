import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream


class TestFunnel:
    def test_funnel_basic(self) -> None:
        df = pd.DataFrame([
            ["user_1", "A", "2020-01-01 00:00:00"],
            ["user_1", "B", "2020-01-01 00:01:00"],
            ["user_1", "C", "2020-01-01 00:02:00"],
            ["user_2", "A", "2020-01-01 00:00:00"],
            ["user_2", "B", "2020-01-01 00:01:00"],
            ["user_3", "A", "2020-01-01 00:00:00"],
            ["user_4", "B", "2020-01-01 00:00:00"],
        ], columns=["user_id", "event", "timestamp"])

        stream = Eventstream(df, {"event_cols": ["event"]})
        result = stream.funnel_data(steps=["A", "B", "C"])

        assert len(result["steps"]) == 3
        assert result["steps"][0]["step"] == "A"
        assert result["steps"][0]["unique_paths"] == 3
        assert result["steps"][0]["conversion_rate"] == 0.75
        assert result["steps"][1]["step"] == "B"
        assert result["steps"][1]["unique_paths"] == 2
        assert result["steps"][1]["conversion_rate"] == 0.5
        assert result["steps"][2]["step"] == "C"
        assert result["steps"][2]["unique_paths"] == 1
        assert result["steps"][2]["conversion_rate"] == 0.25

    def test_funnel_no_conversions(self) -> None:
        df = pd.DataFrame([
            ["user_1", "A", "2020-01-01 00:00:00"],
            ["user_2", "A", "2020-01-01 00:00:00"],
            ["user_3", "C", "2020-01-01 00:00:00"],
        ], columns=["user_id", "event", "timestamp"])

        stream = Eventstream(df, {"event_cols": ["event"]})
        result = stream.funnel_data(steps=["A", "B", "C"])

        assert result["steps"][0]["unique_paths"] == 2
        assert result["steps"][1]["unique_paths"] == 0
        assert result["steps"][2]["unique_paths"] == 0

    def test_funnel_single_step(self) -> None:
        df = pd.DataFrame([
            ["user_1", "A", "2020-01-01 00:00:00"],
            ["user_2", "B", "2020-01-01 00:00:00"],
            ["user_3", "A", "2020-01-01 00:00:00"],
        ], columns=["user_id", "event", "timestamp"])

        stream = Eventstream(df, {"event_cols": ["event"]})
        result = stream.funnel_data(steps=["A"])

        assert len(result["steps"]) == 1
        assert result["steps"][0]["unique_paths"] == 2
        assert result["steps"][0]["conversion_rate"] == pytest.approx(0.6667, rel=1e-3)

    def test_funnel_order_matters(self) -> None:
        df = pd.DataFrame([
            ["user_1", "C", "2020-01-01 00:00:00"],
            ["user_1", "B", "2020-01-01 00:01:00"],
            ["user_1", "A", "2020-01-01 00:02:00"],
            ["user_2", "A", "2020-01-01 00:00:00"],
            ["user_2", "B", "2020-01-01 00:01:00"],
            ["user_2", "C", "2020-01-01 00:02:00"],
        ], columns=["user_id", "event", "timestamp"])

        stream = Eventstream(df, {"event_cols": ["event"]})
        result = stream.funnel_data(steps=["A", "B", "C"])

        assert result["steps"][0]["unique_paths"] == 2
        assert result["steps"][1]["unique_paths"] == 1
        assert result["steps"][2]["unique_paths"] == 1

    def test_funnel_monotonicity(self) -> None:
        df = pd.DataFrame([
            ["user_1", "B", "2020-01-01 00:01:00"],
            ["user_1", "A", "2020-01-01 00:02:00"],
            ["user_1", "C", "2020-01-01 00:03:00"],
        ], columns=["user_id", "event", "timestamp"])

        stream = Eventstream(df, {"event_cols": ["event"]})
        result = stream.funnel_data(steps=["A", "B", "C"])

        assert result["steps"][0]["unique_paths"] == 1
        assert result["steps"][1]["unique_paths"] == 0
        assert result["steps"][2]["unique_paths"] == 0

    def test_funnel_with_diff(self) -> None:
        df = pd.DataFrame([
            ["user_1", "A", "segment_1", "2020-01-01 00:00:00"],
            ["user_1", "B", "segment_1", "2020-01-01 00:01:00"],
            ["user_2", "A", "segment_1", "2020-01-01 00:00:00"],
            ["user_3", "A", "segment_2", "2020-01-01 00:00:00"],
            ["user_3", "B", "segment_2", "2020-01-01 00:01:00"],
            ["user_3", "C", "segment_2", "2020-01-01 00:02:00"],
            ["user_4", "A", "segment_2", "2020-01-01 00:00:00"],
        ], columns=["user_id", "event", "segment", "timestamp"])

        stream = Eventstream(df, {"event_cols": ["event"], "segment_cols": ["segment"]})
        result = stream.funnel_data(steps=["A", "B", "C"], diff=("segment", "segment_1", "segment_2"))

        assert len(result["steps"]) == 3
        assert result["steps"][0]["funnel1_unique_paths"] == 2
        assert result["steps"][0]["funnel2_unique_paths"] == 2
        assert result["steps"][0]["delta_unique_paths"] == 0
        assert result["steps"][1]["funnel1_unique_paths"] == 1
        assert result["steps"][1]["funnel2_unique_paths"] == 1
        assert result["steps"][2]["funnel1_unique_paths"] == 0
        assert result["steps"][2]["funnel2_unique_paths"] == 1
        assert result["steps"][2]["delta_unique_paths"] == 1

    def test_funnel_empty_steps(self) -> None:
        df = pd.DataFrame([["user_1", "A", "2020-01-01"]], columns=["user_id", "event", "timestamp"])
        stream = Eventstream(df, {"event_cols": ["event"]})
        result = stream.funnel_data(steps=[])
        assert result == {"steps": []}
