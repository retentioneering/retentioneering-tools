import pandas as pd
from retentioneering.eventstream.eventstream import Eventstream


class TestDescribe:
    def _stream(self) -> Eventstream:
        df = pd.DataFrame(
            [
                ["user_1", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_1", "B", "segment_1", "2020-01-01 00:01:00"],
                ["user_1", "C", "segment_1", "2020-01-01 00:02:00"],
                ["user_2", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_2", "B", "segment_1", "2020-01-01 00:03:00"],
                ["user_3", "A", "segment_2", "2020-01-02 00:00:00"],
                ["user_4", "A", "segment_2", "2020-01-02 00:00:00"],
                ["user_4", "B", "segment_2", "2020-01-02 00:01:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )
        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        return Eventstream(df, schema)

    def test_shape_and_schema(self) -> None:
        stream = self._stream()
        result = stream.describe()

        assert result["shape"]["n_events"] == 8
        assert result["shape"]["n_paths"] == 4
        assert result["shape"]["n_unique_events"] == 3

        assert result["schema"]["event_col"] == "event"
        assert result["schema"]["path_col"] == "user_id"
        assert result["schema"]["segment_cols"] == ["segment"]

    def test_date_range(self) -> None:
        result = self._stream().describe()
        assert str(result["date_range"]["min"])[:10] == "2020-01-01"
        assert str(result["date_range"]["max"])[:10] == "2020-01-02"
        assert result["date_range"]["span"] == pd.Timestamp(
            "2020-01-02 00:01:00"
        ) - pd.Timestamp("2020-01-01 00:00:00")

    def test_event_frequency(self) -> None:
        result = self._stream().describe()
        freq = result["event_frequency"]

        assert list(freq["event"]) == ["A", "B", "C"]
        assert freq.loc[freq["event"] == "A", "count"].item() == 4
        assert freq.loc[freq["event"] == "B", "count"].item() == 3
        assert freq.loc[freq["event"] == "C", "count"].item() == 1
        assert freq["share"].sum() == 1.0

    def test_top_events_limits_rows(self) -> None:
        result = self._stream().describe(top_events=1)
        assert len(result["event_frequency"]) == 1
        assert result["event_frequency"]["event"].iloc[0] == "A"

    def test_path_stats(self) -> None:
        result = self._stream().describe()
        stats = result["path_stats"]["user_id"]
        # user_1: 3 events, user_2: 2, user_3: 1, user_4: 2
        assert list(stats.columns) == ["length", "duration"]
        assert stats.loc["count", "length"] == 4
        assert stats.loc["mean", "length"] == 2.0
        assert stats.loc["min", "length"] == 1.0
        assert stats.loc["max", "length"] == 3.0
        assert {"25%", "50%", "75%", "90%", "99%"} <= set(stats.index)

    def test_path_stats_keyed_by_every_path_col(self) -> None:
        df = pd.DataFrame(
            [
                ["user_1", "session_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "session_1", "B", "2020-01-01 00:01:00"],
                ["user_1", "session_2", "A", "2020-01-02 00:00:00"],
                ["user_2", "session_3", "A", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "session_id", "event", "timestamp"],
        )
        schema = {
            "event_cols": ["event"],
            "path_cols": ["user_id", "session_id"],
        }
        stream = Eventstream(df, schema)
        result = stream.describe()

        assert set(result["path_stats"]) == {"user_id", "session_id"}
        # 2 users vs 3 sessions
        assert result["path_stats"]["user_id"].loc["count", "length"] == 2
        assert result["path_stats"]["session_id"].loc["count", "length"] == 3

    def test_segments(self) -> None:
        result = self._stream().describe()
        segments = result["segments"]

        assert list(segments.columns) == ["segment_col", "value", "count", "share"]
        row_1 = segments[segments["value"] == "segment_1"].iloc[0]
        row_2 = segments[segments["value"] == "segment_2"].iloc[0]
        assert row_1["count"] == 5
        assert row_2["count"] == 3
        assert segments["share"].sum() == 1.0

    def test_segments_empty_when_no_segment_cols(self) -> None:
        df = pd.DataFrame(
            [["user_1", "A", "2020-01-01 00:00:00"]],
            columns=["user_id", "event", "timestamp"],
        )
        result = Eventstream(df, {"event_cols": ["event"]}).describe()
        assert result["segments"].empty

    def test_custom_percentiles(self) -> None:
        result = self._stream().describe(percentiles=(0.1, 0.5))
        assert {"10%", "50%"} <= set(result["path_stats"]["user_id"].index)
