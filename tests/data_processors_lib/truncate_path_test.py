from __future__ import annotations

import pandas as pd

from src.data_processors_lib.rete import TruncatePath, TruncatePathParams
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema, RawDataSchema
from src.graph.p_graph import EventsNode, PGraph


class TestTruncatePath:
    def test_truncate_path_apply__before_first(self):
        source_df = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event3", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
                [2, "end", "end", "2022-01-02 00:01:05"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "_deleted"]

        correct_result = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00", True],
                [1, "event1", "raw", "2022-01-01 00:01:00", True],
                [1, "event2", "raw", "2022-01-01 00:01:02", True],
                [1, "event1", "raw", "2022-01-01 00:02:00", True],
                [1, "event1", "raw", "2022-01-01 00:03:00", True],
                [1, "event1", "synthetic", "2022-01-01 00:03:00", True],
                [2, "event1", "raw", "2022-01-02 00:00:00", True],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = TruncatePath(params=TruncatePathParams(drop_before="event3"))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncate_path_apply__before_last(self):
        source_df = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event3", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
                [2, "end", "end", "2022-01-02 00:01:05"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "_deleted"]

        correct_result = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00", True],
                [1, "event1", "raw", "2022-01-01 00:01:00", True],
                [1, "event2", "raw", "2022-01-01 00:01:02", True],
                [1, "event1", "raw", "2022-01-01 00:02:00", True],
                [1, "event1", "raw", "2022-01-01 00:03:00", True],
                [1, "event1", "synthetic", "2022-01-01 00:03:00", True],
                [1, "session_start", "session_start", "2022-01-01 00:03:30", True],
                [1, "event3", "raw", "2022-01-01 00:03:30", True],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30", True],
                [1, "event1", "raw", "2022-01-01 00:04:00", True],
                [2, "event1", "raw", "2022-01-02 00:00:00", True],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = TruncatePath(params=TruncatePathParams(drop_before="event3", occurrence_before="last"))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncate_path_apply__before_first_positive_shift(self):
        source_df = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event3", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
                [2, "end", "end", "2022-01-02 00:01:05"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "_deleted"]

        correct_result = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00", True],
                [1, "event1", "raw", "2022-01-01 00:01:00", True],
                [1, "event2", "raw", "2022-01-01 00:01:02", True],
                [1, "event1", "raw", "2022-01-01 00:02:00", True],
                [1, "event1", "raw", "2022-01-01 00:03:00", True],
                [1, "event1", "synthetic", "2022-01-01 00:03:00", True],
                [1, "session_start", "session_start", "2022-01-01 00:03:30", True],
                [1, "event3", "raw", "2022-01-01 00:03:30", True],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30", True],
                [1, "event1", "raw", "2022-01-01 00:04:00", True],
                [2, "event1", "raw", "2022-01-02 00:00:00", True],
                [2, "event3", "raw", "2022-01-02 00:00:05", True],
                [2, "event2", "raw", "2022-01-02 00:01:05", True],
                [2, "end", "end", "2022-01-02 00:01:05", True],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = TruncatePath(params=TruncatePathParams(drop_before="event3", shift_before=2))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncate_path_apply__before_first_negative_shift(self):
        source_df = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event3", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
                [2, "end", "end", "2022-01-02 00:01:05"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "_deleted"]

        correct_result = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00", True],
                [1, "event1", "raw", "2022-01-01 00:01:00", True],
                [1, "event2", "raw", "2022-01-01 00:01:02", True],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = TruncatePath(params=TruncatePathParams(drop_before="event3", shift_before=-2))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncate_path_apply__before_last_positive_shift(self):
        source_df = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event3", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
                [2, "end", "end", "2022-01-02 00:01:05"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "_deleted"]

        correct_result = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00", True],
                [1, "event1", "raw", "2022-01-01 00:01:00", True],
                [1, "event2", "raw", "2022-01-01 00:01:02", True],
                [1, "event1", "raw", "2022-01-01 00:02:00", True],
                [1, "event1", "raw", "2022-01-01 00:03:00", True],
                [1, "event1", "synthetic", "2022-01-01 00:03:00", True],
                [1, "session_start", "session_start", "2022-01-01 00:03:30", True],
                [1, "event3", "raw", "2022-01-01 00:03:30", True],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30", True],
                [1, "event1", "raw", "2022-01-01 00:04:00", True],
                [1, "event3", "raw", "2022-01-01 00:04:30", True],
                [1, "event1", "raw", "2022-01-01 00:05:00", True],
                [2, "event1", "raw", "2022-01-02 00:00:00", True],
                [2, "event3", "raw", "2022-01-02 00:00:05", True],
                [2, "event2", "raw", "2022-01-02 00:01:05", True],
                [2, "end", "end", "2022-01-02 00:01:05", True],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = TruncatePath(params=TruncatePathParams(drop_before="event3", occurrence_before="last", shift_before=2))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncate_path_apply__before_last_negative_shift(self):
        source_df = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event3", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
                [2, "end", "end", "2022-01-02 00:01:05"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "_deleted"]

        correct_result = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00", True],
                [1, "event1", "raw", "2022-01-01 00:01:00", True],
                [1, "event2", "raw", "2022-01-01 00:01:02", True],
                [1, "event1", "raw", "2022-01-01 00:02:00", True],
                [1, "event1", "raw", "2022-01-01 00:03:00", True],
                [1, "event1", "synthetic", "2022-01-01 00:03:00", True],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = TruncatePath(
            params=TruncatePathParams(drop_before="event3", occurrence_before="last", shift_before=-2)
        )

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncate_path_apply__after_first(self):
        source_df = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event3", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
                [2, "end", "end", "2022-01-02 00:01:05"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "_deleted"]

        correct_result = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:04:00", True],
                [1, "event3", "raw", "2022-01-01 00:04:30", True],
                [1, "event1", "raw", "2022-01-01 00:05:00", True],
                [2, "event2", "raw", "2022-01-02 00:01:05", True],
                [2, "end", "end", "2022-01-02 00:01:05", True],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = TruncatePath(params=TruncatePathParams(drop_after="event3"))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncate_path_apply__after_last(self):
        source_df = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event3", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
                [2, "end", "end", "2022-01-02 00:01:05"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "_deleted"]

        correct_result = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:05:00", True],
                [2, "event2", "raw", "2022-01-02 00:01:05", True],
                [2, "end", "end", "2022-01-02 00:01:05", True],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = TruncatePath(params=TruncatePathParams(drop_after="event3", occurrence_after="last"))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncate_path_apply__after_first_positive_shift(self):
        source_df = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event3", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
                [2, "end", "end", "2022-01-02 00:01:05"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "_deleted"]

        correct_result = pd.DataFrame(
            [[1, "event1", "raw", "2022-01-01 00:05:00", True]], columns=correct_result_columns
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = TruncatePath(params=TruncatePathParams(drop_after="event3", shift_after=2))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncate_path_apply__after_first_negative_shift(self):
        source_df = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event3", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
                [2, "end", "end", "2022-01-02 00:01:05"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "_deleted"]

        correct_result = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:03:00", True],
                [1, "event1", "synthetic", "2022-01-01 00:03:00", True],
                [1, "session_start", "session_start", "2022-01-01 00:03:30", True],
                [1, "event3", "raw", "2022-01-01 00:03:30", True],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30", True],
                [1, "event1", "raw", "2022-01-01 00:04:00", True],
                [1, "event3", "raw", "2022-01-01 00:04:30", True],
                [1, "event1", "raw", "2022-01-01 00:05:00", True],
                [2, "event1", "raw", "2022-01-02 00:00:00", True],
                [2, "event3", "raw", "2022-01-02 00:00:05", True],
                [2, "event2", "raw", "2022-01-02 00:01:05", True],
                [2, "end", "end", "2022-01-02 00:01:05", True],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = TruncatePath(params=TruncatePathParams(drop_after="event3", shift_after=-2))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncate_path_apply__after_last_positive_shift(self):
        source_df = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event3", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
                [2, "end", "end", "2022-01-02 00:01:05"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "_deleted"]

        correct_result = pd.DataFrame([], columns=correct_result_columns)

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = TruncatePath(params=TruncatePathParams(drop_after="event3", occurrence_after="last", shift_after=2))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncate_path_apply__after_last_negative_shift(self):
        source_df = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event3", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
                [2, "end", "end", "2022-01-02 00:01:05"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "_deleted"]

        correct_result = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:04:00", True],
                [1, "event3", "raw", "2022-01-01 00:04:30", True],
                [1, "event1", "raw", "2022-01-01 00:05:00", True],
                [2, "event1", "raw", "2022-01-02 00:00:00", True],
                [2, "event3", "raw", "2022-01-02 00:00:05", True],
                [2, "event2", "raw", "2022-01-02 00:01:05", True],
                [2, "end", "end", "2022-01-02 00:01:05", True],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = TruncatePath(params=TruncatePathParams(drop_after="event3", occurrence_after="last", shift_after=-2))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncate_path_apply__before_after_first(self):
        source_df = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [1, "event1", "raw", "2022-01-02 00:00:00"],
                [1, "event3", "raw", "2022-01-02 00:00:05"],
                [1, "event5", "raw", "2022-01-02 00:01:05"],
                [1, "end", "end", "2022-01-02 00:01:05"],
                [1, "event1", "raw", "2022-01-02 00:01:10"],
                [1, "event1", "raw", "2022-01-02 00:02:05"],
                [1, "event4", "raw", "2022-01-02 00:03:05"],
                [1, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "_deleted"]

        correct_result = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00", True],
                [1, "event1", "raw", "2022-01-01 00:01:00", True],
                [1, "event2", "raw", "2022-01-01 00:01:02", True],
                [1, "event1", "raw", "2022-01-01 00:02:00", True],
                [1, "event1", "raw", "2022-01-01 00:03:00", True],
                [1, "event1", "synthetic", "2022-01-01 00:03:00", True],
                [1, "event1", "raw", "2022-01-02 00:01:10", True],
                [1, "event1", "raw", "2022-01-02 00:02:05", True],
                [1, "event4", "raw", "2022-01-02 00:03:05", True],
                [1, "end", "end", "2022-01-02 00:03:05", True],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = TruncatePath(params=TruncatePathParams(drop_before="event3", drop_after="event5"))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)


class TestTruncatePathGraph:
    def test_truncate_path_graph__before_after_first(self):
        source_df = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [1, "event1", "raw", "2022-01-02 00:00:00"],
                [1, "event3", "raw", "2022-01-02 00:00:05"],
                [1, "event5", "raw", "2022-01-02 00:01:05"],
                [1, "end", "end", "2022-01-02 00:01:05"],
                [1, "event1", "raw", "2022-01-02 00:01:10"],
                [1, "event1", "raw", "2022-01-02 00:02:05"],
                [1, "event4", "raw", "2022-01-02 00:03:05"],
                [1, "end", "end", "2022-01-02 00:03:05"],
                [2, "event1", "raw", "2022-01-02 00:01:10"],
                [2, "event1", "raw", "2022-01-02 00:02:05"],
                [2, "event4", "raw", "2022-01-02 00:03:05"],
                [2, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp"]

        correct_result = pd.DataFrame(
            [
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [1, "event1", "raw", "2022-01-02 00:00:00"],
                [1, "event3", "raw", "2022-01-02 00:00:05"],
                [1, "event5", "raw", "2022-01-02 00:01:05"],
                [1, "end", "end", "2022-01-02 00:01:05"],
                [2, "event1", "raw", "2022-01-02 00:01:10"],
                [2, "event1", "raw", "2022-01-02 00:02:05"],
                [2, "event4", "raw", "2022-01-02 00:03:05"],
                [2, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = EventsNode(TruncatePath(params=TruncatePathParams(drop_before="event3", drop_after="event5")))
        graph = PGraph(source_stream=source)
        graph.add_node(node=events, parents=[graph.root])

        result = graph.combine(node=events)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncate_path_graph__inversed_bounds(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:00:00"],
                [1, "event2", "raw", "2022-01-01 00:01:00"],
                [1, "event3", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event2", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:05:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp"]
        correct_result = pd.DataFrame([], columns=correct_result_columns)

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = EventsNode(
            TruncatePath(
                params=TruncatePathParams(
                    drop_before="event3",
                    occurrence_before="first",
                    shift_before=2,
                    drop_after="event3",
                    occurrence_after="last",
                    shift_after=-2,
                )
            )
        )
        graph = PGraph(source_stream=source)
        graph.add_node(node=events, parents=[graph.root])

        result = graph.combine(node=events)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncate_path_graph__irrelevant_before_event(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:00:00"],
                [1, "event2", "raw", "2022-01-01 00:01:00"],
                [1, "event3", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event2", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:05:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp"]
        correct_result = pd.DataFrame(source_df, copy=True)
        correct_result.columns = correct_result_columns

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = EventsNode(TruncatePath(params=TruncatePathParams(drop_before="missing_event")))

        graph = PGraph(source_stream=source)
        graph.add_node(node=events, parents=[graph.root])

        result = graph.combine(node=events)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncate_path_graph__irrelevant_after_event(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:00:00"],
                [1, "event2", "raw", "2022-01-01 00:01:00"],
                [1, "event3", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event2", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:05:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp"]
        correct_result = pd.DataFrame(source_df, copy=True)
        correct_result.columns = correct_result_columns

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = EventsNode(TruncatePath(params=TruncatePathParams(drop_after="missing_event")))

        graph = PGraph(source_stream=source)
        graph.add_node(node=events, parents=[graph.root])

        result = graph.combine(node=events)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)


class TestTruncatePathHelper:
    def test_truncate_path_graph__before_after_first(self):
        source_df = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [1, "event1", "raw", "2022-01-02 00:00:00"],
                [1, "event3", "raw", "2022-01-02 00:00:05"],
                [1, "event5", "raw", "2022-01-02 00:01:05"],
                [1, "end", "end", "2022-01-02 00:01:05"],
                [1, "event1", "raw", "2022-01-02 00:01:10"],
                [1, "event1", "raw", "2022-01-02 00:02:05"],
                [1, "event4", "raw", "2022-01-02 00:03:05"],
                [1, "end", "end", "2022-01-02 00:03:05"],
                [2, "event1", "raw", "2022-01-02 00:01:10"],
                [2, "event1", "raw", "2022-01-02 00:02:05"],
                [2, "event4", "raw", "2022-01-02 00:03:05"],
                [2, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp"]

        correct_result = pd.DataFrame(
            [
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [1, "event1", "raw", "2022-01-02 00:00:00"],
                [1, "event3", "raw", "2022-01-02 00:00:05"],
                [1, "event5", "raw", "2022-01-02 00:01:05"],
                [1, "end", "end", "2022-01-02 00:01:05"],
                [2, "event1", "raw", "2022-01-02 00:01:10"],
                [2, "event1", "raw", "2022-01-02 00:02:05"],
                [2, "event4", "raw", "2022-01-02 00:03:05"],
                [2, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )

        result = source.truncate_path(drop_before="event3", drop_after="event5")
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncate_path_graph__inversed_bounds(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:00:00"],
                [1, "event2", "raw", "2022-01-01 00:01:00"],
                [1, "event3", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event2", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:05:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp"]
        correct_result = pd.DataFrame([], columns=correct_result_columns)

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        result = source.truncate_path(
            drop_before="event3",
            occurrence_before="first",
            shift_before=2,
            drop_after="event3",
            occurrence_after="last",
            shift_after=-2,
        )
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncate_path_graph__irrelevant_before_event(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:00:00"],
                [1, "event2", "raw", "2022-01-01 00:01:00"],
                [1, "event3", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event2", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:05:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp"]
        correct_result = pd.DataFrame(source_df, copy=True)
        correct_result.columns = correct_result_columns

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = EventsNode(TruncatePath(params=TruncatePathParams(drop_before="missing_event")))

        graph = PGraph(source_stream=source)
        graph.add_node(node=events, parents=[graph.root])

        result = source.truncate_path(drop_before="missing_event")
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncate_path_graph__irrelevant_after_event(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:00:00"],
                [1, "event2", "raw", "2022-01-01 00:01:00"],
                [1, "event3", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event2", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:05:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp"]
        correct_result = pd.DataFrame(source_df, copy=True)
        correct_result.columns = correct_result_columns

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )

        result = source.truncate_path(drop_after="missing_event")
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)
