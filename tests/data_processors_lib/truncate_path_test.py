from __future__ import annotations

import pandas as pd

from retentioneering.data_processors_lib import TruncatePaths, TruncatePathsParams
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.eventstream.schema import RawDataSchema
from tests.data_processors_lib.common import ApplyTestBase, GraphTestBase


class TestTruncatePaths(ApplyTestBase):
    _Processor = TruncatePaths
    _source_df_1 = pd.DataFrame(
        [
            [1, "path_start", "path_start", "2022-01-01 00:01:00"],
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
            [2, "path_end", "path_end", "2022-01-02 00:01:05"],
            [3, "event1", "raw", "2022-01-02 00:01:10"],
            [3, "event1", "raw", "2022-01-02 00:02:05"],
            [3, "event4", "raw", "2022-01-02 00:03:05"],
            [3, "path_end", "path_end", "2022-01-02 00:03:05"],
        ],
        columns=["user_id", "event", "event_type", "timestamp"],
    )
    _source_df_2 = pd.DataFrame(
        [
            [1, "path_start", "path_start", "2022-01-01 00:01:00"],
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
            [1, "path_end", "path_end", "2022-01-02 00:01:05"],
            [1, "event1", "raw", "2022-01-02 00:01:10"],
            [1, "event1", "raw", "2022-01-02 00:02:05"],
            [1, "event4", "raw", "2022-01-02 00:03:05"],
            [1, "path_end", "path_end", "2022-01-02 00:03:05"],
        ],
        columns=["user_id", "event", "event_type", "timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_type="event_type",
        event_timestamp="timestamp",
    )

    def test_truncate_paths_apply__before_first(self):
        actual = self._apply(
            TruncatePathsParams(
                drop_before="event3",
            ),
            source_df=self._source_df_1,
        )
        expected = pd.DataFrame(
            [
                [1, "path_start", "path_start", "2022-01-01 00:01:00", True],
                [1, "event1", "raw", "2022-01-01 00:01:00", True],
                [1, "event2", "raw", "2022-01-01 00:01:02", True],
                [1, "event1", "raw", "2022-01-01 00:02:00", True],
                [1, "event1", "raw", "2022-01-01 00:03:00", True],
                [1, "event1", "synthetic", "2022-01-01 00:03:00", True],
                [2, "event1", "raw", "2022-01-02 00:00:00", True],
            ],
            columns=["user_id", "event", "event_type", "timestamp", "_deleted"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncate_paths_apply__before_last(self):
        actual = self._apply(
            TruncatePathsParams(
                drop_before="event3",
                occurrence_before="last",
            ),
            source_df=self._source_df_1,
        )
        expected = pd.DataFrame(
            [
                [1, "path_start", "path_start", "2022-01-01 00:01:00", True],
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
            columns=["user_id", "event", "event_type", "timestamp", "_deleted"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncate_paths_apply__before_first_positive_shift(self):
        actual = self._apply(
            TruncatePathsParams(
                drop_before="event3",
                shift_before=2,
            ),
            source_df=self._source_df_1,
        )
        expected = pd.DataFrame(
            [
                [1, "path_start", "path_start", "2022-01-01 00:01:00", True],
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
                [2, "path_end", "path_end", "2022-01-02 00:01:05", True],
            ],
            columns=["user_id", "event", "event_type", "timestamp", "_deleted"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncate_paths_apply__before_first_negative_shift(self):
        actual = self._apply(
            TruncatePathsParams(
                drop_before="event3",
                shift_before=-2,
            ),
            source_df=self._source_df_1,
        )
        expected = pd.DataFrame(
            [
                [1, "path_start", "path_start", "2022-01-01 00:01:00", True],
                [1, "event1", "raw", "2022-01-01 00:01:00", True],
                [1, "event2", "raw", "2022-01-01 00:01:02", True],
            ],
            columns=["user_id", "event", "event_type", "timestamp", "_deleted"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncate_paths_apply__before_last_positive_shift(self):
        actual = self._apply(
            TruncatePathsParams(
                drop_before="event3",
                occurrence_before="last",
                shift_before=2,
            ),
            source_df=self._source_df_1,
        )
        expected = pd.DataFrame(
            [
                [1, "path_start", "path_start", "2022-01-01 00:01:00", True],
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
                [2, "path_end", "path_end", "2022-01-02 00:01:05", True],
            ],
            columns=["user_id", "event", "event_type", "timestamp", "_deleted"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncate_paths_apply__before_last_negative_shift(self):
        actual = self._apply(
            TruncatePathsParams(
                drop_before="event3",
                occurrence_before="last",
                shift_before=-2,
            ),
            source_df=self._source_df_1,
        )
        expected = pd.DataFrame(
            [
                [1, "path_start", "path_start", "2022-01-01 00:01:00", True],
                [1, "event1", "raw", "2022-01-01 00:01:00", True],
                [1, "event2", "raw", "2022-01-01 00:01:02", True],
                [1, "event1", "raw", "2022-01-01 00:02:00", True],
                [1, "event1", "raw", "2022-01-01 00:03:00", True],
                [1, "event1", "synthetic", "2022-01-01 00:03:00", True],
            ],
            columns=["user_id", "event", "event_type", "timestamp", "_deleted"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncate_paths_apply__after_first(self):
        actual = self._apply(
            TruncatePathsParams(
                drop_after="event3",
            ),
            source_df=self._source_df_1,
        )
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:04:00", True],
                [1, "event3", "raw", "2022-01-01 00:04:30", True],
                [1, "event1", "raw", "2022-01-01 00:05:00", True],
                [2, "event2", "raw", "2022-01-02 00:01:05", True],
                [2, "path_end", "path_end", "2022-01-02 00:01:05", True],
            ],
            columns=["user_id", "event", "event_type", "timestamp", "_deleted"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncate_paths_apply__after_last(self):
        actual = self._apply(
            TruncatePathsParams(
                drop_after="event3",
                occurrence_after="last",
            ),
            source_df=self._source_df_1,
        )
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:05:00", True],
                [2, "event2", "raw", "2022-01-02 00:01:05", True],
                [2, "path_end", "path_end", "2022-01-02 00:01:05", True],
            ],
            columns=["user_id", "event", "event_type", "timestamp", "_deleted"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncate_paths_apply__after_first_positive_shift(self):
        actual = self._apply(
            TruncatePathsParams(
                drop_after="event3",
                shift_after=2,
            ),
            source_df=self._source_df_1,
        )
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:05:00", True],
            ],
            columns=["user_id", "event", "event_type", "timestamp", "_deleted"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncate_paths_apply__after_first_negative_shift(self):
        actual = self._apply(
            TruncatePathsParams(
                drop_after="event3",
                shift_after=-2,
            ),
            source_df=self._source_df_1,
        )
        expected = pd.DataFrame(
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
                [2, "path_end", "path_end", "2022-01-02 00:01:05", True],
            ],
            columns=["user_id", "event", "event_type", "timestamp", "_deleted"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncate_paths_apply__after_last_positive_shift(self):
        actual = self._apply(
            TruncatePathsParams(
                drop_after="event3",
                occurrence_after="last",
                shift_after=2,
            ),
            source_df=self._source_df_1,
        )
        expected = pd.DataFrame([], columns=["user_id", "event", "event_type", "timestamp", "_deleted"])
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncate_paths_apply__after_last_negative_shift(self):
        actual = self._apply(
            TruncatePathsParams(
                drop_after="event3",
                occurrence_after="last",
                shift_after=-2,
            ),
            source_df=self._source_df_1,
        )
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:04:00", True],
                [1, "event3", "raw", "2022-01-01 00:04:30", True],
                [1, "event1", "raw", "2022-01-01 00:05:00", True],
                [2, "event1", "raw", "2022-01-02 00:00:00", True],
                [2, "event3", "raw", "2022-01-02 00:00:05", True],
                [2, "event2", "raw", "2022-01-02 00:01:05", True],
                [2, "path_end", "path_end", "2022-01-02 00:01:05", True],
            ],
            columns=["user_id", "event", "event_type", "timestamp", "_deleted"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncate_paths_apply__before_after_first(self):
        actual = self._apply(
            TruncatePathsParams(
                drop_before="event3",
                drop_after="event5",
            ),
            source_df=self._source_df_2,
        )
        expected = pd.DataFrame(
            [
                [1, "path_start", "path_start", "2022-01-01 00:01:00", True],
                [1, "event1", "raw", "2022-01-01 00:01:00", True],
                [1, "event2", "raw", "2022-01-01 00:01:02", True],
                [1, "event1", "raw", "2022-01-01 00:02:00", True],
                [1, "event1", "raw", "2022-01-01 00:03:00", True],
                [1, "event1", "synthetic", "2022-01-01 00:03:00", True],
                [1, "event1", "raw", "2022-01-02 00:01:10", True],
                [1, "event1", "raw", "2022-01-02 00:02:05", True],
                [1, "event4", "raw", "2022-01-02 00:03:05", True],
                [1, "path_end", "path_end", "2022-01-02 00:03:05", True],
            ],
            columns=["user_id", "event", "event_type", "timestamp", "_deleted"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)


class TestTruncatePathsGraph(GraphTestBase):
    _Processor = TruncatePaths
    _source_df_1 = pd.DataFrame(
        [
            [1, "path_start", "path_start", "2022-01-01 00:01:00"],
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
            [1, "path_end", "path_end", "2022-01-02 00:01:05"],
            [1, "event1", "raw", "2022-01-02 00:01:10"],
            [1, "event1", "raw", "2022-01-02 00:02:05"],
            [1, "event4", "raw", "2022-01-02 00:03:05"],
            [1, "path_end", "path_end", "2022-01-02 00:03:05"],
            [2, "event1", "raw", "2022-01-02 00:01:10"],
            [2, "event1", "raw", "2022-01-02 00:02:05"],
            [2, "event4", "raw", "2022-01-02 00:03:05"],
            [2, "path_end", "path_end", "2022-01-02 00:03:05"],
        ],
        columns=["user_id", "event", "event_type", "timestamp"],
    )
    _source_df_2 = pd.DataFrame(
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
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_type="event_type",
        event_timestamp="timestamp",
    )

    def test_truncate_paths_graph__before_after_first(self):
        actual = self._apply(
            TruncatePathsParams(
                drop_before="event3",
                drop_after="event5",
            ),
            source_df=self._source_df_1,
        )
        expected = pd.DataFrame(
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
                [1, "path_end", "path_end", "2022-01-02 00:01:05"],
                [2, "event1", "raw", "2022-01-02 00:01:10"],
                [2, "event1", "raw", "2022-01-02 00:02:05"],
                [2, "event4", "raw", "2022-01-02 00:03:05"],
                [2, "path_end", "path_end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncate_paths_graph__inversed_bounds(self):
        actual = self._apply(
            TruncatePathsParams(
                drop_before="event3",
                occurrence_before="first",
                shift_before=2,
                drop_after="event3",
                occurrence_after="last",
                shift_after=-2,
            ),
            source_df=self._source_df_2,
        )
        expected = pd.DataFrame([], columns=["user_id", "event", "event_type", "timestamp"])
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncate_paths_graph__irrelevant_before_event(self):
        actual = self._apply(
            TruncatePathsParams(
                drop_before="missing_event",
            ),
            source_df=self._source_df_2,
        )
        expected = pd.DataFrame(
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
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncate_paths_graph__irrelevant_after_event(self):
        actual = self._apply(
            TruncatePathsParams(
                drop_after="missing_event",
            ),
            source_df=self._source_df_2,
        )
        expected = pd.DataFrame(
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
        assert actual[expected.columns].compare(expected).shape == (0, 0)


class TestTruncatePathsHelper:
    def test_truncate_paths_graph__before_after_first(self):
        source_df = pd.DataFrame(
            [
                [1, "path_start", "path_start", "2022-01-01 00:01:00"],
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
                [1, "path_end", "path_end", "2022-01-02 00:01:05"],
                [1, "event1", "raw", "2022-01-02 00:01:10"],
                [1, "event1", "raw", "2022-01-02 00:02:05"],
                [1, "event4", "raw", "2022-01-02 00:03:05"],
                [1, "path_end", "path_end", "2022-01-02 00:03:05"],
                [2, "event1", "raw", "2022-01-02 00:01:10"],
                [2, "event1", "raw", "2022-01-02 00:02:05"],
                [2, "event4", "raw", "2022-01-02 00:03:05"],
                [2, "path_end", "path_end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]

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
                [1, "path_end", "path_end", "2022-01-02 00:01:05"],
                [2, "event1", "raw", "2022-01-02 00:01:10"],
                [2, "event1", "raw", "2022-01-02 00:02:05"],
                [2, "event4", "raw", "2022-01-02 00:03:05"],
                [2, "path_end", "path_end", "2022-01-02 00:03:05"],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(source_df)

        result = source.truncate_paths(drop_before="event3", drop_after="event5")
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncate_paths_graph__inversed_bounds(self):
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

        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]
        correct_result = pd.DataFrame([], columns=correct_result_columns)

        source = Eventstream(source_df)
        result = source.truncate_paths(
            drop_before="event3",
            occurrence_before="first",
            shift_before=2,
            drop_after="event3",
            occurrence_after="last",
            shift_after=-2,
        )
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncate_paths_graph__irrelevant_before_event(self):
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

        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]
        correct_result = pd.DataFrame(source_df, copy=True)
        correct_result.columns = correct_result_columns

        source = Eventstream(source_df)

        result = source.truncate_paths(drop_before="missing_event")
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncate_paths_graph__irrelevant_after_event(self):
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

        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]
        correct_result = pd.DataFrame(source_df, copy=True)
        correct_result.columns = correct_result_columns

        source = Eventstream(source_df)

        result = source.truncate_paths(drop_after="missing_event")
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)
