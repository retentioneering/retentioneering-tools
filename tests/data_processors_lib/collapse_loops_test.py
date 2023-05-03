from __future__ import annotations

import pandas as pd

from retentioneering.data_processors_lib import CollapseLoops, CollapseLoopsParams
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.eventstream.schema import RawDataSchema
from tests.data_processors_lib.common import ApplyTestBase, GraphTestBase


class TestCollapseLoops(ApplyTestBase):
    _Processor = CollapseLoops
    _source_df = pd.DataFrame(
        [
            [1, "event1", "2022-01-01 00:01:00"],
            [1, "event1", "2022-01-01 00:02:00"],
            [1, "event2", "2022-01-01 00:01:02"],
            [1, "event1", "2022-01-01 00:03:00"],
            [1, "event1", "2022-01-01 00:04:00"],
            [1, "event1", "2022-01-01 00:05:00"],
            [2, "event1", "2022-01-02 00:00:00"],
            [2, "event1", "2022-01-02 00:00:05"],
            [2, "event2", "2022-01-02 00:00:05"],
        ],
        columns=["user_id", "event", "timestamp"],
    )

    _source_df_custom_cols = pd.DataFrame(
        [
            [1, "event1", "2022-01-01 00:01:00", None, 1],
            [1, "event1", "2022-01-01 00:02:00", "A", 2],
            [1, "event2", "2022-01-01 00:02:02", "A", None],
            [1, "event1", "2022-01-01 00:03:00", "A", None],
            [1, "event1", "2022-01-01 00:04:00", "A", 2],
            [1, "event1", "2022-01-01 00:05:00", "A", 2],
            [2, "event1", "2022-01-02 00:00:00", "A", 5],
            [2, "event1", "2022-01-02 00:00:05", "B", 10],
            [2, "event2", "2022-01-02 00:00:05", "A", 1],
        ],
        columns=["user_id", "event", "timestamp", "str_col", "num_col"],
    )

    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
    )

    _raw_data_schema_custom_cols = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
        custom_cols=[
            {"raw_data_col": "str_col", "custom_col": "str_col"},
            {"raw_data_col": "num_col", "custom_col": "num_col"},
        ],
    )

    def test_collapse_loops_apply__suffix_count__agg_min(self):
        expected = pd.DataFrame(
            [
                [1, "event1_loop_4", "group_alias", "2022-01-01 00:02:00", False],
                [1, "event1", "raw", "2022-01-01 00:02:00", True],
                [1, "event1", "raw", "2022-01-01 00:03:00", True],
                [1, "event1", "raw", "2022-01-01 00:04:00", True],
                [1, "event1", "raw", "2022-01-01 00:05:00", True],
                [2, "event1_loop_2", "group_alias", "2022-01-02 00:00:00", False],
                [2, "event1", "raw", "2022-01-02 00:00:00", True],
                [2, "event1", "raw", "2022-01-02 00:00:05", True],
            ],
            columns=["user_id", "event", "event_type", "timestamp", "_deleted"],
        )
        expected["timestamp"] = pd.to_datetime(expected["timestamp"])

        actual = self._apply(
            CollapseLoopsParams(
                suffix="count",
                time_agg="min",
            )
        )
        assert pd.testing.assert_frame_equal(actual[expected.columns], expected) is None

    def test_collapse_loops_apply__suffix_count__agg_max(self):
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:02:00", True],
                [1, "event1", "raw", "2022-01-01 00:03:00", True],
                [1, "event1", "raw", "2022-01-01 00:04:00", True],
                [1, "event1_loop_4", "group_alias", "2022-01-01 00:05:00", False],
                [1, "event1", "raw", "2022-01-01 00:05:00", True],
                [2, "event1", "raw", "2022-01-02 00:00:00", True],
                [2, "event1_loop_2", "group_alias", "2022-01-02 00:00:05", False],
                [2, "event1", "raw", "2022-01-02 00:00:05", True],
            ],
            columns=["user_id", "event", "event_type", "timestamp", "_deleted"],
        )
        expected["timestamp"] = pd.to_datetime(expected["timestamp"])

        actual = self._apply(
            CollapseLoopsParams(
                suffix="count",
                time_agg="max",
            )
        )
        assert pd.testing.assert_frame_equal(actual[expected.columns], expected) is None

    def test_collapse_loops_apply__suffix_loop__agg_mean(self):
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:02:00", True],
                [1, "event1", "raw", "2022-01-01 00:03:00", True],
                [1, "event1_loop", "group_alias", "2022-01-01 00:03:30", False],
                [1, "event1", "raw", "2022-01-01 00:04:00", True],
                [1, "event1", "raw", "2022-01-01 00:05:00", True],
                [2, "event1", "raw", "2022-01-02 00:00:00", True],
                [2, "event1_loop", "group_alias", "2022-01-02 00:00:02.5", False],
                [2, "event1", "raw", "2022-01-02 00:00:05", True],
            ],
            columns=["user_id", "event", "event_type", "timestamp", "_deleted"],
        )
        expected["timestamp"] = pd.to_datetime(expected["timestamp"])

        actual = self._apply(
            CollapseLoopsParams(
                suffix="loop",
                time_agg="mean",
            )
        )
        assert pd.testing.assert_frame_equal(actual[expected.columns], expected) is None

    def test_collapse_loops_apply__default_custom_cols(self):
        expected = pd.DataFrame(
            [
                [1, "event1", "group_alias", "2022-01-01 00:01:00", "A", 1.5, False],
                [1, "event1", "raw", "2022-01-01 00:01:00", None, 1, True],
                [1, "event1", "raw", "2022-01-01 00:02:00", "A", 2, True],
                [1, "event1", "group_alias", "2022-01-01 00:03:00", "A", 2, False],
                [1, "event1", "raw", "2022-01-01 00:03:00", "A", None, True],
                [1, "event1", "raw", "2022-01-01 00:04:00", "A", 2, True],
                [1, "event1", "raw", "2022-01-01 00:05:00", "A", 2, True],
                [2, "event1", "group_alias", "2022-01-02 00:00:00", None, 7.5, False],
                [2, "event1", "raw", "2022-01-02 00:00:00", "A", 5, True],
                [2, "event1", "raw", "2022-01-02 00:00:05", "B", 10, True],
            ],
            columns=["user_id", "event", "event_type", "timestamp", "str_col", "num_col", "_deleted"],
        )
        expected["timestamp"] = pd.to_datetime(expected["timestamp"])

        actual = self._apply(
            CollapseLoopsParams(),
            source_df=self._source_df_custom_cols,
            raw_data_schema=self._raw_data_schema_custom_cols,
        )
        assert pd.testing.assert_frame_equal(actual[expected.columns], expected) is None


class TestCollapseLoopsGraph(GraphTestBase):
    _Processor = CollapseLoops
    _source_df = pd.DataFrame(
        [
            [1, "event1", "2022-01-01 00:01:00"],
            [1, "event1", "2022-01-01 00:02:00"],
            [1, "event2", "2022-01-01 00:01:02"],
            [1, "event1", "2022-01-01 00:03:00"],
            [1, "event1", "2022-01-01 00:04:00"],
            [1, "event1", "2022-01-01 00:05:00"],
            [2, "event1", "2022-01-02 00:00:00"],
            [2, "event1", "2022-01-02 00:00:05"],
            [2, "event2", "2022-01-02 00:00:05"],
        ],
        columns=["user_id", "event", "timestamp"],
    )

    _source_df_custom_cols = pd.DataFrame(
        [
            [1, "event1", "2022-01-01 00:01:00", None, 1],
            [1, "event1", "2022-01-01 00:02:00", "A", 2],
            [1, "event2", "2022-01-01 00:02:02", "A", None],
            [1, "event1", "2022-01-01 00:03:00", "A", None],
            [1, "event1", "2022-01-01 00:04:00", "A", 2],
            [1, "event1", "2022-01-01 00:05:00", "A", 2],
            [2, "event1", "2022-01-02 00:00:00", "A", 5],
            [2, "event1", "2022-01-02 00:00:05", "B", 10],
            [2, "event2", "2022-01-02 00:00:05", "A", 1],
        ],
        columns=["user_id", "event", "timestamp", "str_col", "num_col"],
    )

    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
    )

    _raw_data_schema_custom_cols = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
        custom_cols=[
            {"raw_data_col": "str_col", "custom_col": "str_col"},
            {"raw_data_col": "num_col", "custom_col": "num_col"},
        ],
    )

    def test_collapse_loops_graph__suffix_count__agg_min(self):
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1_loop_4", "group_alias", "2022-01-01 00:02:00"],
                [2, "event1_loop_2", "group_alias", "2022-01-02 00:00:00"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        expected["timestamp"] = pd.to_datetime(expected["timestamp"])

        actual = self._apply(
            CollapseLoopsParams(
                suffix="count",
                time_agg="min",
            )
        )
        assert pd.testing.assert_frame_equal(actual[expected.columns], expected) is None

    def test_collapse_loops_graph__suffix_count__agg_max(self):
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1_loop_4", "group_alias", "2022-01-01 00:05:00"],
                [2, "event1_loop_2", "group_alias", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        expected["timestamp"] = pd.to_datetime(expected["timestamp"])

        actual = self._apply(
            CollapseLoopsParams(
                suffix="count",
                time_agg="max",
            )
        )
        assert pd.testing.assert_frame_equal(actual[expected.columns], expected) is None

    def test_collapse_loops_graph__suffix_loop__agg_mean(self):
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1_loop", "group_alias", "2022-01-01 00:03:30"],
                [2, "event1_loop", "group_alias", "2022-01-02 00:00:02.5"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        expected["timestamp"] = pd.to_datetime(expected["timestamp"])

        actual = self._apply(
            CollapseLoopsParams(
                suffix="loop",
                time_agg="mean",
            )
        )

        assert pd.testing.assert_frame_equal(actual[expected.columns], expected) is None

    def test_collapse_loops_graph__suffix_none__agg_mean(self):
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "group_alias", "2022-01-01 00:03:30"],
                [2, "event1", "group_alias", "2022-01-02 00:00:02.5"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        expected["timestamp"] = pd.to_datetime(expected["timestamp"])

        actual = self._apply(
            CollapseLoopsParams(
                suffix=None,
                time_agg="mean",
            )
        )

        assert pd.testing.assert_frame_equal(actual[expected.columns], expected) is None

    def test_collapse_loops_graph__default_custom_cols(self):
        expected = pd.DataFrame(
            [
                [1, "event1", "group_alias", "2022-01-01 00:01:00", "A", 1.5],
                [1, "event2", "raw", "2022-01-01 00:02:02", "A", None],
                [1, "event1", "group_alias", "2022-01-01 00:03:00", "A", 2],
                [2, "event1", "group_alias", "2022-01-02 00:00:00", None, 7.5],
                [2, "event2", "raw", "2022-01-02 00:00:05", "A", 1],
            ],
            columns=["user_id", "event", "event_type", "timestamp", "str_col", "num_col"],
        )
        expected["timestamp"] = pd.to_datetime(expected["timestamp"])

        actual = self._apply(
            CollapseLoopsParams(),
            source_df=self._source_df_custom_cols,
            raw_data_schema=self._raw_data_schema_custom_cols,
        )
        assert pd.testing.assert_frame_equal(actual[expected.columns], expected) is None


class TestCollapseLoopsHelper:
    def test_collapse_loops_graph__suffix_count__agg_min(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:01:00"],
                [1, "event1", "2022-01-01 00:02:00"],
                [1, "event2", "2022-01-01 00:01:02"],
                [1, "event1", "2022-01-01 00:03:00"],
                [1, "event1", "2022-01-01 00:04:00"],
                [1, "event1", "2022-01-01 00:05:00"],
                [2, "event1", "2022-01-02 00:00:00"],
                [2, "event1", "2022-01-02 00:00:05"],
                [2, "event2", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        source = Eventstream(source_df)

        expected_columns = ["user_id", "event", "event_type", "timestamp"]
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1_loop_4", "group_alias", "2022-01-01 00:02:00"],
                [2, "event1_loop_2", "group_alias", "2022-01-02 00:00:00"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=expected_columns,
        )
        expected["timestamp"] = pd.to_datetime(expected["timestamp"])

        params = {"suffix": "count", "time_agg": "min"}
        actual = source.collapse_loops(**params).to_dataframe()[expected_columns].reset_index(drop=True)

        assert pd.testing.assert_frame_equal(actual, expected) is None

    def test_collapse_loops_graph__suffix_count__agg_max(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:01:00"],
                [1, "event1", "2022-01-01 00:02:00"],
                [1, "event2", "2022-01-01 00:01:02"],
                [1, "event1", "2022-01-01 00:03:00"],
                [1, "event1", "2022-01-01 00:04:00"],
                [1, "event1", "2022-01-01 00:05:00"],
                [2, "event1", "2022-01-02 00:00:00"],
                [2, "event1", "2022-01-02 00:00:05"],
                [2, "event2", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        source = Eventstream(source_df)

        expected_columns = ["user_id", "event", "event_type", "timestamp"]
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1_loop_4", "group_alias", "2022-01-01 00:05:00"],
                [2, "event1_loop_2", "group_alias", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=expected_columns,
        )
        expected["timestamp"] = pd.to_datetime(expected["timestamp"])

        params = {"suffix": "count", "time_agg": "max"}
        actual = source.collapse_loops(**params).to_dataframe()[expected_columns].reset_index(drop=True)

        assert pd.testing.assert_frame_equal(actual, expected) is None

    def test_collapse_loops_graph__suffix_loop__agg_mean(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:01:00"],
                [1, "event1", "2022-01-01 00:02:00"],
                [1, "event2", "2022-01-01 00:01:02"],
                [1, "event1", "2022-01-01 00:03:00"],
                [1, "event1", "2022-01-01 00:04:00"],
                [1, "event1", "2022-01-01 00:05:00"],
                [2, "event1", "2022-01-02 00:00:00"],
                [2, "event1", "2022-01-02 00:00:05"],
                [2, "event2", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        source = Eventstream(source_df)

        expected_columns = ["user_id", "event", "event_type", "timestamp"]
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1_loop", "group_alias", "2022-01-01 00:03:30"],
                [2, "event1_loop", "group_alias", "2022-01-02 00:00:02.5"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=expected_columns,
        )
        expected["timestamp"] = pd.to_datetime(expected["timestamp"])

        params = {"suffix": "loop", "time_agg": "mean"}
        actual = source.collapse_loops(**params).to_dataframe()[expected_columns].reset_index(drop=True)

        assert pd.testing.assert_frame_equal(actual, expected) is None

    def test_collapse_loops_graph__suffix_none__agg_mean(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:01:00"],
                [1, "event1", "2022-01-01 00:02:00"],
                [1, "event2", "2022-01-01 00:01:02"],
                [1, "event1", "2022-01-01 00:03:00"],
                [1, "event1", "2022-01-01 00:04:00"],
                [1, "event1", "2022-01-01 00:05:00"],
                [2, "event1", "2022-01-02 00:00:00"],
                [2, "event1", "2022-01-02 00:00:05"],
                [2, "event2", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        source = Eventstream(source_df)

        params = {"suffix": None, "time_agg": "mean"}

        expected_columns = ["user_id", "event", "event_type", "timestamp"]

        actual = source.collapse_loops(**params).to_dataframe()[expected_columns].reset_index(drop=True)
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "group_alias", "2022-01-01 00:03:30"],
                [2, "event1", "group_alias", "2022-01-02 00:00:02.5"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=expected_columns,
        )
        expected["timestamp"] = pd.to_datetime(expected["timestamp"])
        assert pd.testing.assert_frame_equal(actual, expected) is None

    def test_collapse_loops_graph__default_custom_cols(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:01:00", "A", 1],
                [1, "event1", "2022-01-01 00:02:00", None, 2],
                [1, "event2", "2022-01-01 00:02:02", "A", None],
                [1, "event1", "2022-01-01 00:03:00", "A", None],
                [1, "event1", "2022-01-01 00:04:00", "A", 2],
                [1, "event1", "2022-01-01 00:05:00", "A", 2],
                [2, "event1", "2022-01-02 00:00:00", "A", 5],
                [2, "event1", "2022-01-02 00:00:05", "B", 10],
                [2, "event2", "2022-01-02 00:00:05", "A", 1],
            ],
            columns=["user_id", "event", "timestamp", "str_col", "num_col"],
        )

        source = Eventstream(
            source_df,
            raw_data_schema={
                "custom_cols": [
                    {"raw_data_col": "str_col", "custom_col": "str_col"},
                    {"raw_data_col": "num_col", "custom_col": "num_col"},
                ]
            },
        )

        expected_columns = ["user_id", "event", "event_type", "timestamp", "str_col", "num_col"]
        expected = pd.DataFrame(
            [
                [1, "event1", "group_alias", "2022-01-01 00:01:00", "A", 1.5],
                [1, "event2", "raw", "2022-01-01 00:02:02", "A", None],
                [1, "event1", "group_alias", "2022-01-01 00:03:00", "A", 2],
                [2, "event1", "group_alias", "2022-01-02 00:00:00", None, 7.5],
                [2, "event2", "raw", "2022-01-02 00:00:05", "A", 1],
            ],
            columns=expected_columns,
        )
        expected["timestamp"] = pd.to_datetime(expected["timestamp"])

        params = {}
        actual = source.collapse_loops(**params).to_dataframe()[expected_columns].reset_index(drop=True)

        assert pd.testing.assert_frame_equal(actual, expected) is None
