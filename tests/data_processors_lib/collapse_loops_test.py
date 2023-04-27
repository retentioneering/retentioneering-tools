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
            [1, "event1", "2022-01-01 00:01:00", "A", 1],
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
        actual = self._apply(
            CollapseLoopsParams(
                suffix="count",
                time_agg="min",
            )
        )
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
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_collapse_loops_apply__suffix_count__agg_max(self):
        actual = self._apply(
            CollapseLoopsParams(
                suffix="count",
                time_agg="max",
            )
        )
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
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_collapse_loops_apply__suffix_loop__agg_mean(self):
        actual = self._apply(
            CollapseLoopsParams(
                suffix="loop",
                time_agg="mean",
            )
        )
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
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_collapse_loops_apply__default_custom_cols(self):
        actual = self._apply(
            CollapseLoopsParams(),
            source_df=self._source_df_custom_cols,
            raw_data_schema=self._raw_data_schema_custom_cols,
        )

        expected = pd.DataFrame(
            [
                [1, "event1", "group_alias", "2022-01-01 00:01:00", "A", 1.5, False],
                [1, "event1", "raw", "2022-01-01 00:01:00", "A", 1, True],
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

        actual = actual[expected.columns]
        assert actual[expected.columns].compare(expected).shape == (0, 0)


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
            [1, "event1", "2022-01-01 00:01:00", "A", 1],
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
        actual = self._apply(
            CollapseLoopsParams(
                suffix="count",
                time_agg="min",
            )
        )
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
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_collapse_loops_graph__suffix_count__agg_max(self):
        actual = self._apply(
            CollapseLoopsParams(
                suffix="count",
                time_agg="max",
            )
        )
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
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_collapse_loops_graph__suffix_loop__agg_mean(self):
        actual = self._apply(
            CollapseLoopsParams(
                suffix="loop",
                time_agg="mean",
            )
        )
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
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_collapse_loops_graph__suffix_none__agg_mean(self):
        actual = self._apply(
            CollapseLoopsParams(
                suffix=None,
                time_agg="mean",
            )
        )
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
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_collapse_loops_graph__default_custom_cols(self):
        actual = self._apply(
            CollapseLoopsParams(),
            source_df=self._source_df_custom_cols,
            raw_data_schema=self._raw_data_schema_custom_cols,
        )

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
        assert actual[expected.columns].compare(expected).shape == (0, 0)


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

        params = {"suffix": "count", "time_agg": "min"}

        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]

        res = source.collapse_loops(**params).to_dataframe()[correct_result_columns].reset_index(drop=True)
        correct_result_false_min = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1_loop_4", "group_alias", "2022-01-01 00:02:00"],
                [2, "event1_loop_2", "group_alias", "2022-01-02 00:00:00"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=correct_result_columns,
        )

        assert res.compare(correct_result_false_min).shape == (0, 0)

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

        params = {"suffix": "count", "time_agg": "max"}

        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]

        res = source.collapse_loops(**params).to_dataframe()[correct_result_columns].reset_index(drop=True)
        correct_result_false_max = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1_loop_4", "group_alias", "2022-01-01 00:05:00"],
                [2, "event1_loop_2", "group_alias", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=correct_result_columns,
        )

        assert res.compare(correct_result_false_max).shape == (0, 0)

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

        params = {"suffix": "loop", "time_agg": "mean"}

        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]

        res = source.collapse_loops(**params).to_dataframe()[correct_result_columns].reset_index(drop=True)
        correct_result_true_mean = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1_loop", "group_alias", "2022-01-01 00:03:30"],
                [2, "event1_loop", "group_alias", "2022-01-02 00:00:02.5"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=correct_result_columns,
        )

        assert res.compare(correct_result_true_mean).shape == (0, 0)

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

        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]

        res = source.collapse_loops(**params).to_dataframe()[correct_result_columns].reset_index(drop=True)
        correct_result_true_mean = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "group_alias", "2022-01-01 00:03:30"],
                [2, "event1", "group_alias", "2022-01-02 00:00:02.5"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=correct_result_columns,
        )

        assert res.compare(correct_result_true_mean).shape == (0, 0)

    def test_collapse_loops_graph__default_custom_cols(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:01:00", "A", 1],
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

        source = Eventstream(
            source_df,
            raw_data_schema={
                "custom_cols": [
                    {"raw_data_col": "str_col", "custom_col": "str_col"},
                    {"raw_data_col": "num_col", "custom_col": "num_col"},
                ]
            },
        )

        params = {}
        correct_result_columns = ["user_id", "event", "event_type", "timestamp", "str_col", "num_col"]
        res = source.collapse_loops(**params).to_dataframe()[correct_result_columns].reset_index(drop=True)
        correct_result_true_mean = pd.DataFrame(
            [
                [1, "event1", "group_alias", "2022-01-01 00:01:00", "A", 1.5],
                [1, "event2", "raw", "2022-01-01 00:02:02", "A", None],
                [1, "event1", "group_alias", "2022-01-01 00:03:00", "A", 2],
                [2, "event1", "group_alias", "2022-01-02 00:00:00", None, 7.5],
                [2, "event2", "raw", "2022-01-02 00:00:05", "A", 1],
            ],
            columns=correct_result_columns,
        )

        assert res.compare(correct_result_true_mean).shape == (0, 0)
