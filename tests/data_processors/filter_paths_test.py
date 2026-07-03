import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import PreprocessingConfigError, EmptyEventstreamError, InvalidMetricConfigError


def build_stream():
    df = pd.DataFrame(
        [
            ["user_1", "promo_view", "2020-01-01 00:00:00", "US"],
            ["user_1", "purchase", "2020-01-01 00:10:00", "US"],
            ["user_1", "logout", "2020-01-01 00:20:00", "US"],
            ["user_2", "promo_view", "2020-01-01 00:00:00", "US"],
            ["user_2", "purchase", "2020-01-01 00:05:00", "US"],
            ["user_2", "purchase", "2020-01-01 00:15:00", "US"],
            ["user_3", "promo_view", "2020-01-01 00:00:00", "UK"],
            ["user_3", "purchase", "2020-01-01 00:05:00", "UK"],
            ["user_3", "cancellation", "2020-01-01 00:07:00", "UK"],
        ],
        columns=["user_id", "event", "timestamp", "country"],
    )
    schema = {"segment_cols": ["country"]}
    return Eventstream(df, schema)


class TestFilterPathsAST:
    def test__ast_condition_filters_expected_paths(self) -> None:
        stream = build_stream()

        ast_condition = {
            "op": "and",
            "args": [
                {"op": ">", "metric": "event_count", "value": 1, "metric_args": {"events": "purchase"}},
                {"op": "=", "metric": "has", "value": True, "metric_args": {"events": "promo_view"}},
                {
                    "op": "not",
                    "args": [
                        {
                            "op": "or",
                            "args": [
                                {"op": "=", "metric": "has", "value": True, "metric_args": {"events": "logout"}},
                                {"op": "=", "metric": "has", "value": True, "metric_args": {"events": "cancellation"}},
                            ],
                        }
                    ],
                },
            ],
        }

        res = stream.filter_paths(ast_condition=ast_condition)

        expected = stream.filter_events(by_column={"column": "user_id", "values": ["user_2"]})
        assert res.equals(expected)

    def test__ast_condition_with_unknown_metric_raises(self) -> None:
        stream = build_stream()
        ast_condition = {"op": "=", "metric": "unknown_metric", "value": 1}
        with pytest.raises((PreprocessingConfigError, InvalidMetricConfigError)):
            _ = stream.filter_paths(ast_condition=ast_condition)

    def test__ast_condition_no_matches_raises_empty_result_error(self) -> None:
        stream = build_stream()
        ast_condition = {"op": ">", "metric": "event_count", "value": 10, "metric_args": {"events": "purchase"}}
        with pytest.raises(EmptyEventstreamError):
            _ = stream.filter_paths(ast_condition=ast_condition)

    def test__ast_condition_in_numeric(self) -> None:
        stream = build_stream()
        ast_condition = {"op": "in", "metric": "event_count", "value": [2], "metric_args": {"events": "purchase"}}
        res = stream.filter_paths(ast_condition=ast_condition)

        expected = stream.filter_events(by_column={"column": "user_id", "values": ["user_2"]})
        assert res.equals(expected)

    def test__ast_condition_in_boolean_has_flag(self) -> None:
        stream = build_stream()
        ast_condition = {"op": "in", "metric": "has", "value": [False], "metric_args": {"events": "logout"}}
        res = stream.filter_paths(ast_condition=ast_condition)

        expected = stream.filter_events(by_column={"column": "user_id", "values": ["user_2", "user_3"]})
        assert res.equals(expected)

    def test__pattern_simple_adjacent(self) -> None:
        stream = build_stream()
        ast_condition = {"op": "=", "metric": "matches", "value": True, "metric_args": {"pattern": "purchase->cancellation"}}
        res = stream.filter_paths(ast_condition=ast_condition)

        expected = stream.filter_events(by_column={"column": "user_id", "values": ["user_3"]})
        assert res.equals(expected)

    def test__pattern_with_path_start_end_logout(self) -> None:
        stream = build_stream()
        ast_condition = {"op": "=", "metric": "matches", "value": True, "metric_args": {"pattern": "path_start->.*->logout->path_end"}}
        res = stream.filter_paths(ast_condition=ast_condition)

        expected = stream.filter_events(by_column={"column": "user_id", "values": ["user_1"]})
        assert res.equals(expected)

    def test__pattern_with_custom_path_id_col(self) -> None:
        df = pd.DataFrame(
            [
                ["user_1", "sess_1", "promo_view", "2020-01-01 00:00:00"],
                ["user_1", "sess_1", "purchase", "2020-01-01 00:10:00"],
                ["user_1", "sess_1", "logout", "2020-01-01 00:20:00"],
                ["user_2", "sess_2", "promo_view", "2020-01-01 00:00:00"],
                ["user_2", "sess_2", "purchase", "2020-01-01 00:05:00"],
                ["user_2", "sess_2", "purchase", "2020-01-01 00:15:00"],
            ],
            columns=["user_id", "session_id", "event", "timestamp"],
        )
        schema = {"path_cols": ["user_id", "session_id"], "event_cols": ["event"], "timestamp": "timestamp"}
        stream = Eventstream(df, schema)

        ast_condition = {"op": "=", "metric": "matches", "value": True, "metric_args": {"pattern": "promo_view->purchase"}}
        res = stream.filter_paths(ast_condition=ast_condition, path_id_col="session_id")

        expected = stream.filter_events(by_column={"column": "session_id", "values": ["sess_1", "sess_2"]})
        assert res.equals(expected)

    def test__ast_with_custom_path_id_col(self) -> None:
        df = pd.DataFrame(
            [
                ["user_1", "sess_1", "promo_view", "2020-01-01 00:00:00"],
                ["user_1", "sess_1", "purchase", "2020-01-01 00:10:00"],
                ["user_1", "sess_1", "logout", "2020-01-01 00:20:00"],
                ["user_2", "sess_2", "promo_view", "2020-01-01 00:00:00"],
                ["user_2", "sess_2", "purchase", "2020-01-01 00:05:00"],
                ["user_2", "sess_2", "purchase", "2020-01-01 00:15:00"],
            ],
            columns=["user_id", "session_id", "event", "timestamp"],
        )
        schema = {"path_cols": ["user_id", "session_id"], "event_cols": ["event"], "timestamp": "timestamp"}
        stream = Eventstream(df, schema)

        ast_condition = {"op": ">", "metric": "event_count", "value": 1, "metric_args": {"events": "purchase"}}
        res = stream.filter_paths(ast_condition=ast_condition, path_id_col="session_id")

        expected = stream.filter_events(by_column={"column": "session_id", "values": ["sess_2"]})
        assert res.equals(expected)

    def test__ast_condition_has_with_list_of_events_all_present(self) -> None:
        """Test has metric with list of events - checking if ALL are present"""
        stream = build_stream()

        ast_condition = {"op": "=", "metric": "has", "value": True, "metric_args": {"events": ["promo_view", "purchase"]}}
        res = stream.filter_paths(ast_condition=ast_condition)

        expected = stream.filter_events(by_column={"column": "user_id", "values": ["user_1", "user_2", "user_3"]})
        assert res.equals(expected)

    def test__ast_condition_has_with_list_of_events_all_present_subset(self) -> None:
        """Test has metric with list of events - only some users have all"""
        stream = build_stream()

        ast_condition = {"op": "=", "metric": "has", "value": True, "metric_args": {"events": ["promo_view", "logout"]}}
        res = stream.filter_paths(ast_condition=ast_condition)

        expected = stream.filter_events(by_column={"column": "user_id", "values": ["user_1"]})
        assert res.equals(expected)

    def test__ast_condition_has_with_list_of_events_at_least_one_absent(self) -> None:
        """Test has metric with list of events - checking if at least one is absent"""
        stream = build_stream()

        ast_condition = {"op": "=", "metric": "has", "value": False, "metric_args": {"events": ["logout", "cancellation"]}}
        res = stream.filter_paths(ast_condition=ast_condition)

        expected = stream.filter_events(by_column={"column": "user_id", "values": ["user_1", "user_2", "user_3"]})
        assert res.equals(expected)

    def test__ast_condition_has_with_list_combined_logic(self) -> None:
        """Test complex condition using has metric with list"""
        stream = build_stream()

        ast_condition = {
            "op": "and",
            "args": [
                {"op": ">", "metric": "event_count", "value": 0, "metric_args": {"events": "purchase"}},
                {"op": "=", "metric": "has", "value": True, "metric_args": {"events": ["promo_view", "purchase"]}},
            ],
        }

        res = stream.filter_paths(ast_condition=ast_condition)

        expected = stream.filter_events(by_column={"column": "user_id", "values": ["user_1", "user_2", "user_3"]})
        assert res.equals(expected)

    def test__matches_combined_with_metrics(self) -> None:
        """Test combining matches with other metrics"""
        stream = build_stream()

        ast_condition = {
            "op": "and",
            "args": [
                {"op": "=", "metric": "matches", "value": True, "metric_args": {"pattern": "promo_view->.*->purchase"}},
                {"op": ">", "metric": "event_count", "value": 1, "metric_args": {"events": "purchase"}},
            ],
        }

        res = stream.filter_paths(ast_condition=ast_condition)

        expected = stream.filter_events(by_column={"column": "user_id", "values": ["user_2"]})
        assert res.equals(expected)

    def test__belongs_to_any_mode_scalar_value(self) -> None:
        """belongs_to with mode=any and a scalar segment_value keeps only matching paths"""
        stream = build_stream()
        ast_condition = {
            "op": "=",
            "metric": "belongs_to",
            "metric_args": {"segment_name": "country", "segment_value": "US", "mode": "any"},
            "value": 1,
        }
        res = stream.filter_paths(ast_condition=ast_condition)

        expected = stream.filter_events(by_column={"column": "user_id", "values": ["user_1", "user_2"]})
        assert res.equals(expected)

    def test__belongs_to_all_mode_scalar_value(self) -> None:
        """belongs_to with mode=all keeps paths where segment_value is the only value"""
        stream = build_stream()
        ast_condition = {
            "op": "=",
            "metric": "belongs_to",
            "metric_args": {"segment_name": "country", "segment_value": "UK", "mode": "all"},
            "value": 1,
        }
        res = stream.filter_paths(ast_condition=ast_condition)

        expected = stream.filter_events(by_column={"column": "user_id", "values": ["user_3"]})
        assert res.equals(expected)

    def test__belongs_to_event_share_mode(self) -> None:
        """belongs_to with mode=event_share keeps paths where segment_value covers >= threshold"""
        stream = build_stream()
        ast_condition = {
            "op": "=",
            "metric": "belongs_to",
            "metric_args": {"segment_name": "country", "segment_value": "US", "mode": "event_share", "threshold": 0.5},
            "value": 1,
        }
        res = stream.filter_paths(ast_condition=ast_condition)

        expected = stream.filter_events(by_column={"column": "user_id", "values": ["user_1", "user_2"]})
        assert res.equals(expected)

    def test__belongs_to_combined_with_other_metrics(self) -> None:
        """belongs_to can be combined with other metrics in AND/OR expressions"""
        stream = build_stream()
        ast_condition = {
            "op": "and",
            "args": [
                {
                    "op": "=",
                    "metric": "belongs_to",
                    "metric_args": {"segment_name": "country", "segment_value": "US", "mode": "any"},
                    "value": 1,
                },
                {"op": ">", "metric": "event_count", "value": 1, "metric_args": {"events": "purchase"}},
            ],
        }
        res = stream.filter_paths(ast_condition=ast_condition)

        expected = stream.filter_events(by_column={"column": "user_id", "values": ["user_2"]})
        assert res.equals(expected)

    def test__belongs_to_none_segment_value_raises(self) -> None:
        """belongs_to with segment_value=None cannot be used in ast_condition"""
        stream = build_stream()
        ast_condition = {
            "op": "=",
            "metric": "belongs_to",
            "metric_args": {"segment_name": "country", "segment_value": None, "mode": "any"},
            "value": 1,
        }
        with pytest.raises(PreprocessingConfigError):
            _ = stream.filter_paths(ast_condition=ast_condition)
