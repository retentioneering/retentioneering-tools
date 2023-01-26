from __future__ import annotations

import pandas as pd

from retentioneering.data_processors_lib import (
    NegativeTarget,
    NegativeTargetParams,
    PositiveTarget,
    PositiveTargetParams,
)
from retentioneering.graph.p_graph import EventsNode, PGraph
from tests.data_processors_lib.fixtures.positive_negative_target import (
    negative_helper_custom_func_corr,
    positive_helper_custom_func_corr,
    test_stream,
)


def custom_func(eventstream, events):
    user_col = eventstream.schema.user_id
    time_col = eventstream.schema.event_timestamp
    event_col = eventstream.schema.event_name
    df = eventstream.to_dataframe()
    events_index_1 = df[df[event_col].isin(events)].groupby(user_col)[time_col].idxmin()  # type: ignore
    events_index_2 = df[df[event_col].isin(events)].groupby(user_col)[time_col].idxmax()  # type: ignore
    events_index = pd.concat([events_index_1, events_index_2], axis=0)
    result = df.loc[events_index]
    return result


def apply_data_processor(eventstream, data_processor):
    graph = PGraph(source_stream=eventstream)
    node_0 = EventsNode(data_processor)
    graph.add_node(node=node_0, parents=[graph.root])
    return graph.combine(node=node_0)


class TestPositiveNegativeTarget:
    def test_positive_target__custom_func(self, test_stream, positive_helper_custom_func_corr):
        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]
        correct_result = positive_helper_custom_func_corr
        source = test_stream
        data_processor = PositiveTarget(
            params=PositiveTargetParams(positive_target_events=["event1"], func=custom_func)
        )
        result = apply_data_processor(source, data_processor)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert pd.testing.assert_frame_equal(result_df[correct_result_columns], correct_result) is None

    def test_negative_target__custom_func(self, test_stream, negative_helper_custom_func_corr):
        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]
        correct_result = negative_helper_custom_func_corr
        source = test_stream
        data_processor = NegativeTarget(
            params=NegativeTargetParams(negative_target_events=["event1"], func=custom_func)
        )
        result = apply_data_processor(source, data_processor)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert pd.testing.assert_frame_equal(result_df[correct_result_columns], correct_result) is None

    def test_positive_target_helper__custom_func(self, test_stream, positive_helper_custom_func_corr):
        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]
        correct_result = positive_helper_custom_func_corr
        source = test_stream
        result = source.positive_target(positive_target_events=["event1"], func=custom_func)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert pd.testing.assert_frame_equal(result_df[correct_result_columns], correct_result) is None

    def test_negative_target_helper__custom_func(self, test_stream, negative_helper_custom_func_corr):
        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]
        correct_result = negative_helper_custom_func_corr
        source = test_stream
        result = source.negative_target(negative_target_events=["event1"], func=custom_func)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert pd.testing.assert_frame_equal(result_df[correct_result_columns], correct_result) is None
