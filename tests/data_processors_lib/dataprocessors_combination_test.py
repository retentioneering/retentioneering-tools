from __future__ import annotations

import pandas as pd

from retentioneering.data_processors_lib import (
    AddNegativeEvents,
    AddNegativeEventsParams,
    AddPositiveEvents,
    AddPositiveEventsParams,
    AddStartEndEvents,
    AddStartEndEventsParams,
    DropPaths,
    DropPathsParams,
    FilterEvents,
    FilterEventsParams,
    GroupEvents,
    GroupEventsParams,
    LabelLostUsers,
    LabelLostUsersParams,
    LabelNewUsers,
    LabelNewUsersParams,
    SplitSessions,
    SplitSessionsParams,
    TruncatePaths,
    TruncatePathsParams,
)
from retentioneering.preprocessing_graph.preprocessing_graph import (
    EventsNode,
    PreprocessingGraph,
)
from tests.data_processors_lib.fixtures.combination import (
    test_stream,
    test_stream_custom_col,
)
from tests.data_processors_lib.fixtures.combination_corr import (
    add_positive_events_drop_paths_corr,
    filter_events_add_negative_events_corr,
    group_events_drop_paths_corr,
    new_lost_corr,
    split_start_end_corr,
)


def apply_data_processor(eventstream, data_processor):
    graph = PreprocessingGraph(source_stream=eventstream)
    node_0 = EventsNode(data_processor)
    graph.add_node(node=node_0, parents=[graph.root])
    return graph.combine(node=node_0)


class TestDataprocessorsCombination:
    def test_truncated_path__label_new_lost_users(self, test_stream, new_lost_corr):
        test_stream = test_stream
        data_processor = TruncatePaths(params=TruncatePathsParams(drop_after="event3", occurrence_after="last"))
        res = apply_data_processor(test_stream, data_processor)
        new_users = [1]
        data_processor = LabelNewUsers(params=LabelNewUsersParams(new_users_list=new_users))
        res = apply_data_processor(res, data_processor)
        data_processor = LabelLostUsers(params=LabelLostUsersParams(timeout=(5, "m")))
        res = apply_data_processor(res, data_processor).to_dataframe()
        correct_result = new_lost_corr
        result_df = res[correct_result.columns].reset_index(drop=True)

        assert pd.testing.assert_frame_equal(result_df, correct_result) == None

    def test_split_sessions__start_end(self, test_stream, split_start_end_corr):
        test_stream = test_stream
        data_processor = SplitSessions(params=SplitSessionsParams(timeout=(2, "m")))
        res = apply_data_processor(test_stream, data_processor)
        res = apply_data_processor(res, AddStartEndEvents(AddStartEndEventsParams())).to_dataframe()
        correct_result = split_start_end_corr
        result_df = res[correct_result.columns].reset_index(drop=True)

        assert pd.testing.assert_frame_equal(result_df, correct_result) == None

    def test_add_positive_events__drop_paths(self, test_stream, add_positive_events_drop_paths_corr):
        test_stream = test_stream
        positive_events = ["event3"]
        data_processor = AddPositiveEvents(params=AddPositiveEventsParams(targets=positive_events))
        res = apply_data_processor(test_stream, data_processor)
        data_processor = DropPaths(params=DropPathsParams(min_steps=4))
        res = apply_data_processor(res, data_processor).to_dataframe()
        correct_result = add_positive_events_drop_paths_corr
        result_df = res[correct_result.columns].reset_index(drop=True)

        assert pd.testing.assert_frame_equal(result_df, correct_result) == None

    def test_filter_events__add_negative_events(self, test_stream, filter_events_add_negative_events_corr):
        def save_specific_users(df, schema):
            users_to_save = [1, 2]
            return df[schema.user_id].isin(users_to_save)

        test_stream = test_stream
        data_processor = FilterEvents(params=FilterEventsParams(func=save_specific_users))
        res = apply_data_processor(test_stream, data_processor)
        negative_events = ["event2"]
        data_processor = AddNegativeEvents(params=AddNegativeEventsParams(targets=negative_events))
        res = apply_data_processor(res, data_processor).to_dataframe()
        correct_result = filter_events_add_negative_events_corr
        result_df = res[correct_result.columns].reset_index(drop=True)

        assert pd.testing.assert_frame_equal(result_df, correct_result) == None

    def test_group_events__drop_paths(self, test_stream_custom_col, group_events_drop_paths_corr):
        def group_events(df, schema):
            events_to_group = ["event3", "event4"]
            return df[schema.event_name].isin(events_to_group)

        params = {"event_name": "last_event", "func": group_events}
        data_processor = GroupEvents(params=GroupEventsParams(**params))
        res = apply_data_processor(test_stream_custom_col, data_processor)
        data_proccessor = DropPaths(params=DropPathsParams(min_time=(3, "m")))
        res = apply_data_processor(res, data_proccessor).to_dataframe()
        correct_result = group_events_drop_paths_corr
        result_df = res[correct_result.columns].reset_index(drop=True)

        assert pd.testing.assert_frame_equal(result_df, correct_result) == None
