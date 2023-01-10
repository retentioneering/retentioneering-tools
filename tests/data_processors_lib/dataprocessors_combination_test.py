from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.data_processors_lib import (
    CollapseLoops,
    CollapseLoopsParams,
    DeleteUsersByPathLength,
    DeleteUsersByPathLengthParams,
    FilterEvents,
    FilterEventsParams,
    GroupEvents,
    GroupEventsParams,
    LostUsersEvents,
    LostUsersParams,
    NegativeTarget,
    NegativeTargetParams,
    NewUsersEvents,
    NewUsersParams,
    PositiveTarget,
    PositiveTargetParams,
    SplitSessions,
    SplitSessionsParams,
    StartEndEvents,
    StartEndEventsParams,
    TruncatedEvents,
    TruncatedEventsParams,
    TruncatePath,
    TruncatePathParams,
)
from src.eventstream import Eventstream
from src.graph.p_graph import EventsNode, PGraph
from src.tooling.step_matrix import StepMatrix
from tests.data_processors_lib.fixtures.combination import (
    test_stream,
    test_stream_custom_col,
)
from tests.data_processors_lib.fixtures.combination_corr import (
    filter_events_negative_target_corr,
    group_events_delete_users_corr,
    new_lost_corr,
    positive_target_delete_users_corr,
    split_start_end_corr,
)


def apply_data_processor(eventstream, data_processor):
    graph = PGraph(source_stream=eventstream)
    node_0 = EventsNode(data_processor)
    graph.add_node(node=node_0, parents=[graph.root])
    return graph.combine(node=node_0)


class TestDataprocessorsCombination:
    def test_truncated_path__new_lost_users(self, test_stream, new_lost_corr):
        test_stream = test_stream
        data_processor = TruncatePath(params=TruncatePathParams(drop_after="event3", occurrence_after="last"))
        res = apply_data_processor(test_stream, data_processor)
        new_users = [1]
        data_processor = NewUsersEvents(params=NewUsersParams(new_users_list=new_users))
        res = apply_data_processor(res, data_processor)
        data_processor = LostUsersEvents(params=LostUsersParams(lost_cutoff=(10, "D")))
        res = apply_data_processor(res, data_processor).to_dataframe()
        correct_result = new_lost_corr
        result_df = res[correct_result.columns].reset_index(drop=True)

        assert pd.testing.assert_frame_equal(result_df, correct_result) == None

    def test_split_sessions__start_end(self, test_stream, split_start_end_corr):
        test_stream = test_stream
        data_processor = SplitSessions(params=SplitSessionsParams(session_cutoff=(2, "m")))
        res = apply_data_processor(test_stream, data_processor)
        res = apply_data_processor(res, StartEndEvents(StartEndEventsParams())).to_dataframe()
        correct_result = split_start_end_corr
        result_df = res[correct_result.columns].reset_index(drop=True)

        assert pd.testing.assert_frame_equal(result_df, correct_result) == None

    def test_positive_target__delete_users(self, test_stream, positive_target_delete_users_corr):
        test_stream = test_stream
        positive_events = ["event3"]
        data_processor = PositiveTarget(params=PositiveTargetParams(positive_target_events=positive_events))
        res = apply_data_processor(test_stream, data_processor)
        data_processor = DeleteUsersByPathLength(params=DeleteUsersByPathLengthParams(events_num=4))
        res = apply_data_processor(res, data_processor).to_dataframe()
        correct_result = positive_target_delete_users_corr
        result_df = res[correct_result.columns].reset_index(drop=True)

        assert pd.testing.assert_frame_equal(result_df, correct_result) == None

    def test_filter_events__negative_target(self, test_stream, filter_events_negative_target_corr):
        def save_specific_users(df, schema):
            users_to_save = [1, 2]
            return df[schema.user_id].isin(users_to_save)

        test_stream = test_stream
        data_processor = FilterEvents(params=FilterEventsParams(func=save_specific_users))
        res = apply_data_processor(test_stream, data_processor)
        negative_events = ["event2"]
        data_processor = NegativeTarget(params=NegativeTargetParams(negative_target_events=negative_events))
        res = apply_data_processor(res, data_processor).to_dataframe()
        correct_result = filter_events_negative_target_corr
        result_df = res[correct_result.columns].reset_index(drop=True)

        assert pd.testing.assert_frame_equal(result_df, correct_result) == None

    def test_group_events__delete_users(self, test_stream_custom_col, group_events_delete_users_corr):
        def group_events(df, schema):
            events_to_group = ["event3", "event4"]
            return df[schema.event_name].isin(events_to_group)

        params = {"event_name": "last_event", "func": group_events}
        data_processor = GroupEvents(params=GroupEventsParams(**params))
        res = apply_data_processor(test_stream_custom_col, data_processor)
        data_proccessor = DeleteUsersByPathLength(params=DeleteUsersByPathLengthParams(cutoff=(3, "m")))
        res = apply_data_processor(res, data_proccessor).to_dataframe()
        correct_result = group_events_delete_users_corr
        result_df = res[correct_result.columns].reset_index(drop=True)

        assert pd.testing.assert_frame_equal(result_df, correct_result) == None
