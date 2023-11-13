from __future__ import annotations

import pandas as pd
import pytest

from retentioneering.eventstream import EventstreamSchema

from .common import groups, test_stream


@pytest.fixture
def funnel_full_params(groups: tuple[list, list]) -> dict:
    args = {
        "stages": ["main", ["catalog", "cart"]],
        "stage_names": ["first_stage", "second_stage"],
        "segments": groups,
        "segment_names": ["first_group", "second_group"],
        "funnel_type": "open",
        "show_plot": False,
    }
    expected_args = {
        "stages": {"len": 2, "len_flatten": 3},
        "stage_names": {"len": 2, "len_flatten": 2},
        "segments": {"len": 2, "len_flatten": 3751},
        "segment_names": {"len": 2, "len_flatten": 2},
        "funnel_type": "open",
        "show_plot": False,
    }
    return {"args": args, "expected_args": expected_args, "performance_info": {}}


@pytest.fixture
def cohorts_full_params() -> dict:
    args = {
        "cohort_start_unit": "M",
        "cohort_period": (1, "M"),
        "average": True,
        "cut_bottom": 1,
        "cut_right": 2,
        "cut_diagonal": 3,
        "width": 4.0,
        "height": 3.0,
        "show_plot": False,
    }
    expected_args = {
        "cohort_start_unit": "M",
        "cohort_period": [1, "M"],
        "average": True,
        "cut_bottom": 1,
        "cut_right": 2,
        "cut_diagonal": 3,
        "width": 4.0,
        "height": 3.0,
        "show_plot": False,
    }
    performance_info = {"shape": [6, 3]}
    return {"args": args, "expected_args": expected_args, "performance_info": performance_info}


@pytest.fixture
def step_matrix_full_params(groups: tuple[list, list]) -> dict:
    args = {
        "max_steps": 20,
        "weight_col": "user_id",
        "precision": 3,
        "targets": ["main", ["catalog", "cart"]],
        "accumulated": "only",
        "sorting": None,
        "threshold": 0.05,
        "centered": {
            "event": "catalog",
            "left_gap": 5,
            "occurrence": 1,
        },
        "groups": groups,
        "show_plot": True,
    }
    expected_args = {
        "max_steps": 20,
        "weight_col": "f89d6b6960453241bc5b09b4d0d8ad86d53769e051473350c2bf94e39077967b",
        "precision": 3,
        "targets": {"len": 2, "len_flatten": 3},
        "accumulated": "only",
        "sorting": None,
        "threshold": 0.05,
        "centered": {
            "event": "652f55016243bf1b9f1bbea46d5749ef892dbe394e46de9d66ab1aacf0b4af57",
            "left_gap": 5,
            "occurrence": 1,
        },
        "groups": {"len": 2, "len_flatten": 3751},
        "show_plot": True,
    }
    performance_info = {"shape": [9, 20], "unique_nodes": 4}
    return {"args": args, "expected_args": expected_args, "performance_info": performance_info}


@pytest.fixture
def step_sankey_full_params() -> dict:
    args = {
        "max_steps": 8,
        "threshold": 0.05,
        "sorting": None,
        "targets": ["catalog", "cart"],
        "autosize": False,
        "width": 400,
        "height": 300,
        "show_plot": False,
    }
    expected_args = {
        "max_steps": 8,
        "threshold": 0.05,
        "sorting": None,
        "targets": {"len": 2, "len_flatten": 2},
        "autosize": False,
        "width": 400,
        "height": 300,
        "show_plot": False,
    }
    performance_info = {"links_count": 176, "nodes_count": 55}
    return {"args": args, "expected_args": expected_args, "performance_info": performance_info}


@pytest.fixture
def transition_graph_full_params() -> dict:
    args = {
        "edges_norm_type": "full",
        "nodes_norm_type": None,
        "targets": {"positive": ["payment_done", "cart"], "negative": "path_end", "source": "path_start"},
        "nodes_threshold": {"event_id": 500},
        "edges_threshold": {"user_id": 0.12},
        "nodes_weight_col": "user_id",
        "edges_weight_col": "event_id",
        "custom_weight_cols": ["session_id"],
        "width": 1000,
        "height": 700,
        "show_weights": False,
        "show_percents": True,
        "show_nodes_names": False,
        "show_all_edges_for_targets": False,
        "show_edge_info_on_hover": False,
        "show_nodes_without_links": True,
        "layout_dump": None,
    }
    expected_args = {
        "edges_norm_type": "full",
        "nodes_norm_type": None,
        "targets": {
            "positive": {"len": 2, "len_flatten": 2},
            "negative": "40e19a65499418a9a2a201c92e10b34dd7d291dc2e80c38edd9258148482e801",
            "source": "581d38854a5c8a5676af886277bbf49ad6dc25b9ec2dac2081a03a81659a2482",
        },
        "nodes_threshold": {"57c7c8267db4b2c89f1d3ecdfe37182fc5e9cdb0f70a37a4a5a907c82a1665bd": 500},
        "edges_threshold": {"f89d6b6960453241bc5b09b4d0d8ad86d53769e051473350c2bf94e39077967b": 0.12},
        "nodes_weight_col": "f89d6b6960453241bc5b09b4d0d8ad86d53769e051473350c2bf94e39077967b",
        "edges_weight_col": "57c7c8267db4b2c89f1d3ecdfe37182fc5e9cdb0f70a37a4a5a907c82a1665bd",
        "custom_weight_cols": {"len_flatten": 1, "len": 1},
        "width": 1000,
        "height": 700,
        "show_weights": False,
        "show_percents": True,
        "show_nodes_names": False,
        "show_all_edges_for_targets": False,
        "show_edge_info_on_hover": False,
        "show_nodes_without_links": True,
        "layout_dump": None,
    }
    performance_info = {"unique_links": 35, "unique_nodes": 12}
    return {"args": args, "expected_args": expected_args, "performance_info": performance_info}


@pytest.fixture
def transition_matrix_full_params() -> dict:
    return {
        "args": {"weight_col": "session_id", "norm_type": "node"},
        "expected_args": {
            "weight_col": "5c3a09bf22e12411b6af48dfe0b85c2d9d1181cd391e089574c8cf3ca1e5e4ad",
            "norm_type": "node",
        },
        "performance_info": {},
    }


@pytest.fixture
def stattests_full_params(groups: tuple[list, list]) -> dict:
    args = {
        "alpha": 0.1,
        "func": lambda df: len(df[df["event"] == "cart"]) / len(df),
        "groups": groups,
        "group_names": ["random_group_1", "random_group_2"],
        "test": "ttest",
    }
    expected_args = {
        "alpha": 0.1,
        "func": "4acc13f6c765638372619b822a712108614e4b84d195b5e7e78af49db77aa478",
        "groups": {"len": 2, "len_flatten": 3751},
        "group_names": {"len": 2, "len_flatten": 2},
        "test": "ttest",
    }
    return {"args": args, "expected_args": expected_args, "performance_info": {}}


@pytest.fixture
def pgraph_params() -> dict:
    args = {"width": 800, "height": 600}
    expected_args = {"width": 800, "height": 600}
    performance_info: dict = {}
    return {"args": args, "expected_args": expected_args, "performance_info": performance_info}


@pytest.fixture
def add_start_end_events_full_params() -> dict:
    performance_before = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 12, "unique_users": 3751, "index": 2}
    performance_after = {"custom_cols": 1, "shape": [39785, 7], "unique_events": 14, "unique_users": 3751, "index": 4}
    return {
        "args": {},
        "expected_args": {},
        "performance_before": performance_before,
        "performance_after": performance_after,
    }


@pytest.fixture
def add_negative_events_full_params() -> dict:
    expected_args = {
        "func": "f4ceab15e57d39ae4d83209b81e28b772763cada8ef130f2719ac4b0d8950e10",
        "targets": {"len": 1, "len_flatten": 1},
    }
    performance_before = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 12, "unique_users": 3751, "index": 2}
    performance_after = {"custom_cols": 1, "shape": [64566, 7], "unique_events": 24, "unique_users": 3751, "index": 5}
    return {
        "args": {"func": lambda stream, targets: stream.to_dataframe(), "targets": ["delivery_courier"]},
        "expected_args": expected_args,
        "performance_before": performance_before,
        "performance_after": performance_after,
    }


@pytest.fixture
def add_positive_events_full_params() -> dict:
    expected_args = {
        "func": "f4ceab15e57d39ae4d83209b81e28b772763cada8ef130f2719ac4b0d8950e10",
        "targets": {"len": 1, "len_flatten": 1},
    }
    performance_before = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 12, "unique_users": 3751, "index": 2}
    performance_after = {"custom_cols": 1, "shape": [64566, 7], "unique_events": 24, "unique_users": 3751, "index": 5}
    return {
        "args": {"func": lambda stream, targets: stream.to_dataframe(), "targets": ["delivery_courier"]},
        "expected_args": expected_args,
        "performance_before": performance_before,
        "performance_after": performance_after,
    }


@pytest.fixture
def split_sessions_by_timeout() -> dict:
    args = {"timeout": (10, "m"), "session_col": "custom_session_id", "mark_truncated": True}
    expected_args = {
        "delimiter_col": None,
        "delimiter_events": None,
        "timeout": [10, "m"],
        "session_col": "38f7dc148fd5d753237adbf2ff6c46fbaf3973b66fcf54890d50474e2c0a8f0b",
        "mark_truncated": True,
    }
    performance_before = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 12, "unique_users": 3751, "index": 2}
    performance_after = {"custom_cols": 2, "shape": [45197, 8], "unique_events": 16, "unique_users": 3751, "index": 4}
    return {
        "args": args,
        "expected_args": expected_args,
        "performance_before": performance_before,
        "performance_after": performance_after,
    }


@pytest.fixture
def split_sessions_by_events() -> dict:
    args = {
        "delimiter_events": ["session_start", "session_end"],
        "session_col": "custom_session_id",
        "mark_truncated": True,
    }
    expected_args = {
        "delimiter_col": None,
        "delimiter_events": {"len": 2, "len_flatten": 2},
        "timeout": None,
        "session_col": "38f7dc148fd5d753237adbf2ff6c46fbaf3973b66fcf54890d50474e2c0a8f0b",
        "mark_truncated": True,
    }
    performance_before = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 12, "unique_users": 3751, "index": 2}
    performance_after = {"custom_cols": 2, "shape": [32283, 8], "unique_events": 12, "unique_users": 3751, "index": 4}
    return {
        "args": args,
        "expected_args": expected_args,
        "performance_before": performance_before,
        "performance_after": performance_after,
    }


@pytest.fixture
def split_sessions_by_col() -> dict:
    args = {"delimiter_col": "session_id", "session_col": "custom_session_id", "mark_truncated": True}
    expected_args = {
        "delimiter_col": "5c3a09bf22e12411b6af48dfe0b85c2d9d1181cd391e089574c8cf3ca1e5e4ad",
        "delimiter_events": None,
        "timeout": None,
        "session_col": "38f7dc148fd5d753237adbf2ff6c46fbaf3973b66fcf54890d50474e2c0a8f0b",
        "mark_truncated": True,
    }
    performance_before = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 12, "unique_users": 3751, "index": 2}
    performance_after = {"custom_cols": 2, "shape": [42637, 8], "unique_events": 14, "unique_users": 3751, "index": 4}
    return {
        "args": args,
        "expected_args": expected_args,
        "performance_before": performance_before,
        "performance_after": performance_after,
    }


@pytest.fixture
def label_new_users_full_params(groups: tuple[list, list]) -> dict:
    performance_before = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 12, "unique_users": 3751, "index": 2}
    performance_after = {"custom_cols": 1, "shape": [36034, 7], "unique_events": 14, "unique_users": 3751, "index": 4}
    return {
        "args": {"new_users_list": groups[0]},
        "expected_args": {"new_users_list": {"len": 1875, "len_flatten": 1875}},
        "performance_before": performance_before,
        "performance_after": performance_after,
    }


@pytest.fixture
def label_lost_users_with_list(groups: tuple[list, list]) -> dict:
    performance_before = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 12, "unique_users": 3751, "index": 2}
    performance_after = {"custom_cols": 1, "shape": [36034, 7], "unique_events": 14, "unique_users": 3751, "index": 4}
    return {
        "args": {"lost_users_list": groups[1]},
        "expected_args": {"lost_users_list": {"len": 1876, "len_flatten": 1876}, "timeout": None},
        "performance_before": performance_before,
        "performance_after": performance_after,
    }


@pytest.fixture
def label_lost_users_with_time() -> dict:
    performance_before = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 12, "unique_users": 3751, "index": 2}
    performance_after = {"custom_cols": 1, "shape": [36034, 7], "unique_events": 14, "unique_users": 3751, "index": 4}
    return {
        "args": {"timeout": (5, "m")},
        "expected_args": {"timeout": [5, "m"], "lost_users_list": None},
        "performance_before": performance_before,
        "performance_after": performance_after,
    }


@pytest.fixture
def label_cropped_paths_full_params() -> dict:
    performance_before = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 12, "unique_users": 3751, "index": 2}
    performance_after = {"custom_cols": 1, "shape": [32560, 7], "unique_events": 14, "unique_users": 3751, "index": 4}
    return {
        "args": {"left_cutoff": (4, "D"), "right_cutoff": (3, "D")},
        "expected_args": {"left_cutoff": [4, "D"], "right_cutoff": [3, "D"]},
        "performance_before": performance_before,
        "performance_after": performance_after,
    }


@pytest.fixture
def filter_events_full_params() -> dict:
    events_to_exclude = ["catalog", "main"]

    performance_before = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 12, "unique_users": 3751, "index": 2}
    performance_after = {"custom_cols": 1, "shape": [12130, 7], "unique_events": 10, "unique_users": 2592, "index": 4}
    return {
        "args": {"func": lambda df, schema: ~df[schema.event_name].isin(events_to_exclude)},
        "expected_args": {"func": "d6bca7d722394ff48eccb379df92cda85e28246a3aedd514f152c1b4b2c4f97f"},
        "performance_before": performance_before,
        "performance_after": performance_after,
    }


@pytest.fixture
def drop_paths_with_steps() -> dict:
    performance_before = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 12, "unique_users": 3751, "index": 2}
    performance_after = {"custom_cols": 1, "shape": [28896, 7], "unique_events": 12, "unique_users": 2294, "index": 4}
    return {
        "args": {"min_steps": 5},
        "expected_args": {"min_steps": 5, "min_time": None},
        "performance_before": performance_before,
        "performance_after": performance_after,
    }


@pytest.fixture
def drop_paths_with_time() -> dict:
    performance_before = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 12, "unique_users": 3751, "index": 2}
    performance_after = {"custom_cols": 1, "shape": [20101, 7], "unique_events": 12, "unique_users": 1320, "index": 4}
    return {
        "args": {"min_time": (5, "m")},
        "expected_args": {"min_steps": None, "min_time": [5, "m"]},
        "performance_before": performance_before,
        "performance_after": performance_after,
    }


@pytest.fixture
def truncate_paths_full_params() -> dict:
    args = {
        "drop_before": "cart",
        "drop_after": "product1",
        "occurrence_before": "first",
        "occurrence_after": "first",
        "shift_before": -2,
        "shift_after": 2,
    }
    expected_args = {
        "drop_before": "1c42ddc0285b9c25135be3bf345e6a041271b784b22a6f6cab6588dab93c5980",
        "drop_after": "dfc3ec2142bb9ca648a80ec1b8c82abcdb2f737f4973555f72e6d7986e125504",
        "occurrence_before": "first",
        "occurrence_after": "first",
        "shift_before": -2,
        "shift_after": 2,
    }
    performance_before = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 12, "unique_users": 3751, "index": 2}
    performance_after = {"custom_cols": 1, "shape": [22541, 7], "unique_events": 12, "unique_users": 3622, "index": 4}
    return {
        "args": args,
        "expected_args": expected_args,
        "performance_before": performance_before,
        "performance_after": performance_after,
    }


@pytest.fixture
def group_events_full_params() -> dict:
    events_to_group = ["product1", "product2"]

    def group_func(df: pd.DataFrame, schema: EventstreamSchema) -> pd.Series:
        return df[schema.event_name].isin(events_to_group)  # type: ignore

    args = {
        "event_name": "product",
        "func": group_func,
        "event_type": "raw",
    }
    expected_args = {
        "event_name": "a8792157cb4f27fb949c035f45518c61e884bb86e6f420204379c2baa8beb66e",
        "func": "3022c066185234056c48237e8a6992a73906685bbaeaa4d6e74da83fc44d3082",
        "event_type": "d7439bee24773bcbfa2d0a97947ee36227b10d1022b1a55847e928965bb6bfde",
    }
    performance_before = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 12, "unique_users": 3751, "index": 2}
    performance_after = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 11, "unique_users": 3751, "index": 4}
    return {
        "args": args,
        "expected_args": expected_args,
        "performance_before": performance_before,
        "performance_after": performance_after,
    }


@pytest.fixture
def group_events_bulk_full_params() -> dict:
    grouping_rules = [
        {"event_name": "product", "func": lambda _df: _df["event"].str.startswith("product")},
        {"event_name": "delivery", "func": lambda _df: _df["event"].str.startswith("delivery")},
    ]

    args = {"grouping_rules": grouping_rules, "ignore_intersections": False}

    expected_args = {"grouping_rules": {"len": 2, "len_flatten": 2}, "ignore_intersections": False}
    performance_before = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 12, "unique_users": 3751, "index": 2}
    performance_after = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 9, "unique_users": 3751, "index": 4}
    return {
        "args": args,
        "expected_args": expected_args,
        "performance_before": performance_before,
        "performance_after": performance_after,
    }


@pytest.fixture
def group_events_bulk_dict_full_params() -> dict:
    grouping_rules = {
        "product": lambda _df: _df["event"].str.startswith("product"),
        "delivery": lambda _df: _df["event"].str.startswith("delivery"),
    }

    args = {"grouping_rules": grouping_rules, "ignore_intersections": False}

    expected_args = {
        "grouping_rules": {
            "a8792157cb4f27fb949c035f45518c61e884bb86e6f420204379c2baa8beb66e": "001002d66a5840f440e0dcc55ce6c8e198301c49f0911b455e01567160875e8a",
            "b4af39d5b65a14849e885a9d65f0efe4f4e689989689c28c16cfcb3a6e78db5a": "a1f182eb98a06ba8bf210f0a167d8a3adb6f323e492b14c823b8f1b4fff8cf3d",
        },
        "ignore_intersections": False,
    }
    performance_before = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 12, "unique_users": 3751, "index": 2}
    performance_after = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 9, "unique_users": 3751, "index": 4}
    return {
        "args": args,
        "expected_args": expected_args,
        "performance_before": performance_before,
        "performance_after": performance_after,
    }


@pytest.fixture
def pipe_full_params() -> dict:
    args = {"func": lambda _df: _df.assign(new_column=100, axis=1)}

    expected_args = {"func": "7b83475da05c2d681ab9e8630b81081b3bdfaf0d594649af9ad98266d756fe98"}
    performance_before = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 12, "unique_users": 3751, "index": 2}
    performance_after = {"custom_cols": 3, "shape": [32283, 9], "unique_events": 12, "unique_users": 3751, "index": 4}
    return {
        "args": args,
        "expected_args": expected_args,
        "performance_before": performance_before,
        "performance_after": performance_after,
    }


@pytest.fixture
def collapse_loops_full_params() -> dict:
    performance_before = {"custom_cols": 1, "shape": [32283, 7], "unique_events": 12, "unique_users": 3751, "index": 2}
    performance_after = {"custom_cols": 1, "shape": [26383, 7], "unique_events": 15, "unique_users": 3751, "index": 4}
    return {
        "args": {"suffix": "loop", "time_agg": "max"},
        "expected_args": {"suffix": "loop", "time_agg": "max"},
        "performance_before": performance_before,
        "performance_after": performance_after,
    }


@pytest.fixture
def describe_full_params() -> dict:
    expected_args = {
        "session_col": "5c3a09bf22e12411b6af48dfe0b85c2d9d1181cd391e089574c8cf3ca1e5e4ad",
        "raw_events_only": True,
    }
    return {
        "args": {"session_col": "session_id", "raw_events_only": True},
        "expected_args": expected_args,
        "performance_info": {},
    }


@pytest.fixture
def describe_events_full_params() -> dict:
    expected_args = {
        "session_col": "5c3a09bf22e12411b6af48dfe0b85c2d9d1181cd391e089574c8cf3ca1e5e4ad",
        "raw_events_only": True,
        "event_list": {"len": 3, "len_flatten": 3},
    }
    return {
        "args": {"session_col": "session_id", "raw_events_only": True, "event_list": ["catalog", "main", "cart"]},
        "expected_args": expected_args,
        "performance_info": {},
    }


@pytest.fixture
def timedelta_hist_full_params() -> dict:
    args = {
        "raw_events_only": True,
        "event_pair": ["catalog", "main"],
        "adjacent_events_only": False,
        "weight_col": "session_id",
        "time_agg": "mean",
        "timedelta_unit": "m",
        "log_scale": True,
        "lower_cutoff_quantile": 0.01,
        "upper_cutoff_quantile": 0.99,
        "bins": 10,
        "width": 4,
        "height": 3,
        "show_plot": False,
    }
    expected_args = {
        "raw_events_only": True,
        "event_pair": {"len": 2, "len_flatten": 2},
        "adjacent_events_only": False,
        "weight_col": "5c3a09bf22e12411b6af48dfe0b85c2d9d1181cd391e089574c8cf3ca1e5e4ad",
        "time_agg": "mean",
        "timedelta_unit": "m",
        "log_scale": True,
        "lower_cutoff_quantile": 0.01,
        "upper_cutoff_quantile": 0.99,
        "bins": 10,
        "width": 4,
        "height": 3,
        "show_plot": False,
    }
    return {"args": args, "expected_args": expected_args, "performance_info": {}}


@pytest.fixture
def user_lifetime_hist_full_params() -> dict:
    args = {
        "timedelta_unit": "m",
        "log_scale": True,
        "lower_cutoff_quantile": 0.01,
        "upper_cutoff_quantile": 0.99,
        "bins": 10,
        "width": 4,
        "height": 3,
        "show_plot": False,
    }
    expected_args = {
        "timedelta_unit": "m",
        "log_scale": True,
        "lower_cutoff_quantile": 0.01,
        "upper_cutoff_quantile": 0.99,
        "bins": 10,
        "width": 4,
        "height": 3,
        "show_plot": False,
    }
    return {"args": args, "expected_args": expected_args, "performance_info": {}}


@pytest.fixture
def event_timestamp_hist_full_params() -> dict:
    args = {
        "event_list": ["catalog", "main", "cart"],
        "raw_events_only": True,
        "lower_cutoff_quantile": 0.01,
        "upper_cutoff_quantile": 0.99,
        "bins": 10,
        "width": 4,
        "height": 3,
        "show_plot": False,
    }
    expected_args = {
        "event_list": {"len": 3, "len_flatten": 3},
        "raw_events_only": True,
        "lower_cutoff_quantile": 0.01,
        "upper_cutoff_quantile": 0.99,
        "bins": 10,
        "width": 4,
        "height": 3,
        "show_plot": False,
    }
    return {"args": args, "expected_args": expected_args, "performance_info": {}}
