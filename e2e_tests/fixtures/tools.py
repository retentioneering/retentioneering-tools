from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from .common import custom_X, groups, test_stream


@pytest.fixture
def funnel_fit_full_params(groups: tuple[list, list]) -> dict:
    args = {
        "stages": ["main", ["catalog", "cart"]],
        "stage_names": ["first_stage", "second_stage"],
        "segments": groups,
        "segment_names": ["first_group", "second_group"],
        "funnel_type": "open",
    }
    expected_args = {
        "stages": {"len": 2, "len_flatten": 3},
        "stage_names": {"len": 2, "len_flatten": 2},
        "segments": {"len": 2, "len_flatten": 3751},
        "segment_names": {"len": 2, "len_flatten": 2},
        "funnel_type": "open",
    }
    return {"args": args, "expected_args": expected_args, "performance_info": {}}


@pytest.fixture
def funnel_plot_params() -> dict:
    return {"args": {}, "expected_args": {}, "performance_info": {}}


@pytest.fixture
def cohorts_fit_full_params() -> dict:
    args = {
        "cohort_start_unit": "M",
        "cohort_period": (1, "M"),
        "average": True,
        "cut_bottom": 1,
        "cut_right": 2,
        "cut_diagonal": 3,
    }
    expected_args = {
        "cohort_start_unit": "M",
        "cohort_period": [1, "M"],
        "average": True,
        "cut_bottom": 1,
        "cut_right": 2,
        "cut_diagonal": 3,
    }
    performance_info = {"shape": [6, 3]}
    return {"args": args, "expected_args": expected_args, "performance_info": performance_info}


@pytest.fixture
def cohorts_lineplot_full_params() -> dict:
    args = {
        "plot_type": "all",
        "width": 4.0,
        "height": 3.0,
    }
    expected_args = {
        "plot_type": "all",
        "width": 4.0,
        "height": 3.0,
    }
    performance_info = {"shape": [6, 3]}
    return {"args": args, "expected_args": expected_args, "performance_info": performance_info}


@pytest.fixture
def cohorts_heatmap_full_params() -> dict:
    args = {
        "width": 4.0,
        "height": 3.0,
    }
    expected_args = {
        "width": 4.0,
        "height": 3.0,
    }
    performance_info = {"shape": [6, 3]}
    return {"args": args, "expected_args": expected_args, "performance_info": performance_info}


@pytest.fixture
def step_matrix_fit_full_params(groups: tuple[list, list]) -> dict:
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
    }
    performance_info = {"shape": [9, 20], "unique_nodes": 4}
    return {"args": args, "expected_args": expected_args, "performance_info": performance_info}


@pytest.fixture
def step_matrix_plot_params() -> dict:
    performance_info = {"shape": [9, 20], "unique_nodes": 4}
    return {"args": {}, "expected_args": {}, "performance_info": performance_info}


@pytest.fixture
def step_sankey_fit_full_params() -> dict:
    args = {
        "max_steps": 8,
        "threshold": 0.05,
        "sorting": None,
        "targets": ["catalog", "cart"],
    }
    expected_args = {
        "max_steps": 8,
        "threshold": 0.05,
        "sorting": None,
        "targets": {"len": 2, "len_flatten": 2},
    }
    performance_info = {"links_count": 176, "nodes_count": 55}
    return {"args": args, "expected_args": expected_args, "performance_info": performance_info}


@pytest.fixture
def step_sankey_plot_full_params() -> dict:
    args = {
        "autosize": False,
        "width": 400,
        "height": 300,
    }
    expected_args = {
        "autosize": False,
        "width": 400,
        "height": 300,
    }
    performance_info = {"links_count": 176, "nodes_count": 55}
    return {"args": args, "expected_args": expected_args, "performance_info": performance_info}


@pytest.fixture
def stattests_fit_full_params(groups: tuple[list, list]) -> dict:
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
def stattests_plot_params() -> dict:
    return {"args": {}, "expected_args": {}, "performance_info": {}}


@pytest.fixture
def transition_graph_plot_full_params() -> dict:
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
def clusters_features_params() -> dict:
    args = {"feature_type": "tfidf", "ngram_range": (1, 1)}
    expected_args = {"feature_type": "tfidf", "ngram_range": [1, 1]}
    performance_info = {"shape": [4, 4]}
    return {"args": args, "expected_args": expected_args, "performance_info": performance_info}


@pytest.fixture
def clusters_fit_params(custom_X: pd.DataFrame) -> dict:
    args = {"method": "kmeans", "n_clusters": 4, "X": custom_X, "random_state": 0}

    expected_args = {"method": "kmeans", "n_clusters": 4, "X": [4, 4], "random_state": 0}
    performance_info: dict = {}
    return {"args": args, "expected_args": expected_args, "performance_info": performance_info}


@pytest.fixture
def clusters_plot_params() -> dict:
    args = {"targets": ["event1", "event2"]}

    expected_args = {"targets": {"len": 2, "len_flatten": 2}}
    performance_info: dict = {}
    return {"args": args, "expected_args": expected_args, "performance_info": performance_info}


@pytest.fixture
def clusters_projection_params() -> dict:
    args = {"method": "tsne", "targets": "event1", "color_type": "targets", "perplexity": 3}

    expected_args = {
        "method": "tsne",
        "targets": "29663b9a32ee32c2ca5a645117696ce0888c84b39f8fd91c0ec4ebcd09025df4",
        "color_type": "targets",
    }
    performance_info: dict = {}
    return {"args": args, "expected_args": expected_args, "performance_info": performance_info}


@pytest.fixture
def clusters_diff_params() -> dict:
    args = {"cluster_id1": 1, "cluster_id2": "one", "top_n_events": 3, "weight_col": "user_id", "targets": "event4"}

    expected_args = {
        "cluster_id1": 1,
        "cluster_id2": "7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed",
        "top_n_events": 3,
        "weight_col": "f89d6b6960453241bc5b09b4d0d8ad86d53769e051473350c2bf94e39077967b",
        "targets": "d7fefb21ddf9f1cbe37a0081ad00c92a4cae8c536c926a12a1e3a3682270e32e",
    }

    performance_info: dict = {}
    return {"args": args, "expected_args": expected_args, "performance_info": performance_info}


@pytest.fixture
def clusters_filter_params() -> dict:
    args = {"cluster_id": "1"}

    expected_args = {"cluster_id": "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b"}
    performance_info: dict = {}
    return {"args": args, "expected_args": expected_args, "performance_info": performance_info}


@pytest.fixture
def preprocessing_graph_combine_params() -> dict:
    performance_info = {"custom_cols": 2, "shape": [37329, 8], "unique_events": 21, "unique_users": 2294, "index": 10}
    return {"performance_info": performance_info}
