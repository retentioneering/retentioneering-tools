import itertools

import numpy as np
import pandas as pd
import pytest

FLOAT_PRECISION = 2


@pytest.fixture
def sequences_basic_corr() -> pd.DataFrame:
    correct_result = pd.DataFrame(
        [
            [2, 0.67, 4, 0.13, "other", list(["user1", "user2"])],
            [2, 0.67, 3, 0.1, "other", list(["user1", "user2"])],
            [1, 0.33, 1, 0.03, "other", list(["user1"])],
            [2, 0.67, 3, 0.1, "other", list(["user1", "user2"])],
            [1, 0.33, 1, 0.03, "other", list(["user1"])],
            [1, 0.33, 1, 0.03, "other", list(["user2"])],
            [3, 1, 7, 0.23, "other", list(["user1", "user2", "user3"])],
            [1, 0.33, 1, 0.03, "other", list(["user1"])],
            [1, 0.33, 2, 0.06, "loop", list(["user2"])],
            [1, 0.33, 3, 0.1, "other", list(["user3"])],
            [1, 0.33, 3, 0.1, "other", list(["user3"])],
            [1, 0.33, 2, 0.06, "other", list(["user3"])],
        ],
        columns=["user_id", "user_id_share", "count", "count_share", "sequence_type", "user_id_sample"],
    )
    correct_result.index = pd.Index(
        ["A", "A -> B", "A -> C", "B", "B -> A", "B -> C", "C", "C -> A", "C -> C", "C -> D", "D", "D -> C"],
        dtype=str,
        name="Sequence",
    )
    return correct_result


@pytest.fixture
def sequences_groups_corr() -> pd.DataFrame:
    group_names = ("group_1", "group_2")
    samples_columns = [("user_id_sample", group_names[0]), ("user_id_sample", group_names[1])]
    metrics, group_names = (
        "user_id",
        "user_id_share",
        "count",
        "count_share",
    ), (group_names[0], group_names[1], "delta_abs", "delta")

    final_columns = list(itertools.product(metrics, group_names)) + [("sequence_type", "")] + samples_columns
    index_cols = pd.MultiIndex.from_tuples(final_columns)
    seq_index = pd.Index(
        ["A -> B", "A -> C", "B -> A", "B -> C", "C -> A", "C -> C", "C -> D", "D -> C"], dtype=str, name="Sequence"
    )

    correct_result = pd.DataFrame(
        [
            [2, 0, -2, -1.0, 1, 0.0, -1, -1.0, 3, 0, -3, -1.0, 0.33, 0.0, -0.33, -1.0, "other", ["user1", "user2"], []],
            [1, 0, -1, -1.0, 0.5, 0.0, -0.5, -1.0, 1, 0, -1, -1.0, 0.11, 0.0, -0.11, -1.0, "other", ["user1"], []],
            [1, 0, -1, -1.0, 0.5, 0.0, -0.5, -1.0, 1, 0, -1, -1.0, 0.11, 0.0, -0.11, -1.0, "other", ["user1"], []],
            [
                1,
                0,
                -1,
                -1.0,
                0.5,
                0.0,
                -0.5,
                -1.0,
                1,
                0,
                -1,
                -1.0,
                0.11,
                0.0,
                -0.11,
                -1.0,
                "other",
                list(["user2"]),
                [],
            ],
            [
                1,
                0,
                -1,
                -1.0,
                0.5,
                0.0,
                -0.5,
                -1.0,
                1,
                0,
                -1,
                -1.0,
                0.11,
                0.0,
                -0.11,
                -1.0,
                "other",
                list(["user1"]),
                [],
            ],
            [1, 0, -1, -1.0, 0.5, 0.0, -0.5, -1.0, 2, 0, -2, -1.0, 0.22, 0.0, -0.22, -1.0, "loop", list(["user2"]), []],
            [0, 1, 1, np.inf, 0.0, 1, 1, np.inf, 0, 3, 3, np.inf, 0.0, 0.6, 0.6, np.inf, "other", [], list(["user3"])],
            [0, 1, 1, np.inf, 0.0, 1, 1, np.inf, 0, 2, 2, np.inf, 0.0, 0.4, 0.4, np.inf, "other", [], list(["user3"])],
        ],
        columns=index_cols,
        index=seq_index,
    )

    return correct_result


@pytest.fixture
def sequences_group_names_corr() -> pd.DataFrame:
    group_names = ("pay", "no_pay")
    samples_columns = [("user_id_sample", group_names[0]), ("user_id_sample", group_names[1])]
    metrics, group_names = (
        "user_id",
        "user_id_share",
        "count",
        "count_share",
    ), (group_names[0], group_names[1], "delta_abs", "delta")

    final_columns = list(itertools.product(metrics, group_names)) + [("sequence_type", "")] + samples_columns
    index_cols = pd.MultiIndex.from_tuples(final_columns)
    seq_index = pd.Index(
        ["A -> B", "A -> C", "B -> A", "B -> C", "C -> A", "C -> C", "C -> D", "D -> C"], dtype=str, name="Sequence"
    )

    correct_result = pd.DataFrame(
        [
            [2, 0, -2, -1.0, 1, 0.0, -1, -1.0, 3, 0, -3, -1.0, 0.33, 0.0, -0.33, -1.0, "other", ["user1", "user2"], []],
            [1, 0, -1, -1.0, 0.5, 0.0, -0.5, -1.0, 1, 0, -1, -1.0, 0.11, 0.0, -0.11, -1.0, "other", ["user1"], []],
            [1, 0, -1, -1.0, 0.5, 0.0, -0.5, -1.0, 1, 0, -1, -1.0, 0.11, 0.0, -0.11, -1.0, "other", ["user1"], []],
            [1, 0, -1, -1.0, 0.5, 0.0, -0.5, -1.0, 1, 0, -1, -1.0, 0.11, 0.0, -0.11, -1.0, "other", ["user2"], []],
            [1, 0, -1, -1.0, 0.5, 0.0, -0.5, -1.0, 1, 0, -1, -1.0, 0.11, 0.0, -0.11, -1.0, "other", ["user1"], []],
            [1, 0, -1, -1.0, 0.5, 0.0, -0.5, -1.0, 2, 0, -2, -1.0, 0.22, 0.0, -0.22, -1.0, "loop", ["user2"], []],
            [0, 1, 1, np.inf, 0.0, 1, 1, np.inf, 0, 3, 3, np.inf, 0.0, 0.6, 0.6, np.inf, "other", [], ["user3"]],
            [0, 1, 1, np.inf, 0.0, 1, 1, np.inf, 0, 2, 2, np.inf, 0.0, 0.4, 0.4, np.inf, "other", [], ["user3"]],
        ],
        columns=index_cols,
        index=seq_index,
    )

    return correct_result


@pytest.fixture
def sequences_weight_col_corr() -> pd.DataFrame:
    seq_index = pd.Index(["A -> B", "A -> C", "C -> C", "C -> D", "D -> C"], dtype=str, name="Sequence")

    correct_result = pd.DataFrame(
        [
            [3, 0.50, 3, 0.27, "other", ["user1_1", "user1_3", "user2_1"]],
            [1, 0.17, 1, 0.09, "other", ["user1_2"]],
            [1, 0.17, 2, 0.18, "loop", ["user2_2"]],
            [1, 0.17, 3, 0.27, "other", ["user3_1"]],
            [1, 0.17, 2, 0.18, "other", ["user3_1"]],
        ],
        columns=["session_id", "session_id_share", "count", "count_share", "sequence_type", "session_id_sample"],
        index=seq_index,
    )

    return correct_result


@pytest.fixture
def sequences_sequence_type_corr() -> pd.DataFrame:
    seq_index = pd.Index(
        [
            "A -> B -> A",
            "A -> B -> C",
            "A -> C -> A",
            "B -> A -> C",
            "B -> C -> C",
            "C -> A -> B",
            "C -> C -> C",
            "C -> D -> C",
            "D -> C -> D",
        ],
        dtype=str,
        name="Sequence",
    )

    correct_result = pd.DataFrame(
        [
            [1, 0.33, 1, 0.09, "cycle", list(["user1"])],
            [1, 0.33, 1, 0.09, "other", list(["user2"])],
            [1, 0.33, 1, 0.09, "cycle", list(["user1"])],
            [1, 0.33, 1, 0.09, "other", list(["user1"])],
            [1, 0.33, 1, 0.09, "other", list(["user2"])],
            [1, 0.33, 1, 0.09, "other", list(["user1"])],
            [1, 0.33, 1, 0.09, "loop", list(["user2"])],
            [1, 0.33, 2, 0.18, "cycle", list(["user3"])],
            [1, 0.33, 2, 0.18, "cycle", list(["user3"])],
        ],
        columns=["user_id", "user_id_share", "count", "count_share", "sequence_type", "user_id_sample"],
        index=seq_index,
    )

    return correct_result


@pytest.fixture
def sequences_space_name_corr() -> pd.DataFrame:
    correct_result = pd.DataFrame(
        [
            [2, 0.67, 4, 0.13, "other", list(["user1", "user2"])],
            [2, 0.67, 3, 0.1, "other", list(["user1", "user2"])],
            [1, 0.33, 1, 0.03, "other", list(["user1"])],
            [2, 0.67, 3, 0.1, "other", list(["user1", "user2"])],
            [1, 0.33, 1, 0.03, "other", list(["user1"])],
            [1, 0.33, 1, 0.03, "other", list(["user2"])],
            [3, 1, 7, 0.23, "other", list(["user1", "user2", "user3"])],
            [1, 0.33, 1, 0.03, "other", list(["user1"])],
            [1, 0.33, 2, 0.06, "loop", list(["user2"])],
            [1, 0.33, 3, 0.1, "other", list(["user3"])],
            [1, 0.33, 3, 0.1, "other", list(["user3"])],
            [1, 0.33, 2, 0.06, "other", list(["user3"])],
        ],
        columns=["user_id", "user_id_share", "count", "count_share", "sequence_type", "user_id_sample"],
    )
    correct_result.index = pd.Index(
        [
            "A AA",
            "A AA -> B",
            "A AA -> C",
            "B",
            "B -> A AA",
            "B -> C",
            "C",
            "C -> A AA",
            "C -> C",
            "C -> D",
            "D",
            "D -> C",
        ],
        dtype=str,
        name="Sequence",
    )
    return correct_result


@pytest.fixture
def sequences_metrics_corr() -> pd.DataFrame:
    correct_result = pd.DataFrame(
        [
            [7, "other", list(["user1"])],
            [4, "other", list(["user1"])],
            [3, "other", list(["user1"])],
            [3, "other", list(["user1"])],
            [3, "other", list(["user3"])],
            [3, "other", list(["user3"])],
            [2, "loop", list(["user2"])],
            [2, "other", list(["user3"])],
            [1, "other", list(["user1"])],
            [1, "other", list(["user1"])],
            [1, "other", list(["user2"])],
            [1, "other", list(["user1"])],
        ],
        columns=["count", "sequence_type", "user_id_sample"],
    )
    correct_result.index = pd.Index(
        ["C", "A", "A -> B", "B", "C -> D", "D", "C -> C", "D -> C", "A -> C", "B -> A", "B -> C", "C -> A"],
        dtype=str,
        name="Sequence",
    )

    return correct_result[["count", "sequence_type"]]


@pytest.fixture
def sequences_threshold_corr() -> pd.DataFrame:
    correct_result = pd.DataFrame(
        [
            [3, 1.00, 7, 0.23, "other", list(["user1"])],
            [2, 0.67, 4, 0.13, "other", list(["user1"])],
            [2, 0.67, 3, 0.1, "other", list(["user1"])],
            [2, 0.67, 3, 0.1, "other", list(["user1"])],
            [1, 0.33, 2, 0.06, "loop", list(["user2"])],
            [1, 0.33, 3, 0.1, "other", list(["user3"])],
            [1, 0.33, 3, 0.1, "other", list(["user3"])],
            [1, 0.33, 2, 0.06, "other", list(["user3"])],
        ],
        columns=["user_id", "user_id_share", "count", "count_share", "sequence_type", "user_id_sample"],
    )
    correct_result.index = pd.Index(
        ["C", "A", "A -> B", "B", "C -> C", "C -> D", "D", "D -> C"], dtype=str, name="Sequence"
    )

    return correct_result[["user_id", "user_id_share", "count", "count_share", "sequence_type"]]


@pytest.fixture
def sequences_sorting_corr() -> pd.DataFrame:
    correct_result = pd.DataFrame(
        [
            [1, 0.33, 1, 0.07, "other", list(["user1"])],
            [1, 0.33, 1, 0.07, "other", list(["user1"])],
            [1, 0.33, 1, 0.07, "other", list(["user2"])],
            [1, 0.33, 1, 0.07, "other", list(["user1"])],
            [1, 0.33, 2, 0.14, "loop", list(["user2"])],
            [1, 0.33, 2, 0.14, "other", list(["user3"])],
            [2, 0.67, 3, 0.21, "other", list(["user1"])],
            [1, 0.33, 3, 0.21, "other", list(["user3"])],
        ],
        columns=["user_id", "user_id_share", "count", "count_share", "sequence_type", "user_id_sample"],
    )
    correct_result.index = pd.Index(
        ["A -> C", "B -> A", "B -> C", "C -> A", "C -> C", "D -> C", "A -> B", "C -> D"],
        dtype=str,
        name="Sequence",
    )

    return correct_result[["user_id", "user_id_share", "count", "count_share", "sequence_type"]]


@pytest.fixture
def sequences_sorting_groups_corr() -> pd.DataFrame:
    group_names = ("group_1", "group_2")
    samples_columns = [("user_id_sample", group_names[0]), ("user_id_sample", group_names[1])]
    metrics, group_names = (("user_id"),), (group_names[0], group_names[1], "delta_abs", "delta")

    final_columns = list(itertools.product(metrics, group_names)) + [("sequence_type", "")] + samples_columns
    index_cols = pd.MultiIndex.from_tuples(final_columns)
    correct_result = pd.DataFrame(
        [
            [0, 1, 1, np.inf, "other", [], list(["user3"])],
            [0, 1, 1, np.inf, "other", [], list(["user3"])],
            [1, 0, -1, -1.0, "other", list(["user1"]), []],
            [1, 0, -1, -1.0, "other", list(["user1"]), []],
            [1, 0, -1, -1.0, "other", list(["user2"]), []],
            [1, 0, -1, -1.0, "other", list(["user1"]), []],
            [1, 0, -1, -1.0, "loop", list(["user2"]), []],
            [2, 0, -2, -1.0, "other", list(["user1"]), []],
        ],
        columns=index_cols,
    )
    correct_result.index = pd.Index(
        ["C -> D", "D -> C", "A -> C", "B -> A", "B -> C", "C -> A", "C -> C", "A -> B"], dtype=str, name="Sequence"
    )

    return correct_result[list(itertools.product(metrics, group_names)) + [("sequence_type", "")]]


@pytest.fixture
def sequences_sample_size_none_corr() -> pd.DataFrame:
    correct_result = pd.DataFrame(
        [
            [3, 1, 7, 0.23, "other"],
            [2, 0.67, 4, 0.13, "other"],
            [2, 0.67, 3, 0.1, "other"],
            [2, 0.67, 3, 0.1, "other"],
            [1, 0.33, 1, 0.03, "other"],
            [1, 0.33, 1, 0.03, "other"],
            [1, 0.33, 1, 0.03, "other"],
            [1, 0.33, 1, 0.03, "other"],
            [1, 0.33, 2, 0.06, "loop"],
            [1, 0.33, 3, 0.1, "other"],
            [1, 0.33, 3, 0.1, "other"],
            [1, 0.33, 2, 0.06, "other"],
        ],
        columns=["user_id", "user_id_share", "count", "count_share", "sequence_type"],
    )
    correct_result.index = pd.Index(
        ["C", "A", "A -> B", "B", "A -> C", "B -> A", "B -> C", "C -> A", "C -> C", "C -> D", "D", "D -> C"],
        dtype=str,
        name="Sequence",
    )

    return correct_result


@pytest.fixture
def sequences_sample_size_two_corr() -> pd.DataFrame:
    correct_result = pd.DataFrame(
        [
            [3, 1, 7, 0.23, "other", list(["user1", "user2"])],
            [2, 0.67, 4, 0.13, "other", list(["user1", "user2"])],
            [2, 0.67, 3, 0.1, "other", list(["user1", "user2"])],
            [2, 0.67, 3, 0.1, "other", list(["user1", "user2"])],
            [1, 0.33, 1, 0.03, "other", list(["user1"])],
            [1, 0.33, 1, 0.03, "other", list(["user1"])],
            [1, 0.33, 1, 0.03, "other", list(["user2"])],
            [1, 0.33, 1, 0.03, "other", list(["user1"])],
            [1, 0.33, 2, 0.06, "loop", list(["user2"])],
            [1, 0.33, 3, 0.1, "other", list(["user3"])],
            [1, 0.33, 3, 0.1, "other", list(["user3"])],
            [1, 0.33, 2, 0.06, "other", list(["user3"])],
        ],
        columns=["user_id", "user_id_share", "count", "count_share", "sequence_type", "user_id_sample"],
    )
    correct_result.index = pd.Index(
        ["C", "A", "A -> B", "B", "A -> C", "B -> A", "B -> C", "C -> A", "C -> C", "C -> D", "D", "D -> C"],
        dtype=str,
        name="Sequence",
    )
    return correct_result


@pytest.fixture
def sequences_precision_corr() -> pd.DataFrame:
    correct_result = pd.DataFrame(
        [
            [3, 1, 7, 0.2, "other"],
            [2, 0.7, 4, 0.1, "other"],
            [2, 0.7, 3, 0.1, "other"],
            [2, 0.7, 3, 0.1, "other"],
            [1, 0.3, 1, 0.0, "other"],
            [1, 0.3, 1, 0.0, "other"],
            [1, 0.3, 1, 0.0, "other"],
            [1, 0.3, 1, 0.0, "other"],
            [1, 0.3, 2, 0.1, "loop"],
            [1, 0.3, 3, 0.1, "other"],
            [1, 0.3, 3, 0.1, "other"],
            [1, 0.3, 2, 0.1, "other"],
        ],
        columns=["user_id", "user_id_share", "count", "count_share", "sequence_type"],
    )
    correct_result.index = pd.Index(
        ["C", "A", "A -> B", "B", "A -> C", "B -> A", "B -> C", "C -> A", "C -> C", "C -> D", "D", "D -> C"],
        dtype=str,
        name="Sequence",
    )

    return correct_result


@pytest.fixture
def sequences_sample_heatmap_corr() -> pd.DataFrame:
    correct_result = pd.DataFrame(
        [
            [2, 0.67, 3, 0.21, "other", list(["user1"])],
            [1, 0.33, 1, 0.07, "other", list(["user1"])],
            [1, 0.33, 1, 0.07, "other", list(["user1"])],
            [1, 0.33, 1, 0.07, "other", list(["user2"])],
            [1, 0.33, 1, 0.07, "other", list(["user1"])],
            [1, 0.33, 2, 0.14, "loop", list(["user2"])],
            [1, 0.33, 3, 0.21, "other", list(["user3"])],
            [1, 0.33, 2, 0.14, "other", list(["user3"])],
        ],
        columns=["user_id", "user_id_share", "count", "count_share", "sequence_type", "user_id_sample"],
    )
    correct_result.index = pd.Index(
        ["A -> B", "A -> C", "B -> A", "B -> C", "C -> A", "C -> C", "C -> D", "D -> C"], dtype=str, name="Sequence"
    )
    return correct_result[["user_id", "user_id_share", "count", "count_share", "sequence_type"]]


@pytest.fixture
def sequences_heatmap_groups_corr() -> pd.DataFrame:
    group_names = ("group_1", "group_2")
    samples_columns = [("user_id_sample", group_names[0]), ("user_id_sample", group_names[1])]
    metrics, group_names = (("user_id"),), (group_names[0], group_names[1], "delta_abs", "delta")

    final_columns = list(itertools.product(metrics, group_names)) + [("sequence_type", "")] + samples_columns
    index_cols = pd.MultiIndex.from_tuples(final_columns)
    seq_index = pd.Index(
        ["C -> D", "D -> C", "A -> B", "A -> C", "B -> A", "B -> C", "C -> A", "C -> C"], dtype=str, name="Sequence"
    )

    correct_result = pd.DataFrame(
        [
            [0, 1, 1, np.inf, "other", [], list(["user3"])],
            [0, 1, 1, np.inf, "other", [], list(["user3"])],
            [2, 0, -2, -1.0, "other", list(["user1"]), []],
            [1, 0, -1, -1.0, "other", list(["user1"]), []],
            [1, 0, -1, -1.0, "other", list(["user1"]), []],
            [1, 0, -1, -1.0, "other", list(["user2"]), []],
            [1, 0, -1, -1.0, "other", list(["user1"]), []],
            [1, 0, -1, -1.0, "loop", list(["user2"]), []],
        ],
        columns=index_cols,
        index=seq_index,
    )

    return correct_result[list(itertools.product(metrics, group_names)) + [("sequence_type", "")]]
