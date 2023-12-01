import itertools

import numpy as np
import pandas as pd
import pytest

FLOAT_PRECISION = 2


@pytest.fixture
def sequences_eventstream_corr() -> pd.DataFrame:
    group_names = ("pay", "no_pay")
    samples_columns = [("user_id_sample", group_names[0]), ("user_id_sample", group_names[1])]
    metrics, group_names = (
        "user_id",
        "count",
    ), (group_names[0], group_names[1], "delta_abs", "delta")

    final_columns = list(itertools.product(metrics, group_names)) + [("sequence_type", "")] + samples_columns
    index_cols = pd.MultiIndex.from_tuples(final_columns)
    seq_index = pd.Index(["A -> B", "C -> C", "A -> C", "B -> A", "B -> C", "C -> A"], dtype=str, name="Sequence")

    correct_result = pd.DataFrame(
        [
            [2, 0, -2, -1.0, 3, 0, -3, -1.0, "other", list(["user1"]), []],
            [1, 0, -1, -1.0, 2, 0, -2, -1.0, "loop", list(["user2"]), []],
            [1, 0, -1, -1.0, 1, 0, -1, -1.0, "other", list(["user1"]), []],
            [1, 0, -1, -1.0, 1, 0, -1, -1.0, "other", list(["user1"]), []],
            [1, 0, -1, -1.0, 1, 0, -1, -1.0, "other", list(["user2"]), []],
            [1, 0, -1, -1.0, 1, 0, -1, -1.0, "other", list(["user1"]), []],
        ],
        columns=index_cols,
        index=seq_index,
    )

    return correct_result[list(itertools.product(metrics, group_names)) + [("sequence_type", "")]]
