import pandas as pd
import pytest


@pytest.fixture
def funnel_open_corr() -> pd.DataFrame:
    tuples = [
        ("all users", "catalog"),
        ("all users", "product1 | product2"),
        ("all users", "cart"),
        ("all users", "payment_done"),
    ]
    ind = pd.MultiIndex.from_tuples(tuples, names=("segment_name", "stages"))

    correct_res = pd.DataFrame(
        [[6.0, 100.0, 100.0], [8.0, 133.33, 133.33], [8.0, 100.0, 133.33], [8.0, 100.0, 133.33]],
        index=ind,
        columns=pd.Index(["unique_users", "%_of_initial", "%_of_total"], dtype="object"),
    )
    return correct_res


@pytest.fixture
def funnel_open_stage_names_corr() -> pd.DataFrame:
    tuples = [("all users", "catalog"), ("all users", "product"), ("all users", "cart"), ("all users", "payment_done")]
    ind = pd.MultiIndex.from_tuples(tuples, names=("segment_name", "stages"))

    correct_res = pd.DataFrame(
        [[6.0, 100.0, 100.0], [8.0, 133.33, 133.33], [8.0, 100.0, 133.33], [8.0, 100.0, 133.33]],
        index=ind,
        columns=pd.Index(["unique_users", "%_of_initial", "%_of_total"], dtype="object"),
    )
    return correct_res


@pytest.fixture
def funnel_closed_corr() -> pd.DataFrame:
    tuples = [
        ("all users", "catalog"),
        ("all users", "product1 | product2"),
        ("all users", "cart"),
        ("all users", "payment_done"),
    ]
    ind = pd.MultiIndex.from_tuples(tuples, names=("segment_name", "stages"))

    correct_res = pd.DataFrame(
        [[6.0, 100.0, 100.0], [4.0, 66.67, 66.67], [4.0, 100.0, 66.67], [2.0, 50.00, 33.33]],
        index=ind,
        columns=pd.Index(["unique_users", "%_of_initial", "%_of_total"], dtype="object"),
    )
    return correct_res


@pytest.fixture
def funnel_hybrid_corr() -> pd.DataFrame:
    tuples = [
        ("all users", "catalog"),
        ("all users", "product1 | product2"),
        ("all users", "cart"),
        ("all users", "payment_done"),
    ]
    ind = pd.MultiIndex.from_tuples(tuples, names=("segment_name", "stages"))

    correct_res = pd.DataFrame(
        [[6.0, 100.0, 100.0], [4.0, 66.67, 66.67], [4.0, 100.0, 66.67], [4.0, 100.0, 66.67]],
        index=ind,
        columns=pd.Index(["unique_users", "%_of_initial", "%_of_total"], dtype="object"),
    )
    return correct_res


@pytest.fixture
def funnel_hybrid_segments_corr() -> pd.DataFrame:
    tuples = [
        ("group 0", "catalog"),
        ("group 0", "product1 | product2"),
        ("group 0", "cart"),
        ("group 0", "payment_done"),
        ("group 1", "catalog"),
        ("group 1", "product1 | product2"),
        ("group 1", "cart"),
        ("group 1", "payment_done"),
    ]
    ind = pd.MultiIndex.from_tuples(tuples, names=("segment_name", "stages"))

    correct_res = pd.DataFrame(
        [
            [3.0, 100.0, 100.0],
            [2.0, 66.67, 66.67],
            [2.0, 100.0, 66.67],
            [2.0, 100.0, 66.67],
            [3.0, 100.0, 100.0],
            [2.0, 66.67, 66.67],
            [2.0, 100.0, 66.67],
            [2.0, 100.0, 66.67],
        ],
        index=ind,
        columns=pd.Index(["unique_users", "%_of_initial", "%_of_total"], dtype="object"),
    )
    return correct_res


@pytest.fixture
def funnel_closed_segment_names_corr() -> pd.DataFrame:
    tuples = [
        ("conv_users", "catalog"),
        ("conv_users", "product1 | product2"),
        ("conv_users", "cart"),
        ("conv_users", "payment_done"),
        ("non_conv_users", "catalog"),
        ("non_conv_users", "product1 | product2"),
        ("non_conv_users", "cart"),
        ("non_conv_users", "payment_done"),
    ]
    ind = pd.MultiIndex.from_tuples(tuples, names=("segment_name", "stages"))

    correct_res = pd.DataFrame(
        [
            [3.0, 100.0, 100.0],
            [2.0, 66.67, 66.67],
            [2.0, 100.0, 66.67],
            [1.0, 50.0, 33.33],
            [3.0, 100.0, 100.0],
            [2.0, 66.67, 66.67],
            [2.0, 100.0, 66.67],
            [1.0, 50.0, 33.33],
        ],
        index=ind,
        columns=pd.Index(["unique_users", "%_of_initial", "%_of_total"], dtype="object"),
    )
    return correct_res
