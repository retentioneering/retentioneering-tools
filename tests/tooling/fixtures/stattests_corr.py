import pandas as pd
import pytest


@pytest.fixture
def ks_2samp_corr():
    correct_result = {
        "group_one_name": "group_1",
        "group_one_size": 4,
        "group_one_mean": 69.625,
        "group_one_SD": 19.246,
        "group_two_name": "group_2",
        "group_two_size": 4,
        "group_two_mean": 56.964,
        "group_two_SD": 10.112,
        "greatest_group_name": "group_2",
        "least_group_name": "group_1",
        "is_group_one_greatest": False,
        "p_val": 0.4,
        "power_estimated": 0.227,
    }
    return correct_result


@pytest.fixture
def chi2_contingency_corr():
    correct_result = {
        "group_one_name": "group_1",
        "group_one_size": 4,
        "group_one_mean": 1.0,
        "group_one_SD": 0.0,
        "group_two_name": "group_2",
        "group_two_size": 4,
        "group_two_mean": 1.0,
        "group_two_SD": 0.0,
        "greatest_group_name": "group_1",
        "is_group_one_greatest": True,
        "p_val": 1.0,
        "power_estimated": 0,
    }
    return correct_result


@pytest.fixture
def ztest_corr():
    correct_result = {
        "group_one_name": "group_1",
        "group_one_size": 4,
        "group_one_mean": 7.0,
        "group_one_SD": 1.5811,
        "group_two_name": "group_2",
        "group_two_size": 4,
        "group_two_mean": 8.25,
        "group_two_SD": 0.8291,
        "greatest_group_name": "group_1",
        "is_group_one_greatest": True,
        "p_val": 0.1126,
        "power_estimated": 0.2852,
    }
    return correct_result


@pytest.fixture
def fisher_exact_corr():
    correct_result = {
        "group_one_name": "group_1",
        "group_one_size": 4,
        "group_one_mean": 1.0,
        "group_one_SD": 0.0,
        "group_two_name": "group_2",
        "group_two_size": 4,
        "group_two_mean": 0.75,
        "group_two_SD": 0.4331,
        "greatest_group_name": "group_1",
        "is_group_one_greatest": True,
        "p_val": 0.5,
        "power_estimated": 0.0,
    }
    return correct_result
