import numpy as np
import pytest


@pytest.fixture
def correct_basic() -> np.array:
    list_values = np.array(
        [172802.0, 431940.0, 2505540.0, 863940.0, 1900740.0, 172740.0, 518340.0, 1641540.0, 1468740.0]
    )
    return list_values


@pytest.fixture
def correct_basic_bins() -> np.array:
    correct_bins = np.array([172740.0, 639300.0, 1105860.0, 1572420.0, 2038980.0, 2505540.0])
    return correct_bins


@pytest.fixture
def correct_timedelta_unit() -> np.array:
    list_values = np.array([48.0, 119.98, 695.98, 239.98, 527.98, 47.98, 143.98, 455.98, 407.98])

    return list_values


@pytest.fixture
def correct_timedelta_unit_bins() -> np.array:
    correct_bins = np.array([48.0, 177.6, 307.2, 436.8, 566.4, 696.0])
    return correct_bins


@pytest.fixture
def correct_log_scale() -> np.array:
    list_values = np.array([48.0, 119.98, 695.98, 239.98, 527.98, 47.98, 143.98, 455.98, 407.98])

    return list_values


@pytest.fixture
def correct_log_scale_bins() -> np.array:
    correct_bins = np.array([48.0, 81.9, 139.9, 238.8, 407.7, 696.0])
    return correct_bins


@pytest.fixture
def correct_lower_cutoff_quantile() -> np.array:
    list_values = np.array([695.98, 239.98, 527.98, 455.98, 407.98])

    return list_values


@pytest.fixture
def correct_lower_cutoff_quantile_bins() -> np.array:
    correct_bins = np.array([240.0, 331.2, 422.4, 513.6, 604.8, 696.0])
    return correct_bins


@pytest.fixture
def correct_upper_cutoff_quantile() -> np.array:
    list_values = np.array([48.0, 119.98, 239.98, 47.98, 143.98])

    return list_values


@pytest.fixture
def correct_upper_cutoff_quantile_bins() -> np.array:
    correct_bins = np.array([48.0, 86.4, 124.8, 163.2, 201.6, 240.0])
    return correct_bins


@pytest.fixture
def correct_upper_lower_cutoff_quantile() -> np.array:
    list_values = np.array([239.98])

    return list_values


@pytest.fixture
def correct_upper_lower_cutoff_quantile_bins() -> np.array:
    correct_bins = np.array([239.5, 239.7, 239.9, 240.1, 240.3, 240.5])
    return correct_bins
