import numpy as np
import pytest


@pytest.fixture
def corr_default() -> np.array:
    return np.array([1.0, 1.0, 86398.0, 1.0, 86399.0, 1.0, 1.0, 86340.0, 86400.0])


@pytest.fixture
def corr_default_bins() -> np.array:
    return np.array([1.0, 17280.8, 34560.6, 51840.4, 69120.2, 86400.0])


@pytest.fixture
def corr_event_pair() -> np.array:
    return np.array([1.0, 1.0, 86340.0])


@pytest.fixture
def corr_event_pair_bins() -> np.array:
    return np.array([1.0, 17268.8, 34536.6, 51804.4, 69072.2, 86340.0])


@pytest.fixture
def corr_adjacent_event_pairs() -> np.array:
    return np.array([1.0, 1.0, 2.0, 86340.0])


@pytest.fixture
def corr_adjacent_event_pairs_bins() -> np.array:
    return np.array([1.0, 17268.8, 34536.6, 51804.4, 69072.2, 86340.0])


@pytest.fixture
def corr_timedelta_unit() -> np.array:
    return np.array([0.0167, 0.0167, 1439.0])


@pytest.fixture
def corr_timedelta_unit_bins() -> np.array:
    return np.array([0.0, 287.8, 575.6, 863.4, 1151.2, 1439.0])


@pytest.fixture
def corr_lower_quantile() -> np.array:
    return np.array([86398.0, 86399.0, 86340.0, 86400.0])


@pytest.fixture
def corr_lower_quantile_bins() -> np.array:
    return np.array([86340.0, 86352.0, 86364.0, 86376.0, 86388.0, 86400.0])


@pytest.fixture
def corr_upper_quantile() -> np.array:
    return np.array([1.0, 1.0, 1.0, 1.0, 1.0])


@pytest.fixture
def corr_upper_quantile_bins() -> np.array:
    return np.array([0.5, 0.7, 0.9, 1.1, 1.3, 1.5])


@pytest.fixture
def corr_lower_upper_quantile() -> np.array:
    return np.array([86398.0, 86340.0])


@pytest.fixture
def corr_lower_upper_quantile_bins() -> np.array:
    return np.array([86340.0, 86351.6, 86363.2, 86374.8, 86386.4, 86398.0])


@pytest.fixture
def corr_log_scale_x() -> np.array:
    return np.array([1.0, 1.0, 86398.0, 1.0, 86399.0, 1.0, 1.0, 86400.0, 86400.0, 0.1, 0.1, 0.1])


@pytest.fixture
def corr_log_scale_x_bins() -> np.array:
    return np.array([0.1, 1.5, 23.7, 364.7, 5613.2, 86400.0])


@pytest.fixture
def corr_agg() -> np.array:
    return np.array([24686.0, 86370.0])


@pytest.fixture
def corr_agg_bins() -> np.array:
    return np.array([24686.0, 37022.8, 49359.6, 61696.4, 74033.2, 86370.0])


@pytest.fixture
def corr_es_start_path_end() -> np.array:
    return np.array([172802.0, 172800.0])


@pytest.fixture
def corr_es_start_path_end_bins() -> np.array:
    return np.array([172800.0, 172800.4, 172800.8, 172801.2, 172801.6, 172802.0])


@pytest.fixture
def corr_path_start_es_end() -> np.array:
    return np.array([])


@pytest.fixture
def corr_path_start_es_end_bins() -> np.array:
    return np.array([])


@pytest.fixture
def corr_path_end_es_end() -> np.array:
    return np.array([0.0, 2.0])


@pytest.fixture
def corr_path_end_es_end_bins() -> np.array:
    return np.array([0.0, 0.4, 0.8, 1.2, 1.6, 2.0])


@pytest.fixture
def corr_path_start_path_end() -> np.array:
    return np.array([])


@pytest.fixture
def corr_path_start_path_end_bins() -> np.array:
    return np.array([])


@pytest.fixture
def corr_sessions() -> np.array:
    return np.array([1.0, 1.0, 2.0])


@pytest.fixture
def corr_sessions_bins() -> np.array:
    return np.array([1.0, 1.2, 1.4, 1.6, 1.8, 2.0])
