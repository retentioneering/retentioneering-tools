import pytest

from retentioneering.utils.config import DEFAULT_CONFIG


@pytest.fixture
def retentioneering_config_with_user_pk():
    config = DEFAULT_CONFIG.copy()
    config["user"]["pk"] = "00000000-0000-0000-aaaa-eeeeeeeeeeee"
    return config
