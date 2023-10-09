from __future__ import annotations

import copy
import os
import tempfile

import pytest

from retentioneering.utils.config import Config


@pytest.fixture
def simple_config():
    _TestConfig = copy.deepcopy(Config)
    _TestConfig._get_path_for_config = lambda _: simple_tmp_file.name
    config = _TestConfig()
    config.user.pk = "00000000-0000-0000-aaaa-eeeeeeeeeeee"
    yield config


simple_tmp_file = tempfile.NamedTemporaryFile(delete=False)


@pytest.fixture
def drop_temp_file_simple_config():
    yield
    os.remove(simple_tmp_file.name)
