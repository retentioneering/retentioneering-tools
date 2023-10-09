from __future__ import annotations

import json
import os
from dataclasses import asdict

import pytest

from tests.utils.fixtures.config.default_config import (
    retentioneering_config_with_user_pk,
)
from tests.utils.fixtures.config.simple_config import (
    drop_temp_file_simple_config,
    simple_config,
    simple_tmp_file,
)
from tests.utils.fixtures.config.updated_config import (
    drop_temp_file_updated_config,
    updated_config_type_check_value_error,
    updated_config_type_shut_value_error,
    updated_tmp_file,
)


class TestConfig:
    def test_generate_config(self, simple_config, drop_temp_file_simple_config, retentioneering_config_with_user_pk):
        config_data_after_setup = asdict(simple_config)
        del config_data_after_setup["tracking"]

        assert retentioneering_config_with_user_pk == config_data_after_setup
        assert os.path.exists(simple_config._get_path_for_config())

    def test_load_config(self, simple_config, drop_temp_file_simple_config, retentioneering_config_with_user_pk):
        test_config_data = {**retentioneering_config_with_user_pk}
        test_config_data["transition_graph"]["height"] = 100500
        with open(simple_tmp_file.name, "w") as f:
            json.dump(test_config_data, f)

        simple_config.load()

        assert simple_config.transition_graph.height == 100500

    def test_update_config_correctly(
        self, updated_config_type_shut_value_error, drop_temp_file_updated_config, retentioneering_config_with_user_pk
    ):
        # 1. create "old" config
        with open(updated_tmp_file.name, "w") as f:
            json.dump(retentioneering_config_with_user_pk, f)

        # 2. Update config with new params
        updated_config = updated_config_type_shut_value_error()
        updated_config.user.pk = "00000000-0000-0000-aaaa-eeeeeeeeeeee"

        # 4. Generate test data
        updated_data = {"some_update": {"param1": 20, "param2": "test"}, **retentioneering_config_with_user_pk}

        with open(updated_tmp_file.name, "r") as f:
            updated_config_data = json.load(f)

        # 5. Assert
        assert updated_config_data == updated_data

    def test_update_config_correctly_lost_inner_mapping(
        self, updated_config_type_check_value_error, drop_temp_file_updated_config, retentioneering_config_with_user_pk
    ):
        with pytest.raises(ValueError):
            with open(updated_tmp_file.name, "w") as f:
                json.dump(retentioneering_config_with_user_pk, f)

            # 2. Update config with new params
            updated_config = updated_config_type_check_value_error()
            updated_config.user.pk = "00000000-0000-0000-aaaa-eeeeeeeeeeee"

            # 4. Generate test data
            updated_data = {"some_update": {"param1": 20, "param2": "test"}, **retentioneering_config_with_user_pk}

            with open(updated_tmp_file.name, "r") as f:
                updated_config_data = json.load(f)

            # 5. Assert
            assert updated_config_data == updated_data
