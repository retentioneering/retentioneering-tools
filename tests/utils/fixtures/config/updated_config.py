from __future__ import annotations

import os
import tempfile
from dataclasses import field
from typing import Type

import pytest
from pydantic import BaseConfig
from pydantic.dataclasses import dataclass

# need for dataclass inheritance, not edit! Vladimir Makhanov
from retentioneering.utils.config import (
    Config,
    PreprocessiongGraphConfig,
    TrackingConfig,
    TransitionGraphConfig,
    UserConfig,
)

updated_tmp_file = tempfile.NamedTemporaryFile(delete=False)


@pytest.fixture
def drop_temp_file_updated_config():
    yield
    os.remove(updated_tmp_file.name)


@dataclass
class SomeUpdate(BaseConfig):
    param1: int = 20
    param2: str = "test"


@pytest.fixture
def updated_config_type_shut_value_error():
    @dataclass
    class UpdatedConfig(Config):
        some_update: SomeUpdate = field(default_factory=SomeUpdate)

        def _inner_mapping(self) -> dict[str, Type[BaseConfig]]:
            mapping = super()._inner_mapping(_validate=False)
            mapping["some_update"] = SomeUpdate
            return mapping

        def __post_init__(self) -> None:
            super().__post_init__()

    UpdatedConfig._get_path_for_config = lambda _: updated_tmp_file.name
    UpdatedConfig.__pydantic_model__.update_forward_refs()
    yield UpdatedConfig


@pytest.fixture
def updated_config_type_check_value_error():
    @dataclass
    class UpdatedConfig(Config):
        some_update: SomeUpdate = field(default_factory=SomeUpdate)

        def __post_init__(self) -> None:
            super().__post_init__()

    UpdatedConfig._get_path_for_config = lambda _: updated_tmp_file.name
    UpdatedConfig.__pydantic_model__.update_forward_refs()
    yield UpdatedConfig
