from __future__ import annotations

import typing
from typing import Type

from retentioneering.utils.registry import RegistryValidationError, ReteRegistry

if typing.TYPE_CHECKING:
    from .params_model import ParamsModel


class ParamsModelRegistry:
    REGISTRY: dict[str, "Type[ParamsModel]"] = {}  # type: ignore

    objects = "ParamsModel"

    def __setitem__(self, key: str, value: "type[ParamsModel]") -> None:
        if key not in self.REGISTRY:
            self.REGISTRY[key] = value

    def __delitem__(self, key: str) -> None:
        del self.REGISTRY[key]

    @classmethod
    def get_registry(cls: typing.Type[ParamsModelRegistry]) -> dict:
        return cls.REGISTRY


params_model_registry = ParamsModelRegistry()


def register_params_model(cls: type[ParamsModel]) -> None:
    params_model_registry[cls.__name__] = cls


def unregister_params_model(cls: type[ParamsModel]) -> None:
    del params_model_registry[cls.__name__]
