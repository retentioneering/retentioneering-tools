from __future__ import annotations

import typing

from src.utils.registry import RegistryValidationError, ReteRegistry

if typing.TYPE_CHECKING:
    from .params_model import ParamsModel


class ParamsModelRegistry(ReteRegistry):
    REGISTRY: dict = {}  # type: ignore

    objects = "ParamsModel"

    def __setitem__(self, key, value):
        if key not in self.REGISTRY:
            self.REGISTRY[key] = value

    @classmethod
    def get_registry(cls) -> dict:
        return cls.REGISTRY


params_model_registry = ParamsModelRegistry()


def register_params_model(cls: ParamsModel):
    params_model_registry[cls.__class__.__name__] = cls
