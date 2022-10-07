from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    from src.data_processor.data_processor import DataProcessor


class ParamsModelRegistry:
    REGISTRY: list[dict[str, str]] = []

    def __setitem__(self, key, value):
        self.REGISTRY.append({key: value})

    @classmethod
    def get_registry(cls):
        return cls.REGISTRY


params_model_registry = ParamsModelRegistry()


def register_params_model(cls: DataProcessor):
    params_model_registry[cls.__class__.__name__] = cls.get_view()
