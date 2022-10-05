from __future__ import annotations

import typing
from typing import Type
if typing.TYPE_CHECKING:
    from src.data_processor.data_processor import DataProcessor


class ParamsModelRegistry:
    REGISTRY: dict[str, type] = {}

    def __setitem__(self, key, value):
        self.REGISTRY[key] = value

    @classmethod
    def get_registry(cls):
        return dict(cls.REGISTRY)


params_model_registry = ParamsModelRegistry()


def register_params_model(cls: Type['DataProcessor']):
    print(cls)
    print(dir(cls))
    params_model_registry[cls.__name__] = cls.get_view()
