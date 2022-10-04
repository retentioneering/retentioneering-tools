from __future__ import annotations


class ParamsModelRegistry:
    REGISTRY: dict[str, type] = {}

    def __setitem__(self, key, value):
        self.REGISTRY[key] = value

    @classmethod
    def get_registry(cls):
        return dict(cls.REGISTRY)


params_model_registry = ParamsModelRegistry()


def register_params_model(cls):
    print(f'REGISTER {cls.__name__}')
    params_model_registry[cls.__name__] = cls
