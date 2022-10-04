from utils.singleton import Singleton


class ParamsModelRegistry(metaclass=Singleton):
    REGISTRY = {}

    def __setitem__(self, key, value):
        self.REGISTRY[key] = value

    @classmethod
    def get_registry(cls):
        return dict(cls.REGISTRY)


params_model_registry = ParamsModelRegistry()


def register_params_model(cls):
    params_model_registry[cls.__name__] = cls
