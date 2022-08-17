from pydantic import BaseModel
from pydantic.main import ModelMetaclass

from .allowed_type import AllowedTypes


class ReteMetaModel(ModelMetaclass):
    __allowed_types = AllowedTypes

    def __new__(cls, name: str, bases: tuple, namespace: dict, **kwargs: dict) -> type:
        if annotations := namespace.get('__annotations__', {}):
            annotations: dict
            names = [
                x if isinstance(x, str) else getattr(x, '__name__', None)
                                             or getattr(x, '_name', None) or getattr(x, '__str__')()
                for x in annotations.values()
            ]
            correct_types = [name in cls.__allowed_types for name in names]
            if not all(correct_types):
                raise ValueError('Incorrect type in data, allowed types: {}'.format(cls.__allowed_types))
        obj = super().__new__(cls, name, bases, namespace, **kwargs)
        return obj


class ReteParamsModel(BaseModel, metaclass=ReteMetaModel):
    pass
