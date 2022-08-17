from __future__ import annotations

from pydantic import BaseModel
from pydantic.main import ModelMetaclass

from .allowed_type import AllowedTypes


class ReteMetaModel(ModelMetaclass):
    __allowed_types = AllowedTypes

    def __new__(cls, name: str, bases: tuple, namespace: dict, **kwargs: dict) -> type:  # pyright: ignore
        if annotations := namespace.get("__annotations__", {}):
            # annotations: dict[str, str | Callable | Type]
            # print(type(annotations['a']))
            names: list[str] = [
                x
                if isinstance(x, str)
                else getattr(x, "__name__", None) or getattr(x, "_name", None)  # pyright: ignore
                for x in annotations.values()  # pyright: ignore
            ]
            correct_types: list[bool] = [name in cls.__allowed_types for name in names]
            if not all(correct_types):
                raise ValueError("Incorrect type in data, allowed types: {}".format(cls.__allowed_types))
        obj = super().__new__(cls, name, bases, namespace, **kwargs)
        return obj


class ReteParamsModel(BaseModel, metaclass=ReteMetaModel):
    pass
