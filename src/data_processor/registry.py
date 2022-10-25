from __future__ import annotations

import typing

from src.utils.registry import RegistryValidationError, ReteRegistry

if typing.TYPE_CHECKING:
    from .data_processor import DataProcessor


class DataprocessorViewRegistry(ReteRegistry):
    objects = "Dataprocessor"
    pass


class DataprocessorRegistry(ReteRegistry):

    REGISTRY: dict[str, typing.Type["DataProcessor"]] = {}  # type: ignore

    def __setitem__(self, key, value):
        if key not in self.REGISTRY:
            self.REGISTRY[key] = value
        else:
            raise RegistryValidationError("%s <%s> already exists" % (self.objects, key))

    objects = "Dataprocessor"
    pass


dataprocessor_view_registry = DataprocessorViewRegistry()
dataprocessor_registry = DataprocessorRegistry()


def register_dataprocessor(cls: DataProcessor):
    dataprocessor_view_registry[cls.__class__.__name__] = cls.get_view()
    dataprocessor_registry[cls.__class__.__name__] = cls
