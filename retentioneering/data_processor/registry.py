from __future__ import annotations

import typing

from retentioneering.utils.registry import RegistryValidationError, ReteRegistry

if typing.TYPE_CHECKING:
    from .data_processor import DataProcessor


class DataprocessorViewRegistry(ReteRegistry):
    objects = "Dataprocessor"
    pass


class DataprocessorRegistry(ReteRegistry):

    REGISTRY: dict[str, "DataProcessor"] = {}  # type: ignore
    objects = "Dataprocessor"

    def __setitem__(self, key: str, value: "DataProcessor") -> None:
        if key not in self.REGISTRY:
            self.REGISTRY[key] = value
        else:
            raise RegistryValidationError("%s <%s> already exists" % (self.objects, key))


dataprocessor_view_registry = DataprocessorViewRegistry()
dataprocessor_registry = DataprocessorRegistry()


def register_dataprocessor(cls: DataProcessor) -> None:
    dataprocessor_view_registry[cls.__class__.__name__] = cls.get_view()
    dataprocessor_registry[cls.__class__.__name__] = cls
