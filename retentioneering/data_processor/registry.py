from __future__ import annotations

import typing
from typing import Any, Type

from retentioneering.utils.registry import RegistryValidationError, ReteRegistry

if typing.TYPE_CHECKING:
    from .data_processor import DataProcessor


class DataprocessorViewRegistry(ReteRegistry):
    objects = "Dataprocessor"
    pass


class DataprocessorRegistry:
    REGISTRY: dict[str, "Type[DataProcessor]"] = {}  # type: ignore
    objects = "Dataprocessor"

    def __setitem__(self, key: str, value: "Type[DataProcessor]") -> None:
        if key not in self.REGISTRY:
            self.REGISTRY[key] = value
        else:
            raise RegistryValidationError("%s <%s> already exists" % (self.objects, key))

    def __delitem__(self, key: str) -> None:
        del self.REGISTRY[key]

    def get_registry(self) -> dict[str, "Type[DataProcessor]"]:
        return self.REGISTRY


dataprocessor_view_registry = DataprocessorViewRegistry()
dataprocessor_registry = DataprocessorRegistry()


def register_dataprocessor(cls: type[DataProcessor]) -> None:
    dataprocessor_view_registry[cls.__name__] = cls.get_view()
    dataprocessor_registry[cls.__name__] = cls


def unregister_dataprocessor(cls: type[DataProcessor]) -> None:
    REGISTRY = dataprocessor_registry.get_registry()
    VIEW_REGISTRY = dataprocessor_view_registry.get_registry()

    view = cls.get_view()
    view_name = view["name"]

    found_key: str | None = None
    found_view: dict[str, Any] | None = None

    for key in REGISTRY:
        d_item = REGISTRY[key]
        if d_item == cls:
            found_key = key

    for item in VIEW_REGISTRY:
        i_view = list(item.values())[0]
        if i_view["name"] == view_name:
            found_view = item

    if found_key:
        del dataprocessor_registry[found_key]

    if found_view:
        dataprocessor_view_registry.remove(found_view)
