from __future__ import annotations

from typing import Any, Type


class RegistryValidationError(Exception):
    pass


class ReteRegistry:
    REGISTRY: list[dict[str, Any]] = []
    objects: str = "object"

    def __setitem__(self, key: str, value: Any) -> None:
        # @TODO: fix bug. Vladimir Makhanov
        if key not in self.REGISTRY:  # type: ignore
            self.REGISTRY.append({key: value})
        else:
            raise RegistryValidationError("%s <%s> already exists" % (self.objects, key))

    @classmethod
    def remove(cls: Type[ReteRegistry], value: Any) -> None:
        cls.REGISTRY.remove(value)

    @classmethod
    def get_registry(cls: Type[ReteRegistry]) -> list[dict[str, Any]]:
        return cls.REGISTRY

    @classmethod
    def get_export_registry(cls: Type[ReteRegistry]) -> list:
        data = []
        for processor in cls.REGISTRY:
            key = list(processor.keys())[0]
            data.append(processor[key])
        return data
