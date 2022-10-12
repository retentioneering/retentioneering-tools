from __future__ import annotations


class RegistryValidationError(Exception):
    pass


class ReteRegistry:
    REGISTRY: list[dict[str, str]] = []
    objects: str = "object"

    def __setitem__(self, key, value):
        if key not in self.REGISTRY:
            self.REGISTRY.append({key: value})
        else:
            raise RegistryValidationError("%s <%s> already exists" % (self.objects, key))

    @classmethod
    def get_registry(cls):
        return cls.REGISTRY
