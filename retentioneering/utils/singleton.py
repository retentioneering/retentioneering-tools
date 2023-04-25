from __future__ import annotations


class Singleton:
    _instances = {}  # type: ignore

    def __new__(cls, *args, **kwargs):  # type: ignore
        if cls._instances.get(cls, None) is None:
            cls._instances[cls] = super().__new__(cls)

        return Singleton._instances[cls]
