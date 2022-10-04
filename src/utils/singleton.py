from __future__ import annotations

from backend import ServerManager


class Singleton:
    _instances: dict = {}  # type: ignore

    def __call__(self):  # type: ignore
        if self not in self._instances:
            self._instances[self] = super().__init__()
        return self._instances[self]
