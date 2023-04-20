from __future__ import annotations

from types import TracebackType
from typing import Optional, Type

from .singleton import Singleton


class SimpleLockContextManager(Singleton):
    _locked: bool
    _function_name: str = ""

    @property
    def function_name(self) -> str:
        return self._function_name

    @function_name.setter
    def function_name(self, function_name: str) -> None:
        if self._function_name == "":
            self._function_name = function_name

    def __init__(self) -> None:
        self._locked = False

    def __enter__(self) -> SimpleLockContextManager:
        self._locked = True
        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> None:
        self._locked = False
        self._function_name = ""

    def allow_action(self, function_name: str) -> bool:
        if self._locked is False:
            return self.function_name == function_name
        else:
            return True


simple_lock_context_manager = SimpleLockContextManager()
