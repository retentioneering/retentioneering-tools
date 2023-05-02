from __future__ import annotations

from types import TracebackType
from typing import Optional, Type

from .singleton import Singleton


class SimpleLockContextManager(Singleton):
    is_locked: bool
    _event_name: str = ""
    _last_checked_event_name: str = ""

    @property
    def event_name(self) -> str:
        return self._event_name

    @event_name.setter
    def event_name(self, event_name: str) -> None:
        if self._event_name == "" and (
            self._last_checked_event_name == "" or self._last_checked_event_name == event_name
        ):
            self._event_name = event_name

    def __init__(self) -> None:
        self.is_locked = False

    def __enter__(self) -> SimpleLockContextManager:
        self.is_locked = True
        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> None:
        if self._last_checked_event_name == self._event_name:
            self.is_locked = False
            self._event_name = ""
            self._last_checked_event_name = ""
        if isinstance(exc_type, ValueError) or isinstance(exc_val, ValueError):
            raise exc_val  # type: ignore

    def allow_action(self, event_name: str) -> bool:
        self._last_checked_event_name = event_name
        if self.is_locked is True:
            return self.event_name == event_name
        else:
            return True


simple_lock_context_manager = SimpleLockContextManager()
