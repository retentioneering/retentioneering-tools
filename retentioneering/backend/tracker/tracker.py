from __future__ import annotations

import functools
import uuid
from typing import Any, Callable

from retentioneering.utils.singleton import Singleton

from .connector import ConnectorProtocol
from .tracking_info import TrackingInfo


class Tracker(Singleton):
    connector: ConnectorProtocol
    enabled: bool = True
    _user_id: str | None = None

    def __init__(self, connector: ConnectorProtocol, enabled: bool = True) -> None:
        self.connector = connector
        self.enabled = enabled

    # 15bc4ce3-c343-4833-a0a2-eb13567deec4
    @property
    def user_id(self) -> str:
        if self._user_id is None:
            self._user_id = self.__obtain_user_id()
        return self._user_id

    def __obtain_user_id(self) -> str:
        return str(uuid.uuid4())

    def track(self, tracking_info: dict[str, Any]) -> Callable:
        event_name = tracking_info["event_name"]
        event_custom_name = tracking_info.get("event_custom_name", tracking_info["event_name"])
        _tracking_info = TrackingInfo(
            client_session_id=self.user_id,
            event_name=f"{event_name}_start",
            event_custom_name=event_custom_name,
        )
        self.connector.send_message(data=_tracking_info)

        def tracker_decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args: list[Any], **kwargs: dict[Any, Any]) -> Any:
                return func(*args, **kwargs)

            return wrapper

        _tracking_info = TrackingInfo(
            client_session_id=self.user_id,
            event_name=f"{event_name}_end",
            event_custom_name=event_custom_name,
        )
        self.connector.send_message(data=_tracking_info)

        return tracker_decorator
