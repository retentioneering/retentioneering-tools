from __future__ import annotations

import functools
from typing import Any, Callable

from retentioneering.utils.context_managers import simple_lock_context_manager
from retentioneering.utils.singleton import Singleton

from .connector import ConnectorProtocol
from .hwid import get_hwid  # type: ignore
from .tracking_info import TrackingInfo


class Tracker(Singleton):
    connector: ConnectorProtocol
    enabled: bool = True
    lock: bool = False
    _user_id: str | None = None

    def __init__(self, connector: ConnectorProtocol, enabled: bool = True) -> None:
        self.connector = connector
        self.enabled = enabled

    @property
    def user_id(self) -> str:
        if self._user_id is None:
            self._user_id = self.__obtain_user_id()
        return self._user_id

    def __obtain_user_id(self) -> str:
        return get_hwid()

    def __clean_params(self, params: dict[str, Any], allowed_params: list[str] | None = None) -> dict[str, Any]:
        if allowed_params is None:
            allowed_params = []
        return {key: value for key, value in params.items() if key in allowed_params}
        # return [key for key in params.keys() if key in allowed_params]

    def track(self, tracking_info: dict[str, Any], allowed_params: list[str] | None = None) -> Callable:
        event_name = tracking_info["event_name"]
        event_custom_name = tracking_info.get("event_custom_name", tracking_info["event_name"])

        def tracker_decorator(func: Callable) -> Callable:
            with simple_lock_context_manager as ctx:

                @functools.wraps(func)
                def wrapper(*args: list[Any], **kwargs: dict[Any, Any]) -> Callable:
                    simple_lock_context_manager.function_name = func.__qualname__
                    called_function_params = self.__clean_params(kwargs, allowed_params)
                    _tracking_info_start = TrackingInfo(
                        client_session_id=self.user_id,
                        event_name=f"{event_name}_start",
                        event_custom_name=event_custom_name,
                        params=called_function_params,
                    )
                    res = func(*args, **kwargs)
                    _tracking_info_end = TrackingInfo(
                        client_session_id=self.user_id,
                        event_name=f"{event_name}_end",
                        event_custom_name=event_custom_name,
                        params=called_function_params,
                    )
                    if ctx.allow_action(function_name=func.__qualname__):
                        self.connector.send_message(data=_tracking_info_start)
                        self.connector.send_message(data=_tracking_info_end)

                    return res

                return wrapper

        return tracker_decorator
