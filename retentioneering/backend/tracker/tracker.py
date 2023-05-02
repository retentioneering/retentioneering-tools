from __future__ import annotations

import functools
from datetime import datetime
from typing import Any, Callable

from retentioneering import RETE_CONFIG
from retentioneering.utils.context_managers import simple_lock_context_manager
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

    @property
    def user_id(self) -> str:
        if self._user_id is None:
            self._user_id = self.__obtain_user_id()
        return self._user_id

    def __obtain_user_id(self) -> str:
        return RETE_CONFIG.tracking.tracking_id

    def clear_params(self, params: dict[str, Any], allowed_params: list[str] | None = None) -> list[str]:
        if allowed_params is None:
            allowed_params = []
        return [key for key in params.keys() if key in allowed_params]

    def _track_action(
        self,
        called_function_params: list[str],
        scope: str,
        event_name: str,
        event_custom_name: str,
        event_value: str,
        suffix: str,
        event_time: datetime,
    ) -> None:
        if self.enabled:
            try:
                tracking_info = TrackingInfo(
                    client_session_id=self.user_id,
                    event_name=event_name,
                    event_custom_name=f"{event_custom_name}_{suffix}",
                    event_value=event_value,
                    params=called_function_params,
                    event_time=event_time,
                    scope=scope,
                )
                self.connector.send_message(data=tracking_info)
            except Exception:
                # Maximum suppression. Vladimir Makhanov
                pass
        else:
            pass

    def track(
        self,
        tracking_info: dict[str, Any],
        scope: str,
        event_value: str = "",
        allowed_params: list[str] | None = None,
    ) -> Callable:
        event_name = tracking_info["event_name"]
        event_custom_name = tracking_info.get("event_custom_name", event_name)

        def tracker_decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args: list[Any], **kwargs: dict[Any, Any]) -> Callable:
                with simple_lock_context_manager as ctx:
                    ctx.event_name = event_name

                    called_function_params = self.clear_params(kwargs, allowed_params)
                    start_time = datetime.now()
                    try:
                        res = func(*args, **kwargs)
                    except Exception as e:
                        raise e

                    end_time = datetime.now()

                    if ctx.allow_action(event_name=event_name):
                        self._track_action(
                            called_function_params=called_function_params,
                            scope=scope,
                            event_name=event_name,
                            event_custom_name=event_custom_name,
                            event_value=event_value,
                            event_time=start_time,
                            suffix="start",
                        )
                        self._track_action(
                            called_function_params=called_function_params,
                            scope=scope,
                            event_name=event_name,
                            event_custom_name=event_custom_name,
                            suffix="end",
                            event_value=event_value,
                            event_time=end_time,
                        )
                    return res

            return wrapper

        return tracker_decorator
