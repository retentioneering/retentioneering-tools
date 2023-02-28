from __future__ import annotations

import functools
import uuid
from datetime import datetime
from typing import Any, Callable

from retentioneering.utils.singleton import Singleton

from .connector import ConnectorProtocol, TrackerMainConnector


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
        tracking_info["user_id"] = self.user_id
        tracking_info["timestamp"] = datetime.now().timestamp()
        tracking_info["tz_info"] = datetime.now().tzinfo()

        def tracker_decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                tracking_info["function_name"] = func.__name__
                self.connector.send_message(data=tracking_info)
                return func(*args, **kwargs)

            return wrapper

        return tracker_decorator


tracker = Tracker(connector=TrackerMainConnector())

track = tracker.track
