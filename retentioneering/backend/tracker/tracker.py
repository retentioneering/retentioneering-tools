from __future__ import annotations

import functools
import inspect
import os
import uuid
from datetime import datetime
from typing import Any, Callable, Collection, Final

import pandas as pd

from retentioneering import RETE_CONFIG
from retentioneering.utils.hash_object import hash_value
from retentioneering.utils.singleton import Singleton

from ...utils.flatten_list import flatten
from .connector import ConnectorProtocol
from .tracking_info import TrackingInfo


class Tracker(Singleton):
    connector: ConnectorProtocol
    _enabled: bool = True
    session_id: str
    _user_id: str | None = None
    DO_NOT_TRACK: Final = "DO_NOT_TRACK"

    def __init__(self, connector: ConnectorProtocol, enabled: bool = True) -> None:
        self.connector = connector
        self._enabled = enabled
        # generate session id uuid
        self.session_id = str(uuid.uuid4())
        self.is_colab = 1 if self.check_env() == "colab" else 0

        self._track_action(
            called_function_params={},
            scope="tracker",
            event_name="init_tracker",
            event_custom_name="init_tracker",
            suffix="",
        )

    @staticmethod
    def check_env() -> str:
        try:
            import google.colab  # type: ignore # noqa: F401

            return "colab"
        except ImportError:
            return "classic"

    @property
    def enabled(self) -> bool:
        environment_tracker_status = os.environ.get("RETE_TRACKER_ENABLED", "true").lower()
        if environment_tracker_status not in ["true", "false"]:
            return True
        self._enabled = True if environment_tracker_status == "true" else False
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool | Any) -> None:
        self._enabled = value if isinstance(value, bool) else True

    @property
    def user_id(self) -> str:
        if self._user_id is None:
            self._user_id = self.__obtain_user_id()
        return self._user_id

    def __obtain_user_id(self) -> str:
        return RETE_CONFIG.user.pk

    def clear_params(self, params: dict[str, Any], not_hash_values: list[str] | None = None) -> dict[str, Any]:
        named_only_params = {key: self.DO_NOT_TRACK for key in params.keys()}
        value_allowed_params = self.__obtain_values(params=params, not_hash_values=not_hash_values)
        created_params = {**named_only_params, **value_allowed_params}
        return created_params

    @staticmethod
    def __prepare_value(value: Any, not_hash: bool) -> Any:
        if isinstance(value, (int, float, type(None))):
            return value
        elif isinstance(value, str):
            if not_hash:
                return value
            return hash_value(value)
        elif isinstance(value, pd.DataFrame):
            return value.shape
        elif isinstance(value, Collection):
            if not_hash:
                return value
            return {"len": len(value), "len_flatten": len(flatten(value))}
        elif callable(value):
            try:
                function_source = inspect.getsource(value)
            except Exception:
                function_source = ""
            return hash_value(function_source)
        return f"WRONG_TYPE: {type(value)}"

    def __obtain_values(
        self,
        params: dict[str, Any],
        not_hash_values: list[str] | None = None,
        hash_keys: bool = False,
    ) -> dict[str, Any]:
        prepared_params: dict[str, Any] = {}
        if not_hash_values is None:
            not_hash_values = []
        for key, value in params.items():
            tracking_key = hash_value(key) if hash_keys is True else key

            if isinstance(value, dict):
                prepared_params[tracking_key] = self.__obtain_values(
                    params=value,
                    hash_keys=False if key in not_hash_values else True,
                )
            else:
                prepared_params[tracking_key] = self.__prepare_value(value, key in not_hash_values)

        return prepared_params

    def _track_action(
        self,
        called_function_params: dict[str, Any],
        scope: str,
        event_name: str,
        event_custom_name: str,
        suffix: str,
        event_value: str = "",
        event_time: datetime | None = None,
        eventstream_index: int | None = None,
        parent_eventstream_index: int | None = None,
        child_eventstream_index: int | None = None,
    ) -> None:
        if self.enabled:
            try:
                if event_time is None:
                    event_time = datetime.now()
                tracking_info = TrackingInfo(
                    user_id=self.user_id,
                    event_name=event_name,
                    event_custom_name="_".join([str(_) for _ in [event_custom_name, suffix] if _]),
                    event_value=event_value,
                    params=called_function_params,
                    event_time=event_time,
                    scope=scope,
                    eventstream_index=eventstream_index,
                    jupyter_kernel_id=self.session_id,
                    parent_eventstream_index=parent_eventstream_index,
                    child_eventstream_index=child_eventstream_index,
                    colab=self.is_colab,
                )
                self.connector.send_message(data=tracking_info)
            except Exception as e:
                # Maximum suppression. Vladimir Makhanov
                pass
        else:
            pass

    def __obtaion_evenstream_info(self, kwargs: dict[str, Any]) -> tuple[int | None, str | None]:
        if eventstream := kwargs.get("eventstream", None):
            return eventstream._eventstream_index, eventstream._hash
        return None, None

    def track(
        self,
        tracking_info: dict[str, Any],
        scope: str,
        event_value: str = "",
    ) -> Callable:
        event_name = tracking_info["event_name"]
        event_custom_name = tracking_info.get("event_custom_name", event_name)

        def tracker_decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args: list[Any], **kwargs: dict[Any, Any]) -> Callable:
                called_function_params = self.clear_params(params=kwargs)
                eventstream_index, eventstream_hash = self.__obtaion_evenstream_info(kwargs)
                called_function_params["eventstream_index"] = eventstream_index
                called_function_params["eventstream_hash"] = eventstream_hash
                start_time = datetime.now()
                self._track_action(
                    called_function_params=called_function_params,
                    scope=scope,
                    event_name=event_name,
                    event_custom_name=event_custom_name,
                    event_value=event_value,
                    event_time=start_time,
                    suffix="start",
                    eventstream_index=eventstream_index,
                )

                try:
                    res = func(*args, **kwargs)
                except Exception as e:
                    raise e

                end_time = datetime.now()

                self._track_action(
                    called_function_params=called_function_params,
                    scope=scope,
                    event_name=event_name,
                    event_custom_name=event_custom_name,
                    event_value=event_value,
                    event_time=end_time,
                    suffix="end",
                    eventstream_index=eventstream_index,
                )
                return res

            return wrapper

        return tracker_decorator

    def time_performance(
        self,
        scope: str,
        event_name: str,
        event_value: str = "",
    ) -> Callable:
        def time_performance_decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args: list[Any], **kwargs: dict[Any, Any]) -> Callable:
                eventstream_index, eventstream_hash = self.__obtaion_evenstream_info(kwargs)

                start_time = datetime.now()

                self._track_action(
                    called_function_params={},
                    scope=scope,
                    event_name=event_name,
                    event_custom_name=event_name,
                    event_value=event_value,
                    event_time=start_time,
                    suffix="start",
                    eventstream_index=eventstream_index,
                )
                res = func(*args, **kwargs)
                end_time = datetime.now()
                self._track_action(
                    called_function_params={},
                    scope=scope,
                    event_name=event_name,
                    event_custom_name=event_name,
                    event_value=event_value,
                    event_time=end_time,
                    suffix="end",
                    eventstream_index=eventstream_index,
                )
                return res

            return wrapper

        return time_performance_decorator

    def collect_data_performance(
        self,
        scope: str,
        event_name: str = "metadata",
        called_params: dict[str, Any] | None = None,
        performance_data: dict[str, Any] | None = None,
        event_value: str = "",
        not_hash_values: list[str] | None = None,
        eventstream_index: int | None = None,
        parent_eventstream_index: int | None = None,
        child_eventstream_index: int | None = None,
    ) -> None:
        if called_params is None:
            called_params = {}

        if performance_data is None:
            performance_data = {}

        called_function_params = self.clear_params(params=called_params, not_hash_values=not_hash_values)
        self._track_action(
            called_function_params={"args": called_function_params, "performance_info": performance_data},
            scope=scope,
            event_name=event_name,
            event_custom_name=event_name,
            event_value=event_value,
            suffix="",
            eventstream_index=eventstream_index,
            parent_eventstream_index=parent_eventstream_index,
            child_eventstream_index=child_eventstream_index,
        )
