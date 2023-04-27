from __future__ import annotations

from dataclasses import asdict
from typing import Any

import pytest

from retentioneering.backend.tracker import Tracker
from retentioneering.backend.tracker.connector import ConnectorProtocol


class SimpleTrackerConnector(ConnectorProtocol):
    tracker_log = []

    def __init__(self):
        super().__init__()
        self.tracker_log = []

    def send_message(self, data) -> None:
        return self.tracker_log.append(asdict(data))

    def clear(self):
        self.tracker_log.clear()


@pytest.fixture
def tracker_with_constant_uuid():
    class TrackerWithConstantUUID(Tracker):
        def __init__(self, connector: SimpleTrackerConnector, enabled: bool):
            super().__init__(connector=connector, enabled=enabled)

        @property
        def user_id(self) -> str:
            return "12345678-1234-1234-1234-1234567890ab"

    tracker = TrackerWithConstantUUID(connector=SimpleTrackerConnector(), enabled=True)

    yield tracker


@pytest.fixture
def tracker_with_dict_params():
    class TrackerWithConstantUUIDAndDictParams(Tracker):
        def __init__(self, connector: SimpleTrackerConnector, enabled: bool):
            super().__init__(connector=connector, enabled=enabled)

        @property
        def user_id(self) -> str:
            return "12345678-1234-1234-1234-1234567890ab"

        def clear_params(self, params: dict[str, Any], allowed_params: list[str] | None = None) -> dict[str, Any]:
            if allowed_params is None:
                allowed_params = []
            return {key: value for key, value in params.items() if key in allowed_params}

    tracker = TrackerWithConstantUUIDAndDictParams(connector=SimpleTrackerConnector(), enabled=True)
    yield tracker
