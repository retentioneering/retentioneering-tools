from __future__ import annotations

from dataclasses import asdict

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

        @property
        def enabled(self) -> bool:
            return True

    tracker = TrackerWithConstantUUID(connector=SimpleTrackerConnector(), enabled=True)

    yield tracker
    tracker.connector.clear()
