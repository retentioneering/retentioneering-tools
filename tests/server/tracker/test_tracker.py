from __future__ import annotations

from dataclasses import asdict
from typing import Any, Callable

from retentioneering.backend.tracker.connector import ConnectorProtocol
from retentioneering.backend.tracker.tracker import Tracker

tracker_log = []


class SimpleTrackerConnector(ConnectorProtocol):
    def send_message(self, data) -> None:
        return tracker_log.append(asdict(data))


class TrackerWithConstantUUID(Tracker):
    @property
    def user_id(self) -> str:
        return "12345678-1234-1234-1234-1234567890ab"

    @property
    def constant_timestamp(self) -> float:
        return 1679545355.615132

    def track(self, tracking_info: dict[str, Any]) -> Callable:
        tracking_info["event_name"] = "event_name"
        return super().track(tracking_info=tracking_info)


class TestTracker:
    def test_send_message(self):
        tracker = TrackerWithConstantUUID(SimpleTrackerConnector())

        @tracker.track(tracking_info={"event_custom_name": "event_custom_name"})
        def test():
            return "test"

        test()
        assert "12345678-1234-1234-1234-1234567890ab" == tracker_log[0]["client_session_id"]
        assert "event_name_start" == tracker_log[0]["event_name"]
        assert "event_custom_name" == tracker_log[0]["event_custom_name"]
        assert tracker_log[0]["event_date_local"] is not None
        assert tracker_log[0]["event_day_week"] is not None
        assert tracker_log[0]["user_id"] == "12345678-1234-1234-1234-1234567890ab|none|none|none"
