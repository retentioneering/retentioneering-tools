from __future__ import annotations

from dataclasses import asdict

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


class TestTracker:
    def test_send_message(self):
        tracker = TrackerWithConstantUUID(SimpleTrackerConnector())

        @tracker.track(tracking_info={"event_name": "test_event_name"})
        def test(edges_norm_type: str):
            return "test"

        return_value = test(edges_norm_type="test_norm_type")

        assert return_value == "test"
        assert len(tracker_log) == 2

        assert "12345678-1234-1234-1234-1234567890ab" == tracker_log[0]["client_session_id"]
        assert "test_event_name_end" == tracker_log[1]["event_name"]
        assert "test_event_name_start" == tracker_log[0]["event_name"]
        assert "test_event_name" == tracker_log[0]["event_custom_name"]
        assert tracker_log[0]["event_date_local"] is not None
        assert tracker_log[0]["event_day_week"] is not None
        assert "12345678-1234-1234-1234-1234567890ab|none|none|none" == tracker_log[0]["user_id"]
        assert {"edges_norm_type": "test_norm_type"} == tracker_log[0]["params"]
        assert {"edges_norm_type": "test_norm_type"} == tracker_log[1]["params"]
