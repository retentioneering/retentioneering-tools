from __future__ import annotations

from dataclasses import asdict
from typing import Any

import pytest
from typing_extensions import override

from retentioneering.backend.tracker.connector import ConnectorProtocol
from retentioneering.backend.tracker.tracker import Tracker

tracker_log = []


@pytest.fixture
def clear_tracker_log():
    tracker_log.clear()


class SimpleTrackerConnector(ConnectorProtocol):
    def send_message(self, data) -> None:
        return tracker_log.append(asdict(data))


class TrackerWithConstantUUID(Tracker):
    def __init__(self, connector: ConnectorProtocol):
        super().__init__(connector=connector)

    @property
    def user_id(self) -> str:
        return "12345678-1234-1234-1234-1234567890ab"


class TrackerWithConstantUUIDAndDictParams(TrackerWithConstantUUID):
    def clear_params(self, params: dict[str, Any], allowed_params: list[str] | None = None) -> dict[str, Any]:
        if allowed_params is None:
            allowed_params = []
        return {key: value for key, value in params.items() if key in allowed_params}


class TestTracker:
    def test_send_message(self, clear_tracker_log):
        tracker = TrackerWithConstantUUIDAndDictParams(SimpleTrackerConnector())

        @tracker.track(tracking_info={"event_name": "test_event_name"}, allowed_params=["edges_norm_type"])
        def test(edges_norm_type: str, sensetive_data: str):
            return "test"

        return_value = test(edges_norm_type="test_norm_type", sensetive_data="s0mEp@$s")

        assert "test" == return_value
        assert 2 == len(tracker_log)

        assert "12345678-1234-1234-1234-1234567890ab" == tracker_log[0]["client_session_id"]
        assert "test_event_name_end" == tracker_log[1]["event_name"]
        assert "test_event_name_start" == tracker_log[0]["event_name"]
        assert "test_event_name" == tracker_log[0]["event_custom_name"]
        assert tracker_log[0]["event_date_local"] is not None
        assert tracker_log[0]["event_day_week"] is not None
        assert "12345678-1234-1234-1234-1234567890ab|none|none|none" == tracker_log[0]["user_id"]
        assert {"edges_norm_type": "test_norm_type"} == tracker_log[0]["params"]
        assert {"edges_norm_type": "test_norm_type"} == tracker_log[1]["params"]

    def test_send_message_params_list(self, clear_tracker_log):
        tracker = TrackerWithConstantUUID(SimpleTrackerConnector())

        @tracker.track(tracking_info={"event_name": "test_event_name"}, allowed_params=["edges_norm_type"])
        def test(edges_norm_type: str, sensetive_data: str):
            return "test"

        return_value = test(edges_norm_type="test_norm_type", sensetive_data="s0mEp@$s")

        assert "test" == return_value
        assert 2 == len(tracker_log)

        assert "12345678-1234-1234-1234-1234567890ab" == tracker_log[0]["client_session_id"]
        assert "test_event_name_end" == tracker_log[1]["event_name"]
        assert "test_event_name_start" == tracker_log[0]["event_name"]
        assert "test_event_name" == tracker_log[0]["event_custom_name"]
        assert tracker_log[0]["event_date_local"] is not None
        assert tracker_log[0]["event_day_week"] is not None
        assert "12345678-1234-1234-1234-1234567890ab|none|none|none" == tracker_log[0]["user_id"]
        assert ["edges_norm_type"] == tracker_log[0]["params"]
        assert ["edges_norm_type"] == tracker_log[1]["params"]

    def test_single_message(self, clear_tracker_log):
        tracker = TrackerWithConstantUUIDAndDictParams(SimpleTrackerConnector())

        @tracker.track(tracking_info={"event_name": "inner"})
        def inner(edges_norm_type: str, sensetive_data: str):
            return "test"

        @tracker.track(tracking_info={"event_name": "outer"})
        def outer(edges_norm_type: str, sensetive_data: str):
            return inner(edges_norm_type=edges_norm_type, sensetive_data=sensetive_data)

        return_value = outer(edges_norm_type="test_norm_type", sensetive_data="s0mEp@$s")

        assert "test" == return_value
        assert 2 == len(tracker_log)
        assert "outer_start" == tracker_log[0]["event_name"]
        assert "outer_end" == tracker_log[1]["event_name"]
