from __future__ import annotations

from .fixtures import tracker_with_constant_uuid, tracker_with_dict_params


class TestTracker:
    def test_send_message(self, tracker_with_dict_params):
        @tracker_with_dict_params.track(
            tracking_info={"event_name": "test_event_name_send", "event_custom_name": "test_event_name_custom"},
            allowed_params=["edges_norm_type"],
            scope="test",
            event_value="test_value",
        )
        def test(edges_norm_type: str, sensitive_data: str):
            return "test"

        return_value = test(edges_norm_type="test_norm_type", sensitive_data="s0mEp@$s")

        tracker_log = tracker_with_dict_params.connector.tracker_log
        assert "test" == return_value
        assert 2 == len(tracker_log)

        assert "12345678-1234-1234-1234-1234567890ab" == tracker_log[0]["client_session_id"]
        assert "test_event_name_send_start" == tracker_log[0]["event_name"]
        assert "test_event_name_send_end" == tracker_log[1]["event_name"]
        assert "test_event_name_custom" == tracker_log[0]["event_custom_name"]
        assert "test_event_name_custom" == tracker_log[1]["event_custom_name"]
        assert tracker_log[0]["event_date_local"] is not None
        assert tracker_log[0]["event_day_week"] is not None
        assert "12345678-1234-1234-1234-1234567890ab|none|none|none" == tracker_log[0]["user_id"]
        assert {"edges_norm_type": "test_norm_type"} == tracker_log[0]["params"]
        assert {"edges_norm_type": "test_norm_type"} == tracker_log[1]["params"]
        assert "test" == tracker_log[0]["scope"]
        assert "test" == tracker_log[1]["scope"]
        assert "test_value" == tracker_log[0]["event_value"]
        assert "test_value" == tracker_log[1]["event_value"]

    def test_send_message_params_list(self, tracker_with_constant_uuid):
        @tracker_with_constant_uuid.track(
            tracking_info={"event_name": "test_event_name_params_list", "event_custom_name": "test_event_name_custom"},
            allowed_params=["edges_norm_type"],
            scope="test",
        )
        def test(edges_norm_type: str, sensitive_data: str):
            return "test"

        return_value = test(edges_norm_type="test_norm_type", sensitive_data="s0mEp@$s")
        tracker_log = tracker_with_constant_uuid.connector.tracker_log

        assert "test" == return_value
        assert 2 == len(tracker_log)

        assert "12345678-1234-1234-1234-1234567890ab" == tracker_log[0]["client_session_id"]
        assert "test_event_name_params_list_start" == tracker_log[0]["event_name"]
        assert "test_event_name_params_list_end" == tracker_log[1]["event_name"]
        assert "test_event_name_custom" == tracker_log[0]["event_custom_name"]
        assert "test_event_name_custom" == tracker_log[1]["event_custom_name"]
        assert tracker_log[0]["event_date_local"] is not None
        assert tracker_log[0]["event_day_week"] is not None
        assert "12345678-1234-1234-1234-1234567890ab|none|none|none" == tracker_log[0]["user_id"]
        assert ["edges_norm_type"] == tracker_log[0]["params"]
        assert ["edges_norm_type"] == tracker_log[1]["params"]

    def test_single_message(self, tracker_with_constant_uuid):
        @tracker_with_constant_uuid.track(
            tracking_info={"event_name": "inner1", "event_custom_name": ""},
            allowed_params=["edges_norm_type"],
            scope="test",
            event_value="",
        )
        def inner1(edges_norm_type: str, sensitive_data: str):
            return "test1"

        @tracker_with_constant_uuid.track(
            tracking_info={"event_name": "inner2", "event_custom_name": ""},
            allowed_params=["edges_norm_type"],
            scope="test",
            event_value="",
        )
        def inner2(edges_norm_type: str, sensitive_data: str):
            return "test2"

        @tracker_with_constant_uuid.track(
            tracking_info={"event_name": "outer", "event_custom_name": "outer"},
            allowed_params=["edges_norm_type"],
            scope="test",
            event_value="test",
        )
        def outer(edges_norm_type: str, sensitive_data: str):
            val1 = inner1(edges_norm_type=edges_norm_type, sensitive_data=sensitive_data)
            val2 = inner2(edges_norm_type=edges_norm_type, sensitive_data=sensitive_data)
            return val1 + val2

        return_value = outer(edges_norm_type="test_norm_type", sensitive_data="s0mEp@$s")
        tracker_log = tracker_with_constant_uuid.connector.tracker_log

        assert "test1test2" == return_value
        assert 2 == len(tracker_log)
        assert "outer_start" == tracker_log[0]["event_name"]
        assert "outer_end" == tracker_log[1]["event_name"]
        assert "outer" == tracker_log[0]["event_custom_name"]
        assert "outer" == tracker_log[1]["event_custom_name"]
        assert ["edges_norm_type"] == tracker_log[0]["params"]
        assert ["edges_norm_type"] == tracker_log[1]["params"]
        assert 1 == tracker_log[0]["index"]
        assert 2 == tracker_log[1]["index"]
