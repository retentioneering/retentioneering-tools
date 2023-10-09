from __future__ import annotations

from retentioneering.eventstream import RawDataSchema
from retentioneering.utils.hash_object import hash_value

from .fixtures import tracker_with_constant_uuid


class TestTracker:
    def test_send_message(self, tracker_with_constant_uuid):
        @tracker_with_constant_uuid.time_performance(
            scope="test",
            event_name="test_event_name_send",
            event_value="test_value",
        )
        def test(edges_norm_type: str, sensitive_data: str):
            tracker_with_constant_uuid.collect_data_performance(
                scope="test",
                called_params={"edges_norm_type": edges_norm_type},
            )
            return "test"

        return_value = test(edges_norm_type="test_norm_type", sensitive_data="s0mEp@$s")

        tracker_log = tracker_with_constant_uuid.connector.tracker_log
        assert "test" == return_value
        assert 4 == len(tracker_log)

        assert "12345678-1234-1234-1234-1234567890ab|none|none|none" == tracker_log[1]["user_id"]
        assert "test_event_name_send" == tracker_log[1]["event_name"]
        assert "metadata" == tracker_log[2]["event_name"]
        assert "test_event_name_send" == tracker_log[3]["event_name"]
        assert "test_event_name_send_start" == tracker_log[1]["event_custom_name"]
        assert "test_event_name_send_end" == tracker_log[3]["event_custom_name"]
        assert tracker_log[1]["event_date_local"] is not None
        assert tracker_log[1]["event_day_week"] is not None
        assert "12345678-1234-1234-1234-1234567890ab|none|none|none" == tracker_log[1]["user_id"]
        assert {"args": {"edges_norm_type": hash_value("test_norm_type")}, "performance_info": {}} == tracker_log[2][
            "params"
        ]
        assert "test" == tracker_log[1]["scope"]
        assert "test" == tracker_log[2]["scope"]
        assert "test_value" == tracker_log[1]["event_value"]
        assert "test_value" == tracker_log[3]["event_value"]

    def test_many_messages(self, tracker_with_constant_uuid):
        @tracker_with_constant_uuid.time_performance(
            scope="test",
            event_name="inner1",
        )
        def inner1(edges_norm_type: str, sensitive_data: str):
            return "test1"

        @tracker_with_constant_uuid.time_performance(
            scope="test",
            event_name="inner2",
        )
        def inner2(edges_norm_type: str, sensitive_data: str):
            return "test2"

        @tracker_with_constant_uuid.time_performance(
            scope="test",
            event_name="outer",
        )
        def outer(edges_norm_type: str, sensitive_data: str):
            val1 = inner1(edges_norm_type=edges_norm_type, sensitive_data=sensitive_data)
            val2 = inner2(edges_norm_type=edges_norm_type, sensitive_data=sensitive_data)
            tracker_with_constant_uuid.collect_data_performance(
                scope="test",
                called_params={"edges_norm_type": edges_norm_type},
            )
            return val1 + val2

        return_value = outer(edges_norm_type="test_norm_type", sensitive_data="s0mEp@$s")
        tracker_log = tracker_with_constant_uuid.connector.tracker_log

        assert "test1test2" == return_value
        assert 8 == len(tracker_log)
        assert "outer" == tracker_log[1]["event_name"]
        assert "outer" == tracker_log[-1]["event_name"]
        assert "outer_start" == tracker_log[1]["event_custom_name"]
        assert "outer_end" == tracker_log[-1]["event_custom_name"]
        assert {"args": {"edges_norm_type": hash_value("test_norm_type")}, "performance_info": {}} == tracker_log[6][
            "params"
        ]
        assert 7 == (tracker_log[-1]["index"] - tracker_log[1]["index"] + 1)

    def test_allowed_values(self, tracker_with_constant_uuid):
        @tracker_with_constant_uuid.time_performance(
            scope="test",
            event_name="inner1",
        )
        def inner1(edges_norm_type: str, some_number: int, sensitive_data: str, some_dict: dict[str, dict[str, str]]):
            return "test1"

        @tracker_with_constant_uuid.time_performance(
            scope="test",
            event_name="inner2",
        )
        def inner2(edges_norm_type: str, some_number: int, sensitive_data: str, some_dict: dict[str, dict[str, str]]):
            return "test2"

        @tracker_with_constant_uuid.time_performance(
            scope="test",
            event_name="outer",
        )
        def outer(
            edges_norm_type: str,
            some_number: int,
            some_str: str,
            not_hashed_str: str,
            some_int_list: list[int],
            some_str_list: list[str],
            sensitive_data: str,
            some_dict: dict[str, dict[str, str]],
            not_hashed_dict: dict[str, dict[str, str]],
            some_schema: RawDataSchema,
        ):
            val1 = inner1(
                edges_norm_type=edges_norm_type,
                some_number=some_number,
                sensitive_data=sensitive_data,
                some_dict=some_dict,
            )
            val2 = inner2(
                edges_norm_type=edges_norm_type,
                some_number=some_number,
                sensitive_data=sensitive_data,
                some_dict=some_dict,
            )
            tracker_with_constant_uuid.collect_data_performance(
                scope="test",
                called_params={
                    "edges_norm_type": edges_norm_type,
                    "some_number": some_number,
                    "some_str": some_str,
                    "not_hashed_str": not_hashed_str,
                    "some_int_list": some_int_list,
                    "some_str_list": some_str_list,
                    "some_dict": some_dict,
                    "not_hashed_dict": not_hashed_dict,
                },
                not_hash_values=["not_hashed_str", "param3", "not_hashed_dict", "some_schema"],
            )
            return val1 + val2

        return_value = outer(
            edges_norm_type="test_norm_type",
            some_number=123,
            sensitive_data="s0mEp@$s",
            some_dict={"param1": ["a", "b", "c"], "param2": 2, "param3": {"a": "b", "c": "d"}},
            not_hashed_dict={"param2": "2", "param3": {"c": "a"}},
            some_str="test",
            not_hashed_str="test",
            some_int_list=[1, 2, [3, 4, 5]],
            some_str_list=["a", ["b", "c"], "c"],
            some_schema=RawDataSchema(user_id="test_user_id"),
        )
        tracker_log = tracker_with_constant_uuid.connector.tracker_log

        print(tracker_log[6]["params"])

        assert "test1test2" == return_value
        assert 8 == len(tracker_log)
        assert "outer" == tracker_log[1]["event_name"]
        assert "outer" == tracker_log[-1]["event_name"]
        assert "outer_start" == tracker_log[1]["event_custom_name"]
        assert "outer_end" == tracker_log[-1]["event_custom_name"]
        assert {
            "args": {
                "edges_norm_type": hash_value("test_norm_type"),
                "some_number": 123,
                "some_str": hash_value("test"),
                "not_hashed_str": "test",
                "some_int_list": {"len_flatten": 5, "len": 3},
                "some_str_list": {"len_flatten": 4, "len": 3},
                "some_dict": {
                    hash_value("param1"): {"len_flatten": 3, "len": 3},
                    hash_value("param2"): 2,
                    hash_value("param3"): {hash_value("a"): hash_value("b"), hash_value("c"): hash_value("d")},
                },
                "not_hashed_dict": {"param2": hash_value("2"), "param3": {hash_value("c"): hash_value("a")}},
            },
            "performance_info": {},
        } == tracker_log[6]["params"]
        assert 7 == (tracker_log[-1]["index"] - tracker_log[1]["index"] + 1)
