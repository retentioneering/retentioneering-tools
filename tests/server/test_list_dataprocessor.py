import json

from src.backend.callback import list_dataprocessor


class TestListDataprocessors:
    def test_list_dataprocessors(self) -> None:
        correct_data = [
            {
                "name": "CollapseLoops",
                "params": [
                    {"name": "suffix", "optional": True, "widget": "string"},
                    {
                        "name": "timestamp_aggregation_type",
                        "optional": True,
                        "widget": "string",
                    },
                ],
            },
            {
                "name": "DeleteUsersByPathLength",
                "params": [
                    {"name": "events_num", "optional": True, "widget": "integer"},
                    {
                        "name": "cutoff",
                        "optional": True,
                        "params": [
                            {"widget": "float"},
                            {
                                "params": [
                                    "Y",
                                    "M",
                                    "W",
                                    "D",
                                    "h",
                                    "m",
                                    "s",
                                    "ms",
                                    "us",
                                    "\u03bcs",
                                    "ns",
                                    "ps",
                                    "fs",
                                    "as",
                                ],
                                "widget": "enum",
                            },
                        ],
                        "widget": "time_widget",
                    },
                ],
            },
            {"name": "FilterEvents", "params": []},
            {
                "name": "GroupEvents",
                "params": [
                    {"name": "event_name", "optional": False, "widget": "string"},
                    {"name": "event_type", "optional": True, "widget": "string"},
                ],
            },
            {
                "name": "LostUsersEvents",
                "params": [
                    {
                        "name": "lost_cutoff",
                        "optional": True,
                        "params": [
                            {"widget": "float"},
                            {
                                "params": [
                                    "Y",
                                    "M",
                                    "W",
                                    "D",
                                    "h",
                                    "m",
                                    "s",
                                    "ms",
                                    "us",
                                    "\u03bcs",
                                    "ns",
                                    "ps",
                                    "fs",
                                    "as",
                                ],
                                "widget": "enum",
                            },
                        ],
                        "widget": "time_widget",
                    },
                    {
                        "name": "lost_users_list",
                        "optional": True,
                        "widget": "list_of_int",
                    },
                ],
            },
            {
                "name": "NegativeTarget",
                "params": [
                    {
                        "name": "negative_target_events",
                        "optional": False,
                        "widget": "list_of_string",
                    },
                    {
                        "_source_code": "",
                        "name": "negative_function",
                        "optional": True,
                        "widget": "function",
                    },
                ],
            },
            {
                "name": "NewUsersEvents",
                "params": [
                    {
                        "name": "new_users_list",
                        "optional": False,
                        "widget": "list_of_int",
                        "params": {
                            "disable_value": "all",
                        },
                    }
                ],
            },
            {
                "name": "PositiveTarget",
                "params": [
                    {
                        "name": "positive_target_events",
                        "optional": False,
                        "widget": "list_of_string",
                    },
                    {
                        "_source_code": "",
                        "name": "positive_function",
                        "optional": True,
                        "widget": "function",
                    },
                ],
            },
            {
                "name": "SplitSessions",
                "params": [
                    {
                        "name": "session_cutoff",
                        "optional": False,
                        "params": [
                            {"widget": "float"},
                            {
                                "params": [
                                    "Y",
                                    "M",
                                    "W",
                                    "D",
                                    "h",
                                    "m",
                                    "s",
                                    "ms",
                                    "us",
                                    "\u03bcs",
                                    "ns",
                                    "ps",
                                    "fs",
                                    "as",
                                ],
                                "widget": "enum",
                            },
                        ],
                        "widget": "time_widget",
                    },
                    {"name": "mark_truncated", "optional": True, "widget": "boolean"},
                    {"name": "session_col", "optional": True, "widget": "string"},
                ],
            },
            {"name": "StartEndEvents", "params": []},
            {
                "name": "TruncatePath",
                "params": [
                    {"name": "drop_before", "optional": True, "widget": "string"},
                    {"name": "drop_after", "optional": True, "widget": "string"},
                    {"name": "occurrence_before", "optional": True, "widget": "string"},
                    {"name": "occurrence_after", "optional": True, "widget": "string"},
                    {"name": "shift_before", "optional": True, "widget": "integer"},
                    {"name": "shift_after", "optional": True, "widget": "integer"},
                ],
            },
            {
                "name": "TruncatedEvents",
                "params": [
                    {
                        "name": "left_truncated_cutoff",
                        "optional": True,
                        "params": [
                            {"widget": "float"},
                            {
                                "params": [
                                    "Y",
                                    "M",
                                    "W",
                                    "D",
                                    "h",
                                    "m",
                                    "s",
                                    "ms",
                                    "us",
                                    "\u03bcs",
                                    "ns",
                                    "ps",
                                    "fs",
                                    "as",
                                ],
                                "widget": "enum",
                            },
                        ],
                        "widget": "time_widget",
                    },
                    {
                        "name": "right_truncated_cutoff",
                        "optional": True,
                        "params": [
                            {"widget": "float"},
                            {
                                "params": [
                                    "Y",
                                    "M",
                                    "W",
                                    "D",
                                    "h",
                                    "m",
                                    "s",
                                    "ms",
                                    "us",
                                    "\u03bcs",
                                    "ns",
                                    "ps",
                                    "fs",
                                    "as",
                                ],
                                "widget": "enum",
                            },
                        ],
                        "widget": "time_widget",
                    },
                ],
            },
            {
                "name": "StubProcessor",
                "params": [{"default": None, "name": "A", "optional": False, "widget": "array"}],
            },
            {
                "name": "HelperAddColProcessor",
                "params": [
                    {"name": "event_name", "optional": False, "widget": "string"},
                    {"name": "column_name", "optional": False, "widget": "string"},
                ],
            },
            {
                "name": "NoHelperAddColProcessor",
                "params": [
                    {"name": "event_name", "optional": False, "widget": "string"},
                    {"name": "column_name", "optional": False, "widget": "string"},
                ],
            },
            {
                "name": "DeleteProcessor",
                "params": [{"name": "name", "optional": False, "widget": "string"}],
            },
            {
                "name": "StubProcessorPGraph",
                "params": [{"default": None, "name": "A", "optional": False, "widget": "array"}],
            },
        ]
        correct_data = sorted(correct_data, key=lambda x: x["name"])
        assert json.dumps(correct_data, sort_keys=True, indent=4, separators=(",", ": ")) == json.dumps(
            list_dataprocessor(payload={}), sort_keys=True, indent=4, separators=(",", ": ")
        )
