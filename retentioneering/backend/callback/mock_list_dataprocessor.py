from __future__ import annotations

from typing import Any

data: list[dict] = [
    {
        "name": "CollapseLoops",
        "params": [
            {
                "name": "full_collapse",
                "widget": "bool",
                "default": True,
                "optional": False,
            },
            {
                "name": "time_agg",
                "widget": "enum",
                "default": "max",
                "optional": False,
                "params": ["max", "min", "mean"],
            },
        ],
    },
    {
        "name": "CutPathAfterEvent",
        "params": [
            {
                "name": "cutoff_events",
                "widget": "list_of_string",
                "optional": False,
            },
            {
                "name": "cut_shift",
                "widget": "int",
                "optional": False,
            },
            {
                "name": "min_cjm",
                "widget": "int",
                "optional": False,
            },
        ],
    },
    {
        "name": "CutPathBeforeEvent",
        "params": [
            {
                "name": "cutoff_events",
                "widget": "list_of_string",
                "optional": False,
            },
            {
                "name": "cut_shift",
                "widget": "int",
                "optional": False,
            },
            {
                "name": "min_cjm",
                "widget": "int",
                "optional": False,
            },
        ],
    },
    {
        "name": "LabelLostUsers",
        "params": [
            {
                "name": "timeout",
                "widget": "tuple",
                "optional": True,
                "params": [
                    {
                        "widget": "float",
                    },
                    {
                        "widget": "enum",
                        "params": ["Y", "M", "W", "D", "h", "m", "s", "ms", "us", "μs", "ns", "ps", "fs", "as"],
                    },
                ],
            },
            {
                "name": "lost_users_list",
                "widget": "list_of_string",
                "optional": True,
            },
        ],
    },
    {
        "name": "AddNegativeEvents",
        "params": [
            {
                "name": "targets",
                "widget": "list_of_string",
                "optional": False,
            },
            {
                "name": "negative_function",
                "widget": "callable",
                "optional": True,
            },
        ],
    },
    {
        "name": "LabelNewUsers",
        "params": [
            {
                "name": "new_users_list",
                "widget": "list_of_string",
                "optional": False,
            },
        ],
    },
    {
        "name": "AddPositiveEvents",
        "params": [
            {
                "name": "targets",
                "widget": "list_of_string",
                "optional": False,
            },
            {
                "name": "positive_function",
                "widget": "callable",
                "optional": False,
            },
        ],
    },
    {
        "name": "SplitSessions",
        "params": [
            {
                "name": "timeout",
                "widget": "tuple",
                "optional": False,
                "params": [
                    {
                        "widget": "float",
                    },
                    {
                        "widget": "enum",
                        "params": ["Y", "M", "W", "D", "h", "m", "s", "ms", "us", "μs", "ns", "ps", "fs", "as"],
                    },
                ],
            },
            {
                "name": "mark_truncated",
                "widget": "bool",
                "optional": True,
            },
            {
                "name": "session_col",
                "widget": "string",
                "optional": True,
            },
        ],
    },
    {
        "name": "AddStartEndEvents",
        "params": [],
    },
    {
        "name": "LabelCroppedPaths",
        "params": [
            {
                "name": "left_cutoff",
                "widget": "tuple",
                "optional": False,
                "params": [
                    {
                        "widget": "float",
                    },
                    {
                        "widget": "enum",
                        "params": ["Y", "M", "W", "D", "h", "m", "s", "ms", "us", "μs", "ns", "ps", "fs", "as"],
                    },
                ],
            },
            {
                "name": "right_cutoff",
                "widget": "tuple",
                "optional": False,
                "params": [
                    {
                        "widget": "float",
                    },
                    {
                        "widget": "enum",
                        "params": ["Y", "M", "W", "D", "h", "m", "s", "ms", "us", "μs", "ns", "ps", "fs", "as"],
                    },
                ],
            },
        ],
    },
]


def list_dataprocessor_mock(payload: dict) -> list[dict[str, Any]]:
    return data
