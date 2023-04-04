import json
from typing import Literal, Union

from retentioneering.backend.callback import list_dataprocessor
from retentioneering.data_processor import DataProcessor
from retentioneering.data_processor.registry import (
    dataprocessor_registry,
    unregister_dataprocessor,
)
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.params_model import ParamsModel
from retentioneering.params_model.registry import (
    params_model_registry,
    unregister_params_model,
)
from retentioneering.utils.list import find_item


class TestListDataprocessors:
    def test_register_new_dataprocessor(self):
        class NewProcessorParams(ParamsModel):
            a: Union[Literal["a"], Literal["b"]]

        class NewProcessor(DataProcessor):
            params: NewProcessorParams

            def __init__(self, params: NewProcessorParams):
                super().__init__(params=params)

            def apply(self, eventstream: Eventstream) -> Eventstream:
                return eventstream.copy()

        processors_list = list_dataprocessor(payload={})
        found_processor = find_item(processors_list, lambda p: p["name"] == "NewProcessor")

        registry = dataprocessor_registry.get_registry()
        registry_params_model = params_model_registry.get_registry()

        assert found_processor is not None
        assert found_processor["name"] == "NewProcessor"
        assert registry["NewProcessor"]
        assert registry_params_model["NewProcessorParams"]

        unregister_dataprocessor(NewProcessor)
        unregister_params_model(NewProcessorParams)

        new_registry = dataprocessor_registry.get_registry()
        new_processors_list = list_dataprocessor(payload={})
        new_params_model_registry = params_model_registry.get_registry()

        found_processor = find_item(new_processors_list, lambda p: p["name"] == "NewProcessor")
        assert found_processor is None
        assert "NewProcessor" not in new_registry
        assert "NewProcessorParams" not in new_params_model_registry

    def test_list_dataprocessors(self) -> None:
        correct_data = [
            {
                "name": "RenameProcessor",
                "params": [{"default": None, "name": "rules", "optional": False, "widget": "rename_rules"}],
            },
            {
                "name": "CollapseLoops",
                "params": [
                    {
                        "name": "suffix",
                        "optional": True,
                        "widget": "enum",
                        "default": "loop",
                        "params": ["loop", "count"],
                    },
                    {
                        "name": "timestamp_aggregation_type",
                        "optional": True,
                        "widget": "enum",
                        "default": "max",
                        "params": ["max", "min", "mean"],
                    },
                ],
            },
            {
                "name": "DeleteUsersByPathLength",
                "params": [
                    {"name": "events_num", "optional": True, "widget": "integer", "default": None},
                    {
                        "name": "cutoff",
                        "default": None,
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
                "name": "FilterEvents",
                "params": [
                    {
                        "name": "func",
                        "default": None,
                        "widget": "function",
                        "optional": False,
                    },
                ],
            },
            {
                "name": "GroupEvents",
                "params": [
                    {"name": "event_name", "optional": False, "widget": "string", "default": None},
                    {"name": "event_type", "optional": True, "widget": "string", "default": "group_alias"},
                    {
                        "name": "func",
                        "default": None,
                        "widget": "function",
                        "optional": False,
                    },
                ],
            },
            {
                "name": "LostUsersEvents",
                "params": [
                    {
                        "name": "lost_cutoff",
                        "default": None,
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
                        "default": None,
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
                        "default": None,
                        "optional": False,
                        "widget": "list_of_string",
                    },
                    {
                        "name": "func",
                        "default": 'def _default_func(eventstream: EventstreamType, negative_target_events: List[str]) -> pd.DataFrame:\n    """\n    Filters rows with target events from the input eventstream.\n\n    Parameters\n    ----------\n    eventstream : Eventstream\n        Source eventstream or output from previous nodes.\n\n    negative_target_events : list of str\n        Each event from that list is associated with the bad result (scenario)\n        of user\'s behaviour (experience) in the product.\n        If there are several target events in user path - the event with minimum timestamp is taken.\n\n    Returns\n    -------\n    pd.DataFrame\n        Filtered DataFrame with negative_target_events and its timestamps.\n    """\n    user_col = eventstream.schema.user_id\n    time_col = eventstream.schema.event_timestamp\n    event_col = eventstream.schema.event_name\n    df = eventstream.to_dataframe()\n\n    negative_events_index = (\n        df[df[event_col].isin(negative_target_events)].groupby(user_col)[time_col].idxmin()  # type: ignore\n    )\n\n    return df.loc[negative_events_index]  # type: ignore\n',
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
                        "default": None,
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
                        "default": None,
                        "optional": False,
                        "widget": "list_of_string",
                    },
                    {
                        "name": "func",
                        "default": 'def _default_func(eventstream: EventstreamType, positive_target_events: list[str]) -> pd.DataFrame:\n    """\n    Filters rows with target events from the input eventstream.\n\n    Parameters\n    ----------\n    eventstream : Eventstream\n        Source eventstream or output from previous nodes.\n\n    positive_target_events : list of str\n        Condition for eventstream filtering.\n        Each event from that list is associated with a conversion goal of the user behaviour in the product.\n        If there are several target events in user path - the event with minimum timestamp is taken.\n\n    Returns\n    -------\n    pd.DataFrame\n        Filtered DataFrame with positive_target_events and its timestamps.\n    """\n    user_col = eventstream.schema.user_id\n    time_col = eventstream.schema.event_timestamp\n    event_col = eventstream.schema.event_name\n    df = eventstream.to_dataframe()\n\n    positive_events_index = (\n        df[df[event_col].isin(positive_target_events)].groupby(user_col)[time_col].idxmin()  # type: ignore\n    )\n\n    return df.loc[positive_events_index]  # type: ignore\n',
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
                        "default": None,
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
                    {"name": "mark_truncated", "optional": True, "widget": "boolean", "default": False},
                    {"name": "session_col", "optional": True, "widget": "string", "default": "session_id"},
                ],
            },
            {"name": "StartEndEvents", "params": []},
            {
                "name": "TruncatePath",
                "params": [
                    {"name": "drop_before", "optional": True, "widget": "string", "default": None},
                    {"name": "drop_after", "optional": True, "widget": "string", "default": None},
                    {
                        "name": "occurrence_before",
                        "optional": True,
                        "default": "first",
                        "widget": "enum",
                        "params": ["first", "last"],
                    },
                    {
                        "name": "occurrence_after",
                        "optional": True,
                        "default": "first",
                        "widget": "enum",
                        "params": ["first", "last"],
                    },
                    {"name": "shift_before", "optional": True, "widget": "integer", "default": 0},
                    {"name": "shift_after", "optional": True, "widget": "integer", "default": 0},
                ],
            },
            {
                "name": "TruncatedEvents",
                "params": [
                    {
                        "name": "left_truncated_cutoff",
                        "optional": True,
                        "default": None,
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
                        "default": None,
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
        ]
        correct_data = sorted(correct_data, key=lambda x: x["name"])
        real_data = list_dataprocessor(payload={})

        assert len(correct_data) == len(real_data)

        for idx, real_processor in enumerate(real_data):
            correct_processor = correct_data[idx]
            assert json.dumps(correct_processor, sort_keys=True, indent=4, separators=(",", ": ")) == json.dumps(
                real_processor, sort_keys=True, indent=4, separators=(",", ": ")
            )
