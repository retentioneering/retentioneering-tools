import pandas as pd

from src.data_processors_lib.rete import (
    CollapseLoops,
    CollapseLoopsParams,
    DeleteUsersByPathLength,
    DeleteUsersByPathLengthParams,
    LostUsersEvents,
    LostUsersParams,
    NegativeTarget,
    NegativeTargetParams,
    NewUsersEvents,
    NewUsersParams,
    PositiveTarget,
    PositiveTargetParams,
    SplitSessions,
    SplitSessionsParams,
    StartEndEvents,
    StartEndEventsParams,
    TruncatedEvents,
    TruncatedEventsParams,
    TruncatePath,
    TruncatePathParams,
)
from src.eventstream.eventstream import Eventstream, RawDataSchema
from src.graph.nodes import EventsNode
from src.graph.p_graph import PGraph


class TestPGraphExportImport:
    def create_graph(self) -> PGraph:
        source_df = pd.DataFrame(
            [
                {"event_name": "pageview", "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
                {"event_name": "cart_btn_click", "event_timestamp": "2021-10-26 12:02", "user_id": "1"},
                {"event_name": "pageview", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "trash_event", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "exit_btn_click", "event_timestamp": "2021-10-26 12:04", "user_id": "2"},
                {"event_name": "plus_icon_click", "event_timestamp": "2021-10-26 12:05", "user_id": "1"},
            ]
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event_name", event_timestamp="event_timestamp", user_id="user_id"
            ),
            raw_data=source_df,
        )
        graph = PGraph(source)

        return graph

    def test_start_end__export(self) -> None:
        graph = self.create_graph()
        node = EventsNode(processor=StartEndEvents(params=StartEndEventsParams(**{})))
        graph.add_node(node=node, parents=[graph.root])

        export_data = graph.export(payload={})
        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]

        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {"name": "EventsNode", "processor": {"values": {}, "name": "StartEndEvents"}},
            ],
        } == export_data

    def test_start_end__import(self) -> None:
        graph = self.create_graph()

        graph._set_graph(
            payload={
                "directed": True,
                "nodes": [
                    {"name": "SourceNode", "pk": "a911d6de-48a9-4898-8d4f-c123efc84498"},
                    {
                        "name": "EventsNode",
                        "pk": "81e5ead2-c0ed-43c5-a522-9d2484a1607e",
                        "processor": {"values": {}, "name": "StartEndEvents"},
                    },
                ],
                "links": [
                    {"source": "a911d6de-48a9-4898-8d4f-c123efc84498", "target": "81e5ead2-c0ed-43c5-a522-9d2484a1607e"}
                ],
            }
        )

        export_data = graph.export(payload={})

        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]

        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {"name": "EventsNode", "processor": {"values": {}, "name": "StartEndEvents"}},
            ],
        } == export_data

    def test_truncated__export(self) -> None:
        graph = self.create_graph()

        node = EventsNode(
            processor=TruncatedEvents(
                params=TruncatedEventsParams(left_truncated_cutoff=(1, "h"), right_truncated_cutoff=(1, "h"))
            )
        )
        graph.add_node(node=node, parents=[graph.root])

        export_data = graph.export(payload={})
        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]
        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {
                    "name": "EventsNode",
                    "processor": {
                        "name": "TruncatedEvents",
                        "values": {"left_truncated_cutoff": "1.0,h", "right_truncated_cutoff": "1.0,h"},
                    },
                },
            ],
        } == export_data

    def test_truncated__import(self) -> None:
        graph = self.create_graph()
        graph._set_graph(
            payload={
                "directed": True,
                "nodes": [
                    {"name": "SourceNode", "pk": "0ad30844-66f8-47db-b5f0-221679296fe7"},
                    {
                        "name": "EventsNode",
                        "pk": "f45f7390-d2b4-4414-bcd2-94532ede375d",
                        "processor": {
                            "values": {"left_truncated_cutoff": "1.0,h", "right_truncated_cutoff": "1.0,h"},
                            "name": "TruncatedEvents",
                        },
                    },
                ],
                "links": [
                    {"source": "0ad30844-66f8-47db-b5f0-221679296fe7", "target": "f45f7390-d2b4-4414-bcd2-94532ede375d"}
                ],
            }
        )

        export_data = graph.export(payload={})
        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]
        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {
                    "name": "EventsNode",
                    "processor": {
                        "name": "TruncatedEvents",
                        "values": {"left_truncated_cutoff": "1.0,h", "right_truncated_cutoff": "1.0,h"},
                    },
                },
            ],
        } == export_data

    def test_new_users__export(self) -> None:
        graph = self.create_graph()

        node = EventsNode(processor=NewUsersEvents(params=NewUsersParams(new_users_list=[2])))
        graph.add_node(node=node, parents=[graph.root])

        export_data = graph.export(payload={})
        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]
        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {
                    "name": "EventsNode",
                    "processor": {
                        "name": "NewUsersEvents",
                        "values": {"new_users_list": [2]},
                    },
                },
            ],
        } == export_data

    def test_new_users__import(self) -> None:
        graph = self.create_graph()
        graph._set_graph(
            payload={
                "directed": True,
                "nodes": [
                    {"name": "SourceNode", "pk": "0ad30844-66f8-47db-b5f0-221679296fe7"},
                    {
                        "name": "EventsNode",
                        "pk": "f45f7390-d2b4-4414-bcd2-94532ede375d",
                        "processor": {
                            "name": "NewUsersEvents",
                            "values": {"new_users_list": [2]},
                        },
                    },
                ],
                "links": [
                    {"source": "0ad30844-66f8-47db-b5f0-221679296fe7", "target": "f45f7390-d2b4-4414-bcd2-94532ede375d"}
                ],
            }
        )

        export_data = graph.export(payload={})
        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]
        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {
                    "name": "EventsNode",
                    "processor": {
                        "name": "NewUsersEvents",
                        "values": {"new_users_list": [2]},
                    },
                },
            ],
        } == export_data

    def test_collapse_loops__export(self) -> None:
        graph = self.create_graph()

        node = EventsNode(
            processor=CollapseLoops(
                params=CollapseLoopsParams(**{"full_collapse": False, "timestamp_aggregation_type": "min"})
            )
        )
        graph.add_node(node=node, parents=[graph.root])

        export_data = graph.export(payload={})
        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]
        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {
                    "name": "EventsNode",
                    "processor": {
                        "name": "CollapseLoops",
                        "values": {"full_collapse": False, "timestamp_aggregation_type": "min"},
                    },
                },
            ],
        } == export_data

    def test_collapse_loops__import(self) -> None:
        graph = self.create_graph()
        graph._set_graph(
            payload={
                "directed": True,
                "nodes": [
                    {"name": "SourceNode", "pk": "0ad30844-66f8-47db-b5f0-221679296fe7"},
                    {
                        "name": "EventsNode",
                        "pk": "f45f7390-d2b4-4414-bcd2-94532ede375d",
                        "processor": {
                            "name": "CollapseLoops",
                            "values": {"full_collapse": False, "timestamp_aggregation_type": "min"},
                        },
                    },
                ],
                "links": [
                    {"source": "0ad30844-66f8-47db-b5f0-221679296fe7", "target": "f45f7390-d2b4-4414-bcd2-94532ede375d"}
                ],
            }
        )

        export_data = graph.export(payload={})
        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]
        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {
                    "name": "EventsNode",
                    "processor": {
                        "name": "CollapseLoops",
                        "values": {"full_collapse": False, "timestamp_aggregation_type": "min"},
                    },
                },
            ],
        } == export_data

    def test_delete_user__export(self) -> None:
        graph = self.create_graph()

        node = EventsNode(processor=DeleteUsersByPathLength(params=DeleteUsersByPathLengthParams(cutoff=(1.5, "m"))))
        graph.add_node(node=node, parents=[graph.root])

        export_data = graph.export(payload={})
        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]
        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {
                    "name": "EventsNode",
                    "processor": {
                        "name": "DeleteUsersByPathLength",
                        "values": {"cutoff": "1.5,m", "events_num": None},
                    },
                },
            ],
        } == export_data

    def test_delete_user__import(self) -> None:
        graph = self.create_graph()
        graph._set_graph(
            payload={
                "directed": True,
                "nodes": [
                    {"name": "SourceNode", "pk": "0ad30844-66f8-47db-b5f0-221679296fe7"},
                    {
                        "name": "EventsNode",
                        "pk": "f45f7390-d2b4-4414-bcd2-94532ede375d",
                        "processor": {
                            "name": "DeleteUsersByPathLength",
                            "values": {"cutoff": "1.5,m"},
                        },
                    },
                ],
                "links": [
                    {"source": "0ad30844-66f8-47db-b5f0-221679296fe7", "target": "f45f7390-d2b4-4414-bcd2-94532ede375d"}
                ],
            }
        )

        export_data = graph.export(payload={})
        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]
        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {
                    "name": "EventsNode",
                    "processor": {
                        "name": "DeleteUsersByPathLength",
                        "values": {"cutoff": "1.5,m", "events_num": None},
                    },
                },
            ],
        } == export_data

    def test_lost_users__export(self) -> None:
        graph = self.create_graph()

        node = EventsNode(processor=LostUsersEvents(params=LostUsersParams(lost_users_list=None, lost_cutoff=(4, "h"))))
        graph.add_node(node=node, parents=[graph.root])

        export_data = graph.export(payload={})
        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]
        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {
                    "name": "EventsNode",
                    "processor": {
                        "name": "LostUsersEvents",
                        "values": {"lost_users_list": None, "lost_cutoff": "4.0,h"},
                    },
                },
            ],
        } == export_data

    def test_lost_users__import(self) -> None:
        graph = self.create_graph()
        graph._set_graph(
            payload={
                "directed": True,
                "nodes": [
                    {"name": "SourceNode", "pk": "0ad30844-66f8-47db-b5f0-221679296fe7"},
                    {
                        "name": "EventsNode",
                        "pk": "f45f7390-d2b4-4414-bcd2-94532ede375d",
                        "processor": {
                            "name": "LostUsersEvents",
                            "values": {"lost_users_list": None, "lost_cutoff": "4.0,h"},
                        },
                    },
                ],
                "links": [
                    {"source": "0ad30844-66f8-47db-b5f0-221679296fe7", "target": "f45f7390-d2b4-4414-bcd2-94532ede375d"}
                ],
            }
        )

        export_data = graph.export(payload={})
        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]
        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {
                    "name": "EventsNode",
                    "processor": {
                        "name": "LostUsersEvents",
                        "values": {"lost_users_list": None, "lost_cutoff": "4.0,h"},
                    },
                },
            ],
        } == export_data

    def test_negative_target__export(self) -> None:
        graph = self.create_graph()

        node = EventsNode(
            processor=NegativeTarget(params=NegativeTargetParams(**{"negative_target_events": ["event3", "event2"]}))
        )
        graph.add_node(node=node, parents=[graph.root])

        export_data = graph.export(payload={})
        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]
        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {
                    "name": "EventsNode",
                    "processor": {
                        "name": "NegativeTarget",
                        "values": {
                            "negative_target_events": ["event3", "event2"],
                            "negative_function": "def _default_func_negative(eventstream, "
                            "negative_target_events) -> pd.DataFrame:\n"
                            "    user_col = eventstream.schema.user_id\n"
                            "    time_col = eventstream.schema.event_timestamp\n"
                            "    event_col = eventstream.schema.event_name\n"
                            "    df = eventstream.to_dataframe()\n"
                            "\n"
                            "    negative_events_index = "
                            "df[df[event_col].isin(negative_target_events)].groupby"
                            "(user_col)[time_col].idxmin()\n"
                            "\n"
                            "    return df.iloc[negative_events_index]\n",
                        },
                    },
                },
            ],
        } == export_data

    def test_negative_target__import(self) -> None:
        graph = self.create_graph()
        graph._set_graph(
            payload={
                "directed": True,
                "nodes": [
                    {"name": "SourceNode", "pk": "0ad30844-66f8-47db-b5f0-221679296fe7"},
                    {
                        "name": "EventsNode",
                        "pk": "f45f7390-d2b4-4414-bcd2-94532ede375d",
                        "processor": {
                            "name": "NegativeTarget",
                            "values": {
                                "negative_target_events": ["event3", "event2"],
                                "negative_function": "def _default_func_negative(eventstream, "
                                "negative_target_events) -> pd.DataFrame:\n"
                                "    user_col = eventstream.schema.user_id\n"
                                "    time_col = eventstream.schema.event_timestamp\n"
                                "    event_col = eventstream.schema.event_name\n"
                                "    df = eventstream.to_dataframe()\n"
                                "\n"
                                "    negative_events_index = "
                                "df[df[event_col].isin(negative_target_events)].groupby"
                                "(user_col)[time_col].idxmin()\n"
                                "\n"
                                "    return df.iloc[negative_events_index]\n",
                            },
                        },
                    },
                ],
                "links": [
                    {"source": "0ad30844-66f8-47db-b5f0-221679296fe7", "target": "f45f7390-d2b4-4414-bcd2-94532ede375d"}
                ],
            }
        )

        export_data = graph.export(payload={})
        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]
        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {
                    "name": "EventsNode",
                    "processor": {
                        "name": "NegativeTarget",
                        "values": {
                            "negative_target_events": ["event3", "event2"],
                            "negative_function": "def _default_func_negative(eventstream, "
                            "negative_target_events) -> pd.DataFrame:\n"
                            "    user_col = eventstream.schema.user_id\n"
                            "    time_col = eventstream.schema.event_timestamp\n"
                            "    event_col = eventstream.schema.event_name\n"
                            "    df = eventstream.to_dataframe()\n"
                            "\n"
                            "    negative_events_index = "
                            "df[df[event_col].isin(negative_target_events)].groupby"
                            "(user_col)[time_col].idxmin()\n"
                            "\n"
                            "    return df.iloc[negative_events_index]\n",
                        },
                    },
                },
            ],
        } == export_data

    def test_positive_events__export(self) -> None:
        graph = self.create_graph()

        node = EventsNode(
            processor=PositiveTarget(params=PositiveTargetParams(**{"positive_target_events": ["event3", "event2"]}))
        )
        graph.add_node(node=node, parents=[graph.root])

        export_data = graph.export(payload={})
        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]
        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {
                    "name": "EventsNode",
                    "processor": {
                        "name": "PositiveTarget",
                        "values": {
                            "positive_target_events": ["event3", "event2"],
                            "positive_function": "def _default_func_positive("
                            "eventstream: Eventstream, positive_target_events: list[str]) "
                            "-> pd.DataFrame:\n    user_col = eventstream.schema.user_id"
                            "\n    time_col = eventstream.schema.event_timestamp"
                            "\n    event_col = eventstream.schema.event_name"
                            "\n    df = eventstream.to_dataframe()\n\n    "
                            "positive_events_index = (\n        "
                            "df[df[event_col].isin(positive_target_events)]."
                            "groupby(user_col)[time_col].idxmin()  # type: ignore\n    )\n\n"
                            "    return df.iloc[positive_events_index]  # type: ignore\n",
                        },
                    },
                },
            ],
        } == export_data

    def test_positive_events__import(self) -> None:
        graph = self.create_graph()
        graph._set_graph(
            payload={
                "directed": True,
                "nodes": [
                    {"name": "SourceNode", "pk": "0ad30844-66f8-47db-b5f0-221679296fe7"},
                    {
                        "name": "EventsNode",
                        "pk": "f45f7390-d2b4-4414-bcd2-94532ede375d",
                        "processor": {
                            "name": "PositiveTarget",
                            "values": {"positive_target_events": ["event3", "event2"]},
                        },
                    },
                ],
                "links": [
                    {"source": "0ad30844-66f8-47db-b5f0-221679296fe7", "target": "f45f7390-d2b4-4414-bcd2-94532ede375d"}
                ],
            }
        )

        export_data = graph.export(payload={})
        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]
        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {
                    "name": "EventsNode",
                    "processor": {
                        "name": "PositiveTarget",
                        "values": {
                            "positive_target_events": ["event3", "event2"],
                            "positive_function": "def _default_func_positive("
                            "eventstream: Eventstream, positive_target_events: list[str]) "
                            "-> pd.DataFrame:\n    user_col = eventstream.schema.user_id"
                            "\n    time_col = eventstream.schema.event_timestamp"
                            "\n    event_col = eventstream.schema.event_name"
                            "\n    df = eventstream.to_dataframe()\n\n    "
                            "positive_events_index = (\n        "
                            "df[df[event_col].isin(positive_target_events)]."
                            "groupby(user_col)[time_col].idxmin()  # type: ignore\n    )\n\n"
                            "    return df.iloc[positive_events_index]  # type: ignore\n",
                        },
                    },
                },
            ],
        } == export_data

    def test_split_sesssion__export(self) -> None:
        graph = self.create_graph()

        node = EventsNode(
            processor=SplitSessions(
                params=SplitSessionsParams(session_cutoff=(30, "m"), session_col="session_id", mark_truncated=True)
            )
        )
        graph.add_node(node=node, parents=[graph.root])

        export_data = graph.export(payload={})
        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]
        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {
                    "name": "EventsNode",
                    "processor": {
                        "name": "SplitSessions",
                        "values": {"session_cutoff": "30.0,m", "session_col": "session_id", "mark_truncated": True},
                    },
                },
            ],
        } == export_data

    def test_split_sesssion__import(self) -> None:
        graph = self.create_graph()
        graph._set_graph(
            payload={
                "directed": True,
                "nodes": [
                    {"name": "SourceNode", "pk": "0ad30844-66f8-47db-b5f0-221679296fe7"},
                    {
                        "name": "EventsNode",
                        "pk": "f45f7390-d2b4-4414-bcd2-94532ede375d",
                        "processor": {
                            "name": "SplitSessions",
                            "values": {"session_cutoff": "30.0,m", "session_col": "session_id", "mark_truncated": True},
                        },
                    },
                ],
                "links": [
                    {"source": "0ad30844-66f8-47db-b5f0-221679296fe7", "target": "f45f7390-d2b4-4414-bcd2-94532ede375d"}
                ],
            }
        )

        export_data = graph.export(payload={})
        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]
        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {
                    "name": "EventsNode",
                    "processor": {
                        "name": "SplitSessions",
                        "values": {"session_cutoff": "30.0,m", "session_col": "session_id", "mark_truncated": True},
                    },
                },
            ],
        } == export_data

    def test_truncated_path__export(self) -> None:
        graph = self.create_graph()

        node = EventsNode(
            processor=TruncatePath(
                params=TruncatePathParams(drop_before="event3", occurrence_before="last", shift_before=2)
            )
        )
        graph.add_node(node=node, parents=[graph.root])

        export_data = graph.export(payload={})
        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]
        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {
                    "name": "EventsNode",
                    "processor": {
                        "name": "TruncatePath",
                        "values": {
                            "drop_before": "event3",
                            "occurrence_before": "last",
                            "shift_before": 2,
                            "drop_after": None,
                            "occurrence_after": "first",
                            "shift_after": 0,
                        },
                    },
                },
            ],
        } == export_data

    def test_truncated_path__import(self) -> None:
        graph = self.create_graph()
        graph._set_graph(
            payload={
                "directed": True,
                "nodes": [
                    {"name": "SourceNode", "pk": "0ad30844-66f8-47db-b5f0-221679296fe7"},
                    {
                        "name": "EventsNode",
                        "pk": "f45f7390-d2b4-4414-bcd2-94532ede375d",
                        "processor": {
                            "name": "TruncatePath",
                            "values": {"drop_before": "event3", "occurrence_before": "last", "shift_before": 2},
                        },
                    },
                ],
                "links": [
                    {"source": "0ad30844-66f8-47db-b5f0-221679296fe7", "target": "f45f7390-d2b4-4414-bcd2-94532ede375d"}
                ],
            }
        )

        export_data = graph.export(payload={})
        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]
        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {
                    "name": "EventsNode",
                    "processor": {
                        "name": "TruncatePath",
                        "values": {
                            "drop_before": "event3",
                            "occurrence_before": "last",
                            "shift_before": 2,
                            "drop_after": None,
                            "occurrence_after": "first",
                            "shift_after": 0,
                        },
                    },
                },
            ],
        } == export_data
