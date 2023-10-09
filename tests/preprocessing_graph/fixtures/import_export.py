import os
import tempfile

import pandas as pd
import pytest

from retentioneering.data_processors_lib import FilterEvents, FilterEventsParams
from retentioneering.eventstream import Eventstream, RawDataSchema
from retentioneering.preprocessing_graph import EventsNode, PreprocessingGraph


@pytest.fixture
def config_file():
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    yield tmp_file
    os.remove(tmp_file.name)


@pytest.fixture
def graph_with_source() -> PreprocessingGraph:
    source_df = pd.DataFrame(
        [
            {"event_name": "pageview", "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
            {"event_name": "cart_btn_click", "event_timestamp": "2021-10-26 12:02", "user_id": "1"},
            {"event_name": "pageview", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
            {"event_name": "trash_event", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
            {"event_name": "exit_btn_click", "event_timestamp": "2021-10-26 12:04", "user_id": "1"},
            {"event_name": "plus_icon_click", "event_timestamp": "2021-10-26 12:05", "user_id": "1"},
        ]
    )
    source = Eventstream(
        raw_data_schema=RawDataSchema(event_name="event_name", event_timestamp="event_timestamp", user_id="user_id"),
        raw_data=source_df,
    )
    graph = PreprocessingGraph(source_stream=source)
    graph.root.pk = "3d2e6165-6568-4bbf-aa75-fac3c2e0f054"
    return graph


@pytest.fixture
def simple_graph(graph_with_source) -> PreprocessingGraph:
    graph = graph_with_source

    group_1 = EventsNode(FilterEvents(FilterEventsParams(func=lambda df, schema: df[schema.event_name] == "pageview")))

    group_1.pk = "96e1960c-b038-49e5-9eb8-19fb3814ba3e"

    graph.add_node(node=group_1, parents=[graph.root])
    return graph


@pytest.fixture
def simple_graph_data() -> dict:
    return {
        "directed": True,
        "links": [{"source": "3d2e6165-6568-4bbf-aa75-fac3c2e0f054", "target": "96e1960c-b038-49e5-9eb8-19fb3814ba3e"}],
        "nodes": [
            {"name": "SourceNode", "pk": "3d2e6165-6568-4bbf-aa75-fac3c2e0f054"},
            {
                "name": "EventsNode",
                "pk": "96e1960c-b038-49e5-9eb8-19fb3814ba3e",
                "processor": {
                    "name": "FilterEvents",
                    "values": {
                        "func": "    group_1 = "
                        "EventsNode(FilterEvents(FilterEventsParams(func=lambda "
                        "df, schema: "
                        "df[schema.event_name] == "
                        '"pageview")))\n'
                    },
                },
            },
        ],
    }


@pytest.fixture
def not_full_graph_data() -> dict:
    return {
        "directed": True,
        "links": [{"source": "3d2e6165-6568-4bbf-aa75-fac3c2e0f054", "target": "96e1960c-b038-49e5-9eb8-19fb3814ba3e"}],
        "nodes": [
            {"name": "SourceNode"},
            {
                "pk": "96e1960c-b038-49e5-9eb8-19fb3814ba3e",
                "processor": {
                    "name": "FilterEvents",
                    "values": {
                        "func": "    group_1 = "
                        "EventsNode(FilterEvents(FilterEventsParams(func=lambda "
                        "df, schema: "
                        "df[schema.event_name] == "
                        '"pageview")))\n'
                    },
                },
            },
        ],
    }


@pytest.fixture
def incorrect_data_with_cycle() -> dict:
    return {
        "directed": True,
        "links": [
            {"source": "3d2e6165-6568-4bbf-aa75-fac3c2e0f054", "target": "3d2e6165-6568-4bbf-aa75-fac3c2e0f055"},
            {"source": "3d2e6165-6568-4bbf-aa75-fac3c2e0f055", "target": "3d2e6165-6568-4bbf-aa75-fac3c2e0f055"},
        ],
        "nodes": [
            {"name": "SourceNode", "pk": "3d2e6165-6568-4bbf-aa75-fac3c2e0f054"},
            {
                "name": "EventsNode",
                "pk": "3d2e6165-6568-4bbf-aa75-fac3c2e0f055",
                "processor": {
                    "name": "FilterEvents",
                    "values": {
                        "func": "    group_1 = "
                        "EventsNode(FilterEvents(FilterEventsParams(func=lambda "
                        "df, schema: "
                        "df[schema.event_name] == "
                        '"pageview")))\n'
                    },
                },
            },
            {
                "name": "EventsNode",
                "pk": "3d2e6165-6568-4bbf-aa75-fac3c2e0f055",
                "processor": {
                    "name": "FilterEvents",
                    "values": {
                        "func": "    group_1 = "
                        "EventsNode(FilterEvents(FilterEventsParams(func=lambda "
                        "df, schema: "
                        "df[schema.event_name] == "
                        '"pageview")))\n'
                    },
                },
            },
        ],
    }
