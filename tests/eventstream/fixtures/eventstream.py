import os

import pandas as pd
import pytest

from retentioneering.eventstream import Eventstream, EventstreamSchema, RawDataSchema


@pytest.fixture
def test_stattests_stream_1():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../datasets/tooling/stattests")
    filepath = os.path.join(test_data_dir, "01_simple_data.csv")

    stream = Eventstream(
        raw_data=pd.read_csv(filepath),
        raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
        schema=EventstreamSchema(),
    )
    return stream


@pytest.fixture
def test_data_1():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../datasets/eventstream")
    filepath = os.path.join(test_data_dir, "01_data.csv")

    data = pd.read_csv(filepath)
    return data


@pytest.fixture
def test_schema_1():
    return RawDataSchema(event_name="name", event_timestamp="event_timestamp", user_id="user_id")


@pytest.fixture
def test_stream_1():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../datasets/eventstream")
    filepath = os.path.join(test_data_dir, "01_data.csv")

    stream = Eventstream(
        raw_data=pd.read_csv(filepath),
        raw_data_schema=RawDataSchema(event_name="name", event_timestamp="event_timestamp", user_id="user_id"),
        schema=EventstreamSchema(),
    )

    return stream


@pytest.fixture
def test_source_dataframe_with_custom_col():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../datasets/eventstream")
    filepath = os.path.join(test_data_dir, "01_data_with_custom_col.csv")
    df = pd.read_csv(filepath)
    return df


@pytest.fixture
def test_stream_2():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../datasets/eventstream")
    filepath = os.path.join(test_data_dir, "02_data.csv")

    stream = Eventstream(
        raw_data=pd.read_csv(filepath),
        raw_data_schema=RawDataSchema(
            event_name="name", event_timestamp="event_timestamp", user_id="user_id", event_type="type"
        ),
        schema=EventstreamSchema(),
    )

    return stream


@pytest.fixture
def test_data_join_1():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../datasets/eventstream")
    filepath = os.path.join(test_data_dir, "03_data.csv")

    data = pd.read_csv(filepath)
    return data


@pytest.fixture
def test_data_join_2():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../datasets/eventstream")
    filepath = os.path.join(test_data_dir, "04_data.csv")

    data = pd.read_csv(filepath)
    return data


@pytest.fixture
def test_data_sampling():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../datasets/eventstream")
    filepath = os.path.join(test_data_dir, "05_five_users_data.csv")

    data = pd.read_csv(filepath)
    return data
