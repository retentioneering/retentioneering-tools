import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import SchemaConfigError


@pytest.fixture(params=["schema", "eventstream"])
def make_schema(request):
    def make(config):
        if request.param == "schema":
            return EventstreamSchema(**config)
        df = pd.DataFrame(
            {
                "user_id": [1],
                "event": ["a"],
                "timestamp": [pd.Timestamp("2020-01-01")],
                "session_id": [10],
                "plan": ["free"],
                "revenue": [5],
            }
        )
        return Eventstream(df, config).schema

    return make


@pytest.mark.parametrize("key", ["path_cols", "segment_cols", "custom_cols"])
@pytest.mark.parametrize(
    "value", ["user_id", "", ("user_id",), (), {"user_id"}, {}, 1, False]
)
def test_invalid_containers(make_schema, key, value):
    with pytest.raises(SchemaConfigError) as exc:
        make_schema({key: value})
    assert exc.value.error_code == "SCHEMA_CONFIG_ERROR"
    message = str(exc.value)
    assert key in message
    assert "list of strings" in message
    assert type(value).__name__ in message
    assert repr(value) in message


@pytest.mark.parametrize("key", ["path_cols", "segment_cols", "custom_cols"])
@pytest.mark.parametrize("item", [1, None, False, ["nested"], {"nested": "value"}])
def test_invalid_elements(make_schema, key, item):
    with pytest.raises(SchemaConfigError) as exc:
        make_schema({key: ["user_id", item]})
    message = str(exc.value)
    assert f"{key}[1]" in message
    assert "str" in message
    assert type(item).__name__ in message
    assert repr(item) in message


@pytest.mark.parametrize(
    "key, value", [("path_cols", None), ("segment_cols", None), ("path_cols", [])]
)
def test_invalid_empty_values(make_schema, key, value):
    with pytest.raises(SchemaConfigError) as exc:
        make_schema({key: value})
    message = str(exc.value)
    assert key in message
    assert "list of strings" in message
    assert type(value).__name__ in message
    assert repr(value) in message


@pytest.mark.parametrize(
    "config",
    [
        {},
        {"path_cols": ["user_id"], "segment_cols": [], "custom_cols": []},
        {"segment_cols": [], "custom_cols": None},
        {
            "path_cols": ["user_id", "session_id"],
            "segment_cols": ["plan"],
            "custom_cols": ["revenue"],
        },
    ],
)
def test_valid_configurations(make_schema, config):
    schema = make_schema(config)
    assert schema.path_cols == config.get("path_cols", ["user_id"])
    assert schema.segment_cols == config.get("segment_cols", [])
    if config.get("custom_cols") is not None:
        assert schema.custom_cols == config["custom_cols"]
    assert schema.copy() == schema


@pytest.mark.parametrize("config", [{}, {"custom_cols": None}])
def test_optional_defaults_preserved(config):
    schema = EventstreamSchema(**config)
    assert schema.custom_cols is None
    assert schema.segment_cols == []
    df = pd.DataFrame(
        {
            "user_id": [1],
            "event": ["a"],
            "timestamp": [pd.Timestamp("2020-01-01")],
            "revenue": [5],
        }
    )
    stream = Eventstream(df, config)
    assert stream.schema.custom_cols == ["revenue"]
    assert stream.df["revenue"].tolist() == [5]
