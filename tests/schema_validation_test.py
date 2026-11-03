"""Type validation of the list-valued schema keys (#231).

``path_cols``, ``segment_cols`` and ``custom_cols`` accept only a Python list of
strings (``custom_cols`` also ``None``). Anything else must fail with a
``SchemaConfigError`` that names the key, the expected type and the value.

All three keys go through one helper, so the invalid-value matrix is exercised
once, on ``EventstreamSchema(...)``; the other construction paths
(``from_dict``, ``Eventstream(df, schema)``, ``from_recipe``,
``preprocess=False``, the deprecated ``event_cols`` key) each get one test
showing that the same error reaches the caller.
"""

import warnings

import numpy as np
import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import SchemaConfigError


def make_df():
    return pd.DataFrame(
        {
            "user_id": [1, 1],
            "event": ["a", "b"],
            "timestamp": pd.to_datetime(["2020-01-01", "2020-01-02"]),
            "session_id": [10, 10],
            "plan": ["free", "free"],
            "revenue": [5, 6],
        }
    )


# --- the #231 report, end to end --------------------------------------------


def test_issue_231_example_raises_schema_error_not_type_error():
    """A bare string used to surface as
    ``TypeError: can only concatenate str (not "list") to str``."""
    df = pd.DataFrame(
        {"user_id": [1], "event": ["a"], "timestamp": [pd.Timestamp("2020-01-01")]}
    )
    with pytest.raises(SchemaConfigError) as exc:
        Eventstream(df, {"path_cols": "user_id"})
    assert exc.value.error_code == "SCHEMA_CONFIG_ERROR"
    assert str(exc.value) == (
        "Schema key 'path_cols' must be a non-empty list of strings; "
        "got str 'user_id'. Did you mean path_cols=['user_id']?"
    )


# --- invalid containers: one case per container kind, every key covered -----


@pytest.mark.parametrize(
    "key, value",
    [
        ("path_cols", "user_id"),
        ("segment_cols", "plan"),
        ("custom_cols", "revenue"),
        ("path_cols", ("user_id",)),
        ("segment_cols", {"plan"}),
        ("custom_cols", {"revenue": 1}),
        ("path_cols", 1),
        ("segment_cols", False),
        ("custom_cols", b"revenue"),
        ("path_cols", np.array(["user_id"])),
        ("segment_cols", pd.Index(["plan"])),
        ("path_cols", []),
        ("path_cols", None),
        ("segment_cols", None),
    ],
    ids=lambda v: v if isinstance(v, str) else type(v).__name__,
)
def test_invalid_containers(key, value):
    with pytest.raises(SchemaConfigError) as exc:
        EventstreamSchema(**{key: value})
    message = str(exc.value)
    assert f"Schema key '{key}' must be" in message
    assert "list of strings" in message
    assert f"got {type(value).__name__} " in message
    if not isinstance(value, (np.ndarray, pd.Index)):
        assert repr(value) in message


@pytest.mark.parametrize(
    "key, expected",
    [
        ("path_cols", "a non-empty list of strings"),
        ("segment_cols", "a list of strings"),
        ("custom_cols", "a list of strings or None"),
    ],
)
def test_expected_type_is_stated_per_key(key, expected):
    with pytest.raises(SchemaConfigError, match=f"must be {expected};"):
        EventstreamSchema(**{key: ()})


@pytest.mark.parametrize(
    "value, hinted",
    [
        ("plan", True),
        ("", False),
        (" plan", False),
        ("plan,country", False),
        (("plan",), False),
    ],
    ids=repr,
)
def test_list_hint_only_for_a_plausible_single_column_name(value, hinted):
    with pytest.raises(SchemaConfigError) as exc:
        EventstreamSchema(segment_cols=value)
    assert ("Did you mean segment_cols=['plan']?" in str(exc.value)) is hinted


def test_long_invalid_value_is_abbreviated():
    with pytest.raises(SchemaConfigError) as exc:
        EventstreamSchema(custom_cols=tuple(f"c{i}" for i in range(500)))
    assert "..." in str(exc.value)
    assert len(str(exc.value)) < 300


# --- invalid elements --------------------------------------------------------


@pytest.mark.parametrize(
    "key, value, position, item",
    [
        ("path_cols", ["user_id", 1], 1, 1),
        ("segment_cols", ["plan", None], 1, None),
        ("custom_cols", ["revenue", ["nested"]], 1, ["nested"]),
        ("custom_cols", [1.5, "revenue", b"x"], 0, 1.5),  # first offender wins
    ],
    ids=["int", "None", "nested-list", "first-position"],
)
def test_invalid_elements(key, value, position, item):
    with pytest.raises(SchemaConfigError) as exc:
        EventstreamSchema(**{key: value})
    message = str(exc.value)
    assert f"Schema key '{key}' item {position} must be a string column name" in message
    assert f"got {type(item).__name__} {item!r}." in message
    assert "item 2" not in message


# --- the same error through every construction path -------------------------


def test_from_dict_validates_list_keys():
    with pytest.raises(SchemaConfigError, match="'segment_cols' item 0"):
        EventstreamSchema.from_dict({"segment_cols": [1]})


def test_from_recipe_validates_schema():
    with pytest.raises(
        SchemaConfigError, match="Did you mean segment_cols=\\['plan'\\]"
    ):
        Eventstream.from_recipe(make_df(), [], {"segment_cols": "plan"})


def test_preprocess_false_validates_schema():
    df = Eventstream(make_df()).df
    with pytest.raises(SchemaConfigError, match="'path_cols' must be"):
        Eventstream(df, {"path_cols": "user_id"}, preprocess=False)


def test_deprecated_event_cols_still_validates_list_keys():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        with pytest.raises(SchemaConfigError, match="'path_cols' must be"):
            EventstreamSchema.from_dict(
                {"event_cols": ["event"], "path_cols": "user_id"}
            )


# --- valid configurations keep their meaning --------------------------------


@pytest.mark.parametrize(
    "config",
    [
        {},
        {"path_cols": ["user_id"], "segment_cols": [], "custom_cols": []},
        {
            "path_cols": ["user_id", "session_id"],
            "segment_cols": ["plan"],
            "custom_cols": ["revenue"],
        },
        {"path_cols": [np.str_("user_id")]},
    ],
    ids=["defaults", "explicit-empty", "full", "numpy-str"],
)
def test_valid_configurations_through_eventstream(config):
    schema = Eventstream(make_df(), config).schema
    assert schema.path_cols == config.get("path_cols", ["user_id"])
    assert schema.segment_cols == config.get("segment_cols", [])
    if config.get("custom_cols") is not None:
        assert schema.custom_cols == config["custom_cols"]
    assert schema.copy() == schema


def test_custom_cols_none_still_infers_extra_columns():
    schema = EventstreamSchema()
    assert schema.custom_cols is None
    assert schema.segment_cols == []
    stream = Eventstream(make_df())
    assert stream.schema.custom_cols == ["session_id", "plan", "revenue"]
    assert stream.df["revenue"].tolist() == [5, 6]


def test_inferred_custom_cols_survive_schema_rebuilds():
    """Processors rebuild the schema from ``asdict`` and ``copy()`` re-runs
    ``__post_init__``; inferred string labels must pass both."""
    stream = Eventstream(make_df())
    inferred = ["session_id", "plan", "revenue"]
    assert stream.schema.copy() == stream.schema
    assert stream.add_start_end_events().schema.custom_cols == inferred


def test_non_string_frame_labels_are_rejected_at_construction():
    """An int column label would be inferred into ``custom_cols`` unchecked and
    only fail (blaming ``custom_cols``) once a processor rebuilt the schema."""
    df = make_df()
    df[7] = 0
    with pytest.raises(SchemaConfigError) as exc:
        Eventstream(df)
    message = str(exc.value)
    assert "column labels must be strings" in message
    assert "[7]" in message
    assert "astype(str)" in message
    # Either remedy works: rename the label, or declare custom_cols to drop it.
    df.columns = df.columns.astype(str)
    assert Eventstream(df).schema.custom_cols == ["session_id", "plan", "revenue", "7"]
    df[7] = 0
    assert Eventstream(df, {"custom_cols": []}).schema.custom_cols == []
