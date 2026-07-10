"""Tests for the serializable op model in retentioneering.ops."""

import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.ops import Op, apply_op, apply_ops, registered_ops


@pytest.fixture
def simple_df():
    return pd.DataFrame(
        {
            "user_id": ["u1", "u1", "u1", "u2", "u2"],
            "event": ["home", "catalog", "cart", "home", "checkout"],
            "timestamp": pd.to_datetime(
                [
                    "2024-01-01 10:00",
                    "2024-01-01 10:05",
                    "2024-01-01 10:10",
                    "2024-01-01 11:00",
                    "2024-01-01 11:15",
                ]
            ),
        }
    )


def test_registered_ops_covers_full_processor_surface():
    expected = {
        "filter_events",
        "add_clusters",
        "urls_to_events",
        "filter_paths",
        "add_events",
        "add_segment",
        "collapse_events",
        "to_daily_states",
        "drop_segment",
        "edit_events",
        "rename_events",
        "rename_segment_values",
        "drop_events",
        "sample_paths",
        "split_sessions",
        "truncate_paths",
        "add_start_end_events",
    }
    assert expected <= registered_ops()


def test_op_to_dict_from_dict_round_trip():
    op = Op.from_dict({"type": "filter_events", "keep": {"event": ["home"]}})
    assert op.type == "filter_events"
    assert op.params == {"keep": {"event": ["home"]}}
    assert op.to_dict() == {"type": "filter_events", "keep": {"event": ["home"]}}
    assert Op.from_dict(op.to_dict()) == op


def test_op_from_dict_requires_type():
    with pytest.raises(ValueError):
        Op.from_dict({"keep": {"event": ["home"]}})


def test_apply_op_dispatches_by_type(simple_df):
    stream = Eventstream(simple_df)
    result = apply_op(stream, {"type": "filter_events", "keep": {"event": ["home"]}})
    assert set(result.df["event"].astype(str)) == {"home"}


def test_apply_op_accepts_op_instance(simple_df):
    stream = Eventstream(simple_df)
    result = apply_op(
        stream, Op(type="filter_events", params={"keep": {"event": ["home"]}})
    )
    assert set(result.df["event"].astype(str)) == {"home"}


def test_apply_ops_applies_in_order(simple_df):
    stream = Eventstream(simple_df)
    result = apply_ops(
        stream,
        [
            {"type": "filter_events", "drop": {"event": ["cart"]}},
            {
                "type": "add_segment",
                "name": "seg",
                "rules": [["user_id", "=", "u1", "a"], ["b"]],
            },
        ],
    )
    assert "cart" not in set(result.df["event"].astype(str))
    assert "seg" in result.schema.segment_cols


def test_apply_op_unknown_type_raises(simple_df):
    stream = Eventstream(simple_df)
    with pytest.raises(ValueError, match="Unknown or non-processor op type"):
        apply_op(stream, {"type": "not_a_real_op"})


def test_apply_op_missing_type_raises(simple_df):
    stream = Eventstream(simple_df)
    with pytest.raises(ValueError, match="missing required 'type' key"):
        apply_op(stream, {"keep": {"event": ["home"]}})


def test_filter_paths_accepts_legacy_flattened_condition_shape(simple_df):
    """MCP's historical preprocessor step shape flattens the condition tree
    directly into the step dict (no nested `condition` key) — apply_op must
    still support this for backward compatibility, alongside the nested shape
    lineage recording now produces natively."""
    stream = Eventstream(simple_df)

    flattened = apply_op(
        stream, {"type": "filter_paths", "op": ">", "metric": "length", "value": 1}
    )
    nested = apply_op(
        stream,
        {
            "type": "filter_paths",
            "condition": {"op": ">", "metric": "length", "value": 1},
        },
    )
    assert flattened.fingerprint == nested.fingerprint
