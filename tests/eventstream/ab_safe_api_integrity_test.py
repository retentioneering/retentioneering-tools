"""Public integrity regressions from the independent role-07 API audit."""

import copy
import json

import numpy as np
import pandas as pd
import pytest

from retentioneering import Eventstream
from retentioneering.exceptions import InvalidParameterError, SchemaConfigError


def frame(ids=None):
    return pd.DataFrame(
        {
            "user_id": ids if ids is not None else ["c1", "c2", "t1", "t2"],
            "event": ["pay"] * 4,
            "timestamp": pd.to_datetime(["2026-01-01"] * 4),
            "arm": ["control", "control", "test", "test"],
            "value": [8.0, -2.0, 4.0, 10.0],
        }
    )


SCHEMA = {"segment_cols": ["arm"], "custom_cols": ["value"]}
METRICS = [
    {"name": "value", "metric": "column_sum", "metric_args": {"column": "value"}}
]


@pytest.mark.parametrize("preprocess", [True, False])
@pytest.mark.parametrize("ids", [[1, "1", "01", "2"], [True, 2, 3, 4]])
def test_heterogeneous_path_ids_rejected_before_sql(ids, preprocess):
    data = frame(pd.Series(ids, dtype=object))
    with pytest.raises(SchemaConfigError, match="homogeneous"):
        Eventstream(data, copy.deepcopy(SCHEMA), preprocess=preprocess)


@pytest.mark.parametrize(
    "ids", [[1, 2, 3, 4], [1.5, 2.5, 3.5, 4.5], ["1", "2", "3", "4"]]
)
def test_homogeneous_ids_keep_independent_contributions(ids):
    stream = Eventstream(frame(ids), copy.deepcopy(SCHEMA))
    out = stream.get_ab_paths(("arm", "test", "control"), METRICS)
    assert out["value"].to_dict() == dict(zip(ids, [8.0, -2.0, 4.0, 10.0]))


@pytest.mark.parametrize(
    "ids",
    [
        [2**53 + 1, float(2**53), 2, 3],
        [2**64, 2**64 + 1, 2, 3],
        [2**128, 2**128 + 1, 2, 3],
        [-1, 2**63 + 1, 2, 3],
        [-(2**63) - 1, 2, 3, 4],
    ],
)
def test_non_lossless_numeric_id_domains_reject(ids):
    with pytest.raises(SchemaConfigError, match="homogeneous|64-bit"):
        Eventstream(frame(pd.Series(ids, dtype=object)), copy.deepcopy(SCHEMA))


@pytest.mark.parametrize(
    "ids", [[2**63 - 2, 2**63 - 1, 2, 3], [2**64 - 2, 2**64 - 1, 2, 3]]
)
def test_object_integer_ids_are_normalized_without_precision_loss(ids):
    stream = Eventstream(frame(pd.Series(ids, dtype=object)), copy.deepcopy(SCHEMA))
    values = stream.get_metrics(METRICS)["value"]
    assert values.to_dict() == dict(zip(ids, [8.0, -2.0, 4.0, 10.0]))
    paths = stream.get_ab_paths(("arm", "test", "control"), METRICS)
    assert paths["value"].to_dict() == values.to_dict()


@pytest.mark.parametrize("assignment", [False, True])
def test_exported_typed_arms_reproduce_scorecard(assignment):
    data = frame()
    data["arm"] = pd.Series(["1", "1", 1, 1], dtype=object)
    stream = Eventstream(data, copy.deepcopy(SCHEMA))
    kwargs = {"assignment": data.set_index("user_id")["arm"]} if assignment else {}
    paths = stream.get_ab_paths(("arm", 1, "1"), METRICS, **kwargs)
    score = stream.get_ab_test(("arm", 1, "1"), METRICS, **kwargs).iloc[0]
    grouped = paths.groupby("arm")["value"].mean()
    assert grouped.to_dict() == {1: 7.0, "1": 3.0}
    assert grouped.loc[1] == score.group1_value
    assert grouped.loc["1"] == score.group2_value


@pytest.mark.parametrize("mapping", [dict, pd.Series])
def test_path_mapping_cannot_silently_drop_serialized_key(mapping):
    stream = Eventstream(frame([1, 2, 3, 4]), copy.deepcopy(SCHEMA))
    values = mapping({1: "integer", "1": "string", 2: "second"})
    with pytest.raises(InvalidParameterError, match="collid"):
        stream.add_segment("label", path_values=values)


def test_lineage_owns_input_and_exported_nested_values():
    data = frame()
    stream = Eventstream(data, copy.deepcopy(SCHEMA))
    values = {"c1": "a", "c2": "b", "t1": "a", "t2": "b"}
    derived = stream.add_segment("label", path_values=values)
    before = copy.deepcopy(derived.recipe())
    values["c1"] = "input_changed"
    exported = derived.recipe()
    exported[0]["path_values"]["c2"] = "export_changed"
    assert derived.recipe() == before
    rebuilt = Eventstream.from_recipe(data, derived.recipe(), copy.deepcopy(SCHEMA))
    pd.testing.assert_frame_equal(derived.df, rebuilt.df)


@pytest.mark.parametrize("mapping", [dict, pd.Series])
@pytest.mark.parametrize(
    "ids", [[1, 2, 3, 4], [1.5, 2.5, 3.5, 4.5], ["c1", "c2", "t1", "t2"]]
)
def test_numpy_label_recipe_json_round_trip(ids, mapping):
    data = frame(ids)
    values = mapping({key: np.int64(i) for i, key in enumerate(ids)})
    derived = Eventstream(data, copy.deepcopy(SCHEMA)).add_segment(
        "label", path_values=values
    )
    recipe = json.loads(json.dumps(derived.recipe(), allow_nan=False))
    rebuilt = Eventstream.from_recipe(data, recipe, copy.deepcopy(SCHEMA))
    pd.testing.assert_series_equal(
        derived.get_path_segments("label"), rebuilt.get_path_segments("label")
    )


@pytest.mark.parametrize("mapping", [dict, pd.Series])
def test_integral_float_mapping_keys_round_trip_on_integer_ids(mapping):
    data = frame([1, 2, 3, 4])
    values = mapping({float(i): str(i) for i in [4, 2, 3, 1]})
    derived = Eventstream(data, copy.deepcopy(SCHEMA)).add_segment(
        "label", path_values=values
    )
    recipe = json.loads(json.dumps(derived.recipe(), allow_nan=False, sort_keys=True))
    rebuilt = Eventstream.from_recipe(data, recipe, copy.deepcopy(SCHEMA))
    assert derived.get_path_segments("label").equals(rebuilt.get_path_segments("label"))


@pytest.mark.parametrize("mapping", [dict, pd.Series])
def test_boolean_mapping_keys_round_trip_without_case_coercion(mapping):
    data = frame([False, False, True, True])
    derived = Eventstream(data, copy.deepcopy(SCHEMA)).add_segment(
        "label", path_values=mapping({True: np.int64(7), False: np.int64(3)})
    )
    recipe = json.loads(json.dumps(derived.recipe(), allow_nan=False, sort_keys=True))
    rebuilt = Eventstream.from_recipe(data, recipe, copy.deepcopy(SCHEMA))
    assert derived.get_path_segments("label").equals(rebuilt.get_path_segments("label"))


@pytest.mark.parametrize(
    "value", [pd.Timestamp("2026-01-01", tz="UTC"), float("inf"), object()]
)
@pytest.mark.parametrize("mapping", [dict, pd.Series])
def test_unsupported_persistent_labels_rejected_explicitly(value, mapping):
    stream = Eventstream(frame(), copy.deepcopy(SCHEMA))
    with pytest.raises(InvalidParameterError, match="path_values"):
        stream.add_segment("label", path_values=mapping({"c1": value}))


@pytest.mark.parametrize("route", ["direct", "transferred", "legacy_json"])
def test_cross_review_post_treatment_cut_does_not_certify_strata(route):
    rows = []
    for arm in ["control", "test"]:
        for i in range(12):
            uid = f"{arm}{i}"
            events = [("exposure", 0), ("cut", 10)]
            if i < 6:
                events.append(("pay", 6))
            if (arm == "test" and i < 9) or (arm == "control" and i >= 3):
                events.append(("selected", 5))
            rows.extend(
                (uid, event, pd.Timestamp("2026-01-01") + pd.Timedelta(hours=hour), arm)
                for event, hour in events
            )
    data = pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "arm"])
    stream = Eventstream(data, {"segment_cols": ["arm"]})
    derived = stream.truncate_paths("path_start", "cut").add_segment(
        "selected",
        metric_bins={
            "metric": {"metric": "has_event", "metric_args": {"event": "selected"}},
            "edges": [0.5],
        },
    )
    if route != "direct":
        derived = stream.add_segment(
            "selected", path_values=derived.get_path_segments("selected")
        )
    if route == "legacy_json":
        recipe = json.loads(json.dumps(derived.recipe()))
        recipe[-1]["_path_values_pre_exposure"] = True
        derived = Eventstream.from_recipe(data, recipe, {"segment_cols": ["arm"]})
    with pytest.warns(UserWarning, match="strata"):
        result = derived.get_ab_test(
            ("arm", "test", "control"),
            [{"metric": "has_event", "metric_args": {"event": "pay"}}],
            strata_col="selected",
            min_support=1,
        )
    assert not result.strata_pre_exposure.any()
    assert result.exploratory.all()
    assert set(result.verdict) == {"descriptive"}
    assert result.p_value_adjusted.isna().all()
