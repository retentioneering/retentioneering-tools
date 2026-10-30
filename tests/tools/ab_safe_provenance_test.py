"""Post-treatment collider labels must not become confirmatory through a recipe."""

import json

import pandas as pd
import pytest

from retentioneering import Eventstream


@pytest.fixture
def collider():
    base = pd.Timestamp("2026-01-01")
    events, assignments = [], []
    # Identical potential outcomes in both arms; treatment changes only selection.
    for arm in ("A", "B"):
        for i in range(200):
            user = f"{arm}{i}"
            assignments.append((user, arm, base))
            for event, hour in [("expose", 0), ("cut", 10)]:
                events.append((user, event, base + pd.Timedelta(hours=hour), arm))
            if i < 100:
                events.append((user, "buy", base + pd.Timedelta(hours=6), arm))
            if (arm == "A" and i < 150) or (arm == "B" and i >= 50):
                events.append((user, "engagement", base + pd.Timedelta(hours=5), arm))
    frame = pd.DataFrame(events, columns=["user_id", "event", "timestamp", "arm"])
    roster = pd.DataFrame(
        assignments, columns=["user_id", "arm", "assigned_at"]
    ).set_index("user_id")
    stream = Eventstream(frame, {"segment_cols": ["arm"]})
    cut = stream.truncate_paths("path_start", "cut").add_segment(
        "post",
        metric_bins={
            "metric": {"metric": "has_event", "metric_args": {"event": "engagement"}},
            "edges": [0.5],
        },
    )
    return frame, roster, stream, cut


@pytest.mark.parametrize("route", ["direct", "transferred", "replayed", "legacy"])
def test_post_treatment_stratum_does_not_receive_implicit_causal_certification(
    collider, route
):
    frame, roster, stream, cut = collider
    selected = cut
    if route != "direct":
        selected = stream.add_segment("post", path_values=cut.get_path_segments("post"))
    if route in ("replayed", "legacy"):
        recipe = json.loads(json.dumps(selected.recipe()))
        if route == "legacy":
            # Old versions inferred this boolean from any preceding truncation.
            recipe[-1]["_path_values_pre_exposure"] = True
        selected = Eventstream.from_recipe(frame, recipe)
    with pytest.warns(UserWarning, match="strata"):
        result = selected.ab_test_data(
            ("arm", "A", "B"),
            [{"name": "buy", "metric": "has_event", "metric_args": {"event": "buy"}}],
            assignment=roster,
            strata_col="post",
            observation_end="2026-01-02",
            min_support=1,
        )
    assert not result.strata_pre_exposure.any()
    assert result.exploratory.all()
    assert set(result.verdict) == {"descriptive"}
    for column in [
        "p_value",
        "p_value_adjusted",
        "mde",
        "heterogeneity_p_value",
        "stratum_vs_rest_p_value",
    ]:
        assert result[column].isna().all()


def test_explicit_transfer_declaration_is_preserved(collider):
    _, _, stream, cut = collider
    declared = stream.add_segment(
        "post", path_values=cut.get_path_segments("post"), pre_exposure=True
    )
    # The API records an assertion; it cannot validate the actual assignment mechanism.
    assert declared.recipe()[-1]["pre_exposure"] is True
    assert declared.get_path_segments("post").attrs["pre_exposure"] is True
