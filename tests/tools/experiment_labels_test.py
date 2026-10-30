"""Label storage and measurement units must not change experiment semantics."""

import json

import pandas as pd
import pytest

from retentioneering import Eventstream

OBS = dict(
    start="2026-01-02",
    end="2026-01-04",
    assignment_time_col="assigned_at",
    complete=True,
)


@pytest.mark.parametrize("arms", [("c", "t"), (1, 2)])
@pytest.mark.parametrize("ordered", [False, True])
def test_categorical_variants_with_silent_arm(arms, ordered):
    c, t = arms
    stream = Eventstream(
        pd.DataFrame(
            dict(
                user_id=["c1", "c2"],
                event=["buy", "open"],
                timestamp="2026-01-03",
                variant=[c, c],
            )
        ),
        schema={"segment_cols": ["variant"]},
    )
    roster = pd.DataFrame(
        dict(
            user_id=["c1", "c2", "t1", "t2"],
            variant=pd.Categorical(
                [c, c, t, t], categories=[t, c, "unused"], ordered=ordered
            ),
            assigned_at="2026-01-01",
        )
    )
    kwargs = dict(
        assignments=roster,
        variant_col="variant",
        control=c,
        treatment=t,
        observation=OBS,
        metrics=[
            dict(
                name="converted",
                metric="has_event",
                metric_args={"event": "buy"},
                agg="mean",
            )
        ],
        binary_intervals={"method": "newcombe"},
    )
    categorical = stream.experiment_data(**kwargs)
    roster["variant"] = roster.variant.astype(object)
    ordinary = stream.experiment_data(**kwargs)
    assert categorical["summary"] == ordinary["summary"]
    assert categorical["diagnostics"] == ordinary["diagnostics"]
    assert categorical["summary"][0]["control"] == 0.5
    assert categorical["summary"][0]["treatment"] == 0
    roster.loc[0, "variant"] = t
    with pytest.raises(ValueError, match="conflicts"):
        stream.experiment_data(**kwargs)


def test_numeric_and_string_variants_remain_distinct():
    stream = Eventstream(
        pd.DataFrame(
            dict(
                user_id=["c1"],
                event=["buy"],
                timestamp="2026-01-03",
                variant=pd.Categorical([1]),
            )
        ),
        schema={"segment_cols": ["variant"]},
    )
    roster = pd.DataFrame(
        dict(
            user_id=["c1", "t1"],
            variant=pd.Categorical(["1", "t"]),
            assigned_at="2026-01-01",
        )
    )
    with pytest.raises(ValueError, match="conflicts"):
        stream.experiment_data(
            assignments=roster,
            variant_col="variant",
            control="1",
            treatment="t",
            observation=OBS,
            metrics=[
                dict(
                    name="converted",
                    metric="has_event",
                    metric_args={"event": "buy"},
                    agg="mean",
                )
            ],
        )


@pytest.mark.parametrize("agg", ["ratio_of_sums", "mean_of_ratios"])
@pytest.mark.parametrize(
    "unit,effect_unit", [("%", "pp"), ("GBP / order", "GBP / order")]
)
def test_ratio_display_resolves_effect_units_without_changing_outcome_kind(
    agg, unit, effect_unit
):
    rows = [
        (f"{arm}{i}", event, "2026-01-03")
        for arm, successes in (("c", 2), ("t", 3))
        for i in range(10)
        for event in (["open", "buy"] if i < successes else ["open"])
    ]
    stream = Eventstream(pd.DataFrame(rows, columns=["user_id", "event", "timestamp"]))
    roster = pd.DataFrame(
        dict(
            user_id=[f"{g}{i}" for g in "ct" for i in range(10)],
            variant=["c"] * 10 + ["t"] * 10,
            assigned_at="2026-01-01",
        )
    )
    metric = dict(
        name="rate",
        agg=agg,
        numerator=dict(metric="event_count", metric_args={"event": "buy"}),
        denominator=dict(metric="event_count", metric_args={"event": "open"}),
        display=dict(unit=unit, scale=100),
    )
    if agg == "mean_of_ratios":
        metric["eligibility"] = "denominator_positive"
    kwargs = dict(
        assignments=roster,
        variant_col="variant",
        control="c",
        treatment="t",
        observation=OBS,
        metrics=[metric],
    )
    result = stream.experiment_data(**kwargs)
    row = result["summary"][0]
    assert row["control"] == 0.2 and row["treatment"] == 0.3
    assert row["absolute_effect"] == pytest.approx(0.1)
    assert row["relative_effect"] == pytest.approx(0.5)
    assert row["effect_unit"] == effect_unit
    assert row["outcome_kind"] == "ratio"
    assert json.loads(stream.experiment(**kwargs).result) == result
    metric["display"]["effect_unit"] = "custom difference unit"
    assert (
        stream.experiment_data(**kwargs)["summary"][0]["effect_unit"]
        == "custom difference unit"
    )
    metric["display"]["effect_unit"] = ""
    with pytest.raises(ValueError, match="effect_unit"):
        stream.experiment_data(**kwargs)
