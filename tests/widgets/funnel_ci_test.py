"""FunnelWidget with intervals: strict JSON, totals, persisted level, errors."""

import json

import numpy as np
import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import DiffConfigError
from retentioneering.widgets.funnel import FunnelWidget

DIFF = ("grp", "t", "c")


def _strict(text: str) -> dict:
    return json.loads(
        text, parse_constant=lambda c: pytest.fail(f"bare {c} in widget JSON")
    )


def _stream(seed: int = 0) -> Eventstream:
    rng = np.random.default_rng(seed)
    rows = []
    for g, p in (("t", 0.6), ("c", 0.5)):
        for i in range(60):
            t = pd.Timestamp("2026-01-01")
            rows.append([f"{g}{i}", "home", t, g])
            if rng.random() < p:
                rows.append([f"{g}{i}", "cart", t + pd.Timedelta("1min"), g])
    df = pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "grp"])
    return Eventstream(df, {"segment_cols": ["grp"]})


def _overlapping_stream() -> Eventstream:
    df = pd.DataFrame(
        [
            ["a", "home", "2026-01-01 00:00", "mobile"],
            ["a", "cart", "2026-01-02 00:00", "desktop"],
            ["b", "home", "2026-01-01 00:00", "desktop"],
            ["c", "home", "2026-01-01 00:00", "mobile"],
        ],
        columns=["user_id", "event", "timestamp", "platform"],
    )
    return Eventstream(df, {"segment_cols": ["platform"]})


class TestFunnelWidgetCi:
    def test__result_is_strict_json_with_null_for_nan(self) -> None:
        w = FunnelWidget(
            _stream(), steps=["home", "cart"], diff=DIFF, ci_level=0.9
        )
        assert w.error == ""
        result = _strict(w.result)
        assert result["ci_level"] == 0.9
        # everyone reaches `home`: the z-test is undefined -> NaN -> null
        assert result["steps"][0]["delta_conversion_rate_p_value"] is None
        assert result["group1_total"] == result["funnel1_total_paths"] == 60

    def test__level_and_correction_are_exported(self) -> None:
        w = FunnelWidget(
            _stream(),
            steps=["home", "cart"],
            diff=DIFF,
            ci_level=0.95,
            correction="holm",
        )
        data = w._export_data()
        assert data["ci_level"] == 0.95
        assert data["correction"] == "holm"
        assert data["result"]["correction"] == "holm"
        _strict(json.dumps(data, allow_nan=False))

    def test__traitlet_toggles_recompute(self) -> None:
        w = FunnelWidget(_stream(), steps=["home", "cart"], diff=DIFF)
        assert w.ci_level == 0.0 and w.correction == "none"
        assert "ci_level" not in _strict(w.result)
        w.ci_level = 0.99
        assert _strict(w.result)["ci_level"] == 0.99
        w.correction = "bh"
        assert _strict(w.result)["correction"] == "bh"
        w.ci_level = 0.0
        assert "ci_level" not in _strict(w.result)

    def test__compute_tool_takes_the_level(self) -> None:
        w = FunnelWidget(_stream(), steps=["home", "cart"])
        out = w.dispatch_compute("funnel_data", {"ci_level": 0.9})
        assert out["ci_level"] == 0.9
        assert "conversion_rate_ci_low" in out["steps"][1]

    def test__total_when_a_group_never_reaches_step_one(self) -> None:
        df = pd.DataFrame(
            [
                ["a", "home", "2026-01-01 00:00", "x"],
                ["a", "buy", "2026-01-01 00:01", "x"],
                ["b", "buy", "2026-01-01 00:00", "y"],
                ["c", "buy", "2026-01-01 00:00", "y"],
            ],
            columns=["user_id", "event", "timestamp", "grp"],
        )
        s = Eventstream(df, {"segment_cols": ["grp"]})
        for ci_level in (None, 0.95):
            w = FunnelWidget(
                s, steps=["home", "buy"], diff=("grp", "x", "y"), ci_level=ci_level
            )
            # the old back-calculation from step 1 reported 0 here
            assert _strict(w.result)["group2_total"] == 2

    def test__level_and_correction_are_saved_and_restored(self, tmp_path) -> None:
        path = tmp_path / "funnel.json"
        stream = _stream()
        first = FunnelWidget(stream, steps=["home", "cart"], state_file=str(path))
        first.ci_level = 0.9
        first.correction = "holm"
        saved = json.loads(path.read_text())["state"]
        assert (saved["ci_level"], saved["correction"]) == (0.9, "holm")
        second = FunnelWidget(stream, state_file=str(path))
        assert (second.ci_level, second.correction) == (0.9, "holm")
        assert _strict(second.result)["ci_level"] == 0.9
        # an explicit argument wins over the saved state
        third = FunnelWidget(stream, ci_level=0.99, state_file=str(path))
        assert third.ci_level == 0.99

    def test__overlap_raises_at_construction(self) -> None:
        with pytest.raises(DiffConfigError):
            FunnelWidget(
                _overlapping_stream(),
                steps=["home", "cart"],
                diff=("platform", "mobile", "desktop"),
                ci_level=0.95,
            )

    def test__overlap_from_the_sidebar_is_shown_not_raised(self) -> None:
        w = FunnelWidget(
            _overlapping_stream(),
            steps=["home", "cart"],
            diff=("platform", "mobile", "desktop"),
        )
        assert w.error == ""
        w.ci_level = 0.95
        assert "not constant" in w.error
        assert w.result == "{}"
        w.ci_level = 0.0
        assert w.error == ""
        assert "steps" in _strict(w.result)
