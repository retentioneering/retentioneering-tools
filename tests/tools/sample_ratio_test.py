"""`get_sample_ratio`: sample ratio mismatch check."""

import numpy as np
import pandas as pd
import pytest
from scipy import stats

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import DiffConfigError, InvalidParameterError


def _stream(arms: dict, extra=None, platform=None) -> Eventstream:
    rows = []
    for i, (user, arm) in enumerate(arms.items()):
        day = pd.Timestamp("2024-01-01") + pd.Timedelta(days=i % 3)
        for j, event in enumerate(["visit", "view"]):
            row = {
                "user_id": user,
                "event": event,
                "timestamp": day + pd.Timedelta(minutes=j),
                "arm": arm,
            }
            if platform is not None:
                row["platform"] = platform[user]
            rows.append(row)
    for row in extra or []:
        rows.append(row)
    df = pd.DataFrame(rows)
    segs = ["arm"] + (["platform"] if platform is not None else [])
    return Eventstream(df, {"segment_cols": segs})


def _arms(n_t: int, n_c: int) -> dict:
    users = {f"t{i}": "treatment" for i in range(n_t)}
    users.update({f"c{i}": "control" for i in range(n_c)})
    return users


def test_equal_split() -> None:
    result = _stream(_arms(510, 490)).get_sample_ratio("arm")
    ref = stats.chisquare([490, 510])
    assert result["chi2"] == pytest.approx(ref.statistic)
    assert result["p_value"] == pytest.approx(ref.pvalue)
    assert result["dof"] == 1
    assert result["mismatch"] is False
    levels = result["levels"]
    assert list(levels.columns) == [
        "level",
        "paths",
        "share",
        "expected_share",
        "expected_paths",
    ]
    assert levels.set_index("level")["paths"].to_dict() == {
        "control": 490,
        "treatment": 510,
    }
    assert levels["expected_paths"].tolist() == [500.0, 500.0]
    assert result["multi_valued_paths"] == 0 and result["silent_paths"] == 0
    assert "paths in stream" in result["basis"]


def test_mismatch_detected() -> None:
    result = _stream(_arms(600, 400)).get_sample_ratio("arm")
    assert result["mismatch"] is True and result["p_value"] < 0.001


def test_custom_shares() -> None:
    stream = _stream(_arms(200, 800))
    result = stream.get_sample_ratio(
        "arm", expected_shares={"control": 0.8, "treatment": 0.2}
    )
    assert result["chi2"] == pytest.approx(0.0)
    assert result["mismatch"] is False
    assert stream.get_sample_ratio("arm")["mismatch"] is True


def test_shares_validated() -> None:
    stream = _stream(_arms(5, 5))
    with pytest.raises(InvalidParameterError):
        stream.get_sample_ratio(
            "arm", expected_shares={"control": 0.6, "treatment": 0.6}
        )
    with pytest.raises(InvalidParameterError):
        stream.get_sample_ratio("arm", expected_shares={"control": 0.5, "nope": 0.5})
    with pytest.raises(InvalidParameterError):
        stream.get_sample_ratio("arm", alpha=0)
    with pytest.raises(InvalidParameterError):
        stream.get_sample_ratio("event")


def test_multi_valued_paths_raise_the_alarm() -> None:
    extra = [
        {
            "user_id": "t0",
            "event": "x",
            "timestamp": pd.Timestamp("2024-02-01"),
            "arm": "control",
        }
    ]
    result = _stream(_arms(500, 500), extra=extra).get_sample_ratio("arm")
    assert result["multi_valued_paths"] == 1
    assert result["mismatch"] is True
    # the multi-valued path is not counted in any arm
    assert result["levels"]["paths"].sum() == 999


def test_assignment_with_silent_paths() -> None:
    stream = _stream(_arms(480, 500))
    table = pd.DataFrame(
        {"arm": ["treatment"] * 500 + ["control"] * 500},
        index=[f"t{i}" for i in range(500)] + [f"c{i}" for i in range(500)],
    )
    result = stream.get_sample_ratio("arm", assignment=table)
    assert result["silent_paths"] == 20
    assert result["levels"].set_index("level")["paths"].to_dict() == {
        "control": 500,
        "treatment": 500,
    }
    assert result["mismatch"] is False
    assert "assignment table" in result["basis"]
    # the stream alone (20 silent treatment users missing) is a different count
    assert stream.get_sample_ratio("arm")["levels"]["paths"].sum() == 980


def test_assignment_disagreement_counts_as_multi_valued() -> None:
    stream = _stream(_arms(3, 3))
    table = pd.Series(
        {"t0": "control", "t1": "treatment", "t2": "treatment", "c0": "control"}
    )
    result = stream.get_sample_ratio("arm", assignment=table)
    assert result["multi_valued_paths"] == 1 and result["mismatch"] is True


def test_assignment_validation() -> None:
    stream = _stream(_arms(3, 3))
    with pytest.raises(DiffConfigError):
        stream.get_sample_ratio(
            "arm", assignment=pd.Series(["t", "c"], index=["t0", "t0"])
        )


def test_filtered_stream_with_inherited_shares() -> None:
    # Shares measured on the full stream are the expectation for a filtered one:
    # a filter that drops paths of one arm more than the other shows up as SRM.
    full = _stream(_arms(300, 700))
    shares = (
        full.get_sample_ratio("arm")["levels"].set_index("level")["share"].to_dict()
    )
    assert full.get_sample_ratio("arm", expected_shares=shares)["mismatch"] is False
    dropped = [f"c{i}" for i in range(200)]
    filtered = full.filter_events(drop={"user_id": dropped})
    result = filtered.get_sample_ratio("arm", expected_shares=shares)
    assert result["levels"].set_index("level")["paths"].to_dict() == {
        "treatment": 300,
        "control": 500,
    }
    assert result["mismatch"] is True


def test_unassigned_paths() -> None:
    arms = _arms(10, 10)
    arms["x0"] = "holdout"
    result = _stream(arms).get_sample_ratio(
        "arm", expected_shares={"control": 0.5, "treatment": 0.5}
    )
    assert result["unassigned_paths"] == 1
    assert result["levels"]["paths"].sum() == 20


def test_by_segment_and_day() -> None:
    arms = _arms(300, 300)
    platform = {u: ("ios" if i % 2 else "web") for i, u in enumerate(arms)}
    stream = _stream(arms, platform=platform)
    result = stream.get_sample_ratio("arm", by="platform")
    levels = result["levels"]
    assert list(levels.columns) == [
        "by",
        "level",
        "paths",
        "share",
        "expected_share",
        "expected_paths",
    ]
    assert levels[levels.by == "<ALL>"]["paths"].sum() == 600
    by_levels = result["by_levels"]
    assert by_levels["by"].tolist() == ["ios", "web"]
    assert by_levels["paths"].tolist() == [300, 300]
    assert list(by_levels.columns) == [
        "by",
        "paths",
        "chi2",
        "dof",
        "p_value",
        "p_value_adjusted",
        "mismatch",
    ]
    assert result["chi2"] == pytest.approx(stream.get_sample_ratio("arm")["chi2"])

    by_day = stream.get_sample_ratio("arm", by="day")["by_levels"]
    assert by_day["by"].tolist() == ["2024-01-01", "2024-01-02", "2024-01-03"]
    assert by_day["paths"].sum() == 600


def test_by_day_uses_assigned_at_and_assignment_columns() -> None:
    stream = _stream(_arms(4, 4))
    users = [f"t{i}" for i in range(4)] + [f"c{i}" for i in range(4)]
    table = pd.DataFrame(
        {
            "arm": ["treatment"] * 4 + ["control"] * 4,
            "assigned_at": ["2024-03-01"] * 6 + ["2024-03-02"] * 2,
            "country": ["de", "fr"] * 4,
        },
        index=users,
    )
    by_day = stream.get_sample_ratio("arm", assignment=table, by="day")["by_levels"]
    assert by_day.set_index("by")["paths"].to_dict() == {
        "2024-03-01": 6,
        "2024-03-02": 2,
    }
    by_country = stream.get_sample_ratio("arm", assignment=table, by="country")[
        "by_levels"
    ]
    assert by_country["paths"].tolist() == [4, 4]


def test_by_rejects_non_constant_segment() -> None:
    arms = _arms(2, 2)
    stream = _stream(arms, platform={u: "web" for u in arms})
    df = stream.to_dataframe()
    df["platform"] = df["platform"].astype(object)
    df.loc[0, "platform"] = "ios"
    changed = Eventstream(df, {"segment_cols": ["arm", "platform"]})
    with pytest.raises(InvalidParameterError, match="changes within"):
        changed.get_sample_ratio("arm", by="platform")
    with pytest.raises(InvalidParameterError):
        changed.get_sample_ratio("arm", by="event")


def test_by_adjusts_over_groups() -> None:
    rng = np.random.default_rng(0)
    arms = {f"u{i}": rng.choice(["a", "b"]) for i in range(400)}
    result = _stream(arms).get_sample_ratio("arm", by="day")
    by = result["by_levels"]
    assert (by["p_value_adjusted"] >= by["p_value"]).all()


def test_unit_guard() -> None:
    # sessions inside users: a per-user table with session paths is refused
    rows = []
    for user, arm in _arms(20, 20).items():
        for s in range(2):
            rows.append(
                {
                    "user_id": user,
                    "session_id": f"{user}_s{s}",
                    "event": "visit",
                    "timestamp": pd.Timestamp("2024-01-01") + pd.Timedelta(hours=s),
                    "arm": arm,
                }
            )
    stream = Eventstream(
        pd.DataFrame(rows),
        {"path_cols": ["user_id", "session_id"], "segment_cols": ["arm"]},
    )
    table = pd.Series(_arms(20, 20))
    with pytest.raises(InvalidParameterError, match="too narrow"):
        stream.get_sample_ratio("arm", assignment=table, path_col="session_id")
    assert stream.get_sample_ratio("arm", assignment=table)["silent_paths"] == 0


def test_assignment_ids_matched_by_string_form() -> None:
    arms = {str(i): ("treatment" if i % 2 else "control") for i in range(100)}
    stream = _stream({int(k): v for k, v in arms.items()})
    result = stream.get_sample_ratio("arm", assignment=pd.Series(arms), by="day")
    assert result["silent_paths"] == 0
    assert result["levels"]["paths"].sum() == 200  # the <ALL> rows + the days
    assert result["by_levels"]["paths"].sum() == 100


def test_assignment_ids_that_match_nothing_raise() -> None:
    stream = _stream(_arms(50, 50))
    table = pd.Series({k.upper(): v for k, v in _arms(50, 50).items()})
    with pytest.raises(DiffConfigError, match="only 0 of 100"):
        stream.get_sample_ratio("arm", assignment=table)


def test_assignment_ids_mostly_unmatched_warn() -> None:
    # 40 of 100 table ids are paths: silent majority, or another grain -> warn
    with pytest.warns(UserWarning, match="only 40 of 100"):
        _stream(_arms(20, 20)).get_sample_ratio(
            "arm", assignment=pd.Series(_arms(50, 50))
        )
    result = _stream(_arms(30, 30)).get_sample_ratio(
        "arm", assignment=pd.Series(_arms(50, 50))
    )
    assert result["silent_paths"] == 40


def test_unit_guard_with_integer_ids() -> None:
    # integer user and session ids share one range: the counts tie
    rows = []
    for user in range(1, 41):
        for s in range(3):
            rows.append(
                {
                    "user_id": user,
                    "session_id": (user - 1) * 3 + s + 1,
                    "event": "visit",
                    "timestamp": pd.Timestamp("2024-01-01") + pd.Timedelta(hours=s),
                    "arm": "treatment" if user % 2 else "control",
                }
            )
    stream = Eventstream(
        pd.DataFrame(rows),
        {"path_cols": ["user_id", "session_id"], "segment_cols": ["arm"]},
    )
    users = pd.Series({u: ("treatment" if u % 2 else "control") for u in range(1, 41)})
    with pytest.raises(InvalidParameterError, match="too narrow"):
        stream.get_sample_ratio("arm", assignment=users, path_col="session_id")
    # the reverse mistake: a per-session table read at the user grain
    sessions = pd.Series(
        {
            s: ("treatment" if ((s - 1) // 3 + 1) % 2 else "control")
            for s in range(1, 121)
        }
    )
    with pytest.warns(UserWarning, match="only 40 of 120"):
        stream.get_sample_ratio("arm", assignment=sessions)
