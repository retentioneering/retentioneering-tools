"""Path-level `diff` resolution (`tools/_diff.py`)."""

import numpy as np
import pandas as pd
import pytest

from retentioneering.datasets import load_ecom
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import (
    DiffConfigError,
    EmptyEventstreamError,
    PathIdNotFoundError,
    SegmentLevelNotFoundError,
)
from retentioneering.tools._diff import (
    DiffGroups,
    normalize_assignment,
    per_path_label,
    resolve_diff,
)


def _stream(rows, segment_cols=("arm",)) -> Eventstream:
    df = pd.DataFrame(rows, columns=["user_id", "event", "timestamp", *segment_cols])
    return Eventstream(df, {"segment_cols": list(segment_cols)})


@pytest.fixture
def two_users() -> Eventstream:
    # u1 switches arm between its two events; u2 is constant.
    return _stream(
        [
            ("u1", "A", "2024-01-01 00:00", "t"),
            ("u1", "B", "2024-01-01 00:01", "c"),
            ("u2", "A", "2024-01-01 00:00", "c"),
            ("u2", "B", "2024-01-01 00:01", "c"),
        ]
    )


@pytest.fixture
def constant() -> Eventstream:
    rows = []
    for i in range(6):
        arm = "t" if i % 2 else "c"
        rows += [
            (f"u{i}", "A", "2024-01-01 00:00", arm),
            (f"u{i}", "B", "2024-01-01 00:01", arm),
        ]
    rows.append(("u9", "A", "2024-01-01 00:00", None))
    return _stream(rows)


class TestSegmentForm:
    def test_non_constant_path_raises(self, two_users) -> None:
        with pytest.raises(DiffConfigError, match="not constant within 1 .*1 of them"):
            resolve_diff(two_users, ("arm", "t", "c"))

    def test_legacy_mode_keeps_path_in_both(self, two_users) -> None:
        groups = resolve_diff(two_users, ("arm", "t", "c"), require_constant=False)
        assert list(groups.ids1) == ["u1"]
        assert list(groups.ids2) == ["u1", "u2"]
        assert groups.excluded_paths == 1
        assert groups.basis == "segment"

    def test_legacy_split_two_is_unchanged(self, two_users) -> None:
        # The existing widgets keep their event-level behaviour (owner question Q6).
        s1, s2 = two_users._split_two(("arm", "t", "c"))
        assert s1.df["user_id"].tolist() == ["u1"]
        assert sorted(s2.df["user_id"].unique()) == ["u1", "u2"]
        assert len(s2.df) == 3

    def test_constant_segment(self, constant) -> None:
        groups = resolve_diff(constant, ("arm", "t", "c"))
        assert isinstance(groups, DiffGroups)
        assert list(groups.ids1) == ["u1", "u3", "u5"]
        assert list(groups.ids2) == ["u0", "u2", "u4"]
        assert (groups.n1(), groups.n2(), groups.excluded_paths) == (3, 3, 0)
        assert (groups.label1, groups.label2) == ("t", "c")

    def test_rest_and_missing(self, constant) -> None:
        groups = resolve_diff(constant, ("arm", "t", "<REST>"))
        assert list(groups.ids2) == ["u0", "u2", "u4", "u9"]
        assert groups.label2 == "<REST>"
        groups = resolve_diff(constant, ("arm", "<MISSING>", "<REST>"))
        assert list(groups.ids1) == ["u9"]
        assert groups.n2() == 6

    def test_unknown_level(self, constant) -> None:
        with pytest.raises(SegmentLevelNotFoundError):
            resolve_diff(constant, ("arm", "t", "x"))

    def test_not_a_segment(self, constant) -> None:
        with pytest.raises(DiffConfigError, match="not a segment column"):
            resolve_diff(constant, ("event", "A", "B"))

    def test_malformed(self, constant) -> None:
        for diff in [None, "arm", ("arm",), ("a", "b", "c", "d")]:
            with pytest.raises(DiffConfigError):
                resolve_diff(constant, diff)

    def test_json_string_levels_are_coerced(self) -> None:
        stream = _stream(
            [
                ("u1", "A", "2024-01-01", True),
                ("u2", "A", "2024-01-01", False),
            ],
            segment_cols=("flag",),
        )
        groups = resolve_diff(stream, ("flag", "true", "false"))
        assert list(groups.ids1) == ["u1"] and list(groups.ids2) == ["u2"]

    def test_empty_group(self) -> None:
        stream = _stream([("u1", "A", "2024-01-01", "t")])
        stream._df["arm"] = stream._df["arm"].cat.add_categories(["c"])
        with pytest.raises(EmptyEventstreamError):
            resolve_diff(stream, ("arm", "t", "c"))


class TestIdForm:
    def test_overlap_raises(self, constant) -> None:
        with pytest.raises(DiffConfigError, match="1 path id"):
            resolve_diff(constant, (["u0", "u1"], ["u1", "u2"]))

    def test_overlap_legacy(self, constant) -> None:
        groups = resolve_diff(
            constant, (["u1", "u0", "u0"], ["u1", "u2"]), require_constant=False
        )
        assert list(groups.ids1) == ["u0", "u1"]
        assert groups.excluded_paths == 1
        assert (groups.label1, groups.basis) == ("group1", "ids")

    def test_unknown_id(self, constant) -> None:
        with pytest.raises(PathIdNotFoundError):
            resolve_diff(constant, (["u0", "zz"], ["u1"]))

    def test_empty_list(self, constant) -> None:
        with pytest.raises(EmptyEventstreamError):
            resolve_diff(constant, ([], ["u1"]))


class TestAssignment:
    def test_silent_paths_are_included(self, constant) -> None:
        table = pd.Series(
            {
                "u0": "c",
                "u1": "t",
                "u2": "c",
                "u3": "t",
                "u4": "c",
                "u5": "t",
                "s1": "t",
            }
        )
        groups = resolve_diff(constant, ("arm", "t", "c"), assignment=table)
        assert list(groups.ids1) == ["s1", "u1", "u3", "u5"]
        assert groups.n2() == 3
        assert groups.basis == "assignment"
        # u9 has events but no row in the table: outside the ITT population
        assert groups.excluded_paths == 1

    def test_dataframe_and_segment_not_on_stream(self, constant) -> None:
        table = pd.DataFrame(
            {"arm": ["a", "b"], "assigned_at": ["2024-01-01", "2024-01-02"]},
            index=["u0", "u1"],
        )
        groups = resolve_diff(constant, ("variant", "a", "b"), assignment=table)
        assert (list(groups.ids1), list(groups.ids2)) == (["u0"], ["u1"])

    def test_disagreement_raises(self, constant) -> None:
        # u0 carries "c" on its events but the table says "t"
        table = pd.Series({"u0": "t", "u1": "t", "u2": "c"})
        with pytest.raises(DiffConfigError, match="disagrees .* 1 path"):
            resolve_diff(constant, ("arm", "t", "c"), assignment=table)

    def test_non_unique_index(self, constant) -> None:
        table = pd.Series(["t", "c"], index=["u1", "u1"])
        with pytest.raises(DiffConfigError, match="unique"):
            resolve_diff(constant, ("arm", "t", "c"), assignment=table)

    def test_unknown_arm(self, constant) -> None:
        with pytest.raises(SegmentLevelNotFoundError):
            resolve_diff(constant, ("arm", "t", "x"), assignment=pd.Series({"u1": "t"}))

    def test_normalize(self) -> None:
        with pytest.raises(DiffConfigError, match="'arm' column"):
            normalize_assignment(pd.DataFrame({"group": ["t"]}))
        table = normalize_assignment(pd.Series({"u1": None, "u2": "t"}))
        assert table["arm"].tolist() == ["<MISSING>", "t"]


class TestRealData:
    def test_ecom_platform_overlap(self) -> None:
        # platform changes between sessions for some users: 134 of them used both
        # mobile and desktop, so a user-level comparison cannot attribute them.
        stream = load_ecom()
        groups = resolve_diff(
            stream, ("platform", "mobile", "desktop"), require_constant=False
        )
        assert len(set(groups.ids1) & set(groups.ids2)) == 134
        assert stream.df["user_id"].nunique() == 600
        with pytest.raises(DiffConfigError, match="not constant"):
            resolve_diff(stream, ("platform", "mobile", "desktop"))
        # one platform per session: the session grain resolves cleanly
        groups = resolve_diff(
            stream, ("platform", "mobile", "desktop"), path_col="session_id"
        )
        assert groups.excluded_paths == 0
        assert groups.n1() + groups.n2() > 0

    def test_lumeo_shaped_constant_segment(self) -> None:
        rng = np.random.default_rng(7)
        users = [f"u{i}" for i in range(400)]
        arms = rng.choice(["treatment", "control"], size=400)
        rows = [
            (u, e, pd.Timestamp("2024-01-01") + pd.Timedelta(minutes=j), a)
            for u, a in zip(users, arms)
            for j, e in enumerate(["visit", "view", "order"][: rng.integers(1, 4)])
        ]
        stream = _stream(rows, segment_cols=("exp_group",))
        groups = resolve_diff(stream, ("exp_group", "treatment", "control"))
        assert groups.excluded_paths == 0
        assert groups.n1() == int((arms == "treatment").sum())
        assert groups.n1() + groups.n2() == 400


def test_per_path_label(two_users, constant) -> None:
    labels = per_path_label(two_users.df, "arm", "user_id")
    assert labels.to_dict() == {"u1": "<MULTI>", "u2": "c"}
    labels = per_path_label(constant.df, "arm", "user_id")
    assert labels["u9"] == "<MISSING>" and labels["u0"] == "c"
