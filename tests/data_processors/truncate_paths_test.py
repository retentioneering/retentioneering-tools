import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import PreprocessingConfigError


@pytest.fixture
def simple_eventstream():
    """Create a simple eventstream for testing."""
    data = pd.DataFrame(
        {
            "user_id": [1, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3],
            "event": [
                "A",
                "B",
                "C",
                "D",
                "E",
                "A",
                "B",
                "C",
                "D",
                "X",
                "B",
                "C",
                "Y",
                "Z",
            ],
            "timestamp": pd.to_datetime(
                [
                    "2023-01-01 10:00",
                    "2023-01-01 11:00",
                    "2023-01-01 12:00",
                    "2023-01-01 13:00",
                    "2023-01-01 14:00",
                    "2023-01-02 10:00",
                    "2023-01-02 11:00",
                    "2023-01-02 12:00",
                    "2023-01-02 13:00",
                    "2023-01-03 10:00",
                    "2023-01-03 11:00",
                    "2023-01-03 12:00",
                    "2023-01-03 13:00",
                    "2023-01-03 14:00",
                ]
            ),
        }
    )

    return Eventstream(data)


def test_truncate_paths_basic(simple_eventstream):
    """Test basic truncate_paths functionality."""
    result = simple_eventstream.truncate_paths(start_event="B", end_event="D")
    df = result.df

    # User 1: should have B, C, D
    user1_events = df[df["user_id"] == 1]["event"].tolist()
    assert user1_events == ["B", "C", "D"]

    # User 2: should have B, C, D
    user2_events = df[df["user_id"] == 2]["event"].tolist()
    assert user2_events == ["B", "C", "D"]

    # User 3: should have B, C (no D, so path should be filtered out)
    user3_events = df[df["user_id"] == 3]["event"].tolist()
    assert user3_events == []


def test_truncate_paths_same_boundary(simple_eventstream):
    """Test truncate_paths when left and right are the same event."""
    result = simple_eventstream.truncate_paths(start_event="C", end_event="C")
    df = result.df

    # Each path should have exactly one 'C' event
    assert len(df[df["user_id"] == 1]) == 1
    assert len(df[df["user_id"] == 2]) == 1
    assert len(df[df["user_id"] == 3]) == 1

    # All events should be 'C'
    assert all(df["event"] == "C")


def test_truncate_paths_no_left_boundary(simple_eventstream):
    """Test truncate_paths when left boundary doesn't exist in some paths."""
    result = simple_eventstream.truncate_paths(start_event="X", end_event="Z")
    df = result.df

    # Only user 3 has both X and Z
    assert len(df[df["user_id"] == 1]) == 0
    assert len(df[df["user_id"] == 2]) == 0
    assert len(df[df["user_id"] == 3]) == 5  # X, B, C, Y, Z


def test_truncate_paths_no_right_boundary(simple_eventstream):
    """Test truncate_paths when right boundary doesn't exist in some paths."""
    result = simple_eventstream.truncate_paths(start_event="A", end_event="Z")
    df = result.df

    # Only users without Z should be filtered out
    # User 1: has A but no Z - filtered
    # User 2: has A but no Z - filtered
    # User 3: no A but has Z - filtered (no A)
    assert len(df[df["user_id"] == 1]) == 0
    assert len(df[df["user_id"] == 2]) == 0
    assert len(df[df["user_id"] == 3]) == 0


def test_truncate_paths_reverse_order():
    """Test truncate_paths when right boundary appears before left boundary."""
    data = pd.DataFrame(
        {
            "user_id": [1, 1, 1, 1, 1],
            "event": ["D", "C", "B", "A", "E"],
            "timestamp": pd.to_datetime(
                [
                    "2023-01-01 10:00",
                    "2023-01-01 11:00",
                    "2023-01-01 12:00",
                    "2023-01-01 13:00",
                    "2023-01-01 14:00",
                ]
            ),
        }
    )

    stream = Eventstream(data)
    result = stream.truncate_paths(start_event="B", end_event="D")
    df = result.df

    # Should be empty because 'D' appears before 'B' in the path
    assert len(df) == 0


def test_truncate_paths_multiple_occurrences():
    """Test truncate_paths with multiple occurrences of boundary events."""
    data = pd.DataFrame(
        {
            "user_id": [1, 1, 1, 1, 1, 1, 1],
            "event": ["A", "B", "C", "B", "D", "B", "E"],
            "timestamp": pd.to_datetime(
                [
                    "2023-01-01 10:00",
                    "2023-01-01 11:00",
                    "2023-01-01 12:00",
                    "2023-01-01 13:00",
                    "2023-01-01 14:00",
                    "2023-01-01 15:00",
                    "2023-01-01 16:00",
                ]
            ),
        }
    )

    stream = Eventstream(data)
    result = stream.truncate_paths(start_event="B", end_event="D")
    df = result.df

    # Should keep from first B (index 2) to first D after it (index 5)
    # Events: B, C, B, D
    events = df["event"].tolist()
    assert events == ["B", "C", "B", "D"]


def test_truncate_paths_empty_params():
    """Test truncate_paths with invalid parameters."""
    data = pd.DataFrame(
        {
            "user_id": [1, 1, 1],
            "event": ["A", "B", "C"],
            "timestamp": pd.to_datetime(
                ["2023-01-01 10:00", "2023-01-01 11:00", "2023-01-01 12:00"]
            ),
        }
    )

    stream = Eventstream(data)

    # Test with empty start_event parameter
    with pytest.raises(
        PreprocessingConfigError,
        match="Parameter 'start_event' must be a non-empty string",
    ):
        stream.truncate_paths(start_event="", end_event="C")

    # Test with empty end_event parameter
    with pytest.raises(
        PreprocessingConfigError,
        match="Parameter 'end_event' must be a non-empty string",
    ):
        stream.truncate_paths(start_event="A", end_event="")


def test_truncate_paths_path_col_override_finer_grain_preserves_chronological_order():
    """path_cols must be coarsest-first (validated at Eventstream
    construction): user_id then session_id. Overriding to session_id (a
    valid, finer declared path_col) must truncate within each session, not
    merge boundaries across sessions of the same user."""
    data = pd.DataFrame(
        {
            "user_id": ["U1", "U1", "U1", "U1"],
            "session_id": ["S1", "S1", "S2", "S2"],
            "event": ["A", "B", "C", "D"],
            "timestamp": pd.to_datetime(
                [
                    "2024-01-01 10:00",
                    "2024-01-01 10:01",
                    "2024-01-01 10:02",
                    "2024-01-01 10:03",
                ]
            ),
        }
    )
    stream = Eventstream(data, {"path_cols": ["user_id", "session_id"]})

    # A and B both happen in session S1 -> truncates to that range.
    result = stream.truncate_paths(
        start_event="A", end_event="B", path_col="session_id"
    )
    assert result.df["event"].tolist() == ["A", "B"]

    # A is in S1, D is in S2 -> no single session has both boundaries.
    result_cross_session = stream.truncate_paths(
        start_event="A", end_event="D", path_col="session_id"
    )
    assert result_cross_session.df["event"].tolist() == []


def test_truncate_paths_path_col_override_rejects_undeclared_column():
    data = pd.DataFrame(
        {
            "user_id": ["U1", "U1"],
            "event": ["A", "B"],
            "timestamp": pd.to_datetime(["2024-01-01 10:00", "2024-01-01 10:01"]),
        }
    )
    stream = Eventstream(data, {"path_cols": ["user_id"]})
    with pytest.raises(PreprocessingConfigError):
        stream.truncate_paths(start_event="A", end_event="B", path_col="not_a_path_col")


def test_truncate_paths_path_start_sentinel(simple_eventstream):
    """`start_event="path_start"` keeps everything from the path's actual
    first event up to the first occurrence of `end_event`, without requiring
    any specific start anchor to be present."""
    result = simple_eventstream.truncate_paths(start_event="path_start", end_event="C")
    df = result.df

    assert df[df["user_id"] == 1]["event"].tolist() == ["A", "B", "C"]
    assert df[df["user_id"] == 2]["event"].tolist() == ["A", "B", "C"]
    assert df[df["user_id"] == 3]["event"].tolist() == ["X", "B", "C"]


def test_truncate_paths_path_end_sentinel(simple_eventstream):
    """`end_event="path_end"` keeps everything from the first occurrence of
    `start_event` to the path's actual last event, without requiring any
    specific end anchor to be present."""
    result = simple_eventstream.truncate_paths(start_event="C", end_event="path_end")
    df = result.df

    assert df[df["user_id"] == 1]["event"].tolist() == ["C", "D", "E"]
    assert df[df["user_id"] == 2]["event"].tolist() == ["C", "D"]
    assert df[df["user_id"] == 3]["event"].tolist() == ["C", "Y", "Z"]


def test_truncate_paths_path_start_and_path_end_sentinels(simple_eventstream):
    """Both sentinels together keep each path unchanged, start to end."""
    result = simple_eventstream.truncate_paths(
        start_event="path_start", end_event="path_end"
    )
    df = result.df

    assert df[df["user_id"] == 1]["event"].tolist() == ["A", "B", "C", "D", "E"]
    assert df[df["user_id"] == 2]["event"].tolist() == ["A", "B", "C", "D"]
    assert df[df["user_id"] == 3]["event"].tolist() == ["X", "B", "C", "Y", "Z"]


def test_truncate_paths_path_start_sentinel_end_is_first_event():
    """When the target end event is itself the path's first event, the
    window degenerates to that single row instead of being excluded."""
    data = pd.DataFrame(
        {
            "user_id": [1, 1, 1],
            "event": ["B", "C", "D"],
            "timestamp": pd.to_datetime(
                ["2023-01-01 10:00", "2023-01-01 11:00", "2023-01-01 12:00"]
            ),
        }
    )
    stream = Eventstream(data)
    result = stream.truncate_paths(start_event="path_start", end_event="B")
    assert result.df["event"].tolist() == ["B"]


def test_truncate_paths_path_end_sentinel_excludes_paths_missing_start(
    simple_eventstream,
):
    """`end_event="path_end"` still drops paths that never contain
    `start_event` — the sentinel only relaxes the missing-end case."""
    result = simple_eventstream.truncate_paths(start_event="Z", end_event="path_end")
    df = result.df

    assert df[df["user_id"] == 1]["event"].tolist() == []
    assert df[df["user_id"] == 2]["event"].tolist() == []
    assert df[df["user_id"] == 3]["event"].tolist() == ["Z"]


def test_truncate_paths_custom_columns():
    """Test truncate_paths with custom path_col and event_col."""
    data = pd.DataFrame(
        {
            "user_id": [1, 1, 1, 1, 1, 1],
            "session_id": [1, 1, 2, 2, 2, 2],
            "event": ["X", "Y", "Z", "X", "Y", "Z"],
            "custom_event": ["A", "B", "A", "B", "C", "A"],
            "timestamp": pd.to_datetime(
                [
                    "2023-01-01 10:00",
                    "2023-01-01 11:00",
                    "2023-01-01 12:00",
                    "2023-01-01 13:00",
                    "2023-01-01 14:00",
                    "2023-01-01 15:00",
                ]
            ),
        }
    )

    schema = {
        "path_cols": ["user_id", "session_id"],
        "event_cols": ["event", "custom_event"],
    }

    stream = Eventstream(data, schema)
    result = stream.truncate_paths(
        start_event="B", end_event="A", path_col="session_id", event_col="custom_event"
    )
    df = result.df

    events = df["custom_event"].tolist()
    assert events == ["B", "C", "A"]


# ── anchor specs, offsets and lists ──────────────────────────────────────────


def _stream(paths, start="2023-01-01 10:00", step="1h"):
    """Build an eventstream from {path_id: [events]}, one event per `step`."""
    rows = []
    for pid, events in paths.items():
        base = pd.Timestamp(start)
        for i, event in enumerate(events):
            rows.append(
                {
                    "user_id": pid,
                    "event": event,
                    "timestamp": base + i * pd.Timedelta(step),
                }
            )
    return Eventstream(pd.DataFrame(rows))


def _events(stream, pid):
    df = stream.df
    return df[df["user_id"] == pid]["event"].tolist()


class TestAnchorSpecs:
    def test__spec_dict_is_equivalent_to_bare_event_name(self, simple_eventstream):
        by_name = simple_eventstream.truncate_paths(start_event="B", end_event="D")
        by_spec = simple_eventstream.truncate_paths(
            start_event={"pattern": "B"}, end_event={"pattern": "D"}
        )
        pd.testing.assert_frame_equal(by_name.df, by_spec.df)

    def test__pattern_anchor_uses_the_event_completing_the_pattern(self):
        """The anchor is the B that completes 'A->.*->B', not just any B — the
        leading B here must be left outside the window."""
        stream = _stream({1: ["B", "A", "X", "B", "C", "D"]})
        result = stream.truncate_paths(
            start_event={"pattern": "A->.*->B"}, end_event="D"
        )
        assert _events(result, 1) == ["B", "C", "D"]

    def test__at_selects_which_token_anchors(self):
        stream = _stream({1: ["A", "X", "B", "C"]})
        at_start = stream.truncate_paths(
            start_event={"pattern": "A->.*->B", "at": "start"}, end_event="C"
        )
        at_end = stream.truncate_paths(
            start_event={"pattern": "A->.*->B", "at": "end"}, end_event="C"
        )
        assert _events(at_start, 1) == ["A", "X", "B", "C"]
        assert _events(at_end, 1) == ["B", "C"]

    def test__at_accepts_integer_index_over_event_names(self):
        """'.*' is not a position, so at=1 is the second *event name*."""
        stream = _stream({1: ["A", "B", "X", "C", "D"]})
        result = stream.truncate_paths(
            start_event={"pattern": "A->B->.*->C", "at": 1}, end_event="D"
        )
        assert _events(result, 1) == ["B", "X", "C", "D"]

    def test__occurrence_last_anchors_on_the_final_match(self):
        stream = _stream({1: ["A", "B", "A", "B", "C"]})
        first = stream.truncate_paths(start_event={"pattern": "A"}, end_event="C")
        last = stream.truncate_paths(
            start_event={"pattern": "A", "occurrence": "last"}, end_event="C"
        )
        assert _events(first, 1) == ["A", "B", "A", "B", "C"]
        assert _events(last, 1) == ["A", "B", "C"]

    def test__unknown_spec_key_is_rejected(self, simple_eventstream):
        with pytest.raises(PreprocessingConfigError):
            simple_eventstream.truncate_paths(
                start_event={"pattern": "B", "at_event": "end"}, end_event="D"
            )

    def test__spec_without_pattern_is_rejected(self, simple_eventstream):
        with pytest.raises(PreprocessingConfigError):
            simple_eventstream.truncate_paths(
                start_event={"occurrence": "last"}, end_event="D"
            )

    def test__invalid_occurrence_is_rejected(self, simple_eventstream):
        with pytest.raises(PreprocessingConfigError):
            simple_eventstream.truncate_paths(
                start_event={"pattern": "B", "occurrence": "third"}, end_event="D"
            )


class TestAnchorOffsets:
    def test__step_offset_moves_the_bound_forward(self):
        stream = _stream({1: ["A", "B", "C", "D", "E"]})
        result = stream.truncate_paths(
            start_event="A", end_event={"pattern": "A", "offset": 2}
        )
        assert _events(result, 1) == ["A", "B", "C"]

    def test__step_offset_clamps_at_the_path_end(self):
        """A window wider than what is left of the path is the rest of the path,
        not an empty window and not a dropped path."""
        stream = _stream({1: ["A", "B", "C"]})
        result = stream.truncate_paths(
            start_event="A", end_event={"pattern": "A", "offset": 99}
        )
        assert _events(result, 1) == ["A", "B", "C"]

    def test__negative_step_offset_moves_the_start_backwards(self):
        stream = _stream({1: ["A", "B", "C", "D"]})
        result = stream.truncate_paths(
            start_event={"pattern": "C", "offset": -2}, end_event="D"
        )
        assert _events(result, 1) == ["A", "B", "C", "D"]

    def test__time_offset_snaps_to_the_last_event_inside_the_window(self):
        stream = _stream({1: ["A", "B", "C", "D"]}, step="10m")
        result = stream.truncate_paths(
            start_event="A", end_event={"pattern": "A", "offset": "25m"}
        )
        assert _events(result, 1) == ["A", "B", "C"]

    def test__time_offset_includes_an_exact_timestamp_hit(self):
        stream = _stream({1: ["A", "B", "C", "D"]}, step="10m")
        result = stream.truncate_paths(
            start_event="A", end_event={"pattern": "A", "offset": "20m"}
        )
        assert _events(result, 1) == ["A", "B", "C"]

    def test__time_offset_requires_an_explicit_unit(self, simple_eventstream):
        with pytest.raises(ValueError, match="unit"):
            simple_eventstream.truncate_paths(
                start_event="A", end_event={"pattern": "A", "offset": "1800"}
            )

    def test__time_offset_accepts_timedelta(self):
        stream = _stream({1: ["A", "B", "C", "D"]}, step="10m")
        result = stream.truncate_paths(
            start_event="A",
            end_event={"pattern": "A", "offset": pd.Timedelta("25m")},
        )
        assert _events(result, 1) == ["A", "B", "C"]


class TestAnchorLists:
    def test__list_keeps_the_narrowest_window(self):
        """Both bounds resolve; the earlier end wins."""
        stream = _stream({1: ["A", "B", "C", "D", "E"]})
        result = stream.truncate_paths(
            start_event="A",
            end_event=["D", {"pattern": "A", "offset": 2}],
        )
        assert _events(result, 1) == ["A", "B", "C"]

    def test__sentinel_in_a_list_acts_as_a_fallback(self):
        """The converted path is cut at its purchase; the one that never
        purchased is kept whole instead of being dropped."""
        stream = _stream(
            {
                1: ["catalog", "cart", "purchase", "review"],
                2: ["catalog", "cart", "support"],
            }
        )
        result = stream.truncate_paths(
            start_event="path_start", end_event=["purchase", "path_end"]
        )
        assert _events(result, 1) == ["catalog", "cart", "purchase"]
        assert _events(result, 2) == ["catalog", "cart", "support"]

    def test__without_the_sentinel_the_unmatched_path_is_dropped(self):
        """The same call without the fallback keeps the old drop behaviour —
        this is the asymmetry the fallback exists to fix."""
        stream = _stream(
            {
                1: ["catalog", "cart", "purchase", "review"],
                2: ["catalog", "cart", "support"],
            }
        )
        result = stream.truncate_paths(start_event="path_start", end_event="purchase")
        assert _events(result, 1) == ["catalog", "cart", "purchase"]
        assert _events(result, 2) == []

    def test__list_bounds_symmetrise_two_groups(self):
        """The motivating case: cut converters at their purchase and everyone
        else at the same number of steps, so the two windows are comparable."""
        stream = _stream(
            {
                1: ["cart", "a", "purchase", "x", "y", "z"],
                2: ["cart", "a", "b", "c", "d", "e"],
            }
        )
        result = stream.truncate_paths(
            start_event="cart",
            end_event=["purchase", {"pattern": "cart", "offset": 3}],
        )
        assert _events(result, 1) == ["cart", "a", "purchase"]
        assert _events(result, 2) == ["cart", "a", "b", "c"]

    def test__start_list_keeps_the_latest_bound(self):
        stream = _stream({1: ["A", "B", "C", "D"]})
        result = stream.truncate_paths(start_event=["A", "C"], end_event="D")
        assert _events(result, 1) == ["C", "D"]

    def test__path_dropped_when_no_spec_on_a_side_resolves(self):
        stream = _stream({1: ["A", "B"], 2: ["X", "Y"]})
        result = stream.truncate_paths(start_event=["A", "Q"], end_event="B")
        assert _events(result, 1) == ["A", "B"]
        assert _events(result, 2) == []

    def test__empty_list_is_rejected(self, simple_eventstream):
        with pytest.raises(PreprocessingConfigError):
            simple_eventstream.truncate_paths(start_event=[], end_event="D")


class TestEndAnchorOrdering:
    """The end anchor may not land before the window opened — but its pattern is
    still matched against the whole path, not against what is left after the
    start. Getting this wrong makes a pattern that straddles the window's start
    re-match at a later occurrence whose lead-in happens to fall inside."""

    def test__pattern_straddling_the_start_resolves_to_the_same_occurrence(self):
        stream = _stream(
            {1: ["catalog", "cart", "x", "y", "catalog", "cart", "z", "w", "v"]}
        )
        result = stream.truncate_paths(
            start_event={"pattern": "catalog->.*->cart"},
            end_event=[{"pattern": "catalog->.*->cart", "offset": 3}],
        )
        # The window opens at the first cart; +3 events is up to the second
        # catalog. Matching the end pattern against the truncated remainder
        # would instead find the *second* catalog/cart pair and run to "v".
        assert _events(result, 1) == ["cart", "x", "y", "catalog"]

    def test__end_before_start_drops_the_path(self):
        stream = _stream({1: ["D", "C", "B", "A", "E"]})
        result = stream.truncate_paths(start_event="B", end_event="D")
        assert _events(result, 1) == []

    def test__end_takes_the_first_occurrence_after_the_start(self):
        stream = _stream({1: ["D", "B", "C", "D", "E"]})
        result = stream.truncate_paths(start_event="B", end_event="D")
        assert _events(result, 1) == ["B", "C", "D"]

    def test__windows_of_two_groups_are_bounded_by_the_same_budget(self):
        """The motivating case end to end: a converter is cut at the purchase,
        a non-converter at the same number of steps past the shared anchor, so
        neither window can be longer than the budget."""
        stream = _stream(
            {
                1: ["catalog", "cart", "a", "purchase", "x", "y", "z"],
                2: ["catalog", "cart", "a", "b", "c", "d", "e", "f", "g"],
            }
        )
        result = stream.truncate_paths(
            start_event={"pattern": "catalog->.*->cart"},
            end_event=["purchase", {"pattern": "catalog->.*->cart", "offset": 3}],
        )
        assert _events(result, 1) == ["cart", "a", "purchase"]
        assert _events(result, 2) == ["cart", "a", "b", "c"]
        assert len(_events(result, 1)) <= 4 and len(_events(result, 2)) <= 4


class TestDocumentedWorkedExample:
    """Pins the worked example in docs/templates/data-processors/truncate-paths.md.jinja.

    The doc walks through one path and shows what the window becomes when each
    of the spec's four keys is dropped in turn. If any of those windows move,
    the prose is wrong and this test says so.
    """

    #: The doc's path, numbered 1..16, timestamps 10 minutes apart.
    EVENTS = [
        "home",
        "catalog",
        "add_to_cart",
        "support_chat",
        "catalog",
        "add_to_cart",
        "shipping_details",
        "purchase",
        "catalog",
        "add_to_cart",
        "shipping_details",
        "purchase",
        "review_page",
        "catalog",
        "add_to_cart",
        "logout",
    ]
    PATTERN = "catalog->.*->add_to_cart->.*->purchase"

    @pytest.fixture()
    def stream(self):
        return _stream({"u1": self.EVENTS}, step="10m")

    def test__all_four_keys(self, stream):
        converting_cart = {"pattern": self.PATTERN, "at": 1, "occurrence": "last"}
        result = stream.truncate_paths(
            start_event={**converting_cart, "offset": -1},
            end_event={**converting_cart, "offset": "30m"},
        )
        # Events 9-13: the last cart that actually led to a purchase, one event
        # of lead-in, and the half hour that followed it.
        assert _events(result, "u1") == [
            "catalog",
            "add_to_cart",
            "shipping_details",
            "purchase",
            "review_page",
        ]

    def test__without_pattern_it_anchors_on_the_abandoned_cart(self, stream):
        anchor = {"pattern": "add_to_cart", "occurrence": "last"}
        result = stream.truncate_paths(
            start_event={**anchor, "offset": -1},
            end_event={**anchor, "offset": "30m"},
        )
        assert _events(result, "u1") == ["catalog", "add_to_cart", "logout"]

    def test__without_at_it_anchors_on_the_purchase(self, stream):
        anchor = {"pattern": self.PATTERN, "occurrence": "last"}
        result = stream.truncate_paths(
            start_event={**anchor, "offset": -1},
            end_event={**anchor, "offset": "30m"},
        )
        assert _events(result, "u1") == [
            "shipping_details",
            "purchase",
            "review_page",
            "catalog",
            "add_to_cart",
        ]

    def test__without_occurrence_the_leftmost_match_spans_a_different_sequence(
        self, stream
    ):
        """The leftmost match pairs catalog@2 with the cart@3 and the
        purchase@8 — a valid sequence, just not the intended one."""
        anchor = {"pattern": self.PATTERN, "at": 1}
        result = stream.truncate_paths(
            start_event={**anchor, "offset": -1},
            end_event={**anchor, "offset": "30m"},
        )
        assert _events(result, "u1") == [
            "catalog",
            "add_to_cart",
            "support_chat",
            "catalog",
            "add_to_cart",
        ]

    def test__without_offset_the_window_collapses_onto_the_anchor(self, stream):
        anchor = {"pattern": self.PATTERN, "at": 1, "occurrence": "last"}
        result = stream.truncate_paths(start_event=anchor, end_event=anchor)
        assert _events(result, "u1") == ["add_to_cart"]


class TestEventClassAnchors:
    """An anchor spec is a pattern, so a class works there too — and its member
    names must be checked even though a *bare* name that resolves nowhere is
    legitimate here (a list of anchors is a fallback chain)."""

    def test__class_anchor_opens_the_window_at_either_member(self):
        stream = _stream({1: ["A", "B", "C", "D"], 2: ["A", "X", "C", "D"]})
        result = stream.truncate_paths(start_event="[B|X]", end_event="D")
        assert _events(result, 1) == ["B", "C", "D"]
        assert _events(result, 2) == ["X", "C", "D"]

    def test__class_anchor_equals_renaming_the_members_together(self):
        paths = {1: ["A", "B", "C", "D"], 2: ["A", "X", "C", "D"]}
        by_class = _stream(paths).truncate_paths(start_event="[B|X]", end_event="D")
        by_rename = (
            _stream(paths)
            .rename_events({"B": "M", "X": "M"})
            .truncate_paths(start_event="M", end_event="D")
        )
        assert len(by_class.df) == len(by_rename.df)

    def test__negated_anchor_opens_at_the_first_event_that_is_not_named(self):
        stream = _stream({1: ["A", "A", "B", "C"]})
        # A pattern of nothing but negation matches almost anything, which is
        # legal but worth saying out loud.
        with pytest.warns(UserWarning, match="names no events to look for"):
            result = stream.truncate_paths(start_event="[^A]", end_event="C")
        assert _events(result, 1) == ["B", "C"]

    def test__typo_inside_a_class_is_rejected(self):
        stream = _stream({1: ["A", "B", "C"]})
        with pytest.raises(PreprocessingConfigError, match="Q"):
            stream.truncate_paths(start_event="[^Q]", end_event="C")

    def test__bare_name_that_resolves_nowhere_is_still_allowed(self):
        """The fallback-list contract: `["A", "Q"]` must not become an error
        just because Q is absent — it simply does not constrain."""
        stream = _stream({1: ["A", "B", "C"]})
        result = stream.truncate_paths(start_event=["A", "Q"], end_event="C")
        assert _events(result, 1) == ["A", "B", "C"]

    def test__unsupported_syntax_is_reported_as_a_config_error(self):
        stream = _stream({1: ["A", "B", "C"]})
        with pytest.raises(PreprocessingConfigError, match="not a sequence"):
            stream.truncate_paths(start_event="[^A->B]", end_event="C")

    def test__class_anchor_works_inside_a_spec_dict(self):
        stream = _stream({1: ["A", "B", "C", "D", "E"]})
        result = stream.truncate_paths(
            start_event={"pattern": "[B|X]->.*->D", "at": 0}, end_event="E"
        )
        assert _events(result, 1) == ["B", "C", "D", "E"]


class TestRestrictedGapAnchors:
    def test__window_opens_on_a_clean_run(self):
        stream = _stream(
            {
                1: ["A", "p", "B", "C"],
                2: ["A", "X", "B", "C"],
            }
        )
        result = stream.truncate_paths(start_event="A->[^X]*->B", end_event="C")
        assert _events(result, 1) == ["B", "C"]
        assert _events(result, 2) == []

    def test__at_addresses_the_parts_around_the_gap(self):
        stream = _stream({1: ["z", "A", "p", "B", "C"], 2: ["X"]})
        result = stream.truncate_paths(
            start_event={"pattern": "A->[^X]*->B", "at": 0}, end_event="C"
        )
        assert _events(result, 1) == ["A", "p", "B", "C"]

    def test__typo_inside_a_gap_is_rejected(self):
        stream = _stream({1: ["A", "B", "C"]})
        with pytest.raises(PreprocessingConfigError, match="Q"):
            stream.truncate_paths(start_event="A->[^Q]*->B", end_event="C")

    def test__unbounded_gap_is_rejected(self):
        stream = _stream({1: ["A", "B", "C"]})
        with pytest.raises(PreprocessingConfigError, match="nothing on its outer side"):
            stream.truncate_paths(start_event="[^A]*->B", end_event="C")
