"""`occurrence="non_overlapping"`: each match in turn, as `re.finditer` finds them.

The matcher hands the work to Python's own regex engine, so these tests do
not re-derive regex semantics. They pin what the translation adds on top:
which repetitions the captures land on, lazy gaps, sentinels, and the
contract shared with the other occurrence modes — every match is a valid
match, and matches neither overlap nor go backwards.
"""

import itertools
import random

import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import (
    InvalidParameterError,
    PreprocessingConfigError,
)
from retentioneering.paths import anchors
from retentioneering.paths import tokens as tokens_mod


def _stream(paths, boundary_rows=False):
    rows = []
    ts = pd.Timestamp("2024-01-01")
    for pid, events in paths.items():
        for i, event in enumerate(events):
            rows.append(
                {
                    "user_id": pid,
                    "event": event,
                    "timestamp": ts + pd.Timedelta(minutes=i),
                }
            )
    stream = Eventstream(pd.DataFrame(rows))
    return stream.add_start_end_events() if boundary_rows else stream


def _paths(stream):
    df = stream.df.sort_values(["user_id", stream.schema.index, stream.schema.subindex])
    real = df[~df["event"].isin(tokens_mod.SENTINELS)]
    return {pid: "->".join(g["event"]) for pid, g in real.groupby("user_id")}


def _starts(stream, pattern, at=0):
    match = anchors.resolve_anchors(
        stream.df,
        stream.schema,
        anchors.normalize_pattern(pattern, warn=False),
        occurrence="non_overlapping",
        source_pattern=pattern,
    )
    return match.at(at)["step"].tolist()


LOOPS = "B B B C B B D B B B B B E".split()


class TestLoops:
    """The motivating case: one marker per run of at least k."""

    def test__marks_the_start_of_every_long_enough_run(self):
        stream = _stream({"u": LOOPS})
        marked = stream.add_events(
            "B_loop",
            anchor={
                "pattern": "(B){3,}",
                "at": "start",
                "occurrence": "non_overlapping",
            },
        )
        assert _paths(marked)["u"] == (
            "B_loop->B->B->B->C->B->B->D->B_loop->B->B->B->B->B->E"
        )

    def test__end_is_the_last_repetition_of_the_run(self):
        stream = _stream({"u": LOOPS})
        assert _starts(stream, "(B){3,}", at=-1) == [3, 12]

    def test__after_the_run_is_an_offset_from_its_end(self):
        stream = _stream({"u": LOOPS})
        marked = stream.add_events(
            "B_loop_end",
            anchor={
                "pattern": "(B){3,}",
                "at": "end",
                "offset": 1,
                "occurrence": "non_overlapping",
            },
        )
        assert _paths(marked)["u"] == (
            "B->B->B->B_loop_end->C->B->B->D->B->B->B->B->B->B_loop_end->E"
        )

    def test__a_closed_count_is_not_greedy(self):
        """`{3}` is exactly three: a run of six holds two matches."""
        stream = _stream({"u": list("BBBBBB")})
        assert _starts(stream, "(B){3}") == [1, 4]
        assert _starts(stream, "(B){3,}") == [1]

    def test__other_modes_disagree_on_purpose(self):
        stream = _stream({"u": list("BBBBB")})
        normalized = anchors.normalize_pattern("(B){3,}", warn=False)
        everywhere = anchors.resolve_anchors(
            stream.df, stream.schema, normalized, occurrence="all"
        )
        assert sorted(everywhere.at(0)["step"]) == [1, 2, 3]
        assert _starts(stream, "(B){3,}") == [1]

    def test__a_single_copy_is_the_first_repetition(self):
        """With `+` the group has one copy, so start and end coincide."""
        stream = _stream({"u": list("ABBBC")})
        assert _starts(stream, "(B)+", at=0) == [2]
        assert _starts(stream, "(B)+", at=-1) == [2]

    def test__a_run_at_the_path_start(self):
        stream = _stream({"u": list("BBA")})
        assert _starts(stream, "path_start->(B){2,}", at=1) == [1]

    def test__with_real_boundary_rows(self):
        stream = _stream({"u": list("BBABBB")}, boundary_rows=True)
        assert _starts(stream, "path_start->(B){2,}", at=1) == [2]
        assert _starts(stream, "(B){2,}") == [2, 5]

    def test__a_multi_event_group_and_a_class(self):
        stream = _stream({"u": list("ABABABCABAB")})
        assert _starts(stream, "(A->B){2,}") == [1, 8]
        stream = _stream({"u": list("ABCABBAC")})
        assert _starts(stream, "([A|B]){2,}") == [1, 4]


class TestGaps:
    def test__a_gap_is_lazy(self):
        """Each A pairs with the next B, not with the last one."""
        stream = _stream({"u": list("AxBABAAB")})
        assert _starts(stream, "A->.*->B") == [1, 4, 6]
        assert _starts(stream, "A->.*->B", at=-1) == [3, 5, 8]

    def test__a_bounded_gap(self):
        stream = _stream({"u": list("AxxBAB")})
        assert _starts(stream, "A->.{0,1}->B") == [5]
        assert _starts(stream, "A->.{2,}->B") == [1]

    def test__a_restricted_gap(self):
        stream = _stream({"u": list("AXBAyB")})
        assert _starts(stream, "A->[^X]*->B") == [4]


def _brute_matches(events, pattern):
    """Every valid placement of the unrolled pattern, by brute force."""
    normalized = anchors.normalize_pattern(pattern, warn=False)
    parts, gaps = anchors.split_pattern(normalized)
    found = []
    for combo in itertools.product(range(len(events)), repeat=len(parts)):
        prev, ok = -1, True
        for start, part, gap in zip(combo, parts, gaps):
            if start <= prev or start + len(part) > len(events):
                ok = False
                break
            for token, event in zip(part, events[start : start + len(part)]):
                parsed = tokens_mod.parse_token(token)
                hit = (
                    event == parsed
                    if isinstance(parsed, str)
                    else parsed.matches(event)
                )
                if not hit:
                    ok = False
            if gap is not None:
                g = tokens_mod.parse_token(gap)
                run = events[prev + 1 : start]
                if not (g.fits(len(run)) and all(g.allows(e) for e in run)):
                    ok = False
            if not ok:
                break
            prev = start + len(part) - 1
        if ok:
            out = []
            for start, part in zip(combo, parts):
                out.extend(range(start, start + len(part)))
            found.append(tuple(out))
    return found


class TestContract:
    """Whatever the pattern: every match is valid, and they move strictly forward."""

    PATTERNS = [
        "A->B",
        "A->.*->B",
        "A->.{1,2}->B",
        "A->[^C]*->B",
        "[A|B]->C",
        "(A){2}",
        "A->.*->(B->.*->C->.*){2}",
    ]

    @pytest.mark.parametrize("seed", range(20))
    @pytest.mark.parametrize("pattern", PATTERNS)
    def test__matches_are_valid_and_ordered(self, pattern, seed):
        rng = random.Random(seed)
        events = [rng.choice("ABC") for _ in range(rng.randint(1, 9))]
        stream = _stream({"u": events})
        match = anchors.resolve_anchors(
            stream.df,
            stream.schema,
            anchors.normalize_pattern(pattern, warn=False),
            occurrence="non_overlapping",
            source_pattern=pattern,
        )
        width = len(match.tokens)
        steps = match.frame.sort_values(["ordinal", "step"])["step"].tolist()
        n = len(steps) // width if width else 0
        by_ordinal = [steps[i * n : (i + 1) * n] for i in range(width)]
        placements = [tuple(col[k] - 1 for col in by_ordinal) for k in range(n)]

        valid = set(_brute_matches(events, pattern))
        assert all(p in valid for p in placements)
        for earlier, later in zip(placements, placements[1:]):
            assert max(earlier) < min(later)
        # finditer finds a match whenever one exists
        assert bool(placements) == bool(valid)


class TestWhereItIsRefused:
    def test__a_window_bound(self):
        stream = _stream({"u": list("ABBB")})
        with pytest.raises(PreprocessingConfigError, match="non_overlapping"):
            stream.truncate_paths(
                start_anchor={"pattern": "(B){2,}", "occurrence": "non_overlapping"},
                end_anchor="path_end",
            )

    def test__a_matrix_centre(self):
        stream = _stream({"u": list("ABBB")})
        with pytest.raises(InvalidParameterError):
            stream.step_matrix_data(
                max_steps=3,
                anchor={"pattern": "(B){2,}", "occurrence": "non_overlapping"},
            )

    def test__a_floor(self):
        stream = _stream({"u": list("ABBB")})
        floor = pd.DataFrame({"user_id": ["u"], "bound_step": [1]})
        with pytest.raises(InvalidParameterError):
            anchors.resolve_anchors(
                stream.df,
                stream.schema,
                "B->B",
                occurrence="non_overlapping",
                not_before=floor,
            )

    def test__conversion_counts_paths_not_matches(self):
        stream = _stream({"u": list("BBXBBY"), "v": list("XY")})
        result = stream.get_conversion_rate(
            start_anchor={"pattern": "(B){2,}", "occurrence": "non_overlapping"},
            end_anchor="Y",
        )
        assert int(result["paths_with_start"].iloc[0]) == 1
