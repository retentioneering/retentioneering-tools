import pandas as pd
import pytest

from retentioneering.datasets import load_ecom
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError
from retentioneering.paths import anchors


def _reference_match(tokens, pattern, occurrence="first"):
    """Independent pure-Python oracle for the matching semantics.

    Deliberately naive (scan and backtrack over a token list) so that it shares
    no code with the SQL builder under test — its only job is to say where each
    literal token of `pattern` lands in `tokens`, or None if it doesn't match.
    """
    parts = anchors.split_parts(pattern)

    def match_forward(start):
        positions, cursor = [], start
        for part in parts:
            for i in range(cursor, len(tokens) - len(part) + 1):
                if tokens[i : i + len(part)] == part:
                    positions.append(i)
                    cursor = i + len(part)
                    break
            else:
                return None
        return positions

    def match_backward(end):
        positions, cursor = [], end
        for part in reversed(parts):
            for i in range(cursor - len(part), -1, -1):
                if tokens[i : i + len(part)] == part:
                    positions.append(i)
                    cursor = i
                    break
            else:
                return None
        return list(reversed(positions))

    part_starts = (
        match_forward(0) if occurrence == "first" else match_backward(len(tokens))
    )
    if part_starts is None:
        return None
    out = []
    for part, start in zip(parts, part_starts):
        out.extend(range(start, start + len(part)))
    return out


def _tokens_by_path(stream, path_col):
    df = stream.df.sort_values([path_col, stream.schema.index, stream.schema.subindex])
    return {
        pid: group[stream.schema.event_col].tolist()
        for pid, group in df.groupby(path_col, observed=True)
    }


def _assert_matches_reference(stream, pattern, occurrence="first"):
    path_col = stream.schema.path_col
    normalized = anchors.normalize_pattern(pattern, warn=False)
    match = anchors.resolve_anchors(
        stream.df, stream.schema, normalized, occurrence=occurrence, path_col=path_col
    )

    got = {
        pid: group.sort_values("ordinal")["step"].tolist()
        for pid, group in match.frame.groupby(path_col, observed=True)
    }
    expected = {}
    for pid, tokens in _tokens_by_path(stream, path_col).items():
        positions = _reference_match(tokens, normalized, occurrence)
        if positions is not None:
            # `step` is 1-based row_number over the frame; the oracle is 0-based.
            expected[pid] = [p + 1 for p in positions]

    assert got == expected, f"pattern={pattern!r} occurrence={occurrence!r}"
    return match


ECOM_PATTERNS = [
    "cart",
    "purchase",
    "cart->.*->purchase",
    "catalog->product_view",
    "catalog->product_view->.*->purchase",
    "search->.*->catalog->product_view",
    "catalog->.*->cart->.*->purchase",
    "path_start->home",
    "purchase->path_end",
    "path_start->.*->purchase",
    "cart->.*->path_end",
]


@pytest.fixture(scope="module")
def ecom_stream():
    return load_ecom().add_start_end_events()


class TestResolveAnchorsSemantics:
    """resolve_anchors must agree with an independent Python implementation of
    the documented matching semantics, on real data, for both occurrence modes."""

    @pytest.mark.parametrize("pattern", ECOM_PATTERNS)
    def test__first_occurrence_matches_reference(self, ecom_stream, pattern):
        _assert_matches_reference(ecom_stream, pattern, occurrence="first")

    @pytest.mark.parametrize("pattern", ECOM_PATTERNS)
    def test__last_occurrence_matches_reference(self, ecom_stream, pattern):
        _assert_matches_reference(ecom_stream, pattern, occurrence="last")

    def test__first_and_last_differ_on_repeated_pattern(self, ecom_stream):
        """A pattern that repeats within a path must anchor at different places
        under the two occurrence modes — otherwise the parameter is untested."""
        first = anchors.resolve_anchors(
            ecom_stream.df, ecom_stream.schema, "cart", occurrence="first"
        )
        last = anchors.resolve_anchors(
            ecom_stream.df, ecom_stream.schema, "cart", occurrence="last"
        )
        path_col = ecom_stream.schema.path_col
        merged = first.frame.merge(
            last.frame, on=[path_col, "ordinal"], suffixes=("_f", "_l")
        )

        assert set(first.paths()) == set(last.paths())
        assert (merged["step_f"] <= merged["step_l"]).all()
        assert (merged["step_f"] < merged["step_l"]).any()


class TestResolveAnchorsSmallFixtures:
    """Hand-checked cases where the expected positions are obvious by eye."""

    @staticmethod
    def _stream(paths):
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
        return Eventstream(pd.DataFrame(rows))

    def test__gap_matches_zero_events(self):
        """'.*' stands for any run of events *including an empty one', so
        'A->.*->B' must match an adjacent A->B."""
        stream = self._stream({"u1": ["A", "B"], "u2": ["A", "X", "B"]})
        match = anchors.resolve_anchors(stream.df, stream.schema, "A->.*->B")
        assert set(match.paths()) == {"u1", "u2"}

    def test__adjacent_tokens_require_adjacency(self):
        """Tokens not separated by '.*' must be strictly adjacent."""
        stream = self._stream({"u1": ["A", "B"], "u2": ["A", "X", "B"]})
        match = anchors.resolve_anchors(stream.df, stream.schema, "A->B")
        assert set(match.paths()) == {"u1"}

    def test__first_occurrence_is_leftmost_non_greedy(self):
        """A B A B C: the first B completing 'A->.*->B' is the first B, so the
        repeated A/B fall inside a window anchored there rather than being
        skipped over by a greedier match."""
        stream = self._stream({"u1": ["A", "B", "A", "B", "C"]})
        match = anchors.resolve_anchors(stream.df, stream.schema, "A->.*->B")
        assert match.at(0)["step"].tolist() == [1]
        assert match.at(1)["step"].tolist() == [2]

    def test__last_occurrence_is_rightmost(self):
        stream = self._stream({"u1": ["A", "B", "A", "B", "C"]})
        match = anchors.resolve_anchors(
            stream.df, stream.schema, "A->.*->B", occurrence="last"
        )
        assert match.at(0)["step"].tolist() == [3]
        assert match.at(1)["step"].tolist() == [4]

    def test__non_matching_paths_are_absent(self):
        stream = self._stream({"u1": ["A", "B"], "u2": ["X", "Y"]})
        match = anchors.resolve_anchors(stream.df, stream.schema, "A->B")
        assert set(match.paths()) == {"u1"}

    def test__index_column_resolves_to_real_event_index(self):
        stream = self._stream({"u1": ["A", "X", "B"]})
        match = anchors.resolve_anchors(stream.df, stream.schema, "A->.*->B")
        index_col = stream.schema.index
        expected = stream.df.sort_values(index_col)[index_col].tolist()
        assert match.at(0)["index"].tolist() == [expected[0]]
        assert match.at(1)["index"].tolist() == [expected[2]]

    def test__boundary_sentinels_resolve_without_start_end_rows(self):
        """path_start/path_end work on a frame that carries no synthetic
        boundary rows: virtual ones are added for the query, carrying the
        path's first/last real index so the anchor is a usable bound."""
        stream = self._stream({"u1": ["A", "B", "C"]})
        match = anchors.resolve_anchors(stream.df, stream.schema, "path_start->.*->C")
        index_col = stream.schema.index
        indices = stream.df.sort_values(index_col)[index_col].tolist()

        assert match.at(0)["step"].tolist() == [0]
        assert match.at(0)["index"].tolist() == [indices[0]]
        assert match.at(1)["index"].tolist() == [indices[2]]


class TestPatternParsing:
    def test__literal_tokens_excludes_gaps(self):
        assert anchors.literal_tokens("A->.*->B->C") == ["A", "B", "C"]

    def test__split_parts_groups_adjacent_tokens(self):
        assert anchors.split_parts("A->B->.*->C") == [["A", "B"], ["C"]]

    def test__part_ordinals_point_at_each_part_start(self):
        stream = TestResolveAnchorsSmallFixtures._stream({"u1": ["A", "B", "X", "C"]})
        match = anchors.resolve_anchors(stream.df, stream.schema, "A->B->.*->C")
        assert match.part_ordinals == (0, 2)
        assert match.at_part(-1)["step"].tolist() == [4]

    @pytest.mark.parametrize(
        "raw,normalized",
        [
            (".*->A->B", "A->B"),
            ("A->B->.*", "A->B"),
            (".*->A->.*", "A"),
            ("A->.*->.*->B", "A->.*->B"),
        ],
    )
    def test__normalize_strips_redundant_gaps(self, raw, normalized):
        with pytest.warns(UserWarning, match="redundant"):
            assert anchors.normalize_pattern(raw) == normalized

    def test__normalize_leaves_bare_pattern_alone(self):
        assert anchors.normalize_pattern("A->.*->B") == "A->.*->B"

    @pytest.mark.parametrize("bad", ["", "   ", ".*", ".*->.*", "A->->B"])
    def test__normalize_rejects_degenerate_patterns(self, bad):
        with pytest.raises(InvalidParameterError):
            anchors.normalize_pattern(bad, warn=False)

    def test__validate_tokens_rejects_unknown_event(self):
        with pytest.raises(InvalidParameterError) as exc:
            anchors.validate_pattern_tokens("A->typo", {"A", "B"})
        assert "typo" in exc.value.message

    def test__validate_tokens_accepts_boundary_sentinels(self):
        anchors.validate_pattern_tokens("path_start->A->path_end", {"A"})

    def test__validate_tokens_reports_caller_parameter_name(self):
        with pytest.raises(InvalidParameterError) as exc:
            anchors.validate_pattern_tokens("typo", {"A"}, param="path_pattern")
        assert (
            "path_pattern" in str(exc.value.message)
            or exc.value.param == "path_pattern"
        )


class TestAnchorMatchAddressing:
    @pytest.fixture()
    def match(self):
        stream = TestResolveAnchorsSmallFixtures._stream({"u1": ["A", "X", "B"]})
        return anchors.resolve_anchors(stream.df, stream.schema, "A->.*->B")

    def test__at_accepts_negative_ordinal(self, match):
        assert match.at(-1).equals(match.at(1))

    def test__at_rejects_out_of_range(self, match):
        with pytest.raises(IndexError):
            match.at(2)

    def test__at_part_rejects_out_of_range(self, match):
        with pytest.raises(IndexError):
            match.at_part(5)

    def test__invalid_occurrence_rejected(self):
        stream = TestResolveAnchorsSmallFixtures._stream({"u1": ["A"]})
        with pytest.raises(InvalidParameterError):
            anchors.resolve_anchors(stream.df, stream.schema, "A", occurrence="third")


def _all_matches(events, pattern):
    """Every valid assignment of positions to the pattern's parts, brute-forced."""
    import itertools

    parts = anchors.split_parts(pattern)
    out = []
    for combo in itertools.product(*[range(len(events)) for _ in parts]):
        ok, prev = True, -1
        for pi, start in enumerate(combo):
            end = start + len(parts[pi]) - 1
            if (
                start <= prev
                or end >= len(events)
                or events[start : end + 1] != parts[pi]
            ):
                ok = False
                break
            prev = end
        if ok:
            out.append(combo)
    return out


class TestOccurrenceIsAComponentwiseExtremum:
    """`occurrence` is easiest to reason about by its result rather than by the
    matching procedure: "first" puts every token as EARLY as that token can be
    in any valid match of the pattern, "last" as LATE as it can be. Both are
    themselves valid matches."""

    PATTERNS = [
        "A->.*->B",
        "A->.*->B->.*->C",
        "A->B->.*->C",
        "A->.*->B->.*->A",
    ]
    PATHS = [
        list("ABCABC"),
        list("ABAB"),
        list("CBABC"),
        list("AABBCC"),
        list("ABCBA"),
        list("BCA"),
        list("AABCABAC"),
    ]

    @pytest.mark.parametrize("pattern", PATTERNS)
    @pytest.mark.parametrize("events", PATHS)
    @pytest.mark.parametrize("occurrence,pick", [("first", min), ("last", max)])
    def test__matches_the_componentwise_extremum(
        self, pattern, events, occurrence, pick
    ):
        stream = TestResolveAnchorsSmallFixtures._stream({"u1": events})
        parts = anchors.split_parts(pattern)
        matches = _all_matches(events, pattern)

        match = anchors.resolve_anchors(
            stream.df, stream.schema, pattern, occurrence=occurrence
        )
        got = match.frame.sort_values("ordinal")["step"].tolist()

        if not matches:
            assert got == []
            return
        extreme = [pick(m[pi] for m in matches) for pi in range(len(parts))]
        expected = []
        for pi, start in enumerate(extreme):
            expected += [start + 1 + offset for offset in range(len(parts[pi]))]
        assert got == expected

    def test__last_is_not_the_last_occurrence_of_the_anchor_event(self):
        """The trap: a later occurrence of the anchor event that participates in
        no valid match is not a candidate. Here the final cart has no purchase
        after it, so `occurrence="last"` anchors on the earlier one."""
        stream = TestResolveAnchorsSmallFixtures._stream(
            {"u1": ["catalog", "cart", "purchase", "cart"]}
        )
        match = anchors.resolve_anchors(
            stream.df,
            stream.schema,
            "catalog->.*->cart->.*->purchase",
            occurrence="last",
        )
        assert match.at(1)["step"].tolist() == [2]

    def test__both_directions_find_a_match_whenever_one_exists(self):
        stream = TestResolveAnchorsSmallFixtures._stream({"u1": list("BCABCA")})
        for occurrence in ("first", "last"):
            match = anchors.resolve_anchors(
                stream.df, stream.schema, "A->.*->B", occurrence=occurrence
            )
            assert not match.frame.empty
