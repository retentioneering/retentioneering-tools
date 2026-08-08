import pandas as pd
import pytest

from retentioneering.datasets import load_ecom
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError, PatternSyntaxError
from retentioneering.paths import anchors
from retentioneering.paths import tokens as tokens_mod


def _part_matches(events, start, part):
    """Whether `part`'s tokens fill `events` starting at `start`.

    The one place the oracle knows about predicates: a token is satisfied by
    equality if it is a name, and by the class's own membership rule otherwise.
    Everything above this function is unchanged from when tokens were names,
    which is the property being asserted — a class occupies one position and
    nothing else about the matching semantics moves.
    """
    if start < 0 or start + len(part) > len(events):
        return False
    for token, event in zip(part, events[start : start + len(part)]):
        parsed = tokens_mod.parse_token(token)
        ok = event == parsed if isinstance(parsed, str) else parsed.matches(event)
        if not ok:
            return False
    return True


def _restricted_gaps(pattern):
    _, gaps = anchors.split_pattern(pattern)
    return [
        g
        for g in gaps
        if g is not None and tokens_mod.parse_token(g).constraint is not None
    ]


def _gap_allows(gap, events, lo, hi):
    """Whether every event strictly between steps `lo` and `hi` fits `gap`."""
    if gap is None:
        return True
    parsed = tokens_mod.parse_token(gap)
    return all(parsed.allows(event) for event in events[lo + 1 : hi])


def _reference_match(tokens, pattern, occurrence="first"):
    """Independent pure-Python oracle for the matching semantics.

    Deliberately naive (scan and backtrack over a token list) so that it shares
    no code with the SQL builder under test — its only job is to say where each
    literal token of `pattern` lands in `tokens`, or None if it doesn't match.

    Valid only while every gap is unrestricted. Greedily pinning a part at its
    extreme is the least constraining choice for its neighbour *because* an
    unrestricted gap does not care how long it gets; a restricted one does, so
    this oracle would inherit exactly the bug it is meant to catch. Restricted
    gaps are checked by brute force instead (`_all_matches`).
    """
    assert not _restricted_gaps(
        pattern
    ), f"{pattern!r} has a restricted gap; this oracle cannot judge it"
    parts = anchors.split_parts(pattern)

    def match_forward(start):
        positions, cursor = [], start
        for part in parts:
            for i in range(cursor, len(tokens) - len(part) + 1):
                if _part_matches(tokens, i, part):
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
                if _part_matches(tokens, i, part):
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
    # Event classes occupy one position each, so every pattern above has a
    # predicate-carrying twin that must obey the identical semantics.
    "[cart|purchase]",
    "[^cart]->purchase",
    "cart->[^purchase]",
    "catalog->.",
    ".->purchase",
    "[search|catalog]->.*->purchase",
    "path_start->[^home]",
    "[^purchase]->path_end",
    "catalog->[^cart|purchase]->.*->purchase",
    "[search|catalog]->product_view->.*->[cart|purchase]",
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

    parts, gaps = anchors.split_pattern(pattern)
    out = []
    for combo in itertools.product(*[range(len(events)) for _ in parts]):
        ok, prev = True, -1
        for pi, start in enumerate(combo):
            end = start + len(parts[pi]) - 1
            if start <= prev or not _part_matches(events, start, parts[pi]):
                ok = False
                break
            if not _gap_allows(gaps[pi], events, prev, start):
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
        "[^A]->.*->C",
        "A->.*->[B|C]",
        "[A|B]->.*->[B|C]",
        "A->[^A]->.*->C",
        ".->.*->C",
        "A->[^B]*->C",
        "A->[^A]*->C",
        "A->[^A|B]*->C",
        "A->[B|C]*->C",
        "A->[^B]*->B->.*->C",
        "A->.*->B->[^A]*->C",
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


class TestSentinelsAreNotOrdinaryEvents:
    """A boundary sentinel fills a class only when the class names it. The trap
    this closes: `A->[^B]` on a path whose A is last would otherwise be true,
    because the synthetic path_end row is "not B"."""

    def test__negated_class_does_not_match_the_end_of_a_path(self):
        stream = TestResolveAnchorsSmallFixtures._stream({"u1": ["A", "B"]})
        match = anchors.resolve_anchors(stream.df, stream.schema, "B->[^Z]")
        assert set(match.paths()) == set()

    def test__wildcard_does_not_match_the_start_of_a_path(self):
        stream = TestResolveAnchorsSmallFixtures._stream({"u1": ["A", "B"]})
        match = anchors.resolve_anchors(stream.df, stream.schema, ".->A")
        assert set(match.paths()) == set()

    def test__named_sentinel_still_matches(self):
        stream = TestResolveAnchorsSmallFixtures._stream({"u1": ["A", "B"]})
        match = anchors.resolve_anchors(
            stream.df, stream.schema, f"B->[{anchors.PATH_END}|Z]"
        )
        assert set(match.paths()) == {"u1"}

    @pytest.mark.parametrize(
        "pattern", ["path_end->[^Z]", "path_end->.", "[^Z]->path_start"]
    )
    def test__nothing_lies_past_a_boundary(self, pattern):
        """Past the last row the LEAD window yields NULL, and `NULL NOT IN (...)`
        is NULL, not true — so a negated class cannot invent a position beyond
        the path's own end (nor before its start)."""
        stream = TestResolveAnchorsSmallFixtures._stream({"u1": ["A", "B", "C"]})
        match = anchors.resolve_anchors(stream.df, stream.schema, pattern)
        assert set(match.paths()) == set()

    def test__preceded_by_any_real_event_means_not_first(self):
        """The case this feature exists for: "a visit to a page that was not the
        first event of the path" — inexpressible before, and correct only
        because `.` refuses the path_start row."""
        stream = TestResolveAnchorsSmallFixtures._stream(
            {"first": ["P", "X"], "later": ["X", "P"], "both": ["P", "X", "P"]}
        )
        match = anchors.resolve_anchors(stream.df, stream.schema, ".->P")
        assert set(match.paths()) == {"later", "both"}


class TestEventClassesOnRealData:
    """Acceptance criteria, checked against answers computed without patterns."""

    def test__negated_first_event_agrees_with_a_direct_frame_computation(
        self, ecom_stream
    ):
        path_col = ecom_stream.schema.path_col
        event_col = ecom_stream.schema.event_col
        match = anchors.resolve_anchors(
            ecom_stream.df, ecom_stream.schema, f"{anchors.PATH_START}->[^home]"
        )

        df = ecom_stream.df
        real = df[~df[event_col].isin([anchors.PATH_START, anchors.PATH_END])]
        first = (
            real.sort_values(ecom_stream.schema.index)
            .groupby(path_col, observed=True)[event_col]
            .first()
        )
        expected = set(first[first != "home"].index)

        assert set(match.paths()) == expected
        assert expected and len(expected) < len(first)

    @pytest.mark.parametrize(
        "members", [("search", "catalog"), ("payment_error", "checkout_bug")]
    )
    def test__alternation_equals_renaming_the_members_together(self, members):
        """The workaround this replaces: merging events with `rename_events`
        just to ask one question. Same set of paths, without touching the data."""
        stream = load_ecom()
        merged = stream.rename_events({name: "MERGED" for name in members})
        pattern = f"[{'|'.join(members)}]->.*->purchase"

        via_class = anchors.resolve_anchors(
            stream.add_start_end_events().df,
            stream.schema,
            pattern,
        )
        via_rename = anchors.resolve_anchors(
            merged.add_start_end_events().df,
            merged.schema,
            "MERGED->.*->purchase",
        )
        assert set(via_class.paths()) == set(via_rename.paths())
        assert via_class.frame["step"].tolist() == via_rename.frame["step"].tolist()

    def test__class_occupies_exactly_one_ordinal(self, ecom_stream):
        pattern = "[search|catalog]->.*->product_view->[^cart]"
        match = anchors.resolve_anchors(ecom_stream.df, ecom_stream.schema, pattern)
        assert list(match.tokens) == [
            "[search|catalog]",
            "product_view",
            "[^cart]",
        ]
        assert match.part_ordinals == (0, 1)

    @pytest.mark.parametrize("occurrence", ["first", "last"])
    def test__occurrence_behaves_as_on_plain_patterns(self, ecom_stream, occurrence):
        """Not a new rule: "first" puts each token as early as it can be in any
        valid match, and a class token is a token like any other."""
        _assert_matches_reference(
            ecom_stream, "[search|catalog]->.*->purchase", occurrence=occurrence
        )

    def test__adjacent_class_still_requires_adjacency(self, ecom_stream):
        adjacent = anchors.resolve_anchors(
            ecom_stream.df, ecom_stream.schema, "cart->[^purchase]"
        )
        gapped = anchors.resolve_anchors(
            ecom_stream.df, ecom_stream.schema, "cart->.*->[^purchase]"
        )
        assert set(adjacent.paths()) < set(gapped.paths())


class TestEventClassValidation:
    """A typo inside a class is the dangerous case: it does not empty the
    result, it widens the position to always-true."""

    AVAILABLE = {"cart", "purchase", "catalog"}

    def test__unknown_member_of_a_negated_class_is_rejected(self):
        with pytest.raises(InvalidParameterError) as exc:
            anchors.validate_pattern_tokens("[^Purchse]->cart", self.AVAILABLE)
        assert "Purchse" in exc.value.message
        assert "purchase" in exc.value.message

    def test__unknown_member_of_a_positive_class_is_rejected(self):
        with pytest.raises(InvalidParameterError):
            anchors.validate_pattern_tokens("[cart|Purchse]", self.AVAILABLE)

    def test__sentinels_are_accepted_as_members(self):
        anchors.validate_pattern_tokens(
            f"[{anchors.PATH_START}|cart]->purchase", self.AVAILABLE
        )

    def test__known_members_pass(self):
        anchors.validate_pattern_tokens("[^cart|purchase]->catalog", self.AVAILABLE)

    def test__event_named_like_a_class_is_reported_not_reinterpreted(self):
        with pytest.raises(PatternSyntaxError, match="also"):
            anchors.validate_pattern_tokens("[cart]", self.AVAILABLE | {"[cart]"})

    def test__event_name_containing_the_member_separator_is_reported(self):
        with pytest.raises(PatternSyntaxError, match="alternatives"):
            anchors.validate_pattern_tokens(
                "[cart|purchase]", self.AVAILABLE | {"cart|purchase"}
            )

    def test__pattern_of_only_predicates_warns(self):
        with pytest.warns(UserWarning, match="names no events to look for"):
            anchors.validate_pattern_tokens("[^cart]->.*->.", self.AVAILABLE)

    def test__a_positive_class_is_an_anchor_and_does_not_warn(self, recwarn):
        anchors.validate_pattern_tokens("[cart|purchase]", self.AVAILABLE)
        assert not [w for w in recwarn if issubclass(w.category, UserWarning)]

    def test__one_literal_anchor_is_enough_to_silence_the_warning(self, recwarn):
        anchors.validate_pattern_tokens("[^cart]->.*->purchase", self.AVAILABLE)
        assert not [w for w in recwarn if issubclass(w.category, UserWarning)]

    def test__sentinel_counts_as_an_anchor(self, recwarn):
        anchors.validate_pattern_tokens(
            f"{anchors.PATH_START}->[^cart]", self.AVAILABLE
        )
        assert not [w for w in recwarn if issubclass(w.category, UserWarning)]

    @pytest.mark.parametrize("pattern", ["Q", "[cart|Q]"])
    def test__names_whose_absence_narrows_can_be_exempted(self, pattern):
        """`truncate_paths` takes a fallback list of anchors in which a name
        that resolves nowhere is legitimate — it simply does not constrain."""
        anchors.validate_pattern_tokens(pattern, self.AVAILABLE, check_literals=False)

    def test__names_whose_absence_widens_are_never_exempted(self):
        """A typo in `[^Q]` does not fail to match, it matches everything."""
        with pytest.raises(InvalidParameterError):
            anchors.validate_pattern_tokens(
                "[^Q]", self.AVAILABLE, check_literals=False
            )


class TestRejectedPatternSyntax:
    """The boundary of the feature: negation and alternation are supported
    exactly where their scope is one position."""

    @pytest.mark.parametrize(
        "pattern,message",
        [
            ("[^cart->purchase]->catalog", "not a sequence"),
            ("[^cart->.*->purchase]", "not a sequence"),
            ("cart->purchase|catalog->cart", "alternative sequences"),
            ("(cart|purchase)", "Round brackets"),
            ("^cart", "not a start-of-path anchor"),
            ("cart$", "not an end-of-path anchor"),
            ("[]", "Empty class"),
            ("[^[cart|purchase]]", "Nested brackets"),
            ("[^cart]*->purchase", "nothing on its outer side"),
            ("cart->[^purchase]*", "nothing on its outer side"),
            ("cart->.*->[^purchase]*->catalog", "Two gaps in a row"),
            ("cart->[^purchase]+->catalog", r"only '\*'"),
        ],
    )
    def test__rejected_with_an_explaining_message(self, pattern, message):
        with pytest.raises(PatternSyntaxError, match=message):
            normalized = anchors.normalize_pattern(pattern, warn=False)
            anchors.validate_pattern_tokens(normalized, {"cart", "purchase", "catalog"})

    def test__existing_patterns_are_untouched(self):
        assert anchors.normalize_pattern("cart->.*->purchase") == "cart->.*->purchase"


class TestRestrictedGaps:
    """`A->[^X]*->B` — "reached B from A without passing through X". The scope
    of the negation is the run between two anchors, which is what makes it
    expressible at all."""

    @staticmethod
    def _stream(paths):
        return TestResolveAnchorsSmallFixtures._stream(paths)

    def test__excludes_only_paths_with_the_event_in_between(self):
        stream = self._stream(
            {
                "clean": ["cart", "shipping", "purchase"],
                "support": ["cart", "support", "purchase"],
                "adjacent": ["cart", "purchase"],
                "after": ["cart", "purchase", "support"],
                "before": ["support", "cart", "purchase"],
            }
        )
        match = anchors.resolve_anchors(
            stream.df, stream.schema, "cart->[^support]*->purchase"
        )
        assert set(match.paths()) == {"clean", "adjacent", "after", "before"}

    def test__a_gap_of_zero_events_is_always_clean(self):
        stream = self._stream({"u1": ["A", "B"]})
        match = anchors.resolve_anchors(stream.df, stream.schema, "A->[^X]*->B")
        assert set(match.paths()) == {"u1"}

    def test__positive_gap_admits_only_the_listed_events(self):
        stream = self._stream(
            {
                "tidy": ["cart", "shipping", "payment", "purchase"],
                "wandered": ["cart", "shipping", "catalog", "purchase"],
            }
        )
        match = anchors.resolve_anchors(
            stream.df, stream.schema, "cart->[shipping|payment]*->purchase"
        )
        assert set(match.paths()) == {"tidy"}

    def test__greedy_chaining_would_miss_this_match(self):
        """The regression this rewrite exists for. Matching left to right and
        pinning A at its earliest picks step 1, whose run to D contains the X —
        and reports no match, although the second A reaches D cleanly. Both
        occurrence modes must land on it, since it is the only valid match."""
        stream = self._stream({"u1": ["A", "X", "A", "D"]})
        for occurrence in ("first", "last"):
            match = anchors.resolve_anchors(
                stream.df, stream.schema, "A->[^X]*->D", occurrence=occurrence
            )
            assert match.frame["step"].tolist() == [3, 4], occurrence

    def test__the_mirror_case_for_the_backward_pass(self):
        """Same trap on the other side: pinning D at its latest picks step 4,
        whose run back to A contains the X."""
        stream = self._stream({"u1": ["A", "D", "X", "D"]})
        for occurrence in ("first", "last"):
            match = anchors.resolve_anchors(
                stream.df, stream.schema, "A->[^X]*->D", occurrence=occurrence
            )
            assert match.frame["step"].tolist() == [1, 2], occurrence

    def test__an_unrestricted_gap_keeps_its_answer(self):
        """The same path under `.*` still spreads to the outer occurrences —
        proof that the two-pass matcher did not quietly narrow the plain case."""
        stream = self._stream({"u1": ["A", "X", "A", "D"]})
        first = anchors.resolve_anchors(stream.df, stream.schema, "A->.*->D")
        last = anchors.resolve_anchors(
            stream.df, stream.schema, "A->.*->D", occurrence="last"
        )
        assert first.frame["step"].tolist() == [1, 4]
        assert last.frame["step"].tolist() == [3, 4]

    def test__boundaries_bound_a_gap(self):
        stream = self._stream({"never": ["a", "b"], "did": ["a", "X", "b"]})
        match = anchors.resolve_anchors(
            stream.df,
            stream.schema,
            f"{anchors.PATH_START}->[^X]*->{anchors.PATH_END}",
        )
        assert set(match.paths()) == {"never"}

    def test__a_gap_takes_no_ordinal(self):
        stream = self._stream({"u1": ["A", "B", "C"]})
        match = anchors.resolve_anchors(stream.df, stream.schema, "A->[^X]*->C")
        assert list(match.tokens) == ["A", "C"]
        assert match.part_ordinals == (0, 1)

    def test__several_gaps_are_enforced_independently(self):
        stream = self._stream(
            {
                "ok": ["A", "p", "B", "q", "C"],
                "bad_first": ["A", "q", "B", "q", "C"],
                "bad_second": ["A", "p", "B", "p", "C"],
            }
        )
        match = anchors.resolve_anchors(
            stream.df, stream.schema, "A->[^q]*->B->[^p]*->C"
        )
        assert set(match.paths()) == {"ok"}


class TestRestrictedGapsOnRealData:
    def test__agrees_with_an_independent_scan(self, ecom_stream):
        """Checked against a plain Python walk over each path: remember the last
        `add_to_cart` and the last `support_chat` seen, and a purchase converts
        cleanly when the cart is the more recent of the two."""
        path_col = ecom_stream.schema.path_col
        pattern = "add_to_cart->[^support_chat]*->purchase"

        match = anchors.resolve_anchors(ecom_stream.df, ecom_stream.schema, pattern)

        expected = set()
        for pid, events in _tokens_by_path(ecom_stream, path_col).items():
            last_cart = last_support = None
            for i, event in enumerate(events):
                if event == "add_to_cart":
                    last_cart = i
                elif event == "support_chat":
                    last_support = i
                elif event == "purchase" and last_cart is not None:
                    if last_support is None or last_support < last_cart:
                        expected.add(pid)
                        break
        assert set(match.paths()) == expected
        assert expected  # the pattern must actually select something

    def test__is_a_strict_subset_of_the_unrestricted_pattern(self, ecom_stream):
        restricted = anchors.resolve_anchors(
            ecom_stream.df,
            ecom_stream.schema,
            "add_to_cart->[^support_chat]*->purchase",
        )
        plain = anchors.resolve_anchors(
            ecom_stream.df, ecom_stream.schema, "add_to_cart->.*->purchase"
        )
        assert set(restricted.paths()) < set(plain.paths())

    def test__typo_inside_a_gap_is_rejected(self, ecom_stream):
        events = ecom_stream.df[ecom_stream.schema.event_col].unique().tolist()
        with pytest.raises(InvalidParameterError, match="suport_chat"):
            anchors.validate_pattern_tokens(
                "add_to_cart->[^suport_chat]*->purchase", events
            )
