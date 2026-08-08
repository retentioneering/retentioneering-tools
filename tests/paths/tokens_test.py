"""The pattern-token parser, tested without any SQL behind it.

Split from `anchors_test.py` on purpose: what a token *means* is decided here,
and the matcher is only supposed to execute that decision. A bug that lives in
this file produces a wrong answer that no amount of matcher testing would
catch, because the matcher would be faithfully executing the wrong reading.
"""

import pytest

from retentioneering.exceptions import PatternSyntaxError
from retentioneering.paths import tokens as tokens_mod
from retentioneering.paths.tokens import (
    PATH_END,
    PATH_START,
    EventClass,
    Gap,
    gap_violation_sql,
    parse_token,
    token_sql,
)


class TestParsesTheFourShapes:
    def test__bare_name_stays_a_string(self):
        assert parse_token("cart") == "cart"

    def test__name_is_not_reinterpreted_when_it_contains_regex_characters(self):
        """Only a *whole* bracketed token is a class, so ordinary names that
        happen to carry regex punctuation keep working — the library never
        applied regex to them before and must not start now."""
        for name in ["page.view", "promo*", "search|filter", "checkout [beta]", "a+b"]:
            assert parse_token(name) == name

    def test__positive_class(self):
        token = parse_token("[cart|purchase]")
        assert token == EventClass(
            ("cart", "purchase"), negated=False, raw="[cart|purchase]"
        )

    def test__negated_class(self):
        token = parse_token("[^cart]")
        assert token == EventClass(("cart",), negated=True, raw="[^cart]")

    def test__single_member_class_is_allowed(self):
        assert parse_token("[cart]").members == ("cart",)

    def test__members_are_stripped_of_surrounding_space(self):
        assert parse_token("[cart | purchase]").members == ("cart", "purchase")

    def test__duplicate_members_collapse_keeping_order(self):
        assert parse_token("[b|a|b]").members == ("b", "a")

    def test__dot_is_the_complement_of_nothing(self):
        token = parse_token(".")
        assert token.negated and token.members == ()


class TestSentinelsParticipateOnlyWhenNamed:
    """`.` and `[^...]` must not match a path's own boundaries, mirroring regex,
    where `.` does not match a string boundary. Without this, `A->[^B]` would be
    true for a path whose A is last — the synthetic path_end row satisfying
    "not B" — which inverts the pattern's meaning."""

    @pytest.mark.parametrize("token", [".", "[^cart]", "[cart|purchase]"])
    @pytest.mark.parametrize("sentinel", [PATH_START, PATH_END])
    def test__sentinel_is_not_matched_by_a_class_that_does_not_name_it(
        self, token, sentinel
    ):
        assert not parse_token(token).matches(sentinel)

    def test__named_sentinel_is_matched(self):
        assert parse_token(f"[{PATH_START}|home]").matches(PATH_START)

    def test__negating_one_sentinel_still_excludes_the_other(self):
        token = parse_token(f"[^{PATH_START}]")
        assert not token.matches(PATH_START)
        assert not token.matches(PATH_END)
        assert token.matches("home")

    def test__negated_sentinel_class_is_equivalent_to_any(self):
        """Falls out of the rule rather than being special-cased, which is the
        point: `[^path_start]` excludes the sentinel it names and the other one
        for not being named, leaving exactly `.`."""
        negated = parse_token(f"[^{PATH_START}]")
        for event in ["home", "cart", PATH_START, PATH_END]:
            assert negated.matches(event) == parse_token(".").matches(event)


class TestMatchesSemantics:
    @pytest.mark.parametrize(
        "token,event,expected",
        [
            ("[a|b]", "a", True),
            ("[a|b]", "c", False),
            ("[^a]", "a", False),
            ("[^a]", "b", True),
            ("[^a|b]", "a", False),
            ("[^a|b]", "b", False),
            ("[^a|b]", "c", True),
            (".", "anything", True),
        ],
    )
    def test__membership(self, token, event, expected):
        assert parse_token(token).matches(event) is expected


class TestRejectedConstructs:
    """Negation and alternation are allowed exactly where their scope is one
    position. Everything wider is refused by the parser rather than
    half-supported."""

    def test__sequence_inside_a_class(self):
        """Detected on the token list, because by the time the pattern is split
        on '->' each half only looks like an unbalanced bracket."""
        with pytest.raises(PatternSyntaxError, match="not a sequence"):
            tokens_mod.check_class_spans(["[^cart", "purchase]"])

    def test__sequence_inside_a_class_names_the_whole_span(self):
        with pytest.raises(PatternSyntaxError, match=r"\[\^cart->purchase\]"):
            tokens_mod.check_class_spans(["[^cart", "purchase]"])

    def test__unclosed_class_with_no_closer_anywhere(self):
        with pytest.raises(PatternSyntaxError, match="Unbalanced"):
            tokens_mod.check_class_spans(["[^cart", "purchase"])

    @pytest.mark.parametrize("bad", ["[cart", "cart]", "[a]b"])
    def test__unbalanced_brackets(self, bad):
        with pytest.raises(PatternSyntaxError, match="Unbalanced"):
            parse_token(bad)

    @pytest.mark.parametrize("bad", ["[]", "[^]", "[  ]"])
    def test__empty_class(self, bad):
        with pytest.raises(PatternSyntaxError, match="Empty class"):
            parse_token(bad)

    @pytest.mark.parametrize("bad", ["[a|]", "[|a]", "[^|a]"])
    def test__empty_member(self, bad):
        with pytest.raises(PatternSyntaxError, match="Empty member"):
            parse_token(bad)

    def test__nested_brackets_suggest_the_flat_form(self):
        with pytest.raises(PatternSyntaxError) as exc:
            parse_token("[^[b|c]]")
        assert "'[^b|c]'" in exc.value.message

    @pytest.mark.parametrize("bad", ["[x]+", ".?", "[^x]+", "[a|b]?"])
    def test__only_star_is_a_quantifier(self, bad):
        with pytest.raises(PatternSyntaxError, match=r"only '\*'"):
            parse_token(bad)

    def test__quantifier_on_a_plain_name_is_just_a_name(self):
        """`promo*` is an event name, not a repetition — the quantifier only
        exists after a class or a wildcard."""
        assert parse_token("promo*") == "promo*"


class TestHintsForNamesThatDoNotExist:
    """Deliberately *not* parse errors: the hint is only reached once the name
    has been looked up and found missing, so an event legitimately called
    `total$` or `a|b` keeps working."""

    @pytest.mark.parametrize("name", ["total$", "a|b", "^weird", "(promo)"])
    def test__these_parse_as_plain_names(self, name):
        assert parse_token(name) == name

    def test__caret_points_at_path_start(self):
        hint = tokens_mod.describe_unknown_token("^cart")
        assert PATH_START in hint and "[^cart]" in hint

    def test__dollar_points_at_path_end(self):
        assert PATH_END in tokens_mod.describe_unknown_token("cart$")

    def test__round_brackets_point_at_square_ones(self):
        hint = tokens_mod.describe_unknown_token("(cart|purchase)")
        assert "[cart|purchase]" in hint

    def test__bare_alternation_explains_branching_is_not_supported(self):
        hint = tokens_mod.describe_unknown_token("purchase|catalog")
        assert "alternative sequences" in hint

    def test__ordinary_missing_name_gets_no_hint(self):
        assert tokens_mod.describe_unknown_token("Purchse") is None


class TestAnchorDetection:
    """What earns a pattern the "matches almost anything" warning: only
    negation and wildcards widen. A positive class narrows exactly as a name
    does, just to more than one event."""

    @pytest.mark.parametrize(
        "tokens,expected",
        [
            (["cart"], True),
            (["[cart|purchase]"], True),
            ([PATH_START, "[^home]"], True),
            (["[^cart]", ".*", "purchase"], True),
            (["[^cart]"], False),
            (["."], False),
            (["[^cart]", ".*", "."], False),
        ],
    )
    def test__anchor_detection(self, tokens, expected):
        assert tokens_mod.has_anchor(tokens) is expected


class TestTokenSql:
    """The SQL is checked for shape only — that it is the right *kind* of
    predicate. Whether it selects the right rows is settled in anchors_test.py
    against a Python oracle."""

    def test__literal_compiles_to_equality(self):
        assert token_sql("cart", "e") == "e = 'cart'"

    def test__positive_class_compiles_to_in(self):
        assert token_sql(parse_token("[a|b]"), "e") == "e IN ('a', 'b')"

    def test__negated_class_excludes_members_and_unnamed_sentinels(self):
        sql = token_sql(parse_token("[^a]"), "e")
        assert "e NOT IN ('a')" in sql
        assert PATH_START in sql and PATH_END in sql

    def test__any_excludes_only_the_sentinels(self):
        sql = token_sql(parse_token("."), "e")
        assert sql == f"(e NOT IN ('{PATH_START}', '{PATH_END}'))"

    def test__named_sentinel_is_not_excluded_twice(self):
        sql = token_sql(parse_token(f"[^{PATH_START}]"), "e")
        assert sql.count(PATH_START) == 1

    def test__quotes_in_event_names_are_escaped(self):
        assert token_sql(parse_token("[it's]"), "e") == "e IN ('it''s')"


class TestGaps:
    """A class quantified with `*` stops being a position and becomes a
    restricted gap: a run of any length, including none, in which only certain
    events may appear."""

    def test__plain_gap_is_unrestricted(self):
        gap = parse_token(".*")
        assert isinstance(gap, Gap) and gap.constraint is None

    def test__negated_gap_carries_its_class(self):
        assert parse_token("[^support]*").constraint == parse_token("[^support]")

    def test__positive_gap_carries_its_class(self):
        assert parse_token("[a|b]*").constraint.members == ("a", "b")

    @pytest.mark.parametrize(
        "gap,event,allowed",
        [
            (".*", "anything", True),
            ("[^x]*", "x", False),
            ("[^x]*", "y", True),
            ("[a|b]*", "a", True),
            ("[a|b]*", "c", False),
        ],
    )
    def test__allows(self, gap, event, allowed):
        assert parse_token(gap).allows(event) is allowed

    @pytest.mark.parametrize("sentinel", [PATH_START, PATH_END])
    @pytest.mark.parametrize("gap", [".*", "[^x]*", "[a|b]*"])
    def test__a_boundary_never_violates_a_gap(self, gap, sentinel):
        """A boundary cannot actually fall between two matched positions, but
        the reading stays the one used everywhere else: boundaries are not
        events, so they are not a gap's business."""
        assert parse_token(gap).allows(sentinel)

    def test__unrestricted_gap_has_no_violation_predicate(self):
        assert gap_violation_sql(parse_token(".*"), "e") is None

    def test__restricted_gap_violation_excludes_sentinels(self):
        sql = gap_violation_sql(parse_token("[^x]*"), "e")
        assert sql.startswith("(NOT (")
        assert f"e NOT IN ('{PATH_START}', '{PATH_END}')" in sql

    def test__gaps_are_not_anchors(self):
        assert tokens_mod.is_gap("[^x]*")
        assert not tokens_mod.has_anchor(["[^x]*"])
        assert not tokens_mod.has_anchor(["[a|b]*"])

    def test__a_class_without_a_star_is_still_a_position(self):
        assert not tokens_mod.is_gap("[^x]")
