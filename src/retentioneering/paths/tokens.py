"""
What one position in a path pattern can say.

Until now a pattern token was one event name, compared for equality, and the
only non-name token was the gap ``.*``. This module generalises a token to an
*event class*: a set of event names, optionally negated, occupying exactly one
position — and, quantified with ``*``, a *restricted gap* saying what may lie
between two anchors.

The syntax deliberately borrows Python's regular expressions with one
substitution — a token is an event, not a character:

===============  ===========================================================
``A``            that event, as before
``[A|B|C]``      one event, any of the listed ones
``[^A]``         one event, anything but ``A``
``[^A|B]``       one event, anything but the listed ones
``.``            one event, any of them
``.*``           any run of events, including none — as before
``[^A]*``        a run containing no ``A``
``[A|B]*``       a run containing nothing but ``A`` and ``B``
``.{m,n}``       a run of ``m`` to ``n`` events; also ``{n}``, ``{m,}``,
                 ``{,n}``, ``+`` and ``?``, on ``.`` or any class
``(A->B){k}``    the sequence ``A->B``, ``k`` times back to back — unrolled
                 by :func:`expand_groups` before anything else sees it
===============  ===========================================================

The last two are why the negation goes *inside* the brackets rather than
outside: ``[^A]*`` composes a class with a quantifier exactly as regex does,
while ``^[A]*`` would leave it unreadable whether the repetition is inside the
negation or outside it.

Two deviations from regex, both forced by the substitution:

* members are separated by ``|``. A regex character class is juxtaposition
  (``[ab]``) because a character is one symbol wide; event names are not, so a
  separator is unavoidable.
* ``^`` and ``$`` are not anchors. This language already names a path's
  boundaries — ``path_start`` / ``path_end`` are real events that participate
  in patterns as ordinary tokens — and a second spelling for a concept the
  library wires through everywhere would violate the one-concept-one-name rule
  (ADR-0008). Patterns using them get an error pointing at the sentinels.

Everything else regex offers is deliberately absent. The rule that draws the
line: **negation and alternation are allowed exactly where their scope is
bounded.** One position is bounded by definition; a gap is bounded by the
anchors on either side of it, which is why ``A->[^X]*->B`` is expressible while
``[^A->B]`` is not — complementing an unanchored sequence matches almost every
path and needs an automaton, while the engine is relational. For the same
reason a restricted gap at either end of a pattern is refused: with nothing
pinning its outer side it is satisfied by the empty run, making it a no-op.
Pattern-level branching (``A->B|C->D``) is refused too, since it breaks the
correspondence between tokens and the ordinals that ``at=`` / ``occurrence=`` /
step-matrix centring address.

Sentinels and wildcards
-----------------------

A boundary sentinel participates in a class **only if it is named**. So ``.``
and ``[^A]`` never match ``path_start`` / ``path_end``, mirroring regex, where
``.`` does not match a string boundary because a boundary is not a character.
Without this, ``A->[^B]`` would be true for a path whose ``A`` is last — the
synthetic ``path_end`` row satisfying "not B" — which is the opposite of what
anyone writing that pattern means.

The rule falls out of the definition rather than being special-cased, so
``[^path_start]`` is legal and equivalent to ``.``: it excludes the sentinel it
names, and the other sentinel is excluded for not being named.

Ambiguity
---------

A token is a class only if it is *entirely* bracketed, so an event named
``[checkout] submit`` is unaffected; only an event named exactly
``[checkout]`` collides. Rather than introduce escaping for a case this rare,
:func:`validate_class_collisions` detects it against the real event vocabulary
and fails loudly — a silently reinterpreted token would produce a wrong answer
that looks like a right one.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Union

from retentioneering.eventstream.event_type import EventTypes
from retentioneering.exceptions import PatternSyntaxError
from retentioneering.utils.sequences import PATH_DELIMITER
from retentioneering.utils.sql_quoting import quote_literal

__all__ = [
    "ANY",
    "GAP",
    "PATH_END",
    "PATH_START",
    "SENTINELS",
    "EventClass",
    "Gap",
    "Token",
    "check_class_spans",
    "describe_unknown_token",
    "Group",
    "expand_groups",
    "read_items",
    "gap_violation_sql",
    "has_anchor",
    "is_gap",
    "parse_quantifier",
    "parse_token",
    "token_sql",
]

PATH_START = EventTypes().PATH_START.name
PATH_END = EventTypes().PATH_END.name
SENTINELS = (PATH_START, PATH_END)

GAP = ".*"
ANY_TOKEN = "."

_OPEN, _CLOSE, _NEGATE, _MEMBER_SEP = "[", "]", "^", "|"
_GROUP_OPEN, _GROUP_CLOSE = "(", ")"

#: A trailing quantifier: one of the regex shorthands or a ``{...}`` bound.
_QUANTIFIER_RE = re.compile(r"(\*|\+|\?|\{[^{}]*\})$")
#: The body of ``{...}``: ``n``, ``m,``, ``,n`` or ``m,n``.
_BOUNDS_RE = re.compile(r"^\s*(\d*)\s*(?:(,)\s*(\d*)\s*)?$")


@dataclass(frozen=True)
class EventClass:
    """A set of event names occupying one position, optionally complemented."""

    members: tuple[str, ...]
    negated: bool
    #: The token exactly as the user wrote it, for error messages and for the
    #: ordinal-aligned ``AnchorMatch.tokens``.
    raw: str

    def matches(self, event: str) -> bool:
        """Pure-Python twin of :func:`token_sql`, for tests and for oracles."""
        if event in SENTINELS and event not in self.members:
            return False
        return (event in self.members) != self.negated


#: ``.`` — the complement of the empty set, i.e. any event that is not a
#: boundary sentinel. Spelling it this way rather than special-casing keeps one
#: code path for every wildcard.
ANY = EventClass(members=(), negated=True, raw=ANY_TOKEN)


@dataclass(frozen=True)
class Gap:
    """A run of events between two anchors — optionally restricted, optionally bounded.

    ``.*`` is the unrestricted case and ``[^X]*`` / ``[X|Y]*`` the restricted
    ones, which is why this is a quantified *class* rather than a separate
    construct: ``.*`` is just ``.`` repeated. Unlike a class on its own, a gap
    is not a position — it takes no ordinal, and both its ends are pinned by
    the tokens around it.

    The quantifier bounds how many events the run holds: ``*`` is ``{0,}``,
    ``+`` is ``{1,}``, ``?`` is ``{0,1}``, and ``{m,n}`` is inclusive at both
    ends, as in regex. The anchors themselves are not counted, so
    ``A->.{0,5}->B`` puts at most five events *between* ``A`` and ``B``. This
    is where the language parts with regex on purpose: ``[a|b]{2,3}`` in regex
    is two or three characters, but here it is one gap, never two or three
    positions — a position count that varied from path to path would leave
    ``at=`` and step-matrix centring with nothing stable to address.
    """

    #: What every event inside the run must satisfy; None for ``.*``.
    constraint: EventClass | None
    raw: str
    #: Fewest events the run may hold.
    min_len: int = 0
    #: Most events the run may hold; None for no upper bound.
    max_len: int | None = None

    @property
    def is_plain(self) -> bool:
        """Whether this is ``.*`` in effect: anything, of any length."""
        return self.constraint is None and self.min_len == 0 and self.max_len is None

    def fits(self, length: int) -> bool:
        """Whether a run of `length` events satisfies the quantifier."""
        return length >= self.min_len and (
            self.max_len is None or length <= self.max_len
        )

    def allows(self, event: str) -> bool:
        """Pure-Python twin of :func:`gap_violation_sql`."""
        if event in SENTINELS:
            # A boundary can never actually fall strictly between two matched
            # positions, but if one somehow did, "boundaries are not events" is
            # the reading consistent with the rest of this module.
            return True
        return self.constraint is None or self.constraint.matches(event)


Token = Union[str, EventClass, Gap]


def split_quantifier(token: str) -> tuple[str, str] | None:
    """Split a trailing quantifier off `token`: ``(stem, quantifier)`` or None."""
    found = _QUANTIFIER_RE.search(token)
    if found is None:
        return None
    return token[: found.start()], found.group(1)


def parse_quantifier(quantifier: str, token: str) -> tuple[int, int | None]:
    """
    Read a quantifier as inclusive ``(min, max)`` bounds; max is None if open.

    ``*``, ``+`` and ``?`` are the regex shorthands for ``{0,}``, ``{1,}`` and
    ``{0,1}``. A braced bound accepts ``{n}``, ``{m,}``, ``{,n}`` and
    ``{m,n}`` — ``{,n}`` as in Python's ``re``. Only event counts are read:
    a bound with a unit (``{30m,}``) is a time bound, which is not supported.
    """
    if quantifier == "*":
        return 0, None
    if quantifier == "+":
        return 1, None
    if quantifier == "?":
        return 0, 1

    body = quantifier[1:-1]
    found = _BOUNDS_RE.match(body)
    if found is None:
        if re.search(r"\d\s*[A-Za-z]", body):
            raise PatternSyntaxError(
                f"{quantifier!r} in {token!r}: time bounds are not supported — "
                f"a bound counts events, e.g. '{{0,5}}'."
            )
        raise PatternSyntaxError(
            f"{quantifier!r} in {token!r}: write '{{n}}', '{{m,}}', '{{,n}}' or "
            f"'{{m,n}}' with non-negative whole numbers."
        )
    low, comma, high = found.groups()
    if not comma:
        if not low:
            raise PatternSyntaxError(
                f"Empty quantifier '{{}}' in {token!r} — write '{{n}}', "
                f"'{{m,}}', '{{,n}}' or '{{m,n}}'."
            )
        return int(low), int(low)
    if not low and not high:
        raise PatternSyntaxError(
            f"'{{,}}' in {token!r} bounds nothing — write '*' for a run of any length."
        )
    min_len = int(low) if low else 0
    max_len = int(high) if high else None
    if max_len is not None and min_len > max_len:
        raise PatternSyntaxError(
            f"{quantifier!r} in {token!r}: the lower bound {min_len} exceeds "
            f"the upper bound {max_len}."
        )
    return min_len, max_len


def parse_token(token: str) -> Token:
    """
    Read one ``->``-separated token as an event name, an event class, or a gap.

    Only three shapes are structural: ``.``, a fully bracketed ``[...]``, and
    either of those followed by a quantifier (``*``, ``+``, ``?`` or
    ``{m,n}``). Anything else is an event name, returned
    unchanged — including tokens that merely *look* suspicious (``^cart``,
    ``(A|B)``, ``a|b``). Those are left for :func:`describe_unknown_token` to
    explain, and only if the name turns out not to exist, so that an event
    legitimately named ``total$`` keeps working.
    """
    if token == ANY_TOKEN:
        return ANY

    split = split_quantifier(token)
    if split is not None:
        stem, quantifier = split
        if stem == ANY_TOKEN or _is_bracketed(stem):
            min_len, max_len = parse_quantifier(quantifier, token)
            constraint = None if stem == ANY_TOKEN else _parse_class(stem)
            return Gap(constraint, token, min_len, max_len)

    if _is_bracketed(token):
        return _parse_class(token)

    if _OPEN in token or _CLOSE in token:
        _reject_unbalanced(token)

    return token


def _is_bracketed(token: str) -> bool:
    return len(token) >= 2 and token.startswith(_OPEN) and token.endswith(_CLOSE)


def _reject_unbalanced(token: str) -> None:
    """Raise unless the brackets in a non-class token are incidental to a name.

    An event may legitimately be called ``checkout [beta]``; what cannot be
    read is a token that *starts* a class without finishing it.
    """
    if token.count(_OPEN) != token.count(_CLOSE) or token.startswith(_OPEN):
        raise PatternSyntaxError(f"Unbalanced brackets in {token!r}.")


def _parse_class(token: str) -> EventClass:
    inner = token[1:-1]
    negated = inner.startswith(_NEGATE)
    if negated:
        inner = inner[1:]

    if _OPEN in inner or _CLOSE in inner:
        body = inner.replace(_OPEN, "").replace(_CLOSE, "")
        flat = f"{_OPEN}{_NEGATE if negated else ''}{body}{_CLOSE}"
        raise PatternSyntaxError(
            f"Nested brackets in {token!r}. A class is a flat list of event "
            f"names — write {flat!r}."
        )

    if not inner.strip():
        raise PatternSyntaxError(
            f"Empty class {token!r} — list at least one event name."
        )

    members = tuple(member.strip() for member in inner.split(_MEMBER_SEP))
    if any(not member for member in members):
        raise PatternSyntaxError(
            f"Empty member in {token!r} (check for a stray '{_MEMBER_SEP}')."
        )

    seen: dict[str, None] = {}
    for member in members:
        seen.setdefault(member, None)
    return EventClass(members=tuple(seen), negated=negated, raw=token)


def check_class_spans(tokens: list[str]) -> None:
    """
    Reject a class whose brackets span the ``->`` delimiter.

    Runs on the token list *before* individual tokens are parsed, because by
    then the evidence is gone: ``[^cart->purchase]`` has already been split
    into ``[^cart`` and ``purchase]``, and each half on its own only looks like
    an unbalanced bracket rather than like the thing the user actually tried.
    """
    for i, token in enumerate(tokens):
        if token.count(_OPEN) <= token.count(_CLOSE):
            continue
        for j in range(i + 1, len(tokens)):
            if _CLOSE in tokens[j]:
                span = PATH_DELIMITER.join(tokens[i : j + 1])
                raise PatternSyntaxError(
                    f"A class in [ ] describes one event, not a sequence — "
                    f"{span!r} contains '{PATH_DELIMITER}'."
                )
        raise PatternSyntaxError(f"Unbalanced brackets in {token!r}.")


def is_plain_gap(token: str | None) -> bool:
    """Whether `token` is ``.*`` in effect — a gap that admits any run at all.

    Safe on raw tokens before groups are unrolled: a group's closing half such
    as ``[^X]*){2,}`` does not parse on its own, and it is not a plain gap
    either. Its syntax is checked when the group itself is read.
    """
    if token is None:
        return False
    try:
        parsed = parse_token(token)
    except PatternSyntaxError:
        return False
    return isinstance(parsed, Gap) and parsed.is_plain


def expand_groups(tokens: list[str], pattern: str) -> list[str]:
    """
    Unroll every quantified group ``(P){k}`` into `k` copies of ``P``.

    A group is a sequence, so the copies are joined the way any two tokens
    are — by ``->``, i.e. strictly adjacent: ``(B->C){3}`` is ``B->C->B->C->
    B->C``. Matching is a search, so ``(P){k}`` reads as "k repetitions in a
    row somewhere", which is also what it means in regex. Unrolling is the
    whole implementation: the matcher never sees a group, and every copy
    takes its ordinals like the tokens it was unrolled into.

    Only the forms that unrolling expresses exactly are accepted:

    * ``{k}`` — the repetitions themselves;
    * ``{k,}`` and ``+`` — only where an unrestricted ``.*`` sits on one side
      of the repeated block (at the group's own edge, next to it, or at an
      end of the pattern), which absorbs any repetitions beyond `k` and makes
      ``{k,}`` the same pattern as ``{k}``. Between two adjacent tokens it is
      not: ``A->(B->C){2,}->D`` needs a count, not an unrolling.

    An upper bound (``{,n}``, ``{m,n}``, ``?``) is refused. A search finds the
    first `m` repetitions whether or not more follow, so "at most n" is a
    statement about the *absence* of an (n+1)-th — a condition on a path, not
    something a pattern can locate. The error names the condition to use.

    One shape is refused as a likely mistake: a group with a gap inside but
    none at its edges, repeated more than once — ``(B->.*->C){2}``. Whoever
    writes ``.*`` inside means "somewhere", yet the copies are glued
    ``C->B`` with nothing allowed between them. The error offers both
    readings.
    """
    out: list[str] = []
    for item in read_items(tokens, pattern):
        if isinstance(item, Group):
            for _ in range(item.copies):
                out.extend(item.body)
        else:
            out.append(item)
    return out


@dataclass(frozen=True)
class Group:
    """A quantified group, validated but not yet unrolled.

    Only :func:`expand_groups` and the non-overlapping matcher see one: the
    first unrolls it into `copies` copies of `body`, the second compiles it to
    a regex repetition, greedy when `open` (``{k,}`` / ``+``).
    """

    body: tuple[str, ...]
    copies: int
    open: bool
    raw: str


def read_items(tokens: list[str], pattern: str) -> list[Union[str, Group]]:
    """Split a token list into plain tokens and validated :class:`Group` items."""
    items: list[Union[str, Group]] = []
    i = 0
    while i < len(tokens):
        token = tokens[i]
        single = _single_token_group(token)
        if single is not None:
            body, quantifier, end = [single[0]], single[1], i
        elif token.startswith(_GROUP_OPEN) and token.count(_GROUP_OPEN) > token.count(
            _GROUP_CLOSE
        ):
            body, quantifier, end = _read_group(tokens, i, pattern)
        else:
            items.append(token)
            i += 1
            continue

        raw = PATH_DELIMITER.join(tokens[i : end + 1])
        if any(not t for t in body):
            raise PatternSyntaxError(f"Empty token inside the group {raw!r}.")
        previous = items[-1] if items else None
        left = previous.body[-1] if isinstance(previous, Group) else previous
        right = tokens[end + 1] if end + 1 < len(tokens) else None
        copies = _repetitions(body, quantifier, raw, left, right, pattern)
        _, max_len = parse_quantifier(quantifier, raw)
        items.append(Group(tuple(body), copies, max_len is None, raw))
        i = end + 1
    return items


def _single_token_group(token: str) -> tuple[str, str] | None:
    """``(X){2}`` as ``("X", "{2}")``; None unless the token is exactly that shape."""
    if not token.startswith(_GROUP_OPEN):
        return None
    split = split_quantifier(token)
    if split is None:
        return None
    stem, quantifier = split
    if not stem.endswith(_GROUP_CLOSE):
        return None
    inner = stem[1:-1]
    if _GROUP_OPEN in inner or _GROUP_CLOSE in inner:
        return None
    return inner, quantifier


def _read_group(
    tokens: list[str], start: int, pattern: str
) -> tuple[list[str], str, int]:
    """Read a group opened at ``tokens[start]``: (body tokens, quantifier, closing index)."""
    for j in range(start + 1, len(tokens)):
        token = tokens[j]
        if token.startswith(_GROUP_OPEN) and token.count(_GROUP_OPEN) > token.count(
            _GROUP_CLOSE
        ):
            raise PatternSyntaxError(
                f"Nested groups in {pattern!r} are not supported — unroll the "
                f"inner one by hand."
            )
        if token.count(_GROUP_CLOSE) <= token.count(_GROUP_OPEN):
            continue
        raw = PATH_DELIMITER.join(tokens[start : j + 1])
        split = split_quantifier(token)
        if split is None or not split[0].endswith(_GROUP_CLOSE):
            if token.endswith(_GROUP_CLOSE):
                raise PatternSyntaxError(
                    f"The group {raw!r} has no quantifier. Brackets alone change "
                    f"nothing — drop them, or say how many times it repeats, "
                    f"e.g. '{raw}{{2}}'."
                )
            raise PatternSyntaxError(
                f"Unexpected text after ')' in {token!r}; a group ends with ')' "
                f"and a quantifier, e.g. ')' + '{{2}}'."
            )
        stem, quantifier = split
        body = [tokens[start][1:], *tokens[start + 1 : j], stem[:-1]]
        return body, quantifier, j
    raise PatternSyntaxError(
        f"Unbalanced parentheses in {pattern!r}: the group opened at "
        f"{tokens[start]!r} is never closed."
    )


def _repetitions(
    body: list[str],
    quantifier: str,
    raw: str,
    left: str | None,
    right: str | None,
    pattern: str,
) -> int:
    """How many copies of `body` a group unrolls into, or a PatternSyntaxError."""
    min_len, max_len = parse_quantifier(quantifier, raw)
    inner = PATH_DELIMITER.join(body)

    if max_len is not None and max_len != min_len:
        floor = max(min_len, 1)
        raise PatternSyntaxError(
            f"{raw!r}: a pattern can require a group at least k times, but not "
            f"at most — a path with more repetitions still contains the first "
            f"ones. Express the upper bound as a condition on the path instead: "
            f"'({inner}){{{floor}}}' matches and '({inner}){{{max_len + 1}}}' "
            f"does not."
        )
    if min_len == 0:
        raise PatternSyntaxError(
            f"{raw!r} allows zero repetitions, which every path satisfies. "
            f"A group must repeat at least once."
        )

    edge_gap = is_plain_gap(body[0]) or is_plain_gap(body[-1])
    if min_len > 1 and not (is_gap(body[0]) or is_gap(body[-1])):
        if any(is_gap(token) for token in body):
            spread = f"({inner}{PATH_DELIMITER}{GAP}){quantifier}"
            glued = PATH_DELIMITER.join(body * min_len)
            raise PatternSyntaxError(
                f"In {raw!r} the repetitions are joined back to back, so the "
                f"group's last event must be followed immediately by its "
                f"first ({body[-1]!r} then {body[0]!r}, nothing between). "
                f"For repetitions anywhere along the path write {spread!r}; "
                f"if back to back is what you mean, write it out: {glued!r}."
            )

    if max_len is None:
        absorbed = (
            edge_gap
            or left is None
            or right is None
            or is_plain_gap(left)
            or is_plain_gap(right)
        )
        if not absorbed:
            raise PatternSyntaxError(
                f"{raw!r} in {pattern!r}: 'at least {min_len}' between two "
                f"adjacent tokens would need to count repetitions, which a "
                f"pattern cannot. Use an exact count, '({inner}){{{min_len}}}', "
                f"or allow a gap next to the group."
            )
    return min_len


def class_inner(token: EventClass) -> str:
    """The class body as written, brackets and ``^`` stripped, members not split.

    Used to tell a class apart from an event name that happens to contain
    ``|``: if the whole body is itself an event, splitting it into members was
    the wrong reading.
    """
    inner = token.raw[1:-1]
    return inner[1:] if inner.startswith(_NEGATE) else inner


def is_gap(token: str) -> bool:
    """Whether a token spans a run of events rather than filling one position."""
    return isinstance(parse_token(token), Gap)


def has_anchor(tokens: Iterable[str]) -> bool:
    """Whether any position narrows the pattern to particular events.

    A name and a positive class both do — ``[cart|purchase]`` is as specific as
    ``cart``, just about two events instead of one. A negated class and ``.``
    do the opposite: they admit everything except a few names, so a pattern
    built only from those is syntactically fine and almost always true, which
    is worth a warning rather than an error. A gap is never an anchor: it fills
    no position, and even a restricted one only says what may lie *between*
    anchors.
    """
    for token in tokens:
        parsed = parse_token(token)
        if isinstance(parsed, Gap):
            continue
        if isinstance(parsed, str) or not parsed.negated:
            return True
    return False


def token_sql(token: Token, column: str) -> str:
    """SQL predicate deciding whether the event in `column` fills this position."""
    if isinstance(token, str):
        return f"{column} = {quote_literal(token)}"

    listed = ", ".join(quote_literal(member) for member in token.members)
    if not token.negated:
        return f"{column} IN ({listed})"

    clauses = []
    if token.members:
        clauses.append(f"{column} NOT IN ({listed})")
    # A sentinel is excluded unless the class names it; NULL (past the end of a
    # path, from the LEAD window) fails every NOT IN, which is also correct.
    hidden = [name for name in SENTINELS if name not in token.members]
    if hidden:
        clauses.append(
            f"{column} NOT IN ({', '.join(quote_literal(name) for name in hidden)})"
        )
    return "(" + " AND ".join(clauses) + ")"


def gap_violation_sql(gap: Gap, column: str) -> str | None:
    """SQL predicate: the event in `column` is *not* allowed inside this gap.

    None for an unrestricted gap, which nothing violates. Phrased as the
    negative because that is what the matcher needs — a restricted gap is
    enforced by proving no violating event lies between the anchors, not by
    walking the run.
    """
    if gap.constraint is None:
        return None
    sentinels = ", ".join(quote_literal(name) for name in SENTINELS)
    return (
        f"(NOT ({token_sql(gap.constraint, column)}) AND {column} NOT IN ({sentinels}))"
    )


def describe_unknown_token(token: str) -> str | None:
    """
    A hint for an event name that is not in the eventstream, or None.

    Deferred to here — rather than made a parse error — so that the hint costs
    nothing when the name is real: an event called ``total$`` or ``a|b`` keeps
    working, and only a name that does not exist gets read as a typo'd attempt
    at a construct.
    """
    if token.startswith(_NEGATE):
        return (
            f"'{_NEGATE}' is not a start-of-path anchor. To match the beginning "
            f"of a path write '{PATH_START}{PATH_DELIMITER}...'; to match one "
            f"event that is not {token[1:]!r} write '[^{token[1:]}]'."
        )
    if token.endswith("$"):
        return (
            f"'$' is not an end-of-path anchor. Write "
            f"'...{PATH_DELIMITER}{PATH_END}' instead."
        )
    split = split_quantifier(token)
    if split is not None and split[0] and not split[0].startswith(_GROUP_OPEN):
        stem, quantifier = split
        return (
            f"A quantifier applies to '{ANY_TOKEN}', a class in brackets or a "
            f"group, not to an event name. For {stem!r} repeated back to back "
            f"write '({stem}){quantifier}'; for a run of nothing but {stem!r} "
            f"between two other events, '[{stem}]{quantifier}'."
        )
    if token.startswith("(") and token.endswith(")") and _MEMBER_SEP in token:
        return (
            f"Round brackets are not supported. For one event that is any of "
            f"several, write '[{token[1:-1]}]'."
        )
    if _MEMBER_SEP in token:
        return (
            f"'{_MEMBER_SEP}' only works inside brackets, where it means one "
            f"event that is any of several: '[{token}]'. A pattern cannot list "
            f"alternative sequences."
        )
    return None
