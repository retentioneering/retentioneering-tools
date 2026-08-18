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
    "gap_violation_sql",
    "has_anchor",
    "is_gap",
    "parse_token",
    "token_sql",
]

PATH_START = EventTypes().PATH_START.name
PATH_END = EventTypes().PATH_END.name
SENTINELS = (PATH_START, PATH_END)

GAP = ".*"
ANY_TOKEN = "."

_OPEN, _CLOSE, _NEGATE, _MEMBER_SEP = "[", "]", "^", "|"
_QUANTIFIERS = ("*", "+", "?")


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
    """A run of events of any length, including none — optionally restricted.

    ``.*`` is the unrestricted case and ``[^X]*`` / ``[X|Y]*`` the restricted
    ones, which is why this is a quantified *class* rather than a separate
    construct: ``.*`` is just ``.`` repeated. Unlike a class on its own, a gap
    is not a position — it takes no ordinal, and both its ends are pinned by
    the tokens around it.
    """

    #: What every event inside the run must satisfy; None for ``.*``.
    constraint: EventClass | None
    raw: str

    def allows(self, event: str) -> bool:
        """Pure-Python twin of :func:`gap_violation_sql`."""
        if event in SENTINELS:
            # A boundary can never actually fall strictly between two matched
            # positions, but if one somehow did, "boundaries are not events" is
            # the reading consistent with the rest of this module.
            return True
        return self.constraint is None or self.constraint.matches(event)


Token = Union[str, EventClass, Gap]


def parse_token(token: str) -> Token:
    """
    Read one ``->``-separated token as an event name, an event class, or a gap.

    Only three shapes are structural: ``.``, a fully bracketed ``[...]``, and
    either of those followed by ``*``. Anything else is an event name, returned
    unchanged — including tokens that merely *look* suspicious (``^cart``,
    ``(A|B)``, ``a|b``). Those are left for :func:`describe_unknown_token` to
    explain, and only if the name turns out not to exist, so that an event
    legitimately named ``total$`` keeps working.
    """
    if token == ANY_TOKEN:
        return ANY

    if token.endswith(_QUANTIFIERS):
        stem, quantifier = token[:-1], token[-1]
        if stem == ANY_TOKEN or _is_bracketed(stem):
            if quantifier != "*":
                raise PatternSyntaxError(
                    f"{token!r}: only '*' (any number of events, including none) "
                    f"is supported after a class."
                )
            return Gap(None if stem == ANY_TOKEN else _parse_class(stem), token)

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
