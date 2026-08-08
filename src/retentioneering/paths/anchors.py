"""
Positional path-pattern anchoring (L1).

One place that answers "where in each path does this pattern match", replacing
two independent implementations that had drifted apart:

* ``tools/step_matrix.py``'s ``_find_center_position`` — pure Python, applied
  row-by-row over ``->``-joined path strings, returning the *position* of the
  pattern's last element;
* ``metrics/metric_builder.py``'s ``matches_pattern`` — DuckDB RE2 over the same
  joined strings, returning a *boolean* per path.

They used different engines, returned different things, and only the first
validated that a pattern's tokens actually exist in the eventstream. This module
supersedes both: :func:`resolve_anchors` runs relationally in DuckDB (no string
joining, no regex, no per-row ``.apply``) and returns positions, of which the
boolean "did it match at all" is a degenerate case.

Matching semantics (unchanged from ``_find_center_position``, which this module
is tested for equivalence against):

* a pattern is a ``->``-separated sequence of tokens, where ``.*`` stands for
  any run of events, *including an empty one* — ``"A->.*->B"`` matches both
  ``A->X->B`` and ``A->B``;
* tokens not separated by ``.*`` must be strictly adjacent in the path;
* a pattern matches anywhere in the path — no implicit anchoring at either end.

A token is an event name or an *event class* — ``[A|B]``, ``[^A]``, ``.`` — as
parsed by :mod:`retentioneering.paths.tokens`. A class occupies exactly one
position and one ordinal, so everything below about parts, ordinals and
occurrences is written in terms of tokens and holds unchanged either way.

A pattern usually has *several* valid matches in a path: a match is any
assignment of positions to its gap-separated parts that keeps them in order and
non-overlapping. ``occurrence`` picks between them, and is easiest to reason
about by its result rather than by the procedure that produces it:

* ``"first"`` puts every token as **early** as that token can be in any valid
  match; ``"last"`` puts it as **late** as it can be. Both results are
  themselves valid matches, and either finds one whenever one exists.

Two consequences worth knowing. ``"last"`` is *not* "the last occurrence of the
anchor event" — an occurrence that participates in no valid match is not a
candidate at all, so on ``catalog, cart, purchase, cart`` the anchor of
``"catalog->.*->cart->.*->purchase"`` at the ``cart`` token is the *second*
event, not the fourth. And ``"first"`` is not "the earliest complete run of the
pattern": it minimises each token independently, so it will happily pair an
early ``catalog`` and ``cart`` with a much later ``purchase``.

Implementation-wise this falls out of matching the parts as a chain from one end
— left to right for ``"first"``, right to left for ``"last"`` — which is
componentwise optimal because pinning the neighbour at its own extreme is the
least constraining choice for the part being matched next.

``path_start`` / ``path_end`` are reserved sentinels naming a path's own
boundaries. They participate in patterns as ordinary tokens; if the frame does
not already carry the synthetic boundary rows, virtual ones are added for the
duration of the query, positioned just outside the real events and carrying the
path's first/last real index so that a boundary anchor resolves to a usable
bound.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Iterable, Sequence

import pandas as pd

from retentioneering import engine
from retentioneering.engine import dialect
from retentioneering.eventstream.event_type import EventTypes
from retentioneering.exceptions import InvalidParameterError, PatternSyntaxError
from retentioneering.paths import tokens as tokens_mod
from retentioneering.utils.durations import parse_duration
from retentioneering.utils.sequences import PATH_DELIMITER
from retentioneering.utils.sql_quoting import quote_literal

if TYPE_CHECKING:
    from retentioneering.eventstream.schema import EventstreamSchema

__all__ = [
    "GAP",
    "PATH_END",
    "PATH_START",
    "AnchorMatch",
    "AnchorSpec",
    "literal_tokens",
    "normalize_pattern",
    "parse_spec",
    "resolve_anchors",
    "resolve_bound",
    "split_parts",
    "validate_pattern_tokens",
]

PATH_START = tokens_mod.PATH_START
PATH_END = tokens_mod.PATH_END
GAP = tokens_mod.GAP

OCCURRENCES = ("first", "last")

# Internal column aliases. Prefixed so they cannot collide with a user column
# that happens to be named "step" or "path".
_PATH = "__rete_path"
_EVENT = "__rete_event"
_IDX = "__rete_idx"
_STEP = "__rete_step"
_ORDINAL = "__rete_ordinal"


@dataclass(frozen=True)
class AnchorMatch:
    """Result of :func:`resolve_anchors`, keyed for the caller's convenience."""

    #: Long frame: one row per (path, literal token ordinal) — columns
    #: ``path_col``, ``"ordinal"``, ``"step"``, ``"index"``. Paths that did not
    #: match the pattern are absent entirely.
    frame: pd.DataFrame
    #: Name of the path column in :attr:`frame`.
    path_col: str
    #: The literal tokens of the pattern, positionally aligned with ``ordinal``.
    tokens: Sequence[str] = field(default_factory=tuple)
    #: Ordinal of each gap-separated part's *first* token. Step Matrix anchors on
    #: these rather than on individual tokens: a part is laid out to the right of
    #: its own start, so the block's centre is where the part begins.
    part_ordinals: Sequence[int] = field(default_factory=tuple)

    def paths(self) -> pd.Index:
        """Unique ids of the paths that matched."""
        return pd.Index(self.frame[self.path_col].unique(), name=self.path_col)

    def at_part(self, part: int) -> pd.DataFrame:
        """Rows for the first token of a gap-separated part (negatives count from the end)."""
        resolved = part if part >= 0 else len(self.part_ordinals) + part
        if not 0 <= resolved < len(self.part_ordinals):
            raise IndexError(
                f"part {part} is out of range for a pattern with "
                f"{len(self.part_ordinals)} part(s)"
            )
        return self.at(self.part_ordinals[resolved])

    def at(self, ordinal: int) -> pd.DataFrame:
        """Rows for a single token ordinal, resolved Python-style (negatives count from the end)."""
        resolved = ordinal if ordinal >= 0 else len(self.tokens) + ordinal
        if not 0 <= resolved < len(self.tokens):
            raise IndexError(
                f"anchor ordinal {ordinal} is out of range for a pattern with "
                f"{len(self.tokens)} literal token(s): {list(self.tokens)}"
            )
        return self.frame[self.frame["ordinal"] == resolved]


def normalize_pattern(
    pattern: str, *, warn: bool = True, stacklevel: int = 3, param: str = "pattern"
) -> str:
    """
    Strip redundant leading ``".*->"`` / trailing ``"->.*"`` and collapse repeated gaps.

    A pattern already matches anywhere in the path, so boundary wildcards select
    exactly the same paths — but they also shift anchor bookkeeping, putting the
    anchor's own column off 0. Normalizing up front keeps every pattern on the
    bare-literal form, which anchors correctly.
    """
    if not isinstance(pattern, str) or not pattern.strip():
        raise InvalidParameterError(param, pattern, ["a non-empty pattern string"])

    tokens = [t for t in pattern.split(PATH_DELIMITER)]
    if any(t == "" for t in tokens):
        raise InvalidParameterError(
            param,
            pattern,
            [f"a pattern with no empty tokens (check for a stray '{PATH_DELIMITER}')"],
        )

    # Syntax first: a class whose brackets span '->' has to be diagnosed on the
    # token list, before the halves are looked at one by one (see
    # `tokens.check_class_spans`), and a token that cannot be read at all should
    # not reach the gap bookkeeping below.
    tokens_mod.check_class_spans(tokens)
    for token in tokens:
        if token != GAP:
            tokens_mod.parse_token(token)

    # A restricted gap says what may lie *between* two anchors, so it needs one
    # on each side. At either end of a pattern its outer side is unpinned, and
    # since a gap also matches the empty run it would simply be true — silently
    # doing nothing. Refuse it and name the fix.
    for token, side, fix in (
        (tokens[0], "start", f"{PATH_START}{PATH_DELIMITER}{pattern}"),
        (tokens[-1], "end", f"{pattern}{PATH_DELIMITER}{PATH_END}"),
    ):
        parsed = tokens_mod.parse_token(token)
        if isinstance(parsed, tokens_mod.Gap) and parsed.constraint is not None:
            raise PatternSyntaxError(
                f"{token!r} at the {side} of {pattern!r} has nothing on its outer "
                f"side to bound it, so it would match the empty run and mean "
                f"nothing. Anchor it — e.g. {fix!r}."
            )

    collapsed: list[str] = []
    for token in tokens:
        if not tokens_mod.is_gap(token):
            collapsed.append(token)
            continue
        if collapsed and tokens_mod.is_gap(collapsed[-1]):
            if token == GAP and collapsed[-1] == GAP:
                continue  # two plain gaps in a row say nothing extra
            raise PatternSyntaxError(
                f"Two gaps in a row in {pattern!r} ({collapsed[-1]!r} then "
                f"{token!r}). A gap already spans any number of events; to say "
                f"what may lie in it, use a single restricted gap."
            )
        collapsed.append(token)

    trimmed = list(collapsed)
    while trimmed and trimmed[0] == GAP:
        trimmed.pop(0)
    while trimmed and trimmed[-1] == GAP:
        trimmed.pop()

    if not trimmed:
        raise InvalidParameterError(
            param, pattern, ["a pattern with at least one event name"]
        )

    normalized = PATH_DELIMITER.join(trimmed)
    if warn and normalized != pattern:
        warnings.warn(
            f"{param} {pattern!r} has a redundant leading/trailing '{GAP}' or a "
            f"repeated gap — a pattern already matches anywhere in the path by "
            f"default. Using {normalized!r} instead.",
            UserWarning,
            stacklevel=stacklevel,
        )
    return normalized


def split_pattern(pattern: str) -> tuple[list[list[str]], list[str | None]]:
    """
    Split a normalized pattern into parts and the gaps between them.

    A *part* is a run of strictly adjacent tokens; a *gap* is what separates two
    parts. Returns them aligned: ``gaps[i]`` is the gap immediately before
    ``parts[i]``, and ``gaps[0]`` is always None because a normalized pattern
    never begins with one.

    Callers must split on this rather than on the literal string ``"->.*->"``:
    a gap may now be restricted (``"->[^X]*->"``), and a naive string split
    would read ``A->[^X]*->B`` as three strictly adjacent tokens.
    """
    parts: list[list[str]] = []
    gaps: list[str | None] = []
    current: list[str] = []
    pending: str | None = None
    for token in pattern.split(PATH_DELIMITER):
        if tokens_mod.is_gap(token):
            if current:
                parts.append(current)
                gaps.append(pending)
                current = []
            pending = token
        else:
            current.append(token)
    if current:
        parts.append(current)
        gaps.append(pending)
    return parts, gaps


def join_pattern(parts: Sequence[Sequence[str]], gaps: Sequence[str | None]) -> str:
    """Rebuild a pattern string from :func:`split_pattern`'s output."""
    pieces: list[str] = []
    for part, gap in zip(parts, gaps):
        if gap is not None:
            pieces.append(gap)
        pieces.extend(part)
    return PATH_DELIMITER.join(pieces)


def split_parts(pattern: str) -> list[list[str]]:
    """Split a normalized pattern into runs of strictly adjacent tokens, separated by gaps."""
    return split_pattern(pattern)[0]


def literal_tokens(pattern: str) -> list[str]:
    """The pattern's event-name tokens, in order, with gaps removed.

    These are the positions an anchor's ``at`` indexes into — a gap (``.*`` or
    a restricted ``[^X]*``) is not a position, so it is not counted.
    """
    return [t for t in pattern.split(PATH_DELIMITER) if not tokens_mod.is_gap(t)]


def _validate_class(
    token: "tokens_mod.EventClass",
    available: set,
    *,
    param: str,
    check_literals: bool,
) -> None:
    """Check one event class's member names, and that it is a class at all."""
    raw = token.raw
    # An event named exactly like a class shadows it. Too rare to be worth an
    # escape syntax, too damaging to reinterpret silently.
    if raw in available:
        raise PatternSyntaxError(
            f"{raw!r} reads as a class of events, but this eventstream also "
            f"has an event named {raw!r}. Rename the event (see "
            f"`rename_events`) to use it in a pattern."
        )
    inner = tokens_mod.class_inner(token)
    if len(token.members) > 1 and inner in available:
        raise PatternSyntaxError(
            f"{raw!r} was read as {len(token.members)} alternatives, but "
            f"{inner!r} is itself an event in this eventstream. Rename the "
            f"event (see `rename_events`) to use it in a pattern."
        )
    # An absent name narrows a positive class (one fewer alternative) and
    # widens a negated one (one fewer exclusion). Callers that tolerate names
    # resolving nowhere are tolerating the first, never the second.
    if not check_literals and not token.negated:
        return
    for member in token.members:
        if member in (PATH_START, PATH_END) or member in available:
            continue
        raise InvalidParameterError(param, member, sorted(available))


def validate_pattern_tokens(
    pattern: str,
    available_events: Iterable[str],
    *,
    param: str = "pattern",
    warn: bool = True,
    stacklevel: int = 3,
    check_literals: bool = True,
) -> None:
    """
    Guard against typos: a mistyped event name would otherwise silently match
    nothing, indistinguishable from a legitimately empty result.

    Names inside a *negated* class are checked for a sharper reason: a typo in
    ``[^Purchse]`` does not produce an empty result but an always-true
    position, which corrupts the output while looking perfectly healthy. That
    asymmetry is what `check_literals=False` turns on — ``truncate_paths``
    accepts a *list* of anchors of which some are expected not to resolve
    (``["purchase", "path_end"]``), so there a name that names nothing is
    legitimate wherever its absence *narrows* the pattern (a bare token, a
    member of a positive class) and never where its absence widens it.

    `param` names the caller's user-facing parameter, so the error points at
    ``path_pattern`` / ``start_event`` rather than at this module's internals.
    """
    available = set(available_events)
    raw_tokens = pattern.split(PATH_DELIMITER)

    for raw in raw_tokens:
        token = tokens_mod.parse_token(raw)

        # A gap carries no position, but its constraint carries event names,
        # and a typo there is the same always-true hazard one level out: an
        # unknown name in `[^Purchse]*` stops excluding anything, so the gap
        # quietly admits the very events it was written to rule out.
        if isinstance(token, tokens_mod.Gap):
            if token.constraint is not None:
                _validate_class(
                    token.constraint,
                    available,
                    param=param,
                    check_literals=check_literals,
                )
            continue

        if isinstance(token, str):
            if token in (PATH_START, PATH_END) or token in available:
                continue
            hint = tokens_mod.describe_unknown_token(token)
            if hint is not None:
                raise PatternSyntaxError(hint)
            if check_literals:
                raise InvalidParameterError(param, token, sorted(available))
            continue

        _validate_class(token, available, param=param, check_literals=check_literals)

    if warn and raw_tokens and not tokens_mod.has_anchor(raw_tokens):
        warnings.warn(
            f"{param} {pattern!r} names no events to look for — every position is "
            f"negated or a wildcard, so it matches almost any path. Add at "
            f"least one event name or a positive class to anchor it.",
            UserWarning,
            stacklevel=stacklevel,
        )


def _has_boundary_rows(df: pd.DataFrame, schema: "EventstreamSchema") -> bool:
    type_col = schema.event_type
    if type_col not in df.columns:
        return False
    types = EventTypes()
    return bool(df[type_col].isin([types.PATH_START.type, types.PATH_END.type]).any())


def _part_condition(tokens: Sequence[str], alias: str) -> str:
    """SQL predicate: a match of `tokens` *starts* at this row."""
    columns = [f"{alias}.{_EVENT}"] + [
        f"{alias}.__rete_e{lead}" for lead in range(1, len(tokens))
    ]
    return " AND ".join(
        tokens_mod.token_sql(tokens_mod.parse_token(token), column)
        for token, column in zip(tokens, columns)
    )


def resolve_anchors(
    df: pd.DataFrame,
    schema: "EventstreamSchema",
    pattern: str,
    *,
    occurrence: str = "first",
    path_col: str | None = None,
    event_col: str | None = None,
    not_before: pd.DataFrame | None = None,
    not_before_part: int = 0,
) -> AnchorMatch:
    """
    Locate `pattern` in every path and return the position of each of its tokens.

    Parameters
    ----------
    df : pandas.DataFrame
        The eventstream's frame.
    schema : EventstreamSchema
        Schema describing `df`'s columns.
    pattern : str
        ``->``-separated event names; ``.*`` matches any run of events,
        including an empty one. Assumed already normalized by
        :func:`normalize_pattern`.
    occurrence : {"first", "last"}, default "first"
        Which match to anchor on when the pattern has several. ``"first"``
        returns each token at the earliest position it can occupy in any valid
        match, ``"last"`` at the latest — see this module's docstring for what
        that does and does not mean.
    path_col : str, optional
        Path id column; defaults to ``schema.path_col``.
    event_col : str, optional
        Event column the pattern's tokens are matched against; defaults to
        ``schema.event_col``.
    not_before : pandas.DataFrame, optional
        Per-path floor: columns `path_col` and ``"bound"``, a ``schema.index``
        value the match may not start before. Only parts from `not_before_part`
        onward are constrained, so a pattern may still begin *earlier* than the
        floor as long as the part carrying the anchor does not — which is what
        lets an end anchor be searched for "after the window opened" while its
        pattern is still matched against the whole path.

        Without this, a caller wanting the same guarantee would have to truncate
        the frame first, and a pattern spanning the cut could then only match a
        later occurrence: with the window opening at the `cart` of
        ``"catalog->.*->cart"``, the leading `catalog` is behind the cut, so the
        pattern re-matches at the *next* catalog/cart pair instead of the one the
        window opened on.
    not_before_part : int, default 0
        Index into the pattern's gap-separated parts; parts before it are exempt
        from `not_before`.

    Returns
    -------
    AnchorMatch
        Long frame of ``(path, ordinal, step, index)``, one row per literal
        token per matching path. Non-matching paths are absent.
    """
    if occurrence not in OCCURRENCES:
        raise InvalidParameterError("occurrence", occurrence, list(OCCURRENCES))

    path_col = path_col or schema.path_col
    parts, gaps = split_pattern(pattern)
    if not parts:
        raise InvalidParameterError(
            "pattern", pattern, ["a pattern with at least one event name"]
        )

    path_q = engine.quote_ident(path_col)
    event_q = engine.quote_ident(event_col or schema.event_col)
    index_q = engine.quote_ident(schema.index)
    subindex_q = engine.quote_ident(schema.subindex)

    # path_cols is validated (coarsest-first, strictly nested) at Eventstream
    # construction time, so ordering by schema.index is correct at any accepted
    # grain (see ADR-0004).
    base_cte = f"""
        SELECT
            {path_q} AS {_PATH},
            {event_q} AS {_EVENT},
            {index_q} AS {_IDX},
            row_number() OVER (
                PARTITION BY {path_q} ORDER BY {index_q}, {subindex_q}
            ) AS {_STEP}
        FROM df
    """

    if _has_boundary_rows(df, schema):
        stepped_cte = f"SELECT {_PATH}, {_EVENT}, {_IDX}, {_STEP} FROM base"
    else:
        # Virtual boundary rows: just outside the real events in step space, but
        # carrying the path's first/last real index so a boundary anchor resolves
        # to a usable bound rather than to a NULL.
        stepped_cte = f"""
            SELECT {_PATH}, {_EVENT}, {_IDX}, {_STEP} FROM base
            UNION ALL
            SELECT {_PATH}, {quote_literal(PATH_START)}, __rete_min_idx, 0 FROM edges
            UNION ALL
            SELECT {_PATH}, {quote_literal(PATH_END)}, __rete_max_idx, __rete_max_step + 1
            FROM edges
        """

    max_lead = max(len(part) for part in parts) - 1
    lead_cols = "".join(
        f", LEAD({_EVENT}, {i}) OVER w AS __rete_e{i}\n" for i in range(1, max_lead + 1)
    )

    ctes = [
        f"base AS ({base_cte})",
        f"""edges AS (
            SELECT {_PATH},
                   MIN({_IDX}) AS __rete_min_idx,
                   MAX({_IDX}) AS __rete_max_idx,
                   MAX({_STEP}) AS __rete_max_step
            FROM base GROUP BY {_PATH}
        )""",
        f"stepped AS ({stepped_cte})",
        f"""leads AS (
            SELECT {_PATH}, {_EVENT}, {_IDX}, {_STEP}
            {lead_cols}
            FROM stepped
            WINDOW w AS (PARTITION BY {_PATH} ORDER BY {_STEP})
        )""",
    ]

    floor_join = ""
    floor_cond = ""
    if not_before is not None:
        floor_join = f"JOIN not_before nb ON l.{_PATH} = nb.{path_q}"
        floor_cond = f" AND l.{_IDX} >= nb.bound"

    # Every position where each part could start, before any cross-part
    # constraint is applied. The floor constrains only the anchor's own part and
    # whatever follows it; earlier parts are free to sit before the window
    # opened.
    for i, part in enumerate(parts):
        join = floor_join if i >= not_before_part else ""
        extra = floor_cond if i >= not_before_part else ""
        ctes.append(
            f"""cand{i} AS (
                SELECT l.{_PATH}, l.{_STEP} AS s
                FROM leads l {join} WHERE {_part_condition(part, "l")}{extra}
            )"""
        )

    # A restricted gap is enforced by proving that no event it disallows lies
    # between the two parts it separates — one marker relation per such gap,
    # never a walk over the run itself.
    violations: dict[int, bool] = {}
    for i in range(1, len(parts)):
        gap = tokens_mod.parse_token(gaps[i])
        violation = tokens_mod.gap_violation_sql(gap, _EVENT)
        if violation is not None:
            violations[i] = True
            ctes.append(
                f"bad{i} AS (SELECT {_PATH}, {_STEP} FROM stepped WHERE {violation})"
            )

    # Forward pass: fwd{i} keeps the starts of part i that have a valid *prefix*
    # — some placement of parts 0..i-1 before them honouring every gap.
    #
    # It has to be a set, not the single earliest start the previous
    # implementation carried. With a plain gap, an earlier neighbour is always
    # the least constraining choice, so one extreme sufficed. A restricted gap
    # inverts that: the earlier the neighbour, the longer the run that must stay
    # clean, so the earliest is the *most* constraining. On `A, X, A, D` the
    # pattern `A->[^X]*->D` matches only from the second A, which a chain
    # pinning A at its earliest would miss entirely and report as no match.
    #
    # Carrying the whole set costs nothing extra: for each candidate start,
    # `max_e` is the latest feasible end of the previous part below it, and
    # `max_b` the latest disallowed event below it. A previous end at or after
    # that disallowed event means the run between them is clean, so
    # `max_e >= max_b` is exactly the gap condition — and for an unrestricted
    # gap `max_b` is absent and the test degenerates to "some previous end
    # exists". `kind` breaks ties so that markers sharing a step with the
    # candidate stay outside its frame, keeping both comparisons strict.
    ctes.append(f"fwd0 AS (SELECT {_PATH}, s FROM cand0)")
    for i in range(1, len(parts)):
        prev_end = f"s + {len(parts[i - 1]) - 1}"
        marks = [
            f"SELECT {_PATH}, s AS {_STEP}, 0 AS kind, "
            f"CAST(NULL AS BIGINT) AS e, CAST(NULL AS BIGINT) AS b FROM cand{i}",
            f"SELECT {_PATH}, {prev_end} AS {_STEP}, 1, {prev_end}, "
            f"CAST(NULL AS BIGINT) FROM fwd{i - 1}",
        ]
        if i in violations:
            marks.append(
                f"SELECT {_PATH}, {_STEP}, 1, CAST(NULL AS BIGINT), {_STEP} FROM bad{i}"
            )
        ctes.append(
            f"""fscan{i} AS (
                SELECT {_PATH}, {_STEP}, kind,
                       MAX(e) OVER w AS max_e,
                       MAX(b) OVER w AS max_b
                FROM ({" UNION ALL ".join(marks)})
                WINDOW w AS (
                    PARTITION BY {_PATH} ORDER BY {_STEP}, kind
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                )
            )"""
        )
        ctes.append(
            f"""fwd{i} AS (
                SELECT {_PATH}, {_STEP} AS s FROM fscan{i}
                WHERE kind = 0 AND max_e IS NOT NULL AND max_e >= COALESCE(max_b, -1)
            )"""
        )

    # Backward pass: the exact mirror, keeping the starts that have a valid
    # *suffix*. Both are needed whatever `occurrence` is — a start with a prefix
    # but no suffix takes part in no complete match, so it is not a candidate for
    # either extreme.
    last = len(parts) - 1
    ctes.append(f"bwd{last} AS (SELECT {_PATH}, s FROM cand{last})")
    for i in reversed(range(last)):
        own_end = f"s + {len(parts[i]) - 1}"
        marks = [
            f"SELECT {_PATH}, {own_end} AS {_STEP}, 1 AS kind, s AS start, "
            f"CAST(NULL AS BIGINT) AS f, CAST(NULL AS BIGINT) AS b FROM cand{i}",
            f"SELECT {_PATH}, s AS {_STEP}, 0, CAST(NULL AS BIGINT), s, "
            f"CAST(NULL AS BIGINT) FROM bwd{i + 1}",
        ]
        if i + 1 in violations:
            marks.append(
                f"SELECT {_PATH}, {_STEP}, 0, CAST(NULL AS BIGINT), "
                f"CAST(NULL AS BIGINT), {_STEP} FROM bad{i + 1}"
            )
        ctes.append(
            f"""bscan{i} AS (
                SELECT {_PATH}, {_STEP}, kind, start,
                       MIN(f) OVER w AS min_f,
                       MIN(b) OVER w AS min_b
                FROM ({" UNION ALL ".join(marks)})
                WINDOW w AS (
                    PARTITION BY {_PATH} ORDER BY {_STEP}, kind
                    ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
                )
            )"""
        )
        ctes.append(
            f"""bwd{i} AS (
                SELECT {_PATH}, start AS s FROM bscan{i}
                WHERE kind = 1 AND min_f IS NOT NULL
                  AND min_f <= COALESCE(min_b, 9223372036854775807)
            )"""
        )

    # A start with both a prefix and a suffix takes part in some complete match,
    # so the componentwise extremum over these is exactly what `occurrence`
    # names. (That the extremum is itself a valid match still holds with
    # restricted gaps: pushing either end of a run inwards only shortens what
    # has to stay clean.)
    agg = "MIN" if occurrence == "first" else "MAX"
    for i in range(len(parts)):
        ctes.append(
            f"""m{i} AS (
                SELECT f.{_PATH}, {agg}(f.s) AS s
                FROM fwd{i} f JOIN bwd{i} b ON f.{_PATH} = b.{_PATH} AND f.s = b.s
                GROUP BY f.{_PATH}
            )"""
        )

    joins = " ".join(
        f"JOIN m{i} ON m0.{_PATH} = m{i}.{_PATH}" for i in range(1, len(parts))
    )
    selected = ", ".join(f"m{i}.s AS s{i}" for i in range(len(parts)))
    ctes.append(f"matched AS (SELECT m0.{_PATH}, {selected} FROM m0 {joins})")

    token_selects = []
    part_ordinals: list[int] = []
    ordinal = 0
    for i, tokens in enumerate(parts):
        part_ordinals.append(ordinal)
        for offset in range(len(tokens)):
            token_selects.append(
                f"SELECT {_PATH}, {ordinal} AS {_ORDINAL}, s{i} + {offset} AS {_STEP} FROM matched"
            )
            ordinal += 1
    ctes.append("tokens AS (" + " UNION ALL ".join(token_selects) + ")")

    query = f"""
        WITH {", ".join(ctes)}
        SELECT
            t.{_PATH} AS {path_q},
            t.{_ORDINAL} AS ordinal,
            t.{_STEP} AS step,
            st.{_IDX} AS index
        FROM tokens t
        JOIN stepped st ON st.{_PATH} = t.{_PATH} AND st.{_STEP} = t.{_STEP}
        ORDER BY t.{_PATH}, t.{_ORDINAL}
    """

    tables = {"df": df}
    if not_before is not None:
        tables["not_before"] = not_before
    frame = engine.run(query, **tables)
    return AnchorMatch(
        frame=frame,
        path_col=path_col,
        tokens=literal_tokens(pattern),
        part_ordinals=tuple(part_ordinals),
    )


# ── anchor specs: a pattern plus where in it, which occurrence, and an offset ──

SPEC_KEYS = frozenset({"pattern", "at", "occurrence", "offset"})


@dataclass(frozen=True)
class AnchorSpec:
    """A window bound: a pattern, which of its tokens, which occurrence, and an offset."""

    pattern: str
    at: int | str = "end"
    occurrence: str = "first"
    offset: int | str | pd.Timedelta | None = None

    def ordinal(self) -> int:
        """`at` as a token ordinal; ``"start"``/``"end"`` are aliases for 0/-1."""
        if self.at == "start":
            return 0
        if self.at == "end":
            return -1
        if isinstance(self.at, bool) or not isinstance(self.at, int):
            raise InvalidParameterError("at", self.at, ["start", "end", "an integer"])
        return self.at


def parse_spec(value: object, *, param: str = "anchor") -> AnchorSpec:
    """
    Normalize one anchor bound into an :class:`AnchorSpec`.

    A bare string is the degenerate spec ``{"pattern": value}`` — which keeps
    ``start_event="path_start"`` and the rest of the plain string form working
    exactly as before.
    """
    if isinstance(value, str):
        return AnchorSpec(pattern=normalize_pattern(value, warn=False, param=param))
    if isinstance(value, AnchorSpec):
        return value
    if not isinstance(value, dict):
        raise InvalidParameterError(
            param, value, ["an event name, an anchor spec dict, or a list of either"]
        )

    unknown = set(value) - SPEC_KEYS
    if unknown:
        raise InvalidParameterError(param, sorted(unknown), sorted(SPEC_KEYS))
    if "pattern" not in value:
        raise InvalidParameterError(param, value, ["an anchor spec with a 'pattern'"])

    spec = AnchorSpec(
        pattern=normalize_pattern(value["pattern"], warn=False, param=param),
        at=value.get("at", "end"),
        occurrence=value.get("occurrence", "first"),
        offset=value.get("offset"),
    )
    if spec.occurrence not in OCCURRENCES:
        raise InvalidParameterError("occurrence", spec.occurrence, list(OCCURRENCES))
    spec.ordinal()  # validate `at` eagerly, before any query runs
    return spec


def parse_specs(value: object, *, param: str = "anchor") -> list[AnchorSpec]:
    """Normalize an anchor bound that may be a single spec or a list of them."""
    if isinstance(value, (str, dict, AnchorSpec)):
        return [parse_spec(value, param=param)]
    if isinstance(value, (list, tuple)):
        if not value:
            raise InvalidParameterError(param, value, ["a non-empty list of anchors"])
        return [parse_spec(item, param=param) for item in value]
    raise InvalidParameterError(
        param, value, ["an event name, an anchor spec dict, or a list of either"]
    )


def _offset_query(
    schema: "EventstreamSchema",
    path_col: str,
    offset: int | float,
    side: str,
    *,
    in_steps: bool,
) -> str:
    """
    SQL resolving an anchor row to a bound `offset` away from it.

    An offset that runs past the path's own boundary clamps to it — "10 steps
    after B" on a path with 4 events left is those 4 events, not an empty
    window and not a dropped path.
    """
    path_q = engine.quote_ident(path_col)
    index_q = engine.quote_ident(schema.index)
    subindex_q = engine.quote_ident(schema.subindex)
    ts_q = engine.quote_ident(schema.timestamp_col)

    base = f"""
        SELECT {path_q} AS p, {index_q} AS idx, {ts_q} AS ts,
               row_number() OVER (
                   PARTITION BY {path_q} ORDER BY {index_q}, {subindex_q}
               ) AS step
        FROM df
    """
    edges = "SELECT p, MIN(idx) AS min_idx, MAX(idx) AS max_idx, MAX(step) AS max_step FROM base GROUP BY p"

    if in_steps:
        # Out of range in either direction clamps to the corresponding edge.
        return f"""
            WITH base AS ({base}), edges AS ({edges}),
            want AS (
                SELECT a.{path_q} AS p, a.step + {int(offset)} AS want FROM anchor a
            )
            SELECT w.p AS {path_q},
                   COALESCE(
                       b.idx,
                       CASE WHEN w.want > e.max_step THEN e.max_idx ELSE e.min_idx END
                   ) AS bound
            FROM want w
            JOIN edges e ON e.p = w.p
            LEFT JOIN base b ON b.p = w.p AND b.step = w.want
        """

    # A time offset lands on a timestamp, not on an event: a start bound takes
    # the first event at or after the mark, an end bound the last one at or
    # before it. Exact hits are inside the window on both sides.
    mark = f"""
        SELECT a.{path_q} AS p, b.ts + {dialect.interval_seconds(offset)} AS mark
        FROM anchor a JOIN base b ON b.p = a.{path_q} AND b.idx = a.bound
    """
    if side == "start":
        pick, fallback, cmp = "MIN(b.idx)", "e.max_idx", "b.ts >= m.mark"
    else:
        pick, fallback, cmp = "MAX(b.idx)", "e.min_idx", "b.ts <= m.mark"
    return f"""
        WITH base AS ({base}), edges AS ({edges}), mark AS ({mark})
        SELECT m.p AS {path_q}, COALESCE({pick}, {fallback}) AS bound
        FROM mark m
        JOIN edges e ON e.p = m.p
        LEFT JOIN base b ON b.p = m.p AND {cmp}
        GROUP BY m.p, {fallback}
    """


def resolve_bound(
    df: pd.DataFrame,
    schema: "EventstreamSchema",
    spec: AnchorSpec,
    *,
    side: str,
    path_col: str | None = None,
    event_col: str | None = None,
    not_before: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Resolve one :class:`AnchorSpec` to a per-path window bound.

    Parameters
    ----------
    side : {"start", "end"}
        Which end of the window this bound is. Only matters for a time offset,
        whose mark falls between events and so has to round outward-consistently
        (see :func:`_offset_query`).

    Returns
    -------
    pandas.DataFrame
        Two columns — `path_col` and ``"bound"``, the ``schema.index`` value the
        window is bounded at. Paths whose pattern did not match are absent.
    """
    if side not in ("start", "end"):
        raise InvalidParameterError("side", side, ["start", "end"])

    path_col = path_col or schema.path_col

    # Which gap-separated part carries the anchor token — everything from there
    # on is what `not_before` may constrain (see :func:`resolve_anchors`).
    ordinal = spec.ordinal()
    tokens = literal_tokens(spec.pattern)
    resolved_ordinal = ordinal if ordinal >= 0 else len(tokens) + ordinal
    starts = []
    running = 0
    for part in split_parts(spec.pattern):
        starts.append(running)
        running += len(part)
    anchor_part = max(i for i, start in enumerate(starts) if start <= resolved_ordinal)

    match = resolve_anchors(
        df,
        schema,
        spec.pattern,
        occurrence=spec.occurrence,
        path_col=path_col,
        event_col=event_col,
        not_before=not_before,
        not_before_part=anchor_part,
    )
    anchor = match.at(spec.ordinal())[[path_col, "step", "index"]].rename(
        columns={"index": "bound"}
    )
    if spec.offset is None or anchor.empty:
        return anchor[[path_col, "bound"]]

    if isinstance(spec.offset, bool):
        raise InvalidParameterError(
            "offset", spec.offset, ["a step count (int) or a duration"]
        )
    if isinstance(spec.offset, int):
        seconds, in_steps = spec.offset, True
    else:
        seconds, in_steps = parse_duration(spec.offset, param="offset"), False

    query = _offset_query(schema, path_col, seconds, side, in_steps=in_steps)
    return engine.run(query, df=df, anchor=anchor)
