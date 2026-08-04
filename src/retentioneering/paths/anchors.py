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

* a pattern is a ``->``-separated sequence of event names, where ``.*`` stands
  for any run of events, *including an empty one* — ``"A->.*->B"`` matches both
  ``A->X->B`` and ``A->B``;
* tokens not separated by ``.*`` must be strictly adjacent in the path;
* a pattern matches anywhere in the path — no implicit anchoring at either end.

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
from retentioneering.exceptions import InvalidParameterError
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

PATH_START = EventTypes().PATH_START.name
PATH_END = EventTypes().PATH_END.name
GAP = ".*"

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

    collapsed: list[str] = []
    for token in tokens:
        if token == GAP and collapsed and collapsed[-1] == GAP:
            continue
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


def split_parts(pattern: str) -> list[list[str]]:
    """Split a normalized pattern into runs of strictly adjacent tokens, separated by gaps."""
    parts: list[list[str]] = []
    current: list[str] = []
    for token in pattern.split(PATH_DELIMITER):
        if token == GAP:
            if current:
                parts.append(current)
                current = []
        else:
            current.append(token)
    if current:
        parts.append(current)
    return parts


def literal_tokens(pattern: str) -> list[str]:
    """The pattern's event-name tokens, in order, with gaps removed.

    These are the positions an anchor's ``at`` indexes into — ``.*`` is not a
    position, so it is not counted.
    """
    return [t for t in pattern.split(PATH_DELIMITER) if t != GAP]


def validate_pattern_tokens(
    pattern: str, available_events: Iterable[str], *, param: str = "pattern"
) -> None:
    """
    Guard against typos: a mistyped event name would otherwise silently match
    nothing, indistinguishable from a legitimately empty result.

    `param` names the caller's user-facing parameter, so the error points at
    ``path_pattern`` / ``start_event`` rather than at this module's internals.
    """
    available = set(available_events)
    for token in literal_tokens(pattern):
        if token in (PATH_START, PATH_END):
            continue
        if token not in available:
            raise InvalidParameterError(param, token, sorted(available))


def _has_boundary_rows(df: pd.DataFrame, schema: "EventstreamSchema") -> bool:
    type_col = schema.event_type
    if type_col not in df.columns:
        return False
    types = EventTypes()
    return bool(df[type_col].isin([types.PATH_START.type, types.PATH_END.type]).any())


def _part_condition(tokens: Sequence[str], alias: str) -> str:
    """SQL predicate: a match of `tokens` *starts* at this row."""
    clauses = [f"{alias}.{_EVENT} = {quote_literal(tokens[0])}"]
    for lead, token in enumerate(tokens[1:], start=1):
        clauses.append(f"{alias}.__rete_e{lead} = {quote_literal(token)}")
    return " AND ".join(clauses)


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
    parts = split_parts(pattern)
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

    # One CTE per pattern part, chained so that each part is matched relative to
    # the neighbour already pinned down. For "first" that means left to right,
    # taking the earliest position at or after the previous part's end; "last" is
    # the exact mirror, right to left.
    order = range(len(parts)) if occurrence == "first" else reversed(range(len(parts)))
    floor_join = ""
    floor_cond = ""
    if not_before is not None:
        floor_join = f"JOIN not_before nb ON l.{_PATH} = nb.{path_q}"
        floor_cond = f" AND l.{_IDX} >= nb.bound"

    previous: int | None = None
    for i in order:
        tokens = parts[i]
        cond = _part_condition(tokens, "l")
        # The floor constrains only the anchor's own part and whatever follows
        # it; earlier parts are free to sit before the window opened.
        join = floor_join if i >= not_before_part else ""
        extra = floor_cond if i >= not_before_part else ""
        if previous is None:
            agg = "MIN" if occurrence == "first" else "MAX"
            ctes.append(
                f"""m{i} AS (
                    SELECT l.{_PATH}, {agg}(l.{_STEP}) AS s
                    FROM leads l {join} WHERE {cond}{extra} GROUP BY l.{_PATH}
                )"""
            )
        elif occurrence == "first":
            prev_end = f"m{previous}.s + {len(parts[previous]) - 1}"
            ctes.append(
                f"""m{i} AS (
                    SELECT l.{_PATH}, MIN(l.{_STEP}) AS s
                    FROM leads l JOIN m{previous} ON l.{_PATH} = m{previous}.{_PATH} {join}
                    WHERE {cond} AND l.{_STEP} > {prev_end}{extra}
                    GROUP BY l.{_PATH}
                )"""
            )
        else:
            this_end = f"l.{_STEP} + {len(tokens) - 1}"
            ctes.append(
                f"""m{i} AS (
                    SELECT l.{_PATH}, MAX(l.{_STEP}) AS s
                    FROM leads l JOIN m{previous} ON l.{_PATH} = m{previous}.{_PATH} {join}
                    WHERE {cond} AND {this_end} < m{previous}.s{extra}
                    GROUP BY l.{_PATH}
                )"""
            )
        previous = i

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
