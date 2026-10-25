"""
``occurrence="non_overlapping"``: a pattern's matches one after another, as
:func:`re.finditer` finds them.

The other occurrence modes answer "where can this token be?" — `"first"` and
`"last"` with one position per path, `"all"` with every position any match
can give it, overlapping matches included. Marking each *instance* of a
pattern needs a different question: scan the path left to right, take the
leftmost match, resume after it. That is what regex does, so this module
does not reimplement it: each path is encoded as a string, one character per
event, the pattern is compiled to an ordinary Python regex over those
characters, and :func:`re.finditer` supplies the semantics — leftmost,
non-overlapping, with backtracking.

Two choices make the result what an analyst expects rather than what a
literal regex translation would give:

* **A repeated group is greedy.** ``(B){3,}`` consumes the whole run of
  ``B``, so ``B×5`` is one match, not a match of three followed by two
  leftover ``B``. Its first copy is the run's first repetition and its other
  copies the run's last ones, so ``at="start"`` is where the run begins and
  ``at="end"`` where it ends.
* **A gap is lazy.** ``A->.*->B`` pairs each ``A`` with the *next* ``B``
  (regex ``.*?``); a greedy gap would make one match from the first ``A`` to
  the last ``B`` and hide every pair in between.

Positions come from capture groups, one per positional token, emitted in the
order :func:`tokens.expand_groups` unrolls them — so the capture index *is* the
ordinal of the normalized pattern, and ``at=`` addresses the same token here as
in every other mode.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pandas as pd

from retentioneering import engine
from retentioneering.paths import tokens as tokens_mod
from retentioneering.utils.sequences import PATH_DELIMITER
from retentioneering.utils.sql_quoting import quote_literal

if TYPE_CHECKING:
    from retentioneering.eventstream.schema import EventstreamSchema

__all__ = ["compile_pattern", "resolve"]

_NEVER = "(?!)"

# Private-use code points: no event name can collide with regex syntax, and
# there are enough of them for any real vocabulary.
_PLANES = ((0xE000, 0xF8FF), (0xF0000, 0xFFFFD), (0x100000, 0x10FFFD))


def _alphabet(events: list[str]) -> dict[str, str]:
    """One private-use character per event, sentinels included."""
    names = list(dict.fromkeys([*tokens_mod.SENTINELS, *events]))
    chars: dict[str, str] = {}
    planes = iter(_PLANES)
    low, high = next(planes)
    point = low
    for name in names:
        if point > high:
            low, high = next(planes)
            point = low
        chars[name] = chr(point)
        point += 1
    return chars


def _class(token: "tokens_mod.EventClass", chars: dict[str, str]) -> str:
    present = [chars[m] for m in token.members if m in chars]
    if not token.negated:
        return f"[{''.join(map(re.escape, present))}]" if present else _NEVER
    # A sentinel is excluded unless named — and naming it in a negated class
    # excludes it too — so both are always out (see `tokens` docstring).
    excluded = present + [chars[s] for s in tokens_mod.SENTINELS]
    return f"[^{''.join(map(re.escape, excluded))}]"


def _token(raw: str, chars: dict[str, str], *, capture: bool) -> str:
    parsed = tokens_mod.parse_token(raw)
    if isinstance(parsed, tokens_mod.Gap):
        base = (
            _class(tokens_mod.ANY, chars)
            if parsed.constraint is None
            else _class(parsed.constraint, chars)
        )
        high = "" if parsed.max_len is None else str(parsed.max_len)
        return f"{base}{{{parsed.min_len},{high}}}?"  # lazy: see module docstring
    if isinstance(parsed, str):
        piece = re.escape(chars[parsed]) if parsed in chars else _NEVER
    else:
        piece = _class(parsed, chars)
    return f"({piece})" if capture else f"(?:{piece})"


def _sequence(body, chars: dict[str, str], *, capture: bool) -> str:
    return "".join(_token(t, chars, capture=capture) for t in body)


def compile_pattern(pattern: str, chars: dict[str, str]) -> tuple[re.Pattern, int]:
    """
    Compile `pattern` to a regex over `chars`; return it with its capture count.

    A group ``(P){k}`` becomes `k` captured copies of ``P``. Open-ended
    (``{k,}`` / ``+``), it becomes the first copy, then a greedy uncaptured
    ``(?:P)*``, then the remaining ``k - 1`` copies — so the extra repetitions
    sit between the first copy and the last ones, and the captures land on the
    run's first and last repetitions.
    """
    raw_tokens = pattern.split(PATH_DELIMITER)
    tokens_mod.check_class_spans(raw_tokens)
    while raw_tokens and tokens_mod.is_plain_gap(raw_tokens[0]):
        raw_tokens.pop(0)
    while raw_tokens and tokens_mod.is_plain_gap(raw_tokens[-1]):
        raw_tokens.pop()

    pieces: list[str] = []
    for item in tokens_mod.read_items(raw_tokens, pattern):
        if not isinstance(item, tokens_mod.Group):
            pieces.append(_token(item, chars, capture=True))
            continue
        copy = _sequence(item.body, chars, capture=True)
        pieces.append(copy)
        if item.open:
            pieces.append(f"(?:{_sequence(item.body, chars, capture=False)})*")
        pieces.extend([copy] * (item.copies - 1))
    regex = re.compile("".join(pieces))
    return regex, regex.groups


def _stepped(
    df: pd.DataFrame, schema: "EventstreamSchema", path_col: str, event_col: str
) -> pd.DataFrame:
    """Every path's events in order, with steps numbered as `resolve_anchors` does."""
    from retentioneering.paths.anchors import _has_boundary_rows

    path_q = engine.quote_ident(path_col)
    event_q = engine.quote_ident(event_col)
    index_q = engine.quote_ident(schema.index)
    subindex_q = engine.quote_ident(schema.subindex)
    base = f"""
        SELECT {path_q} AS p, {event_q} AS e, {index_q} AS idx,
               row_number() OVER (
                   PARTITION BY {path_q} ORDER BY {index_q}, {subindex_q}
               ) AS step
        FROM df
    """
    if _has_boundary_rows(df, schema):
        stepped = "SELECT p, e, idx, step FROM base"
    else:
        start, end = (quote_literal(s) for s in tokens_mod.SENTINELS)
        stepped = f"""
            SELECT p, e, idx, step FROM base
            UNION ALL
            SELECT p, {start}, MIN(idx), 0 FROM base GROUP BY p
            UNION ALL
            SELECT p, {end}, MAX(idx), MAX(step) + 1 FROM base GROUP BY p
        """
    return engine.run(
        f"WITH base AS ({base}) {stepped} ORDER BY p, step", df=df
    ).rename(columns={"p": path_col})


def resolve(
    df: pd.DataFrame,
    schema: "EventstreamSchema",
    pattern: str,
    *,
    path_col: str,
    event_col: str,
    ordinals: int,
) -> pd.DataFrame:
    """
    Every non-overlapping match of `pattern` in every path.

    Returns the long frame :func:`anchors.resolve_anchors` returns — one row per
    positional token per match, columns `path_col`, ``ordinal``, ``step``,
    ``index`` — with each match's rows describing that one match.
    """
    stepped = _stepped(df, schema, path_col, event_col)
    columns = [path_col, "ordinal", "step", "index"]
    if stepped.empty:
        return pd.DataFrame(columns=columns)

    chars = _alphabet(stepped["e"].dropna().unique().tolist())
    regex, groups = compile_pattern(pattern, chars)
    if groups != ordinals:  # pragma: no cover - guards the ordinal contract
        raise AssertionError(
            f"{pattern!r} compiled to {groups} captures for {ordinals} positions"
        )

    stepped["ch"] = stepped["e"].map(chars).fillna("\x00")
    stepped["pos"] = stepped.groupby(path_col, sort=False).cumcount()
    encoded = stepped.groupby(path_col, sort=False)["ch"].agg("".join)

    paths, ords, positions = [], [], []
    for path, text in encoded.items():
        for match in regex.finditer(text):
            if match.end() == match.start():
                continue  # cannot happen with a positional token; be safe
            for ordinal in range(groups):
                paths.append(path)
                ords.append(ordinal)
                positions.append(match.start(ordinal + 1))
    if not paths:
        return pd.DataFrame(columns=columns)

    hits = pd.DataFrame({path_col: paths, "ordinal": ords, "pos": positions})
    lookup = stepped[[path_col, "pos", "step", "idx"]]
    frame = hits.merge(lookup, on=[path_col, "pos"], how="left").rename(
        columns={"idx": "index"}
    )
    return frame[columns].sort_values([path_col, "ordinal", "step"], ignore_index=True)
