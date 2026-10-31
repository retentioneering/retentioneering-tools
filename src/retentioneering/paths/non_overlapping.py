"""Leftmost non-overlapping matches with bounded, memoized state evaluation.

Groups prefer another repetition; gaps prefer their exit. An ordered Thompson
program expresses those choices without invoking a backtracking regex engine.
Each (instruction, input position) is evaluated at most once per path, including
failed searches and subsequent matches. Captures are shared linked suffixes, not
copies of the whole match. Resource limits fail explicitly rather than returning
partial data or consuming unbounded memory.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from retentioneering import engine
from retentioneering.exceptions import (
    InvalidParameterError,
    PatternMatchLimitError,
    PatternSyntaxError,
)
from retentioneering.paths import tokens as tokens_mod
from retentioneering.utils.sequences import PATH_DELIMITER
from retentioneering.utils.sql_quoting import quote_literal

if TYPE_CHECKING:
    from retentioneering.eventstream.schema import EventstreamSchema

MAX_PROGRAM_STATES = 4096
MAX_PATH_STATES = 2_000_000


@dataclass
class Instruction:
    kind: str
    out: int = 0
    alternative: int = 0
    allowed: frozenset[int] = frozenset()
    # None: gap; -1: an uncaptured positional token; >=0: token ordinal.
    slot: int | None = None


@dataclass(frozen=True, slots=True)
class Capture:
    slot: int
    position: int
    following: Capture | None


@dataclass(frozen=True, slots=True)
class Result:
    end: int
    captures: Capture | None = None


@dataclass(frozen=True)
class Match:
    start: int
    end: int  # inclusive last positional token, not the last capture
    positions: tuple[int, ...]


@dataclass
class Program:
    instructions: list[Instruction]
    start: int
    groups: int

    def finditer(self, values, start=0, end=None, *, max_states=MAX_PATH_STATES):
        """Memoized ordered matching, iterative even for very long paths.

        Cache size is O(path length * program size), capped by max_states.
        Epsilon edges are acyclic: every repeated group contains a position,
        and every gap's loop must consume an event before returning.
        """
        end = len(values) if end is None else end
        memo: dict[tuple[int, int], Result | None] = {}

        def solve(pc, pos):
            stack = [(pc, pos)]
            while stack:
                key = stack[-1]
                if key in memo:
                    stack.pop()
                    continue
                if len(memo) + len(stack) > max_states:
                    raise PatternMatchLimitError(
                        f"non_overlapping exceeded {max_states:,} matching states "
                        "in one path. Split the path or simplify the pattern; "
                        "no partial result was returned."
                    )
                ip, offset = key
                op = self.instructions[ip]
                if op.kind == "accept":
                    memo[key] = Result(offset)
                    continue
                if op.kind == "consume":
                    if offset >= end or values[offset] not in op.allowed:
                        memo[key] = None
                        continue
                    choices = [(op.out, offset + 1)]
                else:
                    choices = [(op.out, offset), (op.alternative, offset)]
                for child in choices:
                    if child not in memo:
                        stack.append(child)
                        break
                    suffix = memo[child]
                    if suffix is not None:
                        if op.kind == "consume" and op.slot is not None:
                            suffix = Result(
                                suffix.end, Capture(op.slot, offset, suffix.captures)
                            )
                        memo[key] = suffix
                        break
                else:
                    memo[key] = None
            return memo[(pc, pos)]

        cursor = start
        while cursor < end:
            result = solve(self.start, cursor)
            if result is None:
                cursor += 1
                continue
            positions = [0] * self.groups
            capture = result.captures
            if capture is None or result.end <= cursor:
                raise AssertionError("A match must contain a positional token")
            first = capture.position
            last = first
            while capture is not None:
                last = capture.position
                if capture.slot >= 0:
                    positions[capture.slot] = capture.position
                capture = capture.following
            yield Match(first, last, tuple(positions))
            cursor = result.end


def compile_pattern(pattern: str, codes: dict[str, int]) -> Program:
    """Compile validated tokens to ordered consume/split/accept instructions."""
    raw = pattern.split(PATH_DELIMITER)
    tokens_mod.check_class_spans(raw)
    while raw and tokens_mod.is_plain_gap(raw[0]):
        raw.pop(0)
    while raw and tokens_mod.is_plain_gap(raw[-1]):
        raw.pop()
    instructions = [Instruction("accept")]
    groups = 0

    def emit(op):
        if len(instructions) >= MAX_PROGRAM_STATES:
            raise PatternMatchLimitError(
                f"non_overlapping supports at most {MAX_PROGRAM_STATES} program "
                "states. Reduce bounded gap lengths or repetition counts."
            )
        instructions.append(op)
        return len(instructions) - 1

    def allowed(token):
        if isinstance(token, tokens_mod.Gap):
            # SQL gap_violation_sql does not reject NULL or boundary rows,
            # even for a restricted gap. Positional classes do reject NULL.
            return frozenset(
                [0] + [code for name, code in codes.items() if token.allows(name)]
            )
        return frozenset(
            code
            for name, code in codes.items()
            if (name == token if isinstance(token, str) else token.matches(name))
        )

    def sequence(items, tail):
        for token, slot in reversed(items):
            admitted = allowed(token)
            if isinstance(token, tokens_mod.Gap):
                if token.max_len is None:
                    split = emit(Instruction("split", tail))
                    repeat = emit(Instruction("consume", split, allowed=admitted))
                    instructions[split].alternative = repeat  # exit first: lazy
                    tail = split
                else:
                    for _ in range(token.max_len - token.min_len):
                        take = emit(Instruction("consume", tail, allowed=admitted))
                        tail = emit(Instruction("split", tail, take))
                for _ in range(token.min_len):
                    tail = emit(Instruction("consume", tail, allowed=admitted))
            else:
                tail = emit(Instruction("consume", tail, allowed=admitted, slot=slot))
        return tail

    def body(raw_body, captured):
        nonlocal groups
        out = []
        for raw_token in raw_body:
            token = tokens_mod.parse_token(raw_token)
            slot = None
            if not isinstance(token, tokens_mod.Gap):
                slot = groups if captured else -1
                groups += int(captured)
            out.append((token, slot))
        return out

    pieces = []
    for item in tokens_mod.read_items(raw, pattern):
        if not isinstance(item, tokens_mod.Group):
            pieces.append(("sequence", body([item], True)))
            continue
        if all(tokens_mod.is_gap(t) for t in item.body):
            raise PatternSyntaxError(
                "A repeated group must contain a positional token."
            )
        if item.copies > MAX_PROGRAM_STATES:
            raise PatternMatchLimitError("Too many group copies for non_overlapping.")
        pieces.append(("sequence", body(item.body, True)))
        if item.open:
            pieces.append(("repeat", body(item.body, False)))
        for _ in range(item.copies - 1):
            pieces.append(("sequence", body(item.body, True)))
    tail = 0
    for kind, items in reversed(pieces):
        if kind == "repeat":
            split = emit(Instruction("split", alternative=tail))
            instructions[split].out = sequence(items, split)  # repeat first: greedy
            tail = split
        else:
            tail = sequence(items, tail)
    return Program(instructions, tail, groups)


def _stepped_sql(df, schema, path_col, event_col):
    """Number steps exactly as the SQL matcher, including virtual boundaries."""
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
    return f"base AS ({base}), stepped AS ({stepped})"


def resolve(df, schema: EventstreamSchema, pattern, *, path_col, event_col, ordinals):
    """Long capture table plus explicit per-match identity and complete bounds."""
    if df.duplicated([path_col, schema.index, schema.subindex]).any():
        raise InvalidParameterError(
            "event order",
            "duplicate (path, index, subindex) rows",
            ["unique ordered rows; rebuild annotations from the raw stream"],
        )
    columns = [
        path_col,
        "ordinal",
        "step",
        "index",
        "match_id",
        "start_step",
        "end_step",
        "start_index",
        "end_index",
    ]
    event_q = engine.quote_ident(event_col)
    vocabulary = engine.run(
        f"SELECT DISTINCT CAST({event_q} AS VARCHAR) AS e FROM df "
        f"WHERE {event_q} IS NOT NULL",
        df=df,
    )["e"].tolist()
    names = list(dict.fromkeys([*tokens_mod.SENTINELS, *vocabulary]))
    codes = {name: i + 1 for i, name in enumerate(names)}
    program = compile_pattern(pattern, codes)
    if program.groups != ordinals:
        raise AssertionError(
            f"{pattern!r}: {program.groups} captures for {ordinals} positions"
        )
    alphabet = pd.DataFrame({"e": list(codes), "code": list(codes.values())})
    rows = engine.run(
        f"""
        WITH {_stepped_sql(df, schema, path_col, event_col)}
        SELECT s.p, s.step, s.idx, COALESCE(a.code, 0) AS code
        FROM stepped s LEFT JOIN alphabet a ON a.e = CAST(s.e AS VARCHAR)
        ORDER BY s.p, s.step
        """,
        df=df,
        alphabet=alphabet,
    )
    if rows.empty:
        return pd.DataFrame(columns=columns)
    values = rows.code.to_numpy()
    path_ids = rows.p.to_numpy()
    steps, indices = rows.step.to_numpy(), rows.idx.to_numpy()
    breaks = np.flatnonzero(path_ids[1:] != path_ids[:-1]) + 1
    starts, ends = np.r_[0, breaks], np.r_[breaks, len(rows)]
    found = []
    for start, end in zip(starts.tolist(), ends.tolist()):
        for mid, match in enumerate(program.finditer(values, start, end), 1):
            for ordinal, at in enumerate(match.positions):
                found.append(
                    (
                        path_ids[at],
                        ordinal,
                        steps[at],
                        indices[at],
                        mid,
                        steps[match.start],
                        steps[match.end],
                        indices[match.start],
                        indices[match.end],
                    )
                )
    return pd.DataFrame(found, columns=columns).sort_values(
        [path_col, "ordinal", "step"], ignore_index=True
    )
