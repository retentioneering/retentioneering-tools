"""Shared helpers for widget modules."""

from __future__ import annotations

import json


def distribution_levels(req: dict) -> dict:
    """
    Pick the `get_metric_distribution` mode out of a JS request.

    The front end sends `segment_levels` (a pair) or `segment_level` (one level
    against every other level of the column); mirroring the Python signature
    keeps the mutual exclusivity expressed in exactly one place.
    """
    if req.get("segment_levels") is not None:
        return {"segment_levels": req["segment_levels"]}
    return {"segment_level": req["segment_level"]}


def step_matrix_blocks(raw, diff, path_pattern):
    """Normalize `step_matrix_data`/`step_sankey_data`'s return value back to
    the always-tuple-of-blocks form: without `path_pattern` those methods
    collapse a single block to a bare DataFrame (or a flat diff triple) for
    ergonomics, but widget rendering needs the uniform per-block tuples."""
    if path_pattern is not None:
        return raw
    if diff is None:
        return (raw,)
    combined, group1, group2 = raw
    return (combined,), (group1,), (group2,)


def pattern_edges(path_pattern, anchor=None) -> tuple[bool, bool]:
    """Whether the rendered strip reaches each of the path's own boundaries.

    `(starts_at_path_start, ends_at_path_end)`. A strip that does not reach a
    boundary is drawn with a serrated edge there, since the paths continue past
    what is shown.

    Derived from the pattern's parsed structure, never from the string: a
    pattern that merely *mentions* a sentinel (`cart->[^path_end]*->purchase`)
    does not end at one, and a substring test would say it does.
    """
    from retentioneering.eventstream.event_type import EventTypes
    from retentioneering.paths import anchors

    if not path_pattern and anchor is None:
        # Neither: the matrix is laid out from the path's first step and cut at
        # max_steps, so it starts at the boundary and does not reach the end.
        return True, False

    types = EventTypes()
    if not path_pattern:
        # An anchor is a single position: it reaches a boundary only by being
        # one.
        spec = anchors.parse_spec(anchor, param="anchor")
        tokens = anchors.literal_tokens(spec.pattern)
        token = tokens[spec.ordinal()]
        return token == types.PATH_START.name, token == types.PATH_END.name

    parts = anchors.split_parts(path_pattern)
    return parts[0][0] == types.PATH_START.name, parts[-1][-1] == types.PATH_END.name


def parse_diff(raw) -> list | None:
    """Parse a diff traitlet value (JSON string or list) into [seg, v1, v2] or
    [path_ids1, path_ids2], or None."""
    if not raw:
        return None
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else list(raw)
        if isinstance(parsed, list) and len(parsed) in (2, 3):
            return parsed
    except Exception:
        pass
    return None


#: Extra steps computed beyond a requested `step_window`, so that widening the
#: window by a notch or two costs nothing rather than a recompute per notch.
STEP_WINDOW_HEADROOM = 10


def max_steps_for_window(step_window: int, max_steps: int) -> int:
    """`max_steps` wide enough to fill a `step_window`-column view.

    `step_window` only slices columns that `max_steps` already computed, so a
    window wider than the data silently shows fewer steps than asked for. When
    that happens, grow `max_steps` past the window instead — see
    `STEP_WINDOW_HEADROOM`. Never shrinks: a narrowed window keeps whatever
    depth was already computed, so widening it back is instant.
    """
    if step_window > max_steps:
        return step_window + STEP_WINDOW_HEADROOM
    return max_steps
