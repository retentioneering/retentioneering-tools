"""Shared helpers for widget modules."""

from __future__ import annotations

import json


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
