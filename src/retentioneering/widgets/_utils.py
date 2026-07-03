"""Shared helpers for widget modules."""

from __future__ import annotations

import json


def parse_diff(raw) -> list | None:
    """Parse a diff traitlet value (JSON string or list) into [seg, v1, v2] or None."""
    if not raw:
        return None
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else list(raw)
        if isinstance(parsed, list) and len(parsed) == 3:
            return parsed
    except Exception:
        pass
    return None
