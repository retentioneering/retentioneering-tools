"""
Sentinel for "argument not given" where `None` is itself a meaningful value.

`get_metric_distribution(segment_level=None)` selects the paths whose segment
value is missing, so the mutually-exclusive `segment_level` / `segment_levels`
modes cannot be told apart by a `None` default.
"""


class _Unset:
    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "<unset>"


UNSET = _Unset()
