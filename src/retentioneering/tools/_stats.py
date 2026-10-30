"""Compatibility namespace for the shared statistical primitives.

All formulas live in retentioneering.stats.primitives so the experiment,
scorecard, conversion, funnel and sample-ratio adapters use one implementation.
"""

from retentioneering.stats import primitives

__all__ = primitives.__all__


def __getattr__(name):
    return getattr(primitives, name)
