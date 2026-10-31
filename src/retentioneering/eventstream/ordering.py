"""
Event order within a path (ADR-0017).

Events are ordered by `(index, subindex)`, unique within a path. `index` is a
position, assigned once at construction; rows added later reuse the index of
the row they were derived from. `subindex` orders the rows sharing an index:
the event type's rank from `EventTypes`, plus a fraction when several markers
of one rank stand at the same position.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from retentioneering.eventstream.schema import EventstreamSchema

# Absorbs float error when recovering n from r + n/(n+1): 2/3 must read back as
# n = 2, not 1.999….
_EPS = 1e-9


def marker_subindex(
    df: pd.DataFrame, new: pd.DataFrame, schema: EventstreamSchema, rank: int
) -> np.ndarray:
    """
    `subindex` values for marker rows `new` of rank `rank`, about to join `df`.

    The n-th marker (n = 0, 1, 2, …) of one rank at one position gets
    `rank + n/(n+1)` — rank, rank + 1/2, rank + 2/3, … — strictly increasing
    and never reaching the next rank. New markers go after the markers of the
    same rank already at that position, and among themselves keep the order of
    the rows they were derived from. Counting starts after the largest value
    present, not after the number of rows, so a position some of whose markers
    were filtered out still gets a fresh value.

    `new` must carry the source rows' `index` and `subindex`; the result is
    aligned with `new`'s row order.
    """
    keys = [schema.path_col, schema.index]
    sub = schema.subindex
    if new.empty:
        return np.empty(0, dtype="float64")

    in_rank = (df[sub] >= rank) & (df[sub] < rank + 1)
    taken = (
        df.loc[in_rank, keys + [sub]]
        .groupby(keys, observed=True, sort=False)[sub]
        .max()
        .rename("_taken")
        .reset_index()
    )
    # The n of the largest value present, plus one.
    frac = taken["_taken"].to_numpy(dtype="float64") - rank
    taken["_next"] = np.floor(frac / (1.0 - frac) + _EPS) + 1

    work = new[keys + [sub]].copy()
    work["_row"] = np.arange(len(work))
    work = work.sort_values(keys + [sub, "_row"], kind="stable")
    work = work.merge(taken[keys + ["_next"]], on=keys, how="left", sort=False)
    n = work["_next"].fillna(0).to_numpy() + work.groupby(
        keys, observed=True, sort=False
    ).cumcount().to_numpy(dtype="float64")

    out = np.empty(len(work), dtype="float64")
    out[work["_row"].to_numpy()] = rank + n / (n + 1)
    return out
