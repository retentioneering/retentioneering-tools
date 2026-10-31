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


# ---------------------------------------------------------------------------
# Layout: within a path, rows strictly increase in (index, subindex), and each
# path is one contiguous block. The order of paths relative to each other is
# not part of the contract.
# ---------------------------------------------------------------------------

#: Set by the test suite (tests/conftest.py). Every eventstream a processor
#: returns is then checked, opted out or not, and one that a processor laid
#: out wrongly raises `EventOrderError` instead of being re-sorted quietly.
STRICT = False


class EventOrderError(AssertionError):
    """A processor returned rows out of event order (a library bug)."""


def _path_values(df: pd.DataFrame, schema: EventstreamSchema) -> np.ndarray:
    col = df[schema.path_col]
    if isinstance(col.dtype, pd.CategoricalDtype):
        return col.cat.codes.to_numpy()
    return col.to_numpy()


def _grows_within_paths(p: np.ndarray, ix: np.ndarray, sx: np.ndarray) -> bool:
    """Neighbours of one path strictly increase in (index, subindex)."""
    same = p[1:] == p[:-1]
    grow = (ix[1:] > ix[:-1]) | ((ix[1:] == ix[:-1]) & (sx[1:] > sx[:-1]))
    return not bool(np.any(same & ~grow))


def _has_missing_position(df: pd.DataFrame, schema: EventstreamSchema) -> bool:
    return bool(df[schema.index].isna().any() or df[schema.subindex].isna().any())


def is_laid_out(df: pd.DataFrame, schema: EventstreamSchema) -> bool:
    """
    One pass over neighbours: path non-decreasing (so each path is one block)
    and `(index, subindex)` strictly increasing within it. This is what a
    processor that ends in `ORDER BY path, event_order(...)` produces.
    """
    if len(df) < 2:
        return not _has_missing_position(df, schema)
    p = _path_values(df, schema)
    ix = df[schema.index].to_numpy()
    sx = df[schema.subindex].to_numpy()
    try:
        if not bool(np.all(p[1:] >= p[:-1])):
            return False
    except TypeError:  # path ids of mixed types: let the sort decide
        return False
    return _grows_within_paths(p, ix, sx) and not _has_missing_position(df, schema)


def is_laid_out_any_path_order(df: pd.DataFrame, schema: EventstreamSchema) -> bool:
    """
    The contract itself: paths may come in any order, but each is one block
    (as many blocks as distinct paths) growing in `(index, subindex)`.
    """
    if _has_missing_position(df, schema):
        return False
    if len(df) < 2:
        return True
    codes, _ = pd.factorize(df[schema.path_col], use_na_sentinel=False)
    blocks = 1 + int(np.count_nonzero(codes[1:] != codes[:-1]))
    if blocks != int(codes.max()) + 1:
        return False
    ix = df[schema.index].to_numpy()
    sx = df[schema.subindex].to_numpy()
    return _grows_within_paths(codes, ix, sx)


def sort_and_validate(
    df: pd.DataFrame, schema: EventstreamSchema, hint: str = ""
) -> pd.DataFrame:
    """Stable sort by `(path, index, subindex)`; raise on missing or repeated positions."""
    from retentioneering.exceptions import SchemaConfigError

    for col in (schema.index, schema.subindex):
        if df[col].isna().any():
            raise SchemaConfigError(
                f"Column '{col}' contains missing values; every event needs a "
                f"position ({schema.index}, {schema.subindex}) in its path.{hint}"
            )
    keys = schema.order_cols
    df = df.sort_values(keys, kind="stable").reset_index(drop=True)
    if len(df) > 1:
        p = _path_values(df, schema)
        ix = df[schema.index].to_numpy()
        sx = df[schema.subindex].to_numpy()
        dup = (p[1:] == p[:-1]) & (ix[1:] == ix[:-1]) & (sx[1:] == sx[:-1])
        if dup.any():
            rows = df.iloc[np.flatnonzero(dup) + 1][keys + [schema.event_col]].head(5)
            raise SchemaConfigError(
                f"Events must have a unique position ({schema.index}, "
                f"{schema.subindex}) within each '{schema.path_col}', but "
                f"{int(dup.sum())} row(s) repeat the position of the row before "
                f"them, e.g.:\n{rows.to_string(index=False)}{hint}"
            )
    return df


_DERIVED_HINT = (
    "\nIf this eventstream comes from a processor's `sql=` query, the query "
    "most likely returned a row twice or invented a position; otherwise this "
    "is a bug in the processor — please report it."
)
_USER_HINT = (
    "\nIf you pass your own '{index}'/'{subindex}' columns, make them unique "
    "per path, or drop them and let the Eventstream number events itself."
)


def establish_order(
    df: pd.DataFrame, schema: EventstreamSchema, mode: str
) -> pd.DataFrame:
    """
    Make sure `df` honours the layout contract (ADR-0017), as cheaply as the
    origin of the frame allows.

    `mode`:
      - "numbered": the constructor just numbered `index` itself after a sort;
        correct by construction.
      - "user": the source frame came with its own `index`; sort and validate.
      - "trusted": a processor that cannot break the order (it only filters
        rows or edits columns); nothing to do.
      - "emitted": a processor that adds, merges or re-reads rows and emits
        them in `ORDER BY path, event_order(...)`; check, and re-sort only
        if the check fails.
      - "unknown": a frame passed with `preprocess=False` by anyone else;
        check, and re-sort if needed.
    """
    from retentioneering.exceptions import SchemaConfigError

    for col in (schema.index, schema.subindex):
        if col not in df.columns:
            raise SchemaConfigError(f"Technical column '{col}' is missing.")
    if df[schema.subindex].dtype != "float64":
        df[schema.subindex] = df[schema.subindex].astype("float64")

    if mode == "numbered":
        return df
    if mode == "user":
        hint = _USER_HINT.format(index=schema.index, subindex=schema.subindex)
        return sort_and_validate(df, schema, hint)

    if STRICT and mode in ("trusted", "emitted"):
        if is_laid_out_any_path_order(df, schema):
            return df
        # A user error (a duplicating sql= query) surfaces as it would in
        # production; anything else is the processor's fault.
        sort_and_validate(df.copy(), schema, _DERIVED_HINT)
        raise EventOrderError(
            f"A processor ({mode}) returned rows out of event order: each "
            f"'{schema.path_col}' must be one block growing in "
            f"({schema.index}, {schema.subindex})."
        )

    if mode == "trusted":
        return df
    if is_laid_out(df, schema):
        return df
    return sort_and_validate(df, schema, _DERIVED_HINT)
