"""Composite paths: split every path by a per-row key.

Several tools need per-path metrics computed separately for each value of something
that varies *within* a path: Segment Overview splits a user's events by segment level,
the A/B scorecard splits them by period. The trick is the same: build a derived stream
whose only path column is the pair ``(path, key)``, run the metric registry on it, and
map the results back. This module is the one place that builds that stream.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from retentioneering.exceptions import InvalidParameterError

if TYPE_CHECKING:
    from retentioneering.eventstream.eventstream import Eventstream

__all__ = ["COMPOSITE_COL", "composite_path_stream"]

#: Name of the path column of the derived stream.
COMPOSITE_COL = "__composite_path_id__"


def _composite_ids(paths: pd.Series, keys: Any) -> tuple[np.ndarray, np.ndarray, Any]:
    """Factorize ``(path, key)`` pairs in order of first appearance.

    Missing keys (None/NaN) get their own code, so every row has a composite id.
    """
    path_codes, _ = pd.factorize(paths, use_na_sentinel=False)
    key_codes, key_uniques = pd.factorize(keys, use_na_sentinel=False)
    n_keys = max(len(key_uniques), 1)
    pair = path_codes.astype(np.int64) * n_keys + key_codes.astype(np.int64)
    composite, _ = pd.factorize(pair)
    return composite, key_codes, key_uniques


def composite_path_stream(
    stream: "Eventstream",
    key_per_row: Any,
    path_col: str | None = None,
) -> tuple["Eventstream", pd.DataFrame]:
    """Derive a stream whose paths are ``(path, key)`` pairs.

    Parameters
    ----------
    stream : Eventstream
        Source stream.
    key_per_row : str or array-like
        One key per row of ``stream.df`` (a segment level, a period label, ...), or the
        name of a column of ``stream.df``. Missing keys form their own composite paths.
    path_col : str, optional
        Path grain that is split; defaults to ``schema.path_col``.

    Returns
    -------
    tuple
        ``(derived, back)``. ``derived`` is an :class:`Eventstream` over the same rows
        with one extra column ``__composite_path_id__`` (int) as its only path column:
        the other path columns are dropped from ``schema.path_cols`` because a coarser
        column would no longer contain the composite paths (ADR-0004 nesting) and a
        finer one would cut them. Event order within a composite path is the source
        order. ``back`` is a DataFrame indexed by composite id (named
        ``__composite_path_id__``) with columns ``path_col`` (the source path id) and
        ``key`` (the key value; None for a missing key).

    Raises
    ------
    InvalidParameterError
        ``path_col`` is not a path column; ``key_per_row`` names an unknown column or
        has the wrong length.
    """
    from retentioneering.eventstream.eventstream import Eventstream

    schema = stream.schema
    path_col = path_col or schema.path_col
    if path_col not in schema.path_cols:
        raise InvalidParameterError("path_col", path_col, list(schema.path_cols))
    df = stream.df
    if isinstance(key_per_row, str):
        if key_per_row not in df.columns:
            raise InvalidParameterError("key_per_row", key_per_row, list(df.columns))
        keys = df[key_per_row].to_numpy()
    else:
        keys = np.asarray(key_per_row, dtype=object)
        if keys.ndim != 1 or len(keys) != len(df):
            raise InvalidParameterError(
                "key_per_row",
                f"length {len(keys)}",
                [f"one key per row ({len(df)})"],
            )

    composite, key_codes, key_uniques = _composite_ids(df[path_col], keys)
    derived_df = df.copy()
    derived_df[COMPOSITE_COL] = composite

    first = pd.Series(np.arange(len(composite))).groupby(composite, sort=True).first()
    rows = first.to_numpy()
    key_values = [
        None if pd.isna(key_uniques[code]) else key_uniques[code]
        for code in key_codes[rows]
    ]
    index = pd.Index(first.index.to_numpy(), name=COMPOSITE_COL)
    back = pd.DataFrame({path_col: df[path_col].to_numpy()[rows]}, index=index)
    back["key"] = pd.Series(key_values, index=index, dtype=object)

    derived_schema = asdict(schema)
    derived_schema["path_cols"] = [COMPOSITE_COL]
    derived = Eventstream(derived_df, derived_schema, preprocess=False)
    return derived, back
