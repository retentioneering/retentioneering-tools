"""Experimental sorted insertion; deliberately not used by production code."""

import numpy as np
import pandas as pd

from retentioneering.data_processors.add_events import _concat_categorical


def merge_sorted(df, new, schema):
    """Place sorted markers with binary search and gather the result once.

    The source already grows in (index, subindex) within each path. Only new
    rows need sorting. Structured keys retain integer precision (including
    uint64); converting the three columns to a float matrix would not.
    """
    path = df[schema.path_col]
    if isinstance(path.dtype, pd.CategoricalDtype):
        paths = path.cat.codes.to_numpy()
        new_paths = pd.Categorical(new[schema.path_col], dtype=path.dtype).codes
    elif path.dtype.kind in "iuf":
        paths = path.to_numpy()
        new_paths = new[schema.path_col].to_numpy()
    else:
        paths, values = pd.factorize(path, sort=True)
        new_paths = values.get_indexer(new[schema.path_col])
    # ADR-0017 also permits non-sorted path blocks. Keep the existing canonical
    # sort for those, and for extension/object position keys.
    index = df[schema.index].to_numpy()
    if index.dtype.kind not in "iuf" or np.any(paths[1:] < paths[:-1]):
        return _concat_categorical(df, new, schema.event_col).sort_values(
            schema.order_cols, kind="stable", ignore_index=True
        )

    old_keys = np.rec.fromarrays([paths, index, df[schema.subindex].to_numpy()])
    new_keys = np.rec.fromarrays(
        [new_paths, new[schema.index].to_numpy(), new[schema.subindex].to_numpy()],
        dtype=old_keys.dtype,
    )
    order = np.lexsort(tuple(new_keys[name] for name in reversed(new_keys.dtype.names)))
    positions = np.searchsorted(old_keys, new_keys[order], side="right")
    del old_keys, new_keys, paths, new_paths

    # The j-th new row is shifted by j earlier inserts. Fill the remaining
    # slots with old row numbers, in their existing order.
    slots = positions + np.arange(len(new))
    take = np.empty(len(df) + len(new), dtype=np.intp)
    old_slots = np.ones(len(take), dtype=bool)
    old_slots[slots] = False
    take[old_slots] = np.arange(len(df))
    take[slots] = len(df) + order
    combined = _concat_categorical(df, new, schema.event_col)
    return combined.take(take).reset_index(drop=True)
