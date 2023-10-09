from __future__ import annotations

import json
from typing import Hashable

import pandas as pd


def set_parent(df: pd.DataFrame, index_col: str = "index", parent_col: str = "parent_index") -> pd.Series:
    """Parses the data as a sequence of brackets and sets the index
    of the parent bracket for all internal data. The algorithm works
    backwards to process the broken sequences.
    """
    df = df.sort_values(by=index_col)
    position = df.shape[0] - 1

    parent_stack, event_stack, parents = [0], [], []
    while position >= 0:
        if parent_stack[-1] == 0 and not (
            df.iloc[position]["event_custom_name"].endswith("_end")
            or df.iloc[position]["event_custom_name"].endswith("_tracker")
        ):
            parents.append(-1)
        else:
            parents.append(parent_stack[-1])
        full_name = f"{df.iloc[position]['scope']}_{df.iloc[position]['event_name']}"
        if df.iloc[position]["event_custom_name"].endswith("_end"):
            event_stack.append(full_name)
            parent_stack.append(df.iloc[position][index_col])
        if df.iloc[position]["event_custom_name"].endswith("_start"):
            while full_name in event_stack:
                parent_stack.pop()
                if event_stack.pop() == full_name:
                    break

        position -= 1

    return pd.Series(parents[::-1], index=df.index, name=parent_col)


def prepare_data(
    df: pd.DataFrame,
    index_col: str = "index",
    parent_col: str = "parent_index",
    full_index: str = "full_index",
    full_parent_index: str = "full_parent_index",
    event_full_name: str = "event_full_name",
    buffer_index_name: str = "_default_index",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Collapses each bracket group into a single event and
    writes the latest metadata for the current group. The broken sequences are
    excluded from consideration. Returns two dataframes: processed and broken data.
    """
    if df.empty:
        raise ValueError("The data is empty")
    df[parent_col] = pd.concat([set_parent(group) for name, group in df.groupby(["user_id", "jupyter_kernel_id"])])
    broken = df[df[parent_col] == -1]
    df = df[df.parent_index != -1]
    df[full_index] = df[index_col].apply(str) + df["user_id"] + df["jupyter_kernel_id"]
    df[full_parent_index] = df[parent_col].apply(str) + df["user_id"] + df["jupyter_kernel_id"]
    df[event_full_name] = df["scope"] + "_" + df["event_name"]

    meta_cols = ["eventstream_index", "parent_eventstream_index", "child_eventstream_index", "params"]

    meta = (
        df[(df["event_name"] == "metadata")]
        .groupby(full_parent_index)
        .agg({col: list if col == "params" else "last" for col in meta_cols})
    )
    df.reset_index(inplace=True, names=[buffer_index_name])
    df.set_index(full_index, inplace=True)

    df["params"] = [[] for _ in range(df.shape[0])]
    df.loc[meta.index, meta_cols] = meta[meta_cols]
    df.reset_index(inplace=True)
    df.set_index(buffer_index_name, inplace=True)
    df.index.name = None
    return df.loc[(df.event_name != "metadata") & ~df.event_custom_name.str.endswith("_start")], broken


def uncover_params(df: pd.DataFrame) -> pd.DataFrame:
    df["args"] = df.params.apply(lambda l: l[0].get("args", {}) if len(l) > 0 else {})
    df["performance_before"] = df.params.apply(lambda l: l[0].get("performance_info", {}) if len(l) > 0 else {})
    df["performance_after"] = df.params.apply(lambda l: l[1].get("performance_info", {}) if len(l) > 1 else {})
    return df.drop("params", axis=1)


def process_data(data: pd.DataFrame, only_calls: bool = True) -> pd.DataFrame:
    """Processes raw data from the database."""
    df = data.copy()
    df["params"] = df["params"].apply(json.loads)
    df = prepare_data(df)[0]
    df = uncover_params(df)
    if only_calls:
        return df[df.parent_index == 0]
    return df


def get_inner_calls(
    data: pd.DataFrame, parent_index: Hashable, index_col: str = "full_index", parent_col: str = "full_parent_index"
) -> pd.DataFrame:
    parent_ids = [parent_index]
    inner_ids = []
    while parent_ids:
        current_parent = parent_ids.pop()
        inner_calls = data[data[parent_col] == current_parent]
        parent_ids.extend(inner_calls[index_col])
        inner_ids.append(current_parent)
    return data[data[index_col].isin(inner_ids)]
