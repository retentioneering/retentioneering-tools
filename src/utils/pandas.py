# type: ignore

import random
from typing import Any

import pandas as pd


def shuffle_df(df: pd.DataFrame, inplace=True) -> pd.DataFrame:
    df = df if inplace else df.copy()
    index = [i for i in range(df.shape[0])]
    random.shuffle(index)
    df.set_index([index]).sort_index()
    return df


def get_merged_col(df: pd.DataFrame, colname: str, suffix: str) -> pd.Series[Any]:
    return df[colname] if colname in df else df[f"{colname}{suffix}"]


__all__ = ["shuffle_df", "get_merged_col"]
