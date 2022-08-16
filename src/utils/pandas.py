import random

import pandas as pd


def shuffle_df(df: pd.DataFrame, inplace=True) -> pd.DataFrame:
    df = df if inplace else df.copy()
    index = [i for i in range(df.shape[0])]
    random.shuffle(index)
    index = pd.Series(data=index)
    df.set_index(index)
    df.sort_index()
    return df


def get_merged_col(df: pd.DataFrame, colname: str, suffix: str):
    return df[colname] if colname in df else df[f"{colname}{suffix}"]
