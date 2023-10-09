from __future__ import annotations

from hashlib import sha256

import pandas as pd


def hash_value(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def hash_dataframe(data: pd.DataFrame | pd.Series) -> str:
    return sha256(
        pd.util.hash_pandas_object(pd.concat([data.iloc[:1000], data.iloc[-1000:]]), index=True).values  # type: ignore
    ).hexdigest()
