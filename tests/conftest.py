import os
from typing import Any

os.environ["RETENTIONEERING_NO_TRACK"] = "1"

import pandas as pd
import pytest

BASE_DIR = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture()
def base_dir() -> str:
    return BASE_DIR


@pytest.fixture()
def fx_read_csv(base_dir: str) -> callable([str, str, Any]):
    def read_csv(filename: str, sep: str = "\t") -> pd.read_csv:
        return pd.read_csv(os.path.join(base_dir, filename), sep=sep)

    return read_csv
