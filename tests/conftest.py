import os
from typing import Any

os.environ["RETENTIONEERING_NO_TRACK"] = "1"

import pandas as pd
import pytest

BASE_DIR = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture(autouse=True)
def _strict_event_order(monkeypatch):
    """Check every eventstream a processor returns, opted out or not, and fail
    on one laid out out of event order instead of re-sorting it (ADR-0017).
    This is what keeps each processor's `may_break_order = False` honest."""
    from retentioneering.eventstream import ordering

    monkeypatch.setattr(ordering, "STRICT", True)


@pytest.fixture()
def base_dir() -> str:
    return BASE_DIR


@pytest.fixture()
def fx_read_csv(base_dir: str) -> callable([str, str, Any]):
    def read_csv(filename: str, sep: str = "\t") -> pd.read_csv:
        return pd.read_csv(os.path.join(base_dir, filename), sep=sep)

    return read_csv
