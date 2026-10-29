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


def pytest_addoption(parser) -> None:
    parser.addoption(
        "--run-slow",
        action="store_true",
        default=False,
        help="also run tests marked slow (Lumeo-scale timings, coverage simulations)",
    )


def pytest_configure(config) -> None:
    config.addinivalue_line(
        "markers", "slow: long simulations and timings; run with --run-slow"
    )


def pytest_collection_modifyitems(config, items) -> None:
    if config.getoption("--run-slow") or os.environ.get("RETENTIONEERING_RUN_SLOW"):
        return
    skip = pytest.mark.skip(reason="slow: pass --run-slow to run it")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip)
