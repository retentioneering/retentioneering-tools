import os
from typing import Any

os.environ["RETENTIONEERING_NO_TRACK"] = "1"

import pandas as pd
import pytest

import retentioneering.data_processors  # noqa: F401  (registers every processor)
from retentioneering.data_processors.data_processor import DataProcessor
from tests.path_order import path_order_problems

BASE_DIR = os.path.abspath(os.path.dirname(__file__))


def _processors(cls=DataProcessor):
    for sub in cls.__subclasses__():
        yield sub
        yield from _processors(sub)


@pytest.fixture(autouse=True)
def _keeps_path_order(monkeypatch):
    """Every processor output in the suite keeps the path order (ADR-0018)."""

    def checked(apply):
        def wrapper(self, df, schema):
            out, out_schema = apply(self, df, schema)
            # Only an input whose positions are a path order can be held to
            # it: some tests feed hand-made frames on purpose. Its row order
            # may be anything, since DuckDB reads rows in any order.
            if not path_order_problems(df, schema, rows_in_order=False):
                problems = path_order_problems(out, out_schema)
                assert not problems, (
                    f"{type(self).__name__}.apply broke the path order: "
                    + "; ".join(problems)
                )
            return out, out_schema

        return wrapper

    for cls in set(_processors()):
        if "apply" in vars(cls):
            monkeypatch.setattr(cls, "apply", checked(vars(cls)["apply"]))


@pytest.fixture()
def base_dir() -> str:
    return BASE_DIR


@pytest.fixture()
def fx_read_csv(base_dir: str) -> callable([str, str, Any]):
    def read_csv(filename: str, sep: str = "\t") -> pd.read_csv:
        return pd.read_csv(os.path.join(base_dir, filename), sep=sep)

    return read_csv
