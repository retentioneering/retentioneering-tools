"""Smoke test of ``notebooks/ab_testing_showcase.ipynb``.

Executes the notebook top to bottom with nbclient. As in the tutorial's smoke test, a
setup cell injected before the first cell makes the default Northwind Home calls of
``load_ab_test`` / ``load_ab_test_assignment`` read the cached fixture
(``tests/datasets/fixtures/``) instead of running the generator; the other datasets and
the small ``n_users`` draws are generated (each in well under a second). The notebook ends
with its own check cell, which asserts every claim its prose makes about the printed
results; this test requires that cell to finish and the whole run to stay within budget.
"""

from __future__ import annotations

import pathlib
import time

import pytest

nbformat = pytest.importorskip("nbformat")
nbclient = pytest.importorskip("nbclient")

ROOT = pathlib.Path(__file__).resolve().parents[2]
NOTEBOOK = ROOT / "notebooks" / "ab_testing_showcase.ipynb"
FIXTURE_DIR = ROOT / "tests" / "datasets" / "fixtures"
BUDGET_S = 120

SETUP = f"""
import os

os.environ["RETENTIONEERING_NO_TRACK"] = "1"

from retentioneering.datasets import ab_test as _ab_test

_fixture = _ab_test._read_fixture({str(FIXTURE_DIR)!r})
_generate = _ab_test._generate


def _generate_from_fixture(n_users, seed):
    if (n_users, seed) == (_ab_test.DEFAULT_N_USERS, _ab_test.DEFAULT_SEED):
        return tuple(frame.copy() for frame in _fixture)
    return _generate(n_users, seed)


_ab_test._generate = _generate_from_fixture
"""


def test__showcase_executes_and_its_claims_hold() -> None:
    nb = nbformat.read(NOTEBOOK, as_version=4)
    assert all(
        not cell.get("outputs") for cell in nb.cells if cell.cell_type == "code"
    ), "commit the notebook with outputs stripped"
    assert "showcase checks passed" in nb.cells[-1].source, "the check cell comes last"
    nb.cells.insert(0, nbformat.v4.new_code_cell(SETUP))

    client = nbclient.NotebookClient(
        nb,
        timeout=BUDGET_S,
        kernel_name="python3",
        resources={"metadata": {"path": str(NOTEBOOK.parent)}},
    )
    started = time.perf_counter()
    client.execute()
    elapsed = time.perf_counter() - started

    errors = [
        out
        for cell in nb.cells
        if cell.cell_type == "code"
        for out in cell.get("outputs", [])
        if out.get("output_type") == "error"
    ]
    assert not errors
    last = nb.cells[-1]["outputs"]
    assert any("showcase checks passed" in out.get("text", "") for out in last)
    assert elapsed < BUDGET_S
