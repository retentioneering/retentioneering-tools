"""Regenerate the cached fixture of the "Northwind Home" A/B dataset.

The story tests in ``tests/datasets/ab_test_test.py`` read the default call of
``retentioneering.datasets.load_ab_test`` from ``tests/datasets/fixtures/`` (three
small parquet files, a lossless compact encoding of the generator's output).
Run this after changing the generator, then rerun the story tests:

    uv run python scripts/refresh_ab_fixture.py
    uv run pytest tests/datasets -q
"""

from __future__ import annotations

import pathlib

from retentioneering.datasets import ab_test

FIXTURE_DIR = (
    pathlib.Path(__file__).resolve().parents[1] / "tests" / "datasets" / "fixtures"
)
MAX_TOTAL_BYTES = 500 * 1024


def main() -> None:
    sizes = ab_test._write_fixture(
        FIXTURE_DIR, n_users=ab_test.DEFAULT_N_USERS, seed=ab_test.DEFAULT_SEED
    )
    for name, size in sizes.items():
        print(f"{name}: {size / 1024:.0f} KB")
    total = sum(sizes.values())
    print(f"total: {total / 1024:.0f} KB")
    if total > MAX_TOTAL_BYTES:
        raise SystemExit(f"fixture is {total} bytes, over the {MAX_TOTAL_BYTES} limit")


if __name__ == "__main__":
    main()
