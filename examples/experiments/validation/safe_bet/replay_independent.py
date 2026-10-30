"""Portable post-review checks, using independent integer/tail arithmetic.

This adapter was assembled after review; it is not the frozen preregistered
oracle. Use the installed wheel or an explicit PYTHONPATH=src. No large coverage
simulation is repeated here, and passing does not certify arbitrary inputs.
"""

from functools import lru_cache
import hashlib
import json
from math import comb, fsum
from pathlib import Path

import numpy as np
import pandas as pd
import retentioneering
from retentioneering import Eventstream
from retentioneering.stats import primitives


def fisher(ka, na, kb, nb):
    total = ka + kb
    observed = comb(na, ka) * comb(nb, kb)
    numerator = sum(
        comb(na, x) * comb(nb, total - x)
        for x in range(max(0, total - nb), min(na, total) + 1)
        if comb(na, x) * comb(nb, total - x) <= observed
    )
    return numerator / comb(na + nb, total)


@lru_cache(maxsize=None)
def cp(k, n, tail):
    def probability(p, start, stop):
        return fsum(comb(n, j) * p**j * (1 - p) ** (n - j) for j in range(start, stop))

    low = 0.0
    high = 1.0
    if k:
        a, b = 0.0, 1.0
        for _ in range(90):
            middle = (a + b) / 2
            if probability(middle, k, n + 1) < tail:
                a = middle
            else:
                b = middle
        low = (a + b) / 2
    if k < n:
        a, b = 0.0, 1.0
        for _ in range(90):
            middle = (a + b) / 2
            if probability(middle, 0, k + 1) > tail:
                a = middle
            else:
                b = middle
        high = (a + b) / 2
    return low, high


def main():
    tables = 0
    fisher_error = interval_error = 0.0
    for na in range(1, 9):
        for nb in range(1, 9):
            for ka in range(na + 1):
                for kb in range(nb + 1):
                    reference = fisher(ka, na, kb, nb)
                    actual = primitives.fisher_exact_p(kb, nb, ka, na)
                    fisher_error = max(fisher_error, abs(actual - reference))
                    la, ua = cp(ka, na, 0.05 / 4)
                    lb, ub = cp(kb, nb, 0.05 / 4)
                    actual_ci = primitives.exact_difference_ci(kb, nb, ka, na)
                    interval_error = max(
                        interval_error,
                        max(
                            abs(actual_ci[0] - (lb - ua)), abs(actual_ci[1] - (ub - la))
                        ),
                    )
                    tables += 1
    assert fisher_error < 2e-14
    assert interval_error < 2e-13

    # Any start may lead to a conversion; an invalid early attempt cannot
    # erase the valid later A -> B. No candidate matcher is used as oracle.
    events = []
    for user, sequence in [
        ("late", ["A", "X", "X", "B", "A", "B"]),
        ("never", ["A", "X", "X", "B"]),
    ]:
        for i, event in enumerate(sequence):
            events.append(
                (user, event, pd.Timestamp("2026-01-01") + pd.Timedelta(seconds=i))
            )
    stream = Eventstream(
        pd.DataFrame(events, columns=["user_id", "event", "timestamp"])
    )
    for within in (1, "1s"):
        row = stream.get_conversion_rate(
            {"pattern": "A", "occurrence": "all"}, "B", within=within
        ).iloc[0]
        assert row["paths_with_start"] == 2
        assert row["converted"] == 1
        assert row["conversion_rate"] == 0.5

    # Missing hypotheses remain in a declared Holm family: min p is .02*3.
    adjusted = primitives.holm(np.array([0.02, np.nan, 0.4]))
    assert np.allclose(adjusted[[0, 2]], [0.06, 0.8])
    assert np.isnan(adjusted[1])
    print(
        json.dumps(
            {
                "package_path": str(Path(retentioneering.__file__).resolve()),
                "primitive_sha256": hashlib.sha256(
                    Path(primitives.__file__).read_bytes()
                ).hexdigest(),
                "tables": tables,
                "fisher_max_abs_error": fisher_error,
                "exact_difference_max_abs_error": interval_error,
                "conversion_windows": 2,
                "declared_family_check": "passed",
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
