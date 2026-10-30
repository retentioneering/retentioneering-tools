"""Enumerate binomial count probabilities: no Monte Carlo uncertainty.

Wilson limits come from SciPy's independently maintained implementation.
Newcombe's published combination is evaluated directly over all count pairs.
This separates discrete method coverage from finite-simulation fluctuations.
"""

import argparse
import json
from pathlib import Path

import numpy as np
from scipy.stats import binom, binomtest

from coverage_validation import scenarios


def evaluate(s):
    arms = []
    for n, p in [(s["nc"], s["pc"]), (s["nt"], s["pt"])]:
        counts = np.arange(n + 1)
        intervals = np.array(
            [tuple(binomtest(int(k), n).proportion_ci(method="wilson")) for k in counts]
        )
        probabilities = binom.pmf(counts, n, p)
        coverage = probabilities @ ((intervals[:, 0] <= p) & (p <= intervals[:, 1]))
        arms.append((counts / n, intervals, probabilities, float(coverage)))
    c, t = arms
    delta = t[0][None, :] - c[0][:, None]
    lo = delta - np.hypot((t[0] - t[1][:, 0])[None, :], (c[1][:, 1] - c[0])[:, None])
    hi = delta + np.hypot((t[1][:, 1] - t[0])[None, :], (c[0] - c[1][:, 0])[:, None])
    truth = s["pt"] - s["pc"]
    weights = c[2][:, None] * t[2][None, :]
    return dict(
        scenario=s["name"],
        control_wilson_coverage=c[3],
        treatment_wilson_coverage=t[3],
        newcombe_coverage=float(np.sum(weights * ((lo <= truth) & (truth <= hi)))),
        probability_mass=float(weights.sum()),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    results = [evaluate(s) for s in scenarios() if s["kind"] == "binary"]
    args.output.write_text(json.dumps(results, indent=2) + "\n")
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
