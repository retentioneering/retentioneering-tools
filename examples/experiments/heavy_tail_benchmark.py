"""Local timing of production percentile versus research candidate components."""

import argparse
import json
from pathlib import Path
import time

import numpy as np

import heavy_tail_validation as h


def benchmark(repeats, resamples):
    m = h.audit.load_stats(h.ROOT / "src/retentioneering/stats/experiments.py")
    result = []
    for index in (7, 9, 10):
        scenario = h.audit.scenarios()[index]
        values, _ = h.audit.generate(scenario, np.random.default_rng(871 + index))
        c, t = values
        timings = {}
        for repetition in range(repeats + 1):
            seed = 312 + repetition
            start = time.perf_counter()
            config = m._bootstrap_config(dict(n_resamples=resamples, random_state=seed))
            streams = np.random.SeedSequence(seed).spawn(2)
            dc, dt = [
                m._draw_sums(v, config, np.random.default_rng(s))
                for v, s in zip(values, streams)
            ]
            m._summary(
                [dict(metric="outcome", aggregation="mean", outcome_kind="continuous")],
                c,
                t,
                dc,
                dt,
                len(c),
                len(t),
                config,
            )
            original = time.perf_counter() - start
            _, costs, _, _ = h.calculate(m, scenario, values, seed, resamples)
            common = costs["shared_resampling"] + costs["percentile_summary"]
            measured = {"production_percentile": original}
            for method in (
                "basic",
                "bca",
                "bootstrap_t",
                "bootstrap_t_symmetric",
                "m_out_of_n_basic",
            ):
                measured[method] = (
                    common
                    + costs[method]
                    + (costs["bca_jackknife"] if method == "bca" else 0)
                )
            start = time.perf_counter()
            h.arm_point(c)
            h.arm_point(t)
            measured["welch"] = costs["welch"] + time.perf_counter() - start
            if repetition:  # discard a warm-up pass
                for key, elapsed in measured.items():
                    timings.setdefault(key, []).append(1000 * elapsed)
        result.append(
            dict(
                scenario=scenario["name"],
                timings_ms={
                    key: dict(
                        median=float(np.median(v)), p90=float(np.quantile(v, 0.9))
                    )
                    for key, v in timings.items()
                },
            )
        )
    return dict(
        repeats=repeats,
        resamples=resamples,
        note="Local wall-clock component estimates on a shared executor; no event extraction/UI. Candidate costs include shared augmented resampling plus only their own postprocessing. Welch includes moments/CI/p-value. No nested bootstrap.",
        results=result,
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repeats", type=int, default=20)
    p.add_argument("--resamples", type=int, default=1999)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    args.output.write_text(
        json.dumps(benchmark(args.repeats, args.resamples), indent=2) + "\n"
    )
