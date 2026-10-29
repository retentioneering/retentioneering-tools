"""Independent, opt-in Monte Carlo audit of PR #214 (not part of routine CI).

Run with PYTHONPATH=src and single-thread BLAS for predictable resource use.
Generators and analytic truths are independent of the implementation under test.
Every trial has separate deterministic data/bootstrap streams; scheduling does
not change results. --stats-file can load a frozen original implementation.
"""

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import subprocess
import time

import numpy as np
import pandas as pd
import scipy
from scipy.stats import binomtest

ROOT = Path(__file__).resolve().parents[2]
ORIGINAL = "aa17374b20407158c687b33abed0fcc3376dbc12"


def scenarios():
    result = []
    for name, kind, nc, nt, pc, pt in [
        ("binary_null", "binary", 250, 250, 0.1, 0.1),
        ("binary_effect", "binary", 250, 250, 0.1, 0.13),
        ("binary_unequal_null", "binary", 100, 500, 0.1, 0.1),
        ("rare_null", "binary", 250, 250, 0.001, 0.001),
        ("rare_effect", "binary", 250, 250, 0.001, 0.004),
        ("zero_boundary", "binary", 250, 250, 0.0, 0.0),
        ("one_boundary", "binary", 250, 250, 1.0, 1.0),
        ("money_null", "money", 250, 250, 0.0, 0.0),
        ("money_effect", "money", 250, 250, 0.0, 8.0),
        ("money_unequal_null", "money", 100, 500, 0.0, 0.0),
        ("money_large_null", "money", 1000, 1000, 0.0, 0.0),
        ("ratio_null", "ratio", 250, 250, 0.0, 0.0),
        ("ratio_effect", "ratio", 250, 250, 0.0, 8.0),
        ("ratio_unequal_null", "ratio", 100, 500, 0.0, 0.0),
        ("ratio_sparse_null", "sparse_ratio", 100, 500, 0.0, 0.0),
    ]:
        result.append(dict(name=name, kind=kind, nc=nc, nt=nt, pc=pc, pt=pt))
    return result


def generate(s, rng):
    """Return paired unit contributions and EXACT population estimands.

    Money: A~Bernoulli(.2); X=A*Lognormal(log(100)-2,2),
    E[X]=20. Treatment scale (20+effect)/20 preserves structural zeros.
    Ratio: D=1+Poisson(2), L~Gamma(4,20),
    N=D*(L+5*(D-3)+effect). E[D]=3, Var[D]=2,
    E[N]/E[D]=80+10/3+effect; numerator and denominator dependent.
    Sparse ratio: D~Bernoulli(.01); N=D*Gamma(4,(80+effect)/4).
    """
    values, truths = [], []
    for n, parameter in [(s["nc"], s["pc"]), (s["nt"], s["pt"])]:
        if s["kind"] == "binary":
            x = rng.binomial(1, parameter, n).astype(float)
            d, truth = np.ones(n), parameter
        elif s["kind"] == "money":
            x = rng.binomial(1, 0.2, n) * rng.lognormal(np.log(100) - 2, 2, n)
            x *= (20 + parameter) / 20
            d, truth = np.ones(n), 20 + parameter
        elif s["kind"] == "ratio":
            d = 1 + rng.poisson(2, n)
            x = d * (rng.gamma(4, 20, n) + 5 * (d - 3) + parameter)
            truth = 80 + 10 / 3 + parameter
        else:
            d = rng.binomial(1, 0.01, n)
            x = d * rng.gamma(4, (80 + parameter) / 4, n)
            truth = 80 + parameter
        values.append(np.column_stack([x, d]))
        truths.append(truth)
    return values, dict(
        control=truths[0],
        treatment=truths[1],
        absolute_effect=truths[1] - truths[0],
        relative_effect=truths[1] / truths[0] - 1 if truths[0] > 0 else None,
    )


def load_stats(path):
    spec = importlib.util.spec_from_file_location("audited_stats", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def calculate(m, s, values, seed, resamples):
    c, t = values
    config = m._bootstrap_config(dict(n_resamples=resamples, random_state=seed))
    streams = np.random.SeedSequence(seed).spawn(2)
    dc, dt = [
        m._draw_sums(v, config, np.random.default_rng(r))
        for v, r in zip(values, streams)
    ]
    spec = [
        dict(
            metric="outcome",
            aggregation="mean" if s["kind"] in {"binary", "money"} else "ratio_of_sums",
            outcome_kind="binary" if s["kind"] == "binary" else "continuous",
        )
    ]
    summary = m._summary(spec, c, t, dc, dt, len(c), len(t), config)
    test = (
        "fisher_exact"
        if s["kind"] == "binary"
        else "welch"
        if s["kind"] == "money"
        else None
    )
    if test:
        m._tests(
            summary, spec, c, t, dict(methods={"outcome": test}, adjustment="holm")
        )
    outputs = {"percentile": summary.iloc[0].to_dict()}
    if s["kind"] == "binary":
        m._binary_intervals(
            summary, spec, c, t, dict(method="newcombe", confidence_level=0.95)
        )
        outputs["wilson_newcombe"] = summary.iloc[0].to_dict()
    return outputs


def rate(k, n):
    if not n:
        return dict(count=int(k), n=0, rate=None, mc95=None)
    ci = binomtest(int(k), int(n)).proportion_ci(confidence_level=0.95, method="exact")
    return dict(count=int(k), n=int(n), rate=k / n, mc95=[ci.low, ci.high])


def run_scenario(job):
    s, index, args = job
    m = load_stats(args["stats_file"])
    counters, estimates = {}, []
    started = time.monotonic()
    for trial in range(args["trials"]):
        data_seed, boot_seed = np.random.SeedSequence(
            [args["seed"], index, trial]
        ).spawn(2)
        values, truths = generate(s, np.random.default_rng(data_seed))
        seed = int(boot_seed.generate_state(1)[0])
        outputs = calculate(m, s, values, seed, args["resamples"])
        estimates.append(outputs["percentile"]["absolute_effect"])
        for method, row in outputs.items():
            for estimand, truth in truths.items():
                key = (method, estimand)
                counter = counters.setdefault(key, Counter())
                status = row[estimand + "_status"]
                counter["status:" + status] += 1
                if truth is None:
                    continue
                counter["trials"] += 1
                if status == "ok":
                    lo, hi = row[estimand + "_ci_low"], row[estimand + "_ci_high"]
                    counter["available"] += 1
                    counter["covered"] += int(lo <= truth <= hi)
                    counter["excluded_null"] += int(not lo <= 0 <= hi)
            counter = counters.setdefault((method, "test"), Counter())
            counter["trials"] += 1
            if row["test_status"] == "ok" and np.isfinite(row["p_value"]):
                counter["available"] += 1
                counter["rejected"] += int(row["p_value"] < 0.05)
    results = []
    null = s["pc"] == s["pt"]
    for (method, estimand), c in counters.items():
        n, available = c["trials"], c["available"]
        r = dict(
            method=method,
            estimand=estimand,
            statuses={k[7:]: v for k, v in c.items() if k.startswith("status:")},
            availability=rate(available, n),
        )
        if estimand == "test":
            r.update(
                rejection_all=rate(c["rejected"], n),
                rejection_available=rate(c["rejected"], available),
                is_type1_error=null,
            )
        else:
            r.update(
                truth=truths[estimand],
                coverage_all=rate(c["covered"], n),
                coverage_available=rate(c["covered"], available),
            )
            if null and estimand in {"absolute_effect", "relative_effect"}:
                r["ci_false_positive_all"] = rate(c["excluded_null"], n)
                r["ci_false_positive_available"] = rate(c["excluded_null"], available)
        results.append(r)
    finite = np.array(estimates)
    finite = finite[np.isfinite(finite)]
    return dict(
        scenario=s,
        results=results,
        mean_finite_effect=float(np.mean(finite)) if len(finite) else None,
        elapsed_seconds=time.monotonic() - started,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trials", type=int, default=2000)
    parser.add_argument("--resamples", type=int, default=1999)
    parser.add_argument("--seed", type=int, default=214202610)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--scenarios", nargs="*")
    parser.add_argument(
        "--stats-file", default=str(ROOT / "src/retentioneering/stats/experiments.py")
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.trials < 1 or args.resamples < 2 or args.workers < 1:
        parser.error("trials/workers >= 1 and resamples >= 2 required")
    all_s = scenarios()
    if args.scenarios and set(args.scenarios) - {s["name"] for s in all_s}:
        parser.error("unknown scenario")
    jobs = [
        (s, i, vars(args))
        for i, s in enumerate(all_s)
        if not args.scenarios or s["name"] in args.scenarios
    ]
    report = dict(
        original_pr="https://github.com/retentioneering/retentioneering-tools/pull/214",
        original_sha=ORIGINAL,
        implementation_sha256=hashlib.sha256(
            Path(args.stats_file).read_bytes()
        ).hexdigest(),
        checkout_sha=subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        python=platform.python_version(),
        numpy=np.__version__,
        pandas=pd.__version__,
        scipy=scipy.__version__,
        config={k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
        thread_settings={
            k: os.environ.get(k) for k in ["OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS"]
        },
        results=[],
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        for result in pool.map(run_scenario, jobs):
            report["results"].append(result)
            args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
            print(
                result["scenario"]["name"],
                round(result["elapsed_seconds"], 1),
                "seconds",
                flush=True,
            )


if __name__ == "__main__":
    main()
