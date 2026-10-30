"""Opt-in paired CI comparison; research code, not a public inference API.

Reuses coverage_validation's unchanged generators and seed identities. All
methods target the same population estimands. No winsorization or log outcome.
Run with OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1. See validation/HEAVY_TAILS.md.
"""

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import platform
import time

import numpy as np
import scipy
from scipy import stats

import coverage_validation as audit

ROOT = Path(__file__).resolve().parents[2]
BASE_SHA = "52615dd93476ef8532487792f26d023e944dfe77"
LABELS = ("control", "treatment", "absolute_effect", "relative_effect")


def estimands(c, t):
    c, t = np.broadcast_arrays(c, t)
    relative = np.divide(t, c, out=np.full(c.shape, np.nan), where=c > 0) - 1
    return np.stack((c, t, t - c, relative), axis=-1)


def arm_point(v):
    total = v.sum(axis=0)
    if total[1] <= 0:
        return np.nan, np.nan
    point = total[0] / total[1]
    residual = v[:, 0] - point * v[:, 1]
    variance = np.sum(residual**2) / (len(v) * (len(v) - 1) * v[:, 1].mean() ** 2)
    return point, variance


def standard_errors(c, t, vc, vt):
    c, t, vc, vt = np.broadcast_arrays(c, t, vc, vt)
    with np.errstate(invalid="ignore", divide="ignore", over="ignore"):
        relative_var = vt / c**2 + t**2 * vc / c**4
    relative_var = np.where(c > 0, relative_var, np.nan)
    return np.sqrt(np.stack((vc, vt, vc + vt, relative_var), axis=-1))


def draw_moments(v, resamples, rng, sample_size=None, batch_size=128):
    """Whole-unit multinomial bootstrap; analytic SE needs no nested bootstrap."""
    n = len(v)
    m = n if sample_size is None else sample_size
    x, d = v.T
    # Scaling prevents avoidable overflow in second moments in this harness.
    scale = max(float(np.max(np.abs(x))), 1.0)
    x = x / scale
    augmented = np.column_stack((x, d, x * x, x * d, d * d))
    sums = np.empty((resamples, 5))
    for start in range(0, resamples, batch_size):
        size = min(batch_size, resamples - start)
        w = rng.multinomial(m, np.full(n, 1 / n), size=size)
        sums[start : start + size] = w @ augmented
    sx, sd, sxx, sxd, sdd = sums.T
    with np.errstate(invalid="ignore", divide="ignore"):
        point = sx / sd
        residual_ss = sxx - 2 * point * sxd + point**2 * sdd
        # Roundoff can make the sum of squared residuals slightly negative.
        residual_ss = np.maximum(residual_ss, 0)
        variance = residual_ss * m / ((m - 1) * sd**2)
    point = np.where(sd > 0, point * scale, np.nan)
    variance = np.where(sd > 0, variance * scale**2, np.nan)
    sums[:, 0] *= scale
    return point, variance, sums[:, :2]


def acceleration(values):
    """Stratified delete-one jackknife, with 1/n_j weights for unequal arms."""
    c, t = values
    cp, _ = arm_point(c)
    tp, _ = arm_point(t)
    influences = []
    for j, v in enumerate(values):
        totals = v.sum(axis=0) - v
        with np.errstate(invalid="ignore", divide="ignore"):
            leave = np.where(totals[:, 1] > 0, totals[:, 0] / totals[:, 1], np.nan)
        jack = estimands(leave, tp) if j == 0 else estimands(cp, leave)
        influences.append((len(v) - 1) * (jack.mean(axis=0) - jack) / len(v))
    u = np.concatenate(influences)
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.sum(u**3, axis=0) / (6 * np.sum(u**2, axis=0) ** 1.5)


def bca_bounds(draws, point, a, alpha=0.025):
    probability = (np.mean(draws < point) + np.mean(draws <= point)) / 2
    z0 = stats.norm.ppf(probability)
    z = z0 + stats.norm.ppf([alpha, 1 - alpha])
    denominator = 1 - a * z
    if not np.isfinite(z0) or not np.isfinite(a) or np.any(denominator <= 0):
        return np.nan, np.nan, "invalid_bca_adjustment"
    probabilities = stats.norm.cdf(z0 + z / denominator)
    if not np.isfinite(probabilities).all() or probabilities[0] >= probabilities[1]:
        return np.nan, np.nan, "invalid_bca_adjustment"
    lo, hi = np.quantile(draws, probabilities)
    return float(lo), float(hi), "ok"


def calculate(m, scenario, values, seed, resamples, m_power=0.7):
    """Return intervals, elapsed component costs and separate production p-values."""
    started = time.perf_counter()
    c, t = values
    cp, vc = arm_point(c)
    tp, vt = arm_point(t)
    point = estimands(cp, tp)
    se = standard_errors(cp, tp, vc, vt)
    streams = np.random.SeedSequence(seed).spawn(2)
    cd, cv, cs = draw_moments(c, resamples, np.random.default_rng(streams[0]))
    td, tv, ts = draw_moments(t, resamples, np.random.default_rng(streams[1]))
    draws = estimands(cd, td)
    draw_se = standard_errors(cd, td, cv, tv)
    draw_seconds = time.perf_counter() - started
    config = m._bootstrap_config(dict(n_resamples=resamples, random_state=seed))
    specs = [
        dict(
            metric="outcome",
            aggregation="mean"
            if scenario["kind"] in {"binary", "money"}
            else "ratio_of_sums",
            outcome_kind="binary" if scenario["kind"] == "binary" else "continuous",
        )
    ]
    start = time.perf_counter()
    row = m._summary(specs, c, t, cs, ts, len(c), len(t), config).iloc[0]
    results = {
        "percentile": [
            (row[k + "_ci_low"], row[k + "_ci_high"], row[k + "_status"])
            for k in LABELS
        ]
    }
    costs = {
        "shared_resampling": draw_seconds,
        "percentile_summary": time.perf_counter() - start,
    }
    start = time.perf_counter()
    acc = acceleration(values)
    costs["bca_jackknife"] = time.perf_counter() - start
    for method in ("basic", "bca", "bootstrap_t", "bootstrap_t_symmetric"):
        start = time.perf_counter()
        bounds = []
        for j in range(4):
            status = results["percentile"][j][2]
            lo = hi = np.nan
            if status == "ok":
                if method == "basic":
                    qlo, qhi = np.quantile(draws[:, j], [0.025, 0.975])
                    lo, hi = 2 * point[j] - qhi, 2 * point[j] - qlo
                elif method == "bca":
                    lo, hi, status = bca_bounds(draws[:, j], point[j], acc[j])
                elif (
                    not np.isfinite(se[j])
                    or se[j] <= 0
                    or np.any(~np.isfinite(draw_se[:, j]))
                    or np.any(draw_se[:, j] <= 0)
                ):
                    status = "invalid_studentizer"
                else:
                    pivot = (draws[:, j] - point[j]) / draw_se[:, j]
                    if method == "bootstrap_t":
                        qlo, qhi = np.quantile(pivot, [0.025, 0.975])
                        lo, hi = point[j] - qhi * se[j], point[j] - qlo * se[j]
                    else:
                        q = np.quantile(np.abs(pivot), 0.95)
                        lo, hi = point[j] - q * se[j], point[j] + q * se[j]
            if status == "ok" and not (
                np.isfinite(lo) and np.isfinite(hi) and lo <= hi
            ):
                status = "numerical_failure"
            bounds.append((float(lo), float(hi), status))
        results[method] = bounds
        costs[method] = time.perf_counter() - start
    start = time.perf_counter()
    # A common subsample fraction preserves allocation asymptotically. Apply
    # each actual sqrt(m_j/n_j) correction before combining arm errors.
    fraction = min(len(c), len(t)) ** (m_power - 1)
    sizes = [max(2, int(np.floor(len(v) * fraction))) for v in values]
    smaller = [
        draw_moments(v, resamples, np.random.default_rng(r), size)[0]
        for v, r, size in zip(values, streams, sizes)
    ]
    ce = (smaller[0] - cp) * np.sqrt(sizes[0] / len(c))
    te = (smaller[1] - tp) * np.sqrt(sizes[1] / len(t))
    with np.errstate(invalid="ignore", divide="ignore"):
        errors = np.stack((ce, te, te - ce, te / cp - tp * ce / cp**2), axis=-1)
    bounds = []
    for j in range(4):
        status = results["percentile"][j][2]
        lo = hi = np.nan
        if status == "ok":
            if not np.isfinite(errors[:, j]).all():
                status = "unstable_denominator"
            elif np.ptp(errors[:, j]) == 0:
                status = "degenerate_distribution"
            else:
                qlo, qhi = np.quantile(errors[:, j], [0.025, 0.975])
                lo, hi = point[j] - qhi, point[j] - qlo
        bounds.append((float(lo), float(hi), status))
    results["m_out_of_n_basic"] = bounds
    costs["m_out_of_n_basic"] = time.perf_counter() - start
    start = time.perf_counter()
    bounds = [(np.nan, np.nan, "not_supported")] * 4
    pvalue, test_status = np.nan, "not_supported"
    if scenario["kind"] in {"money", "binary"}:
        df = (
            (vc + vt) ** 2 / (vc**2 / (len(c) - 1) + vt**2 / (len(t) - 1))
            if vc + vt > 0
            else np.nan
        )
        bounds = []
        for j, dof in enumerate((len(c) - 1, len(t) - 1, df)):
            if np.isfinite(se[j]) and se[j] > 0 and np.isfinite(dof):
                width = stats.t.ppf(0.975, dof) * se[j]
                bounds.append((float(point[j] - width), float(point[j] + width), "ok"))
            else:
                bounds.append((np.nan, np.nan, "degenerate_distribution"))
        bounds.append((np.nan, np.nan, "not_supported"))
        pvalue, test_status = m._welch(c[:, 0], t[:, 0])
    results["welch"] = bounds
    costs["welch"] = time.perf_counter() - start
    if scenario["kind"] == "binary":
        summary = m._summary(specs, c, t, cs, ts, len(c), len(t), config)
        m._binary_intervals(
            summary, specs, c, t, dict(method="newcombe", confidence_level=0.95)
        )
        binary = summary.iloc[0]
        results["wilson_newcombe"] = [
            (binary[k + "_ci_low"], binary[k + "_ci_high"], binary[k + "_status"])
            for k in LABELS[:3]
        ] + [(np.nan, np.nan, "not_supported")]
        pvalue = stats.fisher_exact(
            [
                [int(t[:, 0].sum()), len(t) - int(t[:, 0].sum())],
                [int(c[:, 0].sum()), len(c) - int(c[:, 0].sum())],
            ]
        ).pvalue
        test_status = "ok"
    return results, costs, pvalue, test_status


def scenario_list():
    original = audit.scenarios()
    return original + [
        dict(name="money_reversed_null", kind="money", nc=500, nt=100, pc=0.0, pt=0.0),
        dict(name="money_unequal_effect", kind="money", nc=100, nt=500, pc=0.0, pt=8.0),
        dict(
            name="money_reversed_effect", kind="money", nc=500, nt=100, pc=0.0, pt=8.0
        ),
    ]


def run_scenario(job):
    s, index, args = job
    m = audit.load_stats(ROOT / "src/retentioneering/stats/experiments.py")
    records, elapsed, separate = {}, Counter(), Counter()
    start = time.monotonic()
    for trial in range(args["trials"]):
        data_seed, boot_seed = np.random.SeedSequence(
            [args["seed"], index, trial]
        ).spawn(2)
        values, truths = audit.generate(s, np.random.default_rng(data_seed))
        seed = int(boot_seed.generate_state(1)[0])
        outputs, costs, pvalue, status = calculate(
            m, s, values, seed, args["resamples"], args["m_power"]
        )
        elapsed.update(costs)
        separate["available"] += int(status == "ok")
        separate["rejected"] += int(status == "ok" and pvalue < 0.05)
        for method, bounds in outputs.items():
            for j, (lo, hi, status) in enumerate(bounds):
                r = records.setdefault(
                    (method, LABELS[j]),
                    dict(
                        statuses=Counter(),
                        widths=[],
                        covered=[],
                        available=[],
                        rejected=[],
                        below=[],
                        above=[],
                    ),
                )
                r["statuses"][status] += 1
                available = status == "ok" and truths[LABELS[j]] is not None
                truth = truths[LABELS[j]]
                r["available"].append(available)
                r["covered"].append(bool(available and lo <= truth <= hi))
                r["below"].append(bool(available and hi < truth))
                r["above"].append(bool(available and lo > truth))
                r["rejected"].append(bool(available and not lo <= 0 <= hi))
                if available:
                    r["widths"].append(hi - lo)
    results = []
    for (method, label), r in records.items():
        n, a = args["trials"], sum(r["available"])
        row = dict(
            method=method,
            estimand=label,
            truth=truths[label],
            statuses=dict(r["statuses"]),
            availability=audit.rate(a, n),
            coverage_available=audit.rate(sum(r["covered"]), a),
            coverage_all=audit.rate(sum(r["covered"]), n),
            lower_miss_all=audit.rate(sum(r["below"]), n),
            upper_miss_all=audit.rate(sum(r["above"]), n),
            width_quantiles=dict(
                zip(
                    ("median", "p90", "p99"),
                    np.quantile(r["widths"], [0.5, 0.9, 0.99]).tolist(),
                )
            )
            if a
            else None,
        )
        if label in ("absolute_effect", "relative_effect"):
            row.update(
                rejection_all=audit.rate(sum(r["rejected"]), n),
                rejection_available=audit.rate(sum(r["rejected"]), a),
                is_type1_error=s["pc"] == s["pt"],
            )
        baseline = records[("percentile", label)]
        paired = np.array(r["covered"], dtype=float) - baseline["covered"]
        row["paired_joint_coverage_gain"] = dict(
            estimate=float(paired.mean()),
            mcse=float(paired.std(ddof=1) / np.sqrt(n)) if n > 1 else None,
            improved=int(np.sum(paired == 1)),
            worsened=int(np.sum(paired == -1)),
        )
        # Preserve trial-level decision bits for pairing and independent summaries.
        row["trial_bits"] = {
            k: "".join("1" if x else "0" for x in r[k])
            for k in ("available", "covered", "rejected")
        }
        results.append(row)
    return dict(
        scenario=s,
        results=results,
        elapsed_seconds=time.monotonic() - start,
        component_seconds=dict(elapsed),
        separate_test=dict(
            method="fisher" if s["kind"] == "binary" else "welch",
            availability=audit.rate(separate["available"], args["trials"]),
            rejection_all=audit.rate(separate["rejected"], args["trials"]),
            is_type1_error=s["pc"] == s["pt"],
        ),
    )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--trials", type=int, default=2000)
    p.add_argument("--resamples", type=int, default=1999)
    p.add_argument("--seed", type=int, default=214202610)
    p.add_argument("--workers", type=int, default=6)
    p.add_argument("--m-power", type=float, default=0.7)
    p.add_argument("--scenarios", nargs="*")
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    if (
        args.trials < 1
        or args.resamples < 2
        or args.workers < 1
        or not 0 < args.m_power < 1
    ):
        p.error("trials/workers >= 1, resamples >= 2, 0 < m-power < 1 required")
    scenarios = scenario_list()
    if args.scenarios and set(args.scenarios) - {s["name"] for s in scenarios}:
        p.error("unknown scenario")
    files = [
        Path(__file__),
        Path(audit.__file__),
        ROOT / "src/retentioneering/stats/experiments.py",
    ]
    report = dict(
        base_sha=BASE_SHA,
        python=platform.python_version(),
        numpy=np.__version__,
        scipy=scipy.__version__,
        file_sha256={
            str(f.relative_to(ROOT)): hashlib.sha256(f.read_bytes()).hexdigest()
            for f in files
        },
        config={k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
        thread_settings={
            k: os.environ.get(k) for k in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS")
        },
        results=[],
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    jobs = [
        (s, i, vars(args))
        for i, s in enumerate(scenarios)
        if not args.scenarios or s["name"] in args.scenarios
    ]
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        for result in pool.map(run_scenario, jobs):
            report["results"].append(result)
            args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
            print(
                result["scenario"]["name"],
                round(result["elapsed_seconds"], 2),
                "seconds",
                flush=True,
            )


if __name__ == "__main__":
    main()
