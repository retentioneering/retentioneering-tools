"""Isolated end-to-end add_events benchmark; see README.md for the protocol."""

import argparse
import gc
import hashlib
import importlib
import json
import os
import platform
from pathlib import Path
import subprocess
import sys
import threading
import time

os.environ["RETENTIONEERING_NO_TRACK"] = "1"
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")

BASE = "7f36176ef53eccfb57eb55fd6a14937250007d40"
ROOT = Path(__file__).resolve().parents[2]


def fingerprint(df):
    import pandas as pd

    h = hashlib.sha256()
    for col in df:
        h.update(pd.util.hash_pandas_object(df[col], index=False).values.tobytes())
    return h.hexdigest()


def worker(args):
    import numpy as np
    import pandas as pd
    import duckdb
    import pyarrow

    from retentioneering.eventstream.eventstream import Eventstream

    module = importlib.import_module("retentioneering.data_processors.add_events")
    if args.variant == "baseline":
        source = subprocess.check_output(
            [
                "git",
                "show",
                f"{args.baseline_ref}:src/retentioneering/data_processors/add_events.py",
            ],
            cwd=ROOT,
            text=True,
        )
        namespace = {}
        exec(compile(source, "baseline_add_events.py", "exec"), namespace)
        module.AddEvents.apply = namespace["AddEvents"].apply
    elif args.variant == "merge":
        from merge_candidate import merge_sorted

        module._combine_events = merge_sorted

    rng = np.random.default_rng(args.seed)
    codes = rng.integers(1, 20, args.rows, dtype=np.int8)
    codes[rng.random(args.rows) < args.fraction] = 0
    paths = np.arange(args.rows, dtype=np.int64) // 50
    if args.path_type == "string":
        paths = pd.Series(paths).astype("string")
    frame = pd.DataFrame(
        {
            "user_id": paths,
            "event": pd.Categorical.from_codes(codes, [f"e{i}" for i in range(20)]),
            "timestamp": pd.Timestamp("2020-01-01")
            + pd.to_timedelta(np.arange(args.rows) % 50, unit="s"),
        }
    )
    stream = Eventstream(frame)
    del frame, codes, paths
    if args.mode == "repeat":
        # Use identical production setup, outside the measured interval.
        selected_apply = module.AddEvents.apply
        selected_combine = module._combine_events
        importlib.reload(module)
        stream = stream.add_events("existing", source_event="e0")
        module.AddEvents.apply = selected_apply
        module._combine_events = selected_combine
    gc.collect()
    before = fingerprint(stream.df)
    rss_pid = (
        int(Path("/proc/self").resolve().name)
        if sys.platform == "linux"
        else os.getpid()
    )
    print(
        json.dumps({"ready": True, "input_rows": len(stream.df), "rss_pid": rss_pid}),
        flush=True,
    )
    sys.stdin.readline()
    start = time.perf_counter()
    if args.mode == "sql":
        result = stream.add_events(
            "m",
            sql="SELECT * FROM eventstream WHERE event = 'e0' ORDER BY user_id DESC, index DESC",
        )
    else:
        result = stream.add_events("m", source_event="e0")
    elapsed = time.perf_counter() - start
    print(json.dumps({"seconds": elapsed, "output_rows": len(result.df)}), flush=True)
    sys.stdin.readline()  # Keep output alive until the RSS sampler stops.
    assert fingerprint(stream.df) == before, "input mutated"
    print(
        json.dumps(
            {
                "fingerprint": fingerprint(result.df),
                "python": platform.python_version(),
                "pandas": pd.__version__,
                "numpy": np.__version__,
                "duckdb": duckdb.__version__,
                "pyarrow": pyarrow.__version__,
                "platform": platform.platform(),
            }
        ),
        flush=True,
    )


def run(args):
    import psutil

    results = []
    for rows in args.sizes:
        for fraction in args.fractions:
            for repeat in range(args.repeats):
                # Rotate variant order across repetitions to reduce order bias.
                variants = (
                    args.variants[repeat % len(args.variants) :]
                    + args.variants[: repeat % len(args.variants)]
                )
                for variant in variants:
                    command = [
                        sys.executable,
                        str(Path(__file__).resolve()),
                        "--worker",
                        "--rows",
                        str(rows),
                        "--fraction",
                        str(fraction),
                        "--variant",
                        variant,
                        "--seed",
                        str(args.seed),
                        "--baseline-ref",
                        args.baseline_ref,
                        "--mode",
                        args.mode,
                        "--path-type",
                        args.path_type,
                    ]
                    with subprocess.Popen(
                        command,
                        stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        text=True,
                        cwd=ROOT,
                    ) as child:
                        ready = json.loads(child.stdout.readline())
                        process = psutil.Process(ready.pop("rss_pid"))
                        initial = process.memory_info().rss
                        samples = [initial]
                        stop = threading.Event()

                        def sample():
                            while not stop.wait(0.001):
                                try:
                                    samples.append(process.memory_info().rss)
                                except psutil.NoSuchProcess:
                                    return

                        sampler = threading.Thread(target=sample)
                        sampler.start()
                        child.stdin.write("go\n")
                        child.stdin.flush()
                        measured = json.loads(child.stdout.readline())
                        samples.append(process.memory_info().rss)
                        stop.set()
                        sampler.join()
                        child.stdin.write("done\n")
                        child.stdin.flush()
                        metadata = json.loads(child.stdout.readline())
                        assert child.wait() == 0
                    record = dict(
                        rows=rows,
                        fraction=fraction,
                        repeat=repeat,
                        variant=variant,
                        seed=args.seed,
                        baseline_ref=args.baseline_ref,
                        mode=args.mode,
                        path_type=args.path_type,
                        rss_before_mib=initial / 2**20,
                        peak_rss_mib=max(samples) / 2**20,
                        rss_increase_mib=(max(samples) - initial) / 2**20,
                        rss_sample_count=len(samples),
                        **ready,
                        **measured,
                        **metadata,
                    )
                    results.append(record)
                    print(json.dumps(record), flush=True)
                    if args.output:
                        args.output.parent.mkdir(parents=True, exist_ok=True)
                        with args.output.open("a") as output:
                            output.write(json.dumps(record) + "\n")
    for rows in args.sizes:
        for fraction in args.fractions:
            hashes = {
                r["fingerprint"]
                for r in results
                if r["rows"] == rows and r["fraction"] == fraction
            }
            assert len(hashes) == 1, (rows, fraction, "variant outputs differ")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--fraction", type=float, default=0.05)
    parser.add_argument(
        "--variant", choices=["baseline", "category", "merge"], default="category"
    )
    parser.add_argument(
        "--sizes", nargs="+", type=int, default=[100_000, 1_000_000, 10_000_000]
    )
    parser.add_argument(
        "--fractions", nargs="+", type=float, default=[0.01, 0.05, 0.5, 1.0]
    )
    parser.add_argument(
        "--variants",
        nargs="+",
        choices=["baseline", "category", "merge"],
        default=["baseline", "category", "merge"],
    )
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--seed", type=int, default=229)
    parser.add_argument("--mode", choices=["source", "sql", "repeat"], default="source")
    parser.add_argument("--path-type", choices=["int", "string"], default="int")
    parser.add_argument("--baseline-ref", default=BASE)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.repeats < 1 or any(n < 1 for n in [args.rows, *args.sizes]):
        parser.error("sizes and repeats must be positive")
    if any(not 0 <= f <= 1 for f in [args.fraction, *args.fractions]):
        parser.error("fractions must be between zero and one")
    (worker if args.worker else run)(args)


if __name__ == "__main__":
    main()
