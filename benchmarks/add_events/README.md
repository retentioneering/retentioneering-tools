# add_events: categorical concatenation versus sorted insertion

This benchmark accompanies issue #229. The baseline is **current master after
PR #228**, commit `7f36176ef53eccfb57eb55fd6a14937250007d40`, not the older code
profiled in the issue. Public API, source selection, `marker_subindex`, the
ADR-0017 layout check, and Eventstream construction are identical in every
variant. Only the final combination of original rows and markers differs.

## Reproduce

From the repository root, with the pinned dependencies from `uv.lock`:

```bash
uv sync
uv run python benchmarks/add_events/benchmark.py \
  --sizes 100000 1000000 10000000 --fractions .01 .05 .5 --repeats 3 \
  --output results.jsonl
```

The script uses `psutil` (included in the locked development environment). Run
on an idle machine and avoid running pytest, builds, or other benchmarks beside
it. Output files are **append-only**: use a fresh filename for a new experiment.
The baseline commit must be available locally (`git fetch origin master` if
needed). No checkout or working-tree mutation is performed by the benchmark.

Additional workloads:

```bash
uv run python benchmarks/add_events/benchmark.py --sizes 1000000 \
  --fractions .05 --repeats 3 --mode sql --output sql.jsonl
uv run python benchmarks/add_events/benchmark.py --sizes 1000000 \
  --fractions .05 --repeats 3 --mode repeat --output repeat.jsonl
uv run python benchmarks/add_events/benchmark.py --sizes 1000000 \
  --fractions .05 --repeats 3 --path-type string --output string.jsonl
```

## What is measured

- A fresh subprocess for **every** variant/repetition, with variant execution
  order rotated between repetitions. Seed 229; 50 original events per path,
  20 event categories, integer path IDs unless `--path-type string` is selected.
  Each row independently becomes `e0` with the requested probability. Actual
  input and output counts are recorded, so the realized marker fraction is known.
- Wall time covers the complete public `stream.add_events(...)` call, including
  source selection, fractional marker numbering, result construction and the
  production layout check (`ordering.STRICT` is false). Setup, imports,
  fingerprints, and input Eventstream preprocessing are outside the interval.
- The parent samples the worker's **RSS** every 1 ms between the ready/finished
  handshakes. It records RSS immediately before the operation, the sampled
  operation peak, and their difference, in MiB. Input and output coexist at the
  end of the interval. This captures native pandas/NumPy/Arrow allocations that
  `tracemalloc` alone misses. It is a sampled peak, not an exact allocation
  trace: allocator reuse, scheduling, and sub-millisecond peaks affect it.
  RSS before the operation includes interpreter/import and setup allocator state.
- All columns of the returned frame are fingerprinted, in row order, outside
  the interval. The driver requires identical fingerprints across variants and
  repetitions, and every worker verifies that the original frame was unchanged.
  Category metadata is tested separately: the production change deliberately
  preserves unused levels and their ordering instead of re-inferring them.
- Telemetry is disabled. OpenMP/OpenBLAS default to one thread; the experiment
  does not change DuckDB's own configuration. Environment/package versions are
  recorded with each observation.

## Variants and decision

1. **baseline**: the exact `AddEvents.apply` loaded from the pinned master
   commit, including concat, full stable sort and `astype("category")`.
2. **category**: the production change in this PR. Extend the existing event
   dictionary if necessary, give marker rows exactly that categorical dtype,
   then concatenate and perform the existing stable sort. `ignore_index=True`
   builds the required fresh RangeIndex directly instead of allocating temporary
   old row labels and discarding them afterward. Original event labels never
   pass through a full-length string array or need re-factorization. Appending a
   category can still copy/widen the integer code array; the operation is not
   zero-copy and still has work proportional to the number of original rows.
3. **merge**: an experimental alternative in `merge_candidate.py`, intentionally
   outside production. Sort new keys only, locate their insertion positions in
   the ordered original keys with NumPy `searchsorted`, and gather one combined
   frame. This is vectorized insertion, **not** a compiled linear-time two-pointer
   merge: binary searches cost O(m log n), in addition to new-row sorting and
   O(n + m) key/gather allocations. It preserves integer key precision and falls
   back to full sorting for noncanonical path blocks or nonnumeric position keys.

The category change removes the dominant label decoding/re-encoding cost with
little extra code. The insertion candidate can help at very low marker density,
but introduces key/index arrays, dtype and path-order handling, and substantial
regressions at higher density. We retain the existing stable sort rather than
add a hardware-sensitive density threshold or a compiled/JIT dependency. These
measurements do **not** establish that every possible linear merge is slower;
they explain why this dependency-free candidate is not shipped.

## Results

Measured 2026-11-01 on a shared Linux/x86-64 cloud host (AMD EPYC 9V74,
8 CPU quota, 8 GiB cgroup memory limit). Python 3.12.14, pandas 3.0.3,
NumPy 2.4.6, PyArrow 24.0.0, DuckDB 1.5.3; dependencies from `uv.lock`.
These are not Apple Silicon results. No extrapolation to 50M rows is made:
the memory budget is smaller than the issue author’s 16 GB machine.
Only this interpreter/dependency combination was benchmarked; correctness
on the supported Python versions is covered separately by tests/CI.

All 81 primary observations passed both fingerprint checks. Values below
are medians of three fresh-process runs. Raw observations, timing spread,
RSS before/peak/increase and fingerprints: [results.jsonl](results.jsonl).

| Original rows | Marker probability | Baseline seconds | Category seconds (shipped) | Merge seconds (rejected) | Baseline/category |
|---:|---:|---:|---:|---:|---:|
| 100,000 | 1% | 0.0442 | 0.0268 | 0.0229 | 1.65× |
| 100,000 | 5% | 0.0363 | 0.0216 | 0.0286 | 1.68× |
| 100,000 | 50% | 0.0592 | 0.0356 | 0.0734 | 1.66× |
| 1,000,000 | 1% | 0.2404 | 0.1012 | 0.0949 | 2.38× |
| 1,000,000 | 5% | 0.2285 | 0.1361 | 0.1673 | 1.68× |
| 1,000,000 | 50% | 0.4101 | 0.2779 | 0.5742 | 1.48× |
| 10,000,000 | 1% | 2.2609 | 1.1035 | 0.8758 | 2.05× |
| 10,000,000 | 5% | 2.4668 | 1.2463 | 1.8129 | 1.98× |
| 10,000,000 | 50% | 5.0200 | 3.8644 | 7.2216 | 1.30× |

| Original rows | Marker probability | Baseline peak RSS MiB | Category peak RSS MiB | Merge peak RSS MiB |
|---:|---:|---:|---:|---:|
| 100,000 | 1% | 342.2 | 336.0 | 334.6 |
| 100,000 | 5% | 342.8 | 337.3 | 335.7 |
| 100,000 | 50% | 352.5 | 346.1 | 347.2 |
| 1,000,000 | 1% | 520.3 | 476.0 | 468.9 |
| 1,000,000 | 5% | 526.4 | 481.7 | 481.3 |
| 1,000,000 | 50% | 633.9 | 577.2 | 589.4 |
| 10,000,000 | 1% | 2110.3 | 1801.9 | 1799.9 |
| 10,000,000 | 5% | 2265.3 | 1864.2 | 1904.1 |
| 10,000,000 | 50% | 3268.5 | 2842.5 | 2985.1 |

At 10M rows / 5% marker probability, the shipped change is **1.98× faster**,
with **17.7% lower total peak RSS**. The incremental RSS above the ready
input drops from approximately 1505 MiB to 1105 MiB. Sorting insertion
keys instead of the full frame is 45% slower than categorical full sorting
in this case, and around 87% slower at 50% marker probability.

At 1% density the insertion candidate is faster (0.876 vs 1.104 seconds
on 10M rows), but saves almost no peak RSS. That narrower benefit does not
justify a production density threshold plus fallback/dtype complexity in
this change. This is a deliberate scope decision, not a claim that full
sorting is theoretically optimal.

The 100k-row timings are small and particularly sensitive to shared-host
noise. Use the raw ranges, rerun on the target machine, and do not treat
three repetitions as a formal confidence interval.

### Supplemental workloads

Single observations (not medians), 1M original rows / 5% marker probability;
all outputs matched and inputs were unchanged. Raw data:
[supplemental.jsonl](supplemental.jsonl). These are smoke measurements for
workload diversity, not precise speedup estimates. To repeat them as a
three-run series use the commands above.

| Workload | Baseline s | Category s | Merge s | Baseline peak MiB | Category peak MiB | Merge peak MiB |
|---|---:|---:|---:|---:|---:|---:|
| SQL, reverse row order | 0.7275 | 0.5793 | 0.5754 | 549.6 | 507.1 | 506.4 |
| Second marker call | 0.2374 | 0.1217 | 0.1672 | 545.0 | 498.6 | 498.0 |
| String path IDs | 0.3772 | 0.2240 | 0.3257 | 588.6 | 556.4 | 564.0 |
