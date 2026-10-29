# Experiment examples and validation

Start with [the notebook guide](../../notebooks/EXPERIMENT_TUTORIAL.md).
The synthetic tutorial uses a fixed, unselected event simulation. The Lumeo
notebook then applies the same public API to a selected teaching fixture and
explains the limits of its sparse outcome support.

```bash
python examples/experiments/execute_tutorial.py
python examples/experiments/execute_tutorial.py --lumeo
python examples/experiments/validate_methods.py --trials 40 --resamples 999
```

The validation script uses an independent generator and calls
`Eventstream.experiment_data`, including event extraction and roster completion.
It reports interval availability, coverage over all simulations, conditional
coverage among available intervals, exact Monte Carlo uncertainty, binary-null
test rejections, an all-zero boundary and a repeated-session invariant.

A recorded local smoke with 40 trials per scenario and 250 units per arm gave:

| Scenario | Covered | Exact 95% Monte Carlo interval |
|---|---:|---:|
| Binary null | 37/40 | [79.61%, 98.43%] |
| Binary +3 pp | 39/40 | [86.84%, 99.94%] |
| Rare binary null | 40/40 | [91.19%, 100%] |
| Correlated-order ratio, effect 0 | 39/40 | [86.84%, 99.94%] |
| Correlated-order ratio, effect +8 | 35/40 | [73.20%, 95.81%] |

The +8 result is retained. This is a small smoke check with wide Monte Carlo
uncertainty, not nominal-coverage certification. Use more trials and vary
imbalance, outcome rarity, tail severity and denominator support before making
stronger performance claims. Do not select seeds or discard unavailable trials
to improve reported coverage.

Independent fast tests provide other kinds of evidence: a positional scanner
for ordered reach, hand-calculated means/ratios, literal tiny bootstrap samples,
uniform repeated-session invariance, data/configuration immutability and
headless/widget/export parity. These complement calibration; they do not replace
it. Lumeo is not used as a coverage oracle.

`build_lumeo_sample.py` reproduces the deterministic whole-user sampling stage
from the parent dataset and receipt. Runtime tutorials use the frozen tables
and verify their hashes; they do not regenerate or modify that parent dataset.
