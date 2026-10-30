# Conservative A/B release review

This directory records a ten-role review of the A/B proposals in PRs #214–#218,
#220, #221 and #222. The proposed 5.4.0 release retains assignment-aware point
estimates and exact binary defaults, requires explicit selection of approximate
nonbinary inference, and defers the graph/step statistical screens and their
matrix widgets. See [the detailed review](FINAL_REVIEW.md),
[function matrix](FUNCTION_MATRIX.md), and [source revisions](provenance.json).

## Statistical boundary

- Binary defaults: two-sided Fisher tests, Clopper–Pearson arm intervals and a
  conservative difference interval obtained by allocating alpha across both
  arms. These are valid under the stated independent binary model. The interval
  is not the inversion of the Fisher test; neither CI exclusion nor MDE is a
  substitute for the adjusted p-value decision.
- Nonbinary defaults are descriptive. Explicit Welch and whole-unit bootstrap
  remain approximate. Independent heavy-tail experiments showed material
  undercoverage; more resamples did not repair missing tail information.
- Holm/Bonferroni retain all declared hypotheses, including unavailable rows.
  They require valid input p-values. BH additionally requires independence or
  suitable positive dependence; no correction covers optional stopping or an
  undeclared interactive search.
- Secondary stratum diagnostics are absent in exact/default/descriptive mode.
  Explicit approximate methods can expose supported, labelled exploratory
  diagnostics; these are not part of the primary corrected family.
- Assignment roster, complete logging cutoff, independent unit and pretreatment
  membership are analyst declarations. Event-observed populations, selected
  conditional transitions and missing measurements do not become causal ITT
  estimates merely because a test is available.
- Two-arm SRM uses an exact binomial law under independent Bernoulli assignment.
  Sparse multiarm diagnostics remain unavailable. Fixed/blocked allocation
  needs a design-specific audit.

Unsupported inputs fail clearly: ambiguous identity types, out-of-range integer
IDs, unsupported persistent labels, arithmetic overflow, changing paired-period
measurement eligibility and excessive resampling allocation. Undefined bootstrap
draws and insufficient Bonferroni tail resolution retain unavailable statuses.

## Reproduce the portable checks

After installing the built wheel, from outside the source checkout:

```sh
python /path/to/repo/examples/experiments/validation/safe_bet/replay_independent.py
```

The script compares 1,936 small contingency tables to integer-combinatorial
Fisher arithmetic and direct binomial-tail bisection, verifies a manually counted
repeated-window conversion, and checks a declared Holm family with an unavailable
member. It records the imported module path and source hash. It was assembled
**after** review; it does not pretend to be a newly blinded audit or repeat the
large historical simulations.

Run the candidate regressions and packaging gate from the repository:

```sh
PYTHONPATH=src python -m pytest tests/tools/ab_safe_* tests/eventstream/ab_safe_api_integrity_test.py tests/tools/conversion_safe_windows_test.py tests/tools/conversion_funnel_exact_test.py tests/tools/sample_ratio_safe_boundary_test.py tests/mcp/ab_safe_metadata_test.py tests/packaging -q
make build
uv build
python scripts/verify_wheel.py dist/retentioneering-5.4.0-py3-none-any.whl --output-dir artifacts/wheel
```

The wheel gate verifies all package bytes and the three required widget assets,
creates a fresh environment, checks dependencies, imports from site-packages in
isolated mode, and checks handwritten assignment-level arithmetic and actual
HTML exports. CI runs it on the supported Python matrix. Browser rendering and
full notebook execution are separate checks, not implied by HTML construction.

## Evidence and provenance

Each initial reviewer wrote a protocol and independent oracle before inspecting
implementation and peer reports. Initial evidence is frozen; later unblinded
cross-review and fixes are labelled. This is a staged oracle-first agent review,
not a formal blinded human study. The review reports disclose fixture mistakes,
negative findings, amendments, untested scope and the limits of simulation.

`reviews/` contains selected Markdown reports, small numerical summaries and
`.py.txt` mirrors of original oracles/adapters. Original oracle bytes are preserved;
adapter/report trailing whitespace may be normalized and is recorded. The text extension
prevents automatic source formatting from changing historical oracle bytes;
restore `.py` and follow each report's command/path instructions to rerun those
historical drivers. `EVIDENCE_MANIFEST.json` records the original relative path
and original/packaged SHA256 of each mirrored file, with any presentation transform. The adapters may reference the original audit
workspace; the portable script above and repository regressions are the direct
candidate acceptance entry points.

The complete raw local audit bundle also retains large simulations, original
command logs, screenshots, widget exports, intermediate wheels and failed runs.
Those large derived artifacts are not duplicated in the source distribution.
Historical report links to that workspace are provenance, not portable download
links. Final source, artifact and CI results are recorded in the release review;
passing source tests alone is not an installed-artifact claim.
