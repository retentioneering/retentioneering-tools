# Role 07 API integrity fixes

2026-10-30. Candidate `.work/ab-safe-bet/repo`, branch `codex/ab-safe-bet`. Changes remain uncommitted; no push. Initial oracle, source manifest and reports are preserved. This follow-up was explicitly authorized after the initial independent report.

**API-01 through API-05 are handled within a conservative input contract.** All 38 dedicated integrity/provenance cases pass; the focused existing compatibility run totals 182 passing tests. The original findings were reproduced on the candidate before modification. Five implementation/test files changed; no statistical module or `tools/ab_test.py` was edited by this reviewer.

| Finding | Implementation and retained boundary | Regression |
| --- | --- | --- |
| API-01: mixed IDs merged by SQL | Eventstream rejects heterogeneous scalar ID types before SQL, including `preprocess=False`. Object integer IDs are converted exactly to native signed/unsigned 64-bit columns. Out-of-range integers or one column combining negative integers and values above signed64 max reject with `SchemaConfigError`. | Mixed integer/string and bool/integer IDs reject; explicit object int/float near 2**53 rejects. Homogeneous strings, floats and integer IDs retain their contributions. Near-limit signed64/uint64 values work through metrics and A/B path export. |
| API-02: typed arm labels collapse | `_diff.py` retains original scalar labels instead of stringifying. `get_ab_paths` now exports integer 1 and string `'1'` distinctly, matching its scorecard grouping with and without assignment. | Both routes reproduce group means 7 and 3, never a merged 5. |
| API-03: mapping keys disappear | `add_segment(path_values=...)` rejects keys whose text/JSON forms collide. Numeric-key recipes include typed replay metadata keyed by wire key, so JSON sorting cannot scramble the association. | Colliding `1`/`'1'` mapping rejects even on a homogeneous source. Integer, float and boolean keys round-trip; float keys matching integer IDs and sorted JSON retain labels. |
| API-04: shared mutable lineage | Recording deep-copies inherited lineage and argument state; `recipe()` returns a deep copy. | Mutating caller mapping or nested exported recipe cannot change saved lineage or reconstructed dataframe. |
| API-05: non-JSON labels | NumPy scalar mapping keys/labels normalize to native scalars. `path_values` labels are strings, booleans, finite numbers or missing. Timestamp, custom-object and infinite labels raise `InvalidParameterError` before a derived stream is returned. | Dict and Series NumPy labels JSON round-trip. Unsupported labels explicitly reject. |

## Changed files

- `src/retentioneering/eventstream/eventstream.py`: `_validate_path_id_types`, constructor guards/normalization, defensive recipe export, typed-arm and persistent-label docstrings. This file already contained the root's provenance documentation correction, which was preserved.
- `src/retentioneering/ops.py`: deep recorded snapshots, native scalar normalization and typed mapping-key replay metadata.
- `src/retentioneering/data_processors/add_segment.py`: supported persistent scalar validation and serialized-key collision rejection.
- `src/retentioneering/tools/_diff.py`: typed `DiffGroups` labels and resolution results.
- `tests/eventstream/ab_safe_api_integrity_test.py`: 38 focused regression cases, including three independently constructed provenance cross-checks.

## Failing-before / passing-after evidence

`candidate_before/` and `candidate_followup_before/` rerun the unchanged initial drivers. They reproduce all five integrity findings before code edits. `regression_before.log` records the first 28 dedicated cases: **18 failed, 10 passed**. The passing cases include ordinary supported input and the root's existing provenance fix.

The root requested an additional large-integer boundary check. `numeric_boundary_before.json` preserves that exploration: object integers at 2**128 silently yielded zero metrics; object integers just above uint64 failed late with duplicate-index errors. `numeric_regression_before.log` has **5 failed, 2 passed** for the seven resulting numeric-domain regressions, before the exact-dtype/range correction. One exploratory `correct=false` flag for infinite float IDs came from output ordering; the later source-selectable probe compares by ID and confirms those contributions, so no float-infinity corruption claim is made.

Final focused command (run from the candidate):

```sh
PYTHONPATH=/Users/maximgodzi/Documents/ChatGPT/rete_presentation_case/.work/ab-safe-bet/repo/src \
  /Users/maximgodzi/Documents/ChatGPT/rete_presentation_case/.work/ab-integrated-wheel-env/bin/python -m pytest \
  tests/eventstream/ab_safe_api_integrity_test.py tests/ops_test.py \
  tests/data_processors/add_segment_path_values_test.py tests/tools/diff_test.py \
  tests/test_eventstream.py tests/eventstream/get_path_segments_test.py \
  tests/tools/ab_safe_provenance_test.py tests/eventstream/ab_methods_test.py \
  tests/data_processors/segments_test.py tests/data_processors/add_segment_metric_bins_test.py -q
```

Result: **182 passed** (`focused_final_numeric.log`). The final minor assertion extension checks A/B export as well as metrics at the large-integer boundary; `regression_final.log` records its completed 38-case rerun. Pinned repository pre-commit hooks pass on all five owned files, including Ruff, format, hygiene and gitleaks (`precommit_final.log`). `git diff --check` passes. Direct Ruff 0.17.3 initially reported additional preexisting rules because its defaults differ from repository-pinned Ruff 0.8.6; actual pinned hooks pass. No unrelated lint refactor was introduced.

One broader run (`focused_final.log`) had **178 passes, one failure** in `tests/eventstream/ab_integration_test.py::test_public_binary_intervals_have_same_orientation_and_roster`. The concurrent inference changes make flexible `auto` use Fisher/exact intervals, while that test explicitly requested Newcombe for the strict adapter. It therefore compared different interval methods. This was reported to the root/inference owner; this reviewer did not edit it. After its owner updated the expectation, the single-test cross-check passed (`integration_cross_recheck.log`: **1 passed**). Final integration-wide testing is still required after all reviewers finish.

The unchanged initial drivers were also rerun after the first fixes (`candidate_after/`, `candidate_followup_after/`). Their raw failed hypotheses now include intentional rejections of mixed IDs and unsupported timestamp labels, the known negative-seed rejection and original timezone misuse. They are not the acceptance tests for the narrowed contract. Existing unknown-key/falsy-argument validation observations remain; they were not promoted to broad validation guarantees or changed in unowned files.

## Restrictions, docs and final integration work

The supported integer-ID boundary is per-column signed64 **or** uint64, not arbitrary-precision Python integers. Object integer columns inside that boundary are normalized without any float conversion; mixed scalar families reject. The source dataframe must already retain distinct IDs: this library cannot recover identities rounded before construction. Floating-point and custom-ID domains have not been exhaustively certified.

`path_values` timestamps/custom objects/infinite labels now require explicit conversion to supported scalars. Missing labels remain supported. The recipe format includes `_path_values_keys` when needed to preserve numeric/boolean keys; old ordinary string-key recipes still work. General callable-based processor recipes remain outside this focused serialization audit.

Owned public docstrings document the new restrictions and typed arm output. The integrator should add the same constructor/recipe restrictions to `docs/guide/eventstream.md` and release notes, and regenerate reference pages. Provenance guide/ADR corrections are listed precisely in `CROSS_REVIEW.md`. `.agents/skills/.../references/api-map.md` and the `.claude` mirror were searched for the affected provenance wording; no direct matching contract claim was found in that bounded search.

No full suite, wheel build, package publication, CI or frontend build was performed here. Candidate hashes are snapshots of a shared working tree; final immutable-source/wheel verification belongs to the integration run. Original preregistration/oracle hashes remain unchanged.
