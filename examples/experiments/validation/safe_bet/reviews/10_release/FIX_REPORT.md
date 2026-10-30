# Role 10 — release-gate implementation

Follow-up on 2026-10-30. Candidate `.work/ab-safe-bet/repo`, branch `codex/ab-safe-bet`. This phase is explicitly unblinded implementation and cross-review. No final candidate wheel was built, and no commit, push, tag or publication was performed.

## Implemented

- `scripts/verify_wheel.py` validates nonempty `widget.js`, `widget-static.js` and `widget.css`, compares every non-cache package source/asset byte with the wheel, rejects missing/unexpected package files and duplicate archive entries, and checks the source project version against wheel metadata.
- The runner records source commit/status when the supplied directory is itself a Git checkout, source-file hashes, wheel hash, version and required assets. An archive inside another repository is not misidentified as that outer checkout.
- It creates a fresh environment, installs the exact wheel and runtime dependencies, runs `uv pip check`, then copies `installed_wheel_smoke.py` outside the checkout and invokes it with Python `-I`, no source `PYTHONPATH` or `PYTHONHOME`. Commands have explicit time bounds. An optional `--environment-dir` preserves a **new** environment for subsequent notebook verification; existing environments reject.
- Installed smoke verifies import location under the new environment, non-editable metadata, version and every installed package file against the wheel/source manifest. It checks independently handwritten binary, mean and ratio-of-sums aggregates with silent assignees, alias equality, population counts, and actual A/B/experiment HTML exports. It saves installed paths, dependency versions, numerical expectations and export hashes.
- All Python CI matrix entries build and exercise the installed wheel. Release and TestPyPI workflows run the same gate before their pre-existing publication step. All three workflows upload provenance and exports even when the gate fails. No publication workflow was triggered by this task.
- `AGENTS.md` now documents the installed-wheel gate and the actual `test-v*` trigger instead of stale `workflow_dispatch` instructions.

The installed smoke is a packaging/runtime contract, not a finite-sample inference certificate or replacement for browser/notebook tests.

## Evidence

`tests/packaging/wheel_integrity_test.py`: **7 passing tests**, including an independently absent test for each of the three assets, missing package code, changed package bytes, old version metadata, and a valid artifact. These negative controls directly exercise the reusable guard, rather than reproducing the predicate in test code. `gate_unit_tests.log` records the result.

End-to-end runner validation used the previously built and explicitly labelled **baseline 5.3.0 wheel**, SHA256 `7c62e83a0e35a6ae1b914e542d36ebae87c0056e28108fe7511a3b6267aa4b7b`, plus its clean archived source. A newly created offline environment passed source/installed hash equality, version checks, `uv pip check` (90 packages), all handwritten smoke assertions and both real exports. `gate_baseline_validation.log` and `gate_baseline_validation/` preserve the outcome. This is validation of the runner; it is **not evidence that the in-progress 5.4.0 candidate artifact has passed**.

Repository-pinned pre-commit checks pass on the seven owned paths: Ruff, format, YAML, whitespace/EOF, conflict/line-ending, large-file and secret checks (`gate_precommit.log`). No unrelated source was formatted by this role. Final candidate artifact verification remains deliberately pending the coordinator's freeze.

## Changed paths

- `.github/workflows/ci.yml`
- `.github/workflows/release.yml`
- `.github/workflows/test-release.yml`
- `scripts/verify_wheel.py`
- `scripts/installed_wheel_smoke.py`
- `tests/packaging/wheel_integrity_test.py`
- `AGENTS.md`

## Next final gate

After code/docs/UI are frozen and committed, build JS and the 5.4.0 wheel from that exact state, then run:

```sh
python scripts/verify_wheel.py dist/retentioneering-5.4.0-py3-none-any.whl \
  --output-dir artifacts/wheel --environment-dir /path/to/new/wheel-environment
```

The script contains no local workspace paths. Run the original Role10 independent `run_oracle.py` and `cross_review_candidate.py` from outside the checkout with the new wheel interpreter and **without** `--source`. Coordinate four full public notebooks with role09's `execute_notebook.py --installed-wheel` using copied notebook/support files outside the checkout. Record the final wheel hash, clean commit, actual site-packages imports and notebook outputs. Current partial source tests, the baseline gate validation and installed metadata `5.4.0` are not substitutes for this evidence. Then inspect live CI on the final PR head.
