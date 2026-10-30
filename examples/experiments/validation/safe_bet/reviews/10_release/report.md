# Role 10 — adversarial release and reproducibility review

Date: 2026-10-30. Initial independent pass; no peer reports or CHECKPOINT were read, and no candidate code was changed or used as evidence. Frozen baseline: `38cbe91d5b315584575b132a596890e69eb210aa` (clean). This report does **not** approve the partial candidate checkout or certify every statistical method.

## Result

The frozen baseline passes this role's independently handwritten assigned-user binary, mean and ratio aggregate oracle. A clean archive of that revision builds a wheel containing all 99 tracked package source/assets without byte differences. The wheel installs into a fresh environment, reproduces the source fixture from `/private/tmp`, exports both report widgets, and generates all six teaching datasets at small size. Two wheel builds using the same prepared source and toolchain are byte-identical.

The release boundary still needs strengthening: its automated wheel guard only checks the live widget bundle. A deliberately incomplete wheel missing the static export bundle passes that guard. Final-candidate source identity, artifact verification, full checks, peer cross-review and live CI remain mandatory gates.

## Independence and exact oracle

`PRE-REGISTRATION.md` and `oracle.py` were written and SHA256-recorded before any implementation, public API examples, repository instructions or peer findings were inspected. Only PROTOCOL.md and the assigned paths/revision were known. The adapter `run_oracle.py` was written after unblinding; the oracle was not amended.

- Preregistration SHA256: `9f99e35bc1bd6d621a518c30ef2e284974aef14862247386b7a8cb92ffab804d`.
- Oracle SHA256: `5ab59d156e0ba971a76dbe8607de1df24ffc26bc526e1106e587321352530c2c`.
- Four assigned users per arm; control has two silent users and treatment one. Six event rows across five observed users. Independent unit: assigned user. Complete fixed calendar window is explicitly declared.

| Quantity | Control | Treatment | Treatment − control |
|---|---:|---:|---:|
| Purchase binary | 1/4 | 2/4 | 1/4 |
| Revenue per assigned user | 10/4 | 35/4 | 25/4 |
| Revenue per purchase, ratio of sums | 10/1 | 35/3 | 5/3 |

`Eventstream.experiment_data` and `Eventstream.ab_test_data` match the exact expectations within 1e-12; 26 point/population assertions pass. `get_ab_test` agrees with its alias. The strict and flexible results each repeat exactly under seed 109 and under simultaneous event/assignment row reversal, and source/wheel aggregate payloads agree. The independent file also records sample-variance and ratio delta-variance arithmetic, but this pass does **not** compare or certify those inferential quantities.

A zero-purchase variant is rejected by the flexible API's event-catalog validation (`InvalidMetricConfigError: Event 'purchase' not found`). This is a clear error, not an undefined ratio silently changed to zero. Because that guard precedes the flexible ratio calculation, it is not coverage of every zero-denominator path; other roles must validate that numerical boundary.

## Findings and release gates

### R10-01 — P1 release gate: final candidate provenance remains unverified

**Category:** untested final scope / release evidence, not a demonstrated baseline numerical defect.

The assigned candidate is a partially modified checkout. Every artifact in this role's initial report is explicitly the frozen baseline. Both source imports and installed distributions report version `5.3.0`; this version alone cannot identify a source commit or an uncommitted patch. `src/retentioneering/__init__.py:5` reads installed distribution metadata, and `pyproject.toml:7` fixes the distribution version. In a source-overlay environment that metadata can belong to another installed build even though `__file__` points at the selected checkout.

**Expected final evidence:** exact candidate commit, clean/explicit dirty status, source-file/diff hashes, built wheel hash, imported module paths, dependency versions and test outputs tied to that same artifact. **Actual:** baseline evidence only; no claim is made about candidate readiness.

**Remedy / regression:** commit the retained candidate, build its JS and wheel, install that exact wheel into a fresh environment outside any checkout, rerun the source-selectable runner without `--source`, run retained-method regressions and widget export smoke, record the manifest, then inspect live CI on the same PR head. An older installed wheel must never be relabelled as the candidate. No package publication or release tag is required to open the requested PR.

### R10-02 — P2 verification defect: required static-export asset is outside the release guard

**Exact locations:** `.github/workflows/release.yml:42–68`, `.github/workflows/test-release.yml` build/verify steps, `.github/workflows/ci.yml:55–76`; package inclusion is in `pyproject.toml` `[tool.hatch.build]`.

Source tests run before the wheel is built. The later distribution check only tests for `retentioneering/static/widget.js`, and the PR workflow does not exercise an installed wheel. `widget-static.js` is required by HTML export, and `widget.css` is a separate asset.

**Reproduction:** in a role-local clean archive only, move `src/retentioneering/static/widget-static.js` aside, build a wheel, then apply the current `widget.js` existence predicate. `release_guard_negative_control.json` records `existing_release_widget_js_check_passes=true` and `required_export_bundle_present=false`. Original baseline and valid wheel were preserved. This is a synthetic negative control exposing a guard gap; the baseline wheel itself has all assets.

**Expected:** incomplete runtime artifacts fail before publication. **Actual:** the existing artifact predicate accepts this intentionally incomplete wheel. **Scope:** offline report export packaging, and more generally differences between source-tested files and installed distribution contents.

**Remedy / regression:** check all three assets and run numerical public-API plus real widget `export_html` smoke using the built wheel in an isolated environment. Add a negative guard check that removes each required asset and proves rejection. The valid wheel's all-source hash manifest and actual exports in this report demonstrate a feasible acceptance test.

### R10-03 — P2 verification boundary: narrative smoke tests are checkout tests, not installed examples

**Exact locations:** `tests/notebooks/tutorial_smoke_test.py:27–51`, `tests/notebooks/showcase_smoke_test.py:28–52`, `tests/tools/experiment_coverage_test.py:223`, `tests/conftest.py:35–45`.

Notebook smoke setup injects checkout `src` into `sys.path` and replaces the default Northwind generator with a repository fixture; the coverage CLI test explicitly sets source `PYTHONPATH`. These are legitimate, useful checks, but their success does not establish installed-wheel or unmodified public-example reproducibility. The dataset suite separately tests the fixture against its generator, so fixture use alone is not reported as a dataset bug. Normal pytest also skips tests marked `slow` unless explicitly enabled.

**Remedy / regression:** keep source tests, add a separate wheel-only job with no source path injection or generator monkeypatch. Run representative tutorial code and small named dataset loaders with the packaged library; require browser checks for displayed claims and retained-method statistical validation separately. Record skips and actual run counts. This role's wheel smoke covers all six small generators and HTML production, but not full notebook narrative or browser rendering.

### R10-04 — P3 documentation mismatch: TestPyPI trigger instructions are stale

`AGENTS.md:205` describes a manual `workflow_dispatch` pipeline. `.github/workflows/test-release.yml:20` uses pushed `test-v*` tags; `Makefile:79–100` agrees with the tag flow. Correct the agent-facing description so review and release instructions match the actual trigger. This is incidental documentation cleanup, not a reason to publish or tag during this task.

## Artifact evidence

| Evidence | Result |
|---|---|
| Clean baseline revision | `38cbe91d5b315584575b132a596890e69eb210aa`; clean before and after |
| Build source | `git archive HEAD` from frozen baseline into role-local `.work/ab-safe-bet/role10-build` |
| JS dependency/install/build | `npm ci --offline`; both workspace builds succeed |
| Wheel build | `python -m build --no-isolation --wheel`; existing documented build runtime |
| Valid baseline wheel SHA256 | `7c62e83a0e35a6ae1b914e542d36ebae87c0056e28108fe7511a3b6267aa4b7b` |
| Repeat wheel SHA256 | Identical; same prepared source and toolchain, not independent-machine reproducibility |
| Package integrity | 104 entries; all 99 tracked package files present and byte-matching; three widget assets present |
| Fresh installation | New venv, installed only the locally built baseline wheel and resolved cached runtime dependencies; no editable package |
| Wheel import | `.work/ab-safe-bet/role10-wheel-env/lib/python3.13/site-packages/retentioneering/__init__.py` |
| Oracle execution | `/private/tmp`, `PYTHONPATH` removed; pass; output equal to source |
| Widgets | `ab_test` and `experiment` export actual HTML, empty widget errors; no browser rendering claim |
| Datasets | Six named loaders and assignment loaders run with `n_users=100, seed=109`; these cases deliberately have different roster sizes |
| Dependency compatibility | `uv pip check`: 90 packages compatible |
| Runtime | Python 3.13.17, NumPy 2.5.4, pandas 3.1.0, SciPy 1.18.1, DuckDB 2.0.0, PyArrow 26.0.0 |

Fresh installation used cached packages under declared package constraints, **not** a claim that the exact `uv.lock` environment was reproduced. Full versions and both lock hashes are saved. Initial uv cache access was sandbox-blocked; a narrowly scoped approved offline cache use succeeded. No dependency was downloaded for this role's install. The normal `pip` module is absent in provided environments, so `uv pip check` was used. The `__version__` value is recorded alongside module path and wheel/source hashes rather than used as provenance by itself.

## Inventory and explicit limits

`method_inventory.json` inventories eleven Eventstream surfaces, signatures, documentation, source locations and textual test mentions, plus shared statistical/helper functions. A textual test mention is not validation coverage. Decisions below are this role's validation boundary; final retained/restricted/deferred decisions depend on all independent reviews.

| Surface / branch family | This initial role established | Still needed for retained scope |
|---|---|---|
| `experiment_data` binary/mean/ratio of sums | Assigned-unit point arithmetic, silent units, deterministic fixture, wheel equality | Interval/test boundary validation and nominal-coverage limits |
| Strict mean of ratios, funnels, `session_funnel_data` | Inventoried | Independent eligibility, conditional population and cluster-resampling validation |
| `ab_test_data` / `get_ab_test` | Same tiny point oracle, alias equality, seed/reversal fixture | Metric registry families, missing data, validation, extreme numbers, each supported test |
| Flexible period/strata/horizon/active/ITT branches | Inventoried | Population, joint contrast, multiplicity and pre-exposure provenance checks |
| `get_ab_bootstrap` / `get_ab_paths` | Inventoried | Joint draws, zero denominators, signed contrast mapping, shape and source/wheel regression |
| `get_sample_ratio` | Public call and fixture output saved | Analytic/exact methods, declared allocation design and population diagnostics |
| Wilson/Newcombe, pooled z, Welch, exact Fisher, bootstrap, log-relative intervals | Shared function inventory | Each method's boundaries and assumptions; finite tests are not universal coverage |
| Holm/Bonferroni/BH/none, MDE, heterogeneity and balance | Inventoried | Family definition, unavailable members, zero variance, power/verdict semantics |
| `funnel_data`, `funnel`, `get_conversion_rate` inference | Inventoried | Event-observed vs assignment populations, nesting/conditioning, independence |
| `ab_test` and `experiment` widgets | Actual wheel construction/export with real assets | Browser rendering and payload/status fidelity |
| MCP/report/session persistence | Source inventory/contract inspection | Session-bound roster, serialization and displayed claims |
| PR221 graph/step inferential proposals | Outside frozen baseline tested here | Explicit defer/restrict/retain decision and separate independent validation |

No broad inference endorsement follows from these 26 arithmetic assertions. Only one supported Python/dependency/platform combination was exercised here. Heavy tails, sparse-event coverage, adaptive monitoring, clustered/paired/crossover experiments, all registry metrics, all adjustment branches and all UI paths are not certified.

## Required final PR gates

1. Publish a final function/method matrix with retained, restricted and deferred entries and clear reasons; propagate restrictions to API docs, examples, MCP and widgets.
2. Resolve material peer findings with failing-before public fixtures and independent rechecks; preserve original oracle hashes/evidence.
3. Freeze the candidate commit after code, tests, documentation and generated assets agree. Record dirty-state/diff/source hashes if any noncommitted artifact is used.
4. Build from that commit, verify packaged source/assets, install the wheel separately and rerun the oracle plus targeted regressions and export smoke. Do not substitute the baseline wheel attached here.
5. Run repository-required lint, full Python matrix, JS tests/build, rendered browser tests, documentation generation and tutorials; select and report long statistical validations for retained scope. Report skips honestly.
6. Run cross-review against **all** role reports and the final candidate. Inspect live CI at the final PR head and fix failures. A passing local smoke is not a green CI claim.
7. Create the separate reviewable PR with scope, limitations, provenance and reproduction instructions. No merge, tag or package publication in this task.

## Reproduction

From the workspace root, with the provided runtime:

```sh
.work/ab-integrated-wheel-env/bin/python output/AB_Safe_Bet_Review/roles/10_release/oracle.py
.work/ab-integrated-wheel-env/bin/python output/AB_Safe_Bet_Review/roles/10_release/run_oracle.py --source .work/ab-safe-bet/baseline220 --output /private/tmp/r10-baseline.json
```

For final-candidate source testing, replace `--source` with the candidate checkout and name the output accordingly. For the installed artifact, use the freshly created wheel interpreter, change directory to `/private/tmp`, unset `PYTHONPATH`, invoke `run_oracle.py` by absolute path and omit `--source`. `wheel_smoke.py OUTPUT_DIRECTORY` similarly exercises actual packaged assets. The command log records the baseline artifact build steps. Original raw JSON, logs, source-selectable runner, manifest, environment versions, valid wheel, synthetic negative-control wheel and HTML exports are preserved next to this report.
