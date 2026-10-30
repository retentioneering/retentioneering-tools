# Final installed-artifact verification — frozen 5.4.0 package

**All local artifact gates pass for commit `aac5866c232a48bee3ca071c31c52fa26a818c51`.** No package publication, tag, merge or push was performed by this role. Live CI at the eventual PR head and a refreshed source distribution for any later documentation/test-only commit remain separate final steps.

## Exact artifact identity

| Item | Identity |
|---|---|
| Frozen source | `aac5866c232a48bee3ca071c31c52fa26a818c51` |
| Version | `5.4.0` |
| Wheel SHA256 | `d334f02b8d6d670b17a3e36c178c284d101e6c4790c3b600272f8e325c207879` |
| Source distribution SHA256 | `1e152db54f8a8813d9d378bf8e14bd3192ca46e82ac31f3497cdc5e3203e221c` |
| Canonical package-file hash-map SHA256 | `8405a48e94aa0ace8b0d9863919180f7d364429a031a04f940e22eac5061bb66` |
| Package files | 99; byte-equal across exported source, sdist, wheel and installed files |
| Live widget JS SHA256 | `c46fddb448cca76dddd0a3660af0f31d281c12a7286c550c4e3aeb763624bc47` |
| Static export JS SHA256 | `c18d536a639ba235766bc0474ef31635bdc9d98fe205fd59bb40373a85a75dd1` |
| CSS SHA256 | `486059a2fe1ee7c68084a4e8912ceca76a4afdd5ab3b74435623d96c30ba47f9` |

The package hash-map digest is SHA256 of the `source_package_sha256` mapping serialized with sorted keys and compact JSON separators. This lets a later test/documentation-only commit prove package identity without relying on the version string.

Preflight found one concurrent working-tree edit in `tests/browser/ab_test_test.py`, belonging to the UI review. The coordinator confirmed it was a browser-wording golden correction. To avoid including uncommitted material, this build used **`git archive` of the frozen commit**, plus the three verified generated widget assets. No package/JS source, metadata or build-script difference from the frozen commit was present. `freeze-provenance.json` records the actual working-tree status rather than claiming that the shared checkout was clean.

`python -m build --no-isolation` first built the source distribution and then built the wheel **from that sdist**. `sdist-wheel-parity.json` independently confirms all 99 package files match. The frozen source sdist intentionally contains the earlier browser test; after the final test/docs-only commit, rebuild its sdist and verify whether the wheel remains byte-identical. Existing package verification is tied to exact package hashes, not an unqualified “latest source” claim.

## Fresh installed environment

A new environment was created at `.work/ab-safe-bet/role10-final-wheel-env`. It installed the locally built wheel, not an editable checkout or an existing distribution. The reusable gate copied its smoke script to a temporary directory outside the checkout, removed source path environment variables, used Python isolated mode (`-I`), verified the installed location and every package-file hash, and exported both real widgets.

Actual imported package:

`/Users/maximgodzi/Documents/ChatGPT/rete_presentation_case/.work/ab-safe-bet/role10-final-wheel-env/lib/python3.13/site-packages/retentioneering/__init__.py`

Runtime: Python 3.13.17 on macOS ARM64. Full versions appear in `FINAL_SUMMARY.json`. The runtime-only installation had 90 packages and passed `uv pip check`. The offline cache lacked test/notebook tools, so an approved pinned install added pytest 9.2.0, nbclient 0.11.0, nbformat 5.11.1, nbconvert 7.17.2 and ipykernel 7.4.0. The resulting 117-package environment passes `uv pip check`; a final installed-file hash check confirms the retentioneering wheel contents remain unchanged. Both the unsuccessful offline tooling attempt and successful install logs are preserved. No claim is made that this resolver environment is the exact repository lock environment.

## Executed verification

| Check | Result | Evidence |
|---|---|---|
| Reusable artifact gate | All three nonempty assets; source/version/installed-file parity; isolated public numerical smoke and actual A/B + experiment exports pass | `gate/`, `gate.log` |
| Original preregistered Role10 oracle | **26 point/population checks pass**; assigned binary, mean, ratio and silent users; repeat and row-permutation outputs stable | `independent-oracle.json`, unchanged original oracle hash |
| Fresh post-review adversarial runner | **208/208 pass** against installed wheel; exact/default and secondary-strata restrictions, SRM, provenance, identity, conversion, resource/overflow/pairing guards | `independent-cross-review.json` |
| Portable independent probability replay | **1,936 tables**; maximum Fisher error 1.11e−16, exact-difference bound error 4.44e−16; both finite-window conversions and declared-missing Holm family pass | `portable-replay.json` |
| Copied retained regression suites | **216 passed, zero skipped**, 5.33 seconds across 15 frozen test files | `wheel-regressions.log`, XML, `wheel-regression-imports.json` |
| Full public notebooks | **All four pass**, with complete assertions and real default data generation | Role09 `wheel_verification_summary.json` and `wheel_*` evidence directories |
| Final dependency and installed-file checks | 117 packages compatible; all 99 installed package files still match | `pip-check-final.log`, `FINAL_SUMMARY.json` |

Regression inputs were copied from the frozen source into `external_checks/tests`; no `src` directory was copied. Pytest ran through an isolated interpreter with an external configuration. Its wrapper verified every imported retentioneering module lived under the new environment and matched frozen package hashes. The original oracle is run without `-I` solely to import its sibling independent `oracle.py`; its script/cwd are outside the checkout, `PYTHONPATH`/`PYTHONHOME` are removed, and its recorded package origin is the same installed wheel. The other adversarial and portable runners use `-I`.

The 208-check contract and its explicitly disclosed six constant-outcome availability amendments are described in `../CROSS_REVIEW.md`; no new amendment was needed for the installed wheel. The original oracle remains SHA256 `5ab59d156e0ba971a76dbe8607de1df24ffc26bc526e1106e587321352530c2c`.

## Complete notebook execution

Role09 independently staged 16 notebook/support files directly from frozen Git objects, with no source package directory, then executed them using the new wheel interpreter and `--installed-wheel`. No fixture generator monkeypatch or source-path injection was used. All actual imports point to the installed package above.

| Notebook | Code cells executed | Seconds | Outcome |
|---|---:|---:|---|
| `ab_testing_tutorial` | 33 | 16.96 | Full assertions pass |
| `ab_testing_showcase` | 56 | 21.28 | Embedded full assertions pass |
| `experiment_tutorial` | 15 | 18.91 | Pass |
| `lumeo_experiment_tutorial` | 19 | 20.72 | Pass |

The source commit, wheel hash, input notebook hashes, appended tutorial-check hash, runtime origins, executed notebooks, rendered HTML and raw tables are retained under `roles/09_datasets/followup_docs_20261030`. `FINAL_SUMMARY.json` copies the compact notebook summary for a self-contained release audit. These timings are observations, not performance guarantees.

## Remaining release boundary

This is meaningful local verification of the **actual candidate package**, replacing the earlier baseline-only artifact evidence. It does not certify all possible statistical inputs or designs. Preserve the documented independent-unit, complete-observation, fixed-family, exact/approximate, conditional-population, numeric-domain and resource restrictions, and the explicit deferral of new graph/step inference.

Before the requested PR is presented as complete: build an updated sdist for the final docs/test-only head, prove package/wheel identity to this verified artifact, and inspect live CI on that head. The coordinator owns those branch/PR actions and the separate full source/JS/browser/slow-test evidence. Passing focused counts here must not be relabelled as the full suite or added together as independent statistical experiments. Only one local Python/platform/dependency combination was exercised by this installed-wheel phase; CI supplies its own supported-version checks.
