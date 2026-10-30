# Role 09 — documentation and tutorial integration

Date: 2026-10-30. Candidate: `.work/ab-safe-bet/repo`. This follow-up was authorized after all ten initial independent audits. It is an integration review, not a new independent calibration experiment. The preregistered oracle and initial report remain unchanged.

## Result

The guide, reference templates, public A/B/strict/SRM docstrings, ADR-0018, API-map mirrors, four tutorials and their narrative assertions now describe the conservative release boundary. Dataset generators were not changed. All four notebooks passed a complete source execution before the final MCP precision adjustment. Root subsequently corrected the precision golden to direct parity and reported **2 passed** for the actual `tests/notebooks` suite. The frozen source commit is `aac5866c232a48bee3ca071c31c52fa26a818c51`. Role 10 built and installed the isolated wheel; **all four original notebooks now pass from that wheel**, as recorded below.

## Changes and intended behavior

| Public surface | Retained / restricted / deferred |
|---|---|
| `get_ab_paths` and scorecard point estimates | Retained roster-aligned contributions, silent-user zeros where defined, ratio-of-totals semantics and group1-minus-group2 direction. Documentation distinguishes undefined value measurements from known zeros. |
| `get_ab_test` / `ab_test_data` binary auto | Retained two-sided Fisher exact p-values, CP arm intervals and conservative alpha-split CP difference intervals. Relative intervals withheld; intervals are not Fisher-test inversions. |
| Nonbinary auto and advanced methods | Auto is descriptive. Explicit nonbinary Welch/bootstrap remain approximate advanced examples. Known-binary Welch and scorecard bootstrap reject. Explicit z requires pooled-null expected counts of at least five in each arm and outcome cell. |
| `experiment_data` / `experiment` | Strict binary inference uses explicit `method="exact"`; `binary_intervals=None` requests none. Explicit Newcombe remains approximate. Bootstrap/tests are opt-in and do not acquire exact binary guarantees. |
| Ordinary funnel/conversion | CP/conservative CP/Fisher labels replace stale Wilson/Newcombe default claims. Conditional step comparisons stay descriptive, without p-values or causal verdicts. |
| Segments and secondary diagnostics | Manual pre-treatment declaration is required. A cut, recipe, transferred attribute or legacy inferred flag cannot certify exposure timing. Fisher/descriptive rows suppress unrequested asymptotic diagnostics. Explicit approximate diagnostics are exploratory and separate from the primary corrected family. |
| `get_sample_ratio` | Exact two-arm Bernoulli allocation test; guarded asymptotic multi-arm Pearson test. Method/status fields and tri-state mismatch are explained. A missing p-value is not a pass, and a nonsignificant result does not establish validity. |
| Bootstrap and periods | Joint unit resampling retained as approximate. Common measurement eligibility required for contrasts; caps and overflow failures documented. Insufficient Bonferroni tail draws withhold adjusted intervals, with no normal-SE replacement. |
| Multiplicity and planning | BH assumptions, separate-call limitations and post-selection limits explicit. MDE is labeled `normal_approximation`, not actual Fisher power. Sequential inference, equivalence, arbitrary clustering and post-selection guarantees remain deferred. |

The A/B guide now describes the supported defaults before its examples, keeps existing period/decision links, and restores a concise funnel section for its public anchor. “Six more” datasets was corrected to **five more, six total**. Northwind, the five additional cases and Lumeo are explicitly teaching cases rather than calibration evidence.

The examples show the default descriptive nonbinary rows before explicitly requesting approximate sensitivity analyses. Educational heterogeneity sections request guarded z diagnostics and label them exploratory. Strict tutorials request exact binary intervals. Heavy-tail examples retain their original assertions with explicit methods, rather than silently converting default descriptive output into an inferential claim.

## Verification and raw evidence

| Check | Result / evidence |
|---|---|
| Dataset stories plus Eventstream A/B contract/docfield tests | **128 passed, 5 skipped**; `narrative_tests_first.log`. |
| A/B narrative tutorial | **Passed**, 33 executed code cells, 12.73 s; `final2_ab_testing_tutorial/`. |
| A/B API showcase | **Passed**, 56 executed code cells, 20.37 s; `final_ab_testing_showcase/`. |
| Strict experiment tutorial | **Passed**, 15 executed code cells, 15.13 s; `final_experiment_tutorial/`. |
| Frozen Lumeo tutorial | **Passed**, 19 executed code cells, 16.93 s; `final_lumeo_experiment_tutorial/`. |
| Reference pages and LLM documentation | `render_pages_final2.log`: renderer completed, including `llms.txt` and `llms-full.txt`. |
| Owned-file pinned hooks | All checks passed except the first formatting invocation, which reformatted the two A/B notebooks; `precommit_owned.log`. Root owns the subsequent global run. |
| Formatter and clean-notebook verification | `verification_summary.json` records Python-AST equivalence for changed cells, no saved source outputs, exact notebook hashes and matching API-map mirrors. |

Each notebook evidence directory preserves executed IPYNB, rendered HTML, execution metadata and raw DataFrame/scalar snapshots in `artifacts/namespace.json`. Diagnostic runs and the eight-item showcase assertion audit are preserved separately and **are not acceptance passes**. The first two attempted executions failed before execution because local kernel ports were unavailable in the sandbox; subsequent approved kernel launches executed normally. The first final narrative run exposed a floating-point exact-equality assertion; the rerun uses a 1e-15 tolerance for `0.05 / family_size`, preserving the numerical claim.

Source acceptance above predates the final MCP unrounded-value change. Root identified and corrected the showcase's obsolete six-decimal rounding expectation to direct parity; its actual notebook test rerun reported **2 passed** before freezing commit `aac5866c232a48bee3ca071c31c52fa26a818c51`. Final wheel execution used `execute_notebook.py --installed-wheel`, the frozen notebook/support-file tree outside the checkout, and the isolated interpreter supplied by role 10. It added no source path, used no fixture monkeypatch, and asserted the imported package was under the interpreter prefix.

## Final installed-wheel gate

Wheel SHA256: `d334f02b8d6d670b17a3e36c178c284d101e6c4790c3b600272f8e325c207879`. All runs imported `retentioneering` from `.work/ab-safe-bet/role10-final-wheel-env/lib/python3.13/site-packages/retentioneering/__init__.py`, with `PYTHONPATH` unset. The 16 staged inputs were read directly from frozen commit `aac5866c232a48bee3ca071c31c52fa26a818c51` and hashed in `frozen_wheel_tutorial_inputs/INPUT_MANIFEST.json`. No package source was staged.

| Original notebook | Original code cells | Added verification cells | Runtime | Result |
|---|---:|---:|---:|---|
| A/B narrative tutorial | 30 | 3 | 16.96 s | Passed |
| A/B API showcase | 54 | 2 | 21.28 s | Passed |
| Strict experiment tutorial | 13 | 2 | 18.91 s | Passed |
| Frozen Lumeo tutorial | 17 | 2 | 20.72 s | Passed |

**114 original code cells** executed unchanged. The runner added setup and raw-table capture to each notebook, plus the narrative test's CHECKS cell to the A/B tutorial: nine added cells, 123 executed cells total. Complete original assertions remained enabled. `wheel_verification_summary.json` records hashes, exact import origins, checks hash, original/added cell counts and runtimes; each `wheel_*` directory preserves the executed notebook, HTML, metadata and raw tables. This supersedes the earlier source-only notebook acceptance. A report-aggregation helper initially assumed a Python 3.11 directory; the actual fresh environment is Python 3.13. The prefix/site-packages validation was corrected; no notebook execution failed.

A read-only diff from `aac5866` to `13a24b8` confirmed no changes under `src`, `js` or `notebooks`; the later commit contains review evidence and browser tests. Subsequent evidence-only packaging does not change the executed package or tutorial code.

## Scope and limitations

No statistical implementation, JS/MCP implementation, generator logic, version, changelog or workflow changes were made by role 09. The shared Eventstream file was changed only in owned docstrings; other agents' code edits were preserved. Historical simulation evidence in `examples/experiments` was labeled historical rather than repurposed to certify the new defaults. The initial frozen oracle SHA256 remains `ff98beffd87c78e162f2b39cbf5f9953a8756ee692aac09ef074a19bc0477100`.

No new statistical guarantee follows from these deterministic tutorial runs. Final release acceptance additionally depends on the root's full-suite result and the independent statistical evidence assembled by the other roles; the four-notebook installed-wheel gate described above has passed.
