# Role 08 — candidate frontend and MCP integration

2026-10-30. Candidate: `.work/ab-safe-bet/repo`. No commit, push, merge or publication by this role; the coordinator subsequently froze package/source commit `aac5866`. Initial evidence in this directory and the original workspace `roles/08_ui_mcp` remains preserved. This is the authorized, unblinded implementation/cross-review phase.

## Readiness

The owned widget and MCP changes are ready for the coordinator's final integration gate. They preserve the revised inference contracts through machine payloads and the freshly built static browser UI. Final focused validation: **127 Python tests passed**, **56 JS tests passed**, **83 original oracle/payload comparisons passed**, **10 additional handwritten-contract checks passed**, **4 precision/negative-control checks passed**, and **20 actual Chromium pages / 64 browser assertions passed**.

This is not a whole-repository or release certification. A live Jupyter comm, remote HTTP/SSE MCP client, installed wheel and full CI are not covered by this role. The repository browser suite also exercises an actual Python recompute callback through a mock browser model; this is distinct from a real Jupyter comm. A plain full-widget `tsc --noEmit` still reports missing viz-core declarations and existing implicit-any errors outside the edited A/B/experiment files; no edited A/B/experiment diagnostic remains. Production Vite builds succeeded. Fatal Ruff and `git diff --check` pass; unrestricted Ruff on these legacy modules still reports existing broad-catch/annotation/style findings, recorded rather than claimed green.

## Changes

- Widget method choices now match `auto`, `fisher_exact`, `descriptive`, guarded approximate `z_test`, and explicit nonbinary approximate Welch/bootstrap. Auto's binary-exact/nonbinary-descriptive policy is visible. Exact/approximate CI and p-value method labels are shown on scorecard, strata and periods. MDE is labelled a normal planning approximation; Fisher relative CIs are explicitly unavailable. Strict experiment displays label CP as exact, Newcombe/Wilson and bootstrap/Welch as approximate.
- Bonferroni bounds are separate from primary pointwise intervals. The UI and MCP retain `delta_ci_adjusted_basis/status`; `insufficient_tail_resamples` displays a reason without drawing invented endpoints. Optional secondary strata diagnostics retain `strata_diagnostics_basis` and are explicitly separate from primary exact methods/correction. Default secondary diagnostics display “Not requested.”
- Numeric `1` and string `"1"` retain distinct widget headings and JSON-encoded selection keys. Applying a choice decodes its original scalar type. Strict experiment headers and population diagnostics also disambiguate these labels. MCP `describe()` preserves scalar levels; assignment summaries expose typed `arm_levels` and distinguish colliding display labels.
- MCP SRM no longer coerces unavailable `mismatch=None` to `False`. Overall and split results retain p-value method, sampling assumption, inference status, support warning and minimum expected count. Conversion `DataFrame.attrs` method/population/independence metadata reaches MCP. AB get/add/strata/period responses retain method, support, MDE, relative-CI and adjusted-tail metadata. Add-report replies retain the explicit cutoff and analysis unit.
- Machine-readable AB/SRM/conversion/contrast numbers now preserve finite Python floats. The previous fixed-decimal normalization could turn positive p-values or CI endpoints into zero, or move a just-below-alpha p-value to alpha. Nonfinite values still become null. Display formatting remains in the frontend.
- MCP docstrings, server descriptions, playbook and metric instructions explain the revised defaults, SRM unavailable state, exact versus approximate methods and approximate MDE. The coordinator's corrected “at least one invalid replicate” tooltip was preserved. The separate “one arm only” tooltip no longer asserts a pooled test is valid.

Owned source: `js/widget/src/{ab_test.tsx,ab_test_plot.tsx,experiment.tsx}`, `widgets/ab_test.py`, and `mcp/{tools.py,server.py,_prompts.py,playbook.md}`. Added dedicated MCP metadata/precision and JS metadata regressions. Updated old MCP/widget assertions to the declared Fisher/descriptive policy, plus the single previously coordinated two-arm SRM integration assertion. No core statistical implementation or Eventstream source was edited by this role.

## Failing before and passing after

| Regression | Before | After / evidence |
|---|---|---|
| SRM null/status; two-arm method; conversion attrs; AB basis/relative fields; adjusted-tail metadata; typed headings | 6 dedicated Python failures | All pass; `cross_review/metadata_before.log`, `focused_final.log` |
| Strict report numeric/string headings | One new JS test failed with identical headings | Pass; `js_typed_strict_before.log`, `js_tests_final.log` |
| Positive tiny/threshold numbers and public tiny p/effect/CI fidelity | 7 failures, 3 nonfinite controls passed | 11 precision tests pass in final suite, including added real public near-alpha Welch case; `precision_before.log`, `focused_final.log` |
| Initial R08 tooltip | Real baseline: 996/1000 valid draws, title incorrectly says more than 1% invalid | Candidate browser title says at least one; `candidate/browser_evidence.json` |

The old MCP 200/200 binary fixture now has Fisher p `0.05604779…`, so its z-era assertion `raw p < .05 < adjusted p` was intentionally replaced with direct Fisher semantics (`adjusted >= raw > .05`) and method labels. Its reading now correctly says the conservative interval includes zero. Nonbinary defaults explicitly assert descriptive/null p. The old rounded-SRM assertion was replaced with **exact direct Python parity**, not a weaker tolerance.

## Browser and bundle provenance

`npm run build --prefix .../js/viz-core` and `.../js/widget` rebuilt the actual bundled implementation. Chromium **156.0.8078.4** ran the generated exports with no mocked DOM or host. Final evidence: `cross_review/candidate/browser_evidence.json` (11 pages/30 checks), `cross_review/contracts/browser_contracts.json` (9 pages/34 checks). Includes report links/funnel tabs, exact small-arm inference, guarded z, nonbinary descriptive output, Bonferroni tail withholding, both comparative and strict typed-arm displays, secondary diagnostics, and an unsupported-schema negative control. Screenshots are beside each run.

| Actual bundled asset | SHA256 |
|---|---|
| `widget.js` | `c46fddb448cca76dddd0a3660af0f31d281c12a7286c550c4e3aeb763624bc47` |
| `widget-static.js` | `c18d536a639ba235766bc0474ef31635bdc9d98fe205fd59bb40373a85a75dd1` |
| `widget.css` | `486059a2fe1ee7c68084a4e8912ceca76a4afdd5ab3b74435623d96c30ba47f9` |

The final precision patch only changes Python MCP normalization; no JS changed after these bundles. Browser screenshots validate actual static rendering, not live kernel round trips. In-process FastMCP `call_tool` returns JSON text plus `structured_content={"result": <the same JSON string>}` in its model dump. Both strings decode strictly to the direct adapter result; this is a string wrapper, not a typed AB payload. The initial report incorrectly looked up camelCase `structuredContent` and described null. `TRANSPORT_REPORTING_ERRATUM.json` corrects that reporting mistake while preserving the original raw evidence. No remote transport claim is made.

## Reproduce and remaining limits

See `cross_review/COMMANDS.md` and `FINAL_SOURCE_MANIFEST.json`. Use explicit source `PYTHONPATH`; the environment's installed package is not the edited source. Runners are source-selectable and save candidate evidence separately. `run_candidate.py` is a new adapter derived from the preserved initial runner; it explicitly requests bootstrap for the original ratio-warning fixture because nonbinary auto is now descriptive. The handwritten oracle itself is unchanged.

State restore remains restricted to re-passing the assignment roster, as documented and independently demonstrated. No automatic roster rebind was added. Strict experiment state restore and a new MCP experiment binding remain unsupported. Statistical calibration, dependence and multiple-testing validity beyond the reviewed contracts belong to the specialist evidence; rendered labels do not certify arbitrary data. A precision-evidence exporter initially tried to serialize a NumPy boolean in its own checks; the harness now converts these to Python bool, and its failed log is retained. Actual payload serialization was not implicated.
