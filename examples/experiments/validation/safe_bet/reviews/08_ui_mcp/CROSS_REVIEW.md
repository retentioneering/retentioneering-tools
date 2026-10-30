# Role 08 — independent followup checks of integrated contracts

This phase followed the coordinator's explicit authorization to read CHECKPOINT, role01 FIX_REPORT/CROSS_REVIEW, role04 METADATA_CONTRACT, role03 candidate changes and role06's adapter contract. It is unblinded integration/cross-review; none of these later checks is retroactively claimed preregistered. Initial evidence and oracle hashes remain unchanged.

## Concrete independent checks

| Specialist contract | Independent fixture/check | Result |
|---|---|---|
| Role01 Fisher binary default | 1/1 treatment vs 190/1000 control. Given fixed margins, the small-arm subject is among 191 successes out of 1001; exact conditional p is **191/1001**. | Python and MCP retain that p; actual browser says Fisher exact, +81 pp, inconclusive, conservative exact CI, approximate MDE and unavailable relative CI. No increase verdict. |
| Role01 descriptive nonbinary default | Length metric on the same subjects; no explicit inference request. | Point retained, p/CI absent, descriptive method shown; no CI whisker. |
| Role03 Bonferroni tail support | B=1000, alpha=.05, m=3; expected draws per adjusted tail = **25/3**, below 10. | Core endpoints null; MCP preserves `insufficient_tail_resamples` and percentile-approximate basis; all three browser rows give an unavailable explanation. |
| Role04 SRM unknown status | Three arms with one assignment each, expected counts one each; two-arm balanced control fixture. | Sparse multiarm mismatch remains null overall and within stratum; exact two-arm p=1/method/independent-Bernoulli assumption survives MCP. Unknown is not described as balanced. |
| Role06 conversion metadata | Own observed-path conversion fixture through public direct and MCP APIs. | CP rate, conservative CP difference, Fisher p basis, confidence, population and independence metadata agree. This checks propagation, not all conversion-window logic. |
| Role07 typed group identity | Numeric 1 has 2/2 binary success; string "1" has 0/2. | Delta +1 is preserved; comparative and strict report labels and selector JSON tokens are distinct. |
| Coordinator secondary diagnostics | Tiny-arm strata with default exact primary inference. | All secondary p-values are unavailable and `strata_diagnostics_basis=not_requested`; browser labels the secondary section separately. |

Raw inputs, expectations and full payloads: `cross_review/contracts/contracts.json`, CSVs in original/candidate evidence, and `cross_review/export_contract_cases.py`. Ten additional checks pass. The unsupported `schema_version=99` control renders an alert and no result table in actual Chromium.

## Precision followup

The coordinator flagged numeric rounding after the first integration pass. This was reproduced and fixed in the bounded MCP normalization layer:

- All-success 40-person arm versus all-failure 40-person arm: independent exact p **2 / C(80,40)** is positive and below 1e-9. Old eight-decimal output was zero; current public MCP equals direct Python.
- Nonbinary values on a scale of 1e-10 with a treatment shift of 1e-9: the direct positive CI lower bound lies below 1e-6. Old six-decimal output was zero; current get/add/contrast machine values equal direct Python endpoints.
- A real 40/40 equal-variance Welch fixture is shifted using the t quantile to place the p-value near .0499999998. This is an adapter parity fixture, not an independent Welch calibration study. Direct Python and MCP both remain below .05 with `increase`; deliberately reapplying old rounding gives .05.
- SRM all-one-arm expected-half allocation preserves its positive exact probability **2 / 2^80**. Unconditional path-start conversion preserves its tiny positive Fisher p and all interval endpoints. NaN and infinities still map to null.

`cross_review/precision/precision.json` includes raw direct/MCP rows and deliberately broken rounded controls. All four evidence checks pass. `tests/mcp/ab_safe_precision_test.py` adds eleven targeted regressions; the first ten ran before the fix, with seven failures and three passing nonfinite controls. The real near-alpha public case was added afterward and passes exact direct parity.

## Updated function matrix

| Surface | Disposition | Boundary |
|---|---|---|
| Comparative widget/static report | Retain | Exact/approximate methods, unavailable results, adjusted bounds and secondary diagnostics are distinct; actual browser checked |
| Typed arm display/selection | Retain tested scalar cases | Numeric/string 1 distinction preserved in headings, selector tokens, state and MCP descriptions |
| MCP AB get/add/contrast | Retain | No numeric boundary rounding; support/method/cutoff metadata preserved; text JSON strict and deterministic |
| MCP SRM | Retain | Tri-state mismatch and explicit exact/asymptotic availability; fixed allocation still requires an appropriate design audit |
| MCP conversion | Retain metadata path | Method/population attrs and finite values survive; conversion-window algorithm reviewed separately by role06 |
| Strict experiment display | Retain fixed snapshot | Exact/approximate methods and typed arm labels; no new state restore or MCP binding |
| Saved comparative state | Restricted | Caller must explicitly re-pass assignment roster; omission remains a documented population change |
| Live Jupyter, remote MCP, packaged wheel, final CI | Deferred to coordinator/release gate | Static Chromium, the repository mock-model/Python callback bridge, and in-process dispatch do not establish these paths |

No new statistical method was implemented in this role. Confidence labels, metadata fidelity and finite-case browser correctness are not universal calibration or causal-validity certificates.

## Transport reporting erratum

The frozen initial report incorrectly described structured content as null because it read camelCase `structuredContent` from a snake_case `model_dump`. The raw initial and candidate evidence actually contain `structured_content={"result": <JSON string>}`. Strict decoding of both that result string and text content equals the direct payload, and `is_error` is false. `TRANSPORT_REPORTING_ERRATUM.json` records the correction; initial artifacts are preserved. There is still no remote HTTP/SSE test claim.

## Complete repository browser gate

All five existing browser test files ran against real Chromium: **11 passed in 15.40 seconds**. This covers A/B controls and Python callback bridge, experiment, funnel confidence intervals, report tabs/links, and transition graph rendering. The browser model in the callback test is a mock; the Python callback and browser execution are real. The original 10-pass/one stale-wording-failure log and the corrected final log are both retained. The role-specific 20-page/64-assertion browser evidence is additional and was not rerun because no JS changed.
