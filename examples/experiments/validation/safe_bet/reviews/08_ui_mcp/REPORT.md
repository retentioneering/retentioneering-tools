# Role 08 initial report — frontend and MCP fidelity

Date: 2026-10-30. Frozen baseline: PR220, declared commit `38cbe91d5b315584575b132a596890e69eb210aa`. Source: `.work/ab-safe-bet/baseline220`. No candidate edits, peer reports, CHECKPOINT, or subagent output were read. This is the bounded initial report; candidate cross-review remains separate.

## Decision

Retain the tested headless/widget/static/MCP presentation paths with their documented scope. One reproduced P3 display defect needs a small text correction: the ratio bootstrap warning tooltip claims more than 1% invalid draws when the actual run had 4/1,000 (0.4%). This does not change estimates or wrongly restore an unavailable interval. State restore requires explicitly re-passing the assignment roster. The strict experiment view has no state restore or new MCP binding; these are documented limits, not tested capabilities.

## Preregistration and independence

`PRE-REGISTRATION.md`, `oracle.py`, and `oracle_expected.json` were written and SHA256 recorded before source/API/test inspection. `PREREGISTERED_SHA256.txt` preserves the hashes. The oracle uses direct counts and rational arithmetic; it contains no imported package formulas. Five handwritten cases specify positive and negative effects, zero control, empty treatment, and event-time/assignment rules. Fixtures are our own CSVs, not copied library sample datasets. Source tests were examined only after registration; they count as supplementary implementation tests, not independent oracles.

Two preregistered expectations needed explicit contract corrections after inspection: the public observation endpoint is **exclusive**, whereas oracle D initially specified inclusive; and assignments at/after the cutoff are rejected rather than automatically excluded. Original oracle D is unchanged. The admissible adapter removes late assignment t3 and excludes the t2 success exactly at the cutoff: control 1/3, treatment 1/2, difference 1/6, relative lift 1/2. The original fixture's rejection and adapted result are both retained in `baseline/D_cutoff_binding.json`. Empty treatment is also rejected clearly on every surface rather than returning a null-valued row. These are contract distinctions, not implementation defects.

## Evidence summary

| Layer | Executed evidence | Result and boundary |
|---|---|---|
| Independent Python comparisons | 83 checks in `baseline/checks.json` | 83 passed: A/B/E aggregates, trait payloads, repeat compute, MCP summaries, report tab payloads, strict experiment snapshots, explicit-roster restore, adapted cutoff |
| Additional status/API checks | 5 checks in `baseline/supplement.json` | 5 passed: invalid parameter clears old result and valid edit recovers; unknown strata descriptive; degenerate paired contrast unavailable; actual guide code block produces oracle +0.1 conversion effect |
| Existing JS tests | `js_tests.log` | 23 passed, esbuild + Node with actual React server renderer; no browser and no live comm. Supplementary, not independent tests |
| Actual static bundle in Chromium | `baseline/browser_evidence.json`, `browser_fullchrome.log` | 13 pages, 35 checks: 34 passed, 1 genuine tooltip mismatch. Chromium 156.0.8078.4, full installed browser in headless mode, no DOM stubs |
| MCP server serialization | `baseline/mcp_transport.json`, `transport_integrity.json` | Real in-process FastMCP `call_tool`; returned text JSON equals business payload, strict parsing succeeds. `structuredContent` is null. No HTTP/SSE/client round trip claimed |
| Exports | 13 generated local HTML files plus 4 screenshots | Payloads contain aggregates rather than assigned user IDs; labels, status reasons, funnel caveats and report metric focus are rendered |

Python computation used `.work/ab-integrated-wheel-env/bin/python` with explicit `PYTHONPATH=.../baseline220/src`. Browser execution used `.work/ab-restoration-env/bin/python` solely for Playwright. Full imported-package path, Python/dependency versions, and SHA256 of every Python source plus relevant TSX/static input are in `baseline/manifest.json`.

Actual shipped assets consumed by HTML:

| Asset | SHA256 |
|---|---|
| `src/retentioneering/static/widget-static.js` | `9a7692b6e40b41b189706437939cfb4842ce3b34bd3e1ba69480ac176e5858a7` |
| `src/retentioneering/static/widget.js` | `42fb4c9582d0a1f9b770a72102c5d4d73584fdd50934ca8b1808d87837566ae6` |
| `src/retentioneering/static/widget.css` | `486059a2fe1ee7c68084a4e8912ceca76a4afdd5ab3b74435623d96c30ba47f9` |

The task supplied that these bundles came from the exact earlier baseline build. I hashed and ran the actual files; I did not independently rebuild their historical provenance. TSX unit tests compile separately from the frozen source.

## Finding R08-01 — P3: tooltip overstates invalid bootstrap share

**Type:** implementation display defect. **Scope:** comparative `ab_test` widget, static HTML, and MCP-added report tabs using its renderer. **Source:** `js/widget/src/ab_test_plot.tsx:128` (`SUPPORT_NOTES.invalid_replicates`); rendered at line 262. The backend reading correctly says “at least one.”

**Exact public reproduction:** `run_review.py` creates 4 subjects per arm. Every subject has `open`; the first 3 have one `buy` with amounts 1, 2, 3; the fourth has no buy. Call `stream.ab_test(("arm", "treatment", "control"), [{"name":"order_value","metric":"column_sum","metric_args":{"column":"amount"},"denominator":{"metric":"event_count","metric_args":{"event":"buy"}}}], n_bootstrap=1000, random_state=42)`, export HTML, and inspect the `invalid replicates` badge title.

**Expected:** explanation consistent with any undefined replicate, optionally exact 4/1,000 count. **Actual:** payload has `valid_replicates=996`, `support_warning="invalid_replicates"`, null interval and p-value, while the browser title is `More than 1 % of bootstrap replicates had no value in an arm: no interval or p-value, the estimate is descriptive.` This is false for 0.4%. The visible reading correctly says `at least one bootstrap replicate`.

**Evidence:** `baseline/ratio_invalid.json`, `baseline/ratio_invalid_ab.html`, `baseline/ratio_invalid_ab.png`, and the failing `bootstrap_warning_threshold_truthful` assertion in `baseline/browser_evidence.json`.

**Remedy:** replace the obsolete threshold with “At least one bootstrap replicate had no value in an arm”; optionally expose requested/valid counts if both travel in payload. **Regression:** retain this fixed-seed, 4-per-arm fixture and assert tooltip text is truthful below 1%; check the same title in the actual built static bundle. No backend estimator change is required.

## Contract risks and unavailable scope

**Roster restore is explicit, not automatic.** A saved widget originally has treatment 3/5=60%, control 2/4=50%, delta +10 pp, relative +20%. Reopening with the same assignment table reproduces the entire result. Reopening without the table uses event-bearing subjects: 3/4=75%, 2/3=66.7%, +8.33 pp, +12.5%, without a runtime warning. Raw `population` changes from `assignment` to `stream`; no roster IDs are saved. `docs/templates/widgets/ab-test.md.jinja:31-34` explicitly requires re-passing the table. This is documented user misuse/possible hardening, not counted as a defect. A future binding marker could fail clearly rather than silently accept an omitted roster. Raw reproduction is `baseline/state_restore.json` and screenshot `restored_without_assignment.png`.

**Small samples keep descriptive statuses.** The three nonempty arithmetic cases are intentionally below default binary support thresholds; scorecard p-values remain null and verdicts `insufficient_data`, without manufacturing inference. Strict Newcombe experiment intervals and their method labels remain visible. Zero-control relative lift is null/unavailable; it is not Infinity. A degenerate percentile experiment displays unavailable reasons. A constant paired-change MCP bootstrap response retains `inference_status="degenerate_distribution"`, null interval/p-values, and explicit numeric/display units. Unknown-pre-exposure strata preserve exploratory warnings and descriptive rows.

**Report/code checks.** Static cumulative and conditional funnel tabs state their actual denominators and conditional-selection caveat. Report analysis links activate and focus the first metric table cell. The exact first code block from `docs/guide/experiments.md` was executed against this role's compatible data, including Fisher/Welch declarations and bootstrap/Newcombe options; the conversion estimate remains +0.1. This is execution validity, not a statistical coverage certificate for those estimators.

**Finite/transport handling.** All persisted actual result payloads pass strict `json.dumps(..., allow_nan=False)`; FastMCP text JSON is parsed with nonfinite tokens rejected. There is no typed `structuredContent` in that response, so consumers must decode the text. Strict experiment widgets detach a serialized snapshot, and repeat calls agree. Not tested: malicious custom Python objects, every possible numeric dtype, remote MCP clients, a real Jupyter comm lifecycle, browser editing that recomputes through a live kernel, full wheel/package installation, or all screen sizes.

## Function matrix

| Public function/surface | Initial disposition | Evidence / constraint |
|---|---|---|
| `get_ab_test` / `ab_test_data` | Retain within documented contract | Counts, point values, absolute and relative directions match oracle; explicit cutoff respected; empty arm rejected |
| `ab_test` trait/result/compute RPC | Retain | Complete headless rows preserved; repeat computation equal; invalid edit clears result and recovers |
| Comparative static HTML / report tab | Retain after text correction | Numeric display and focus correct; fix R08-01 warning title |
| `ab_test(..., state_file=...)` | Restrict to explicit assignment rebind | Passing roster reproduces; omitting it changes target population as documented |
| `experiment_data` / `experiment` | Retain fixed-calendar snapshot scope | Identical strict aggregate payloads, correct percent/pp/relative distinction, Newcombe/Wilson labels and unavailable states |
| Experiment funnel tabs | Retain display scope | All-assignment and prior-step denominators described; conditional causal caveat rendered |
| MCP `get_ab_test` / `add_ab_test` | Retain | Rounded numbers preserve oracle, warnings and population/cutoff metadata in headless reply, report rows identical |
| MCP `get_ab_bootstrap` | Retain tested status serialization | Degenerate contrast stays unavailable with explicit status/scale; no remote transport validation |
| Experiment state restore / new MCP experiment binding | Deferred | Explicitly unsupported by this baseline; no inference of availability from comparative names |
| Live Jupyter frontend comm / remote MCP / installed wheel | Deferred to release integration | Python traits, actual static browser and in-process dispatch are not those end-to-end paths |

## Reproduction and review-harness deviations

Run commands in `COMMANDS.md`. Preserve the entire `baseline` directory for candidate comparison. The first browser launch failed because macOS denied a sandboxed Mach-port handshake. An escalated headless-shell launch then failed because its GPU helper could not locate ICU data. Using the installed full Chromium binary succeeded. These environment failures are not package defects. Initial report-link assertions wrongly searched an experiment-only `data-metric` attribute, then a row outline; the actual comparative target is the first `td` within `tr[data-event]`. Corrected assertions passed. Earlier evidence and the failed harness log remain available. No candidate source was changed to make tests pass.
