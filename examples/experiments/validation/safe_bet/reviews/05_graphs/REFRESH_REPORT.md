# Role 5 bounded refresh: PR221 and PR222

**Decision: continue to exclude both PR221's inferential screens/overlay and PR222's dependent matrix/table surfaces from the conservative release until a separate fix review.** The updates improve presentation and no-p-value labeling. They do not fix either material inferential failure.

Scope is frozen at **2026-10-30 20:26 UTC**. Reviewed commits:

| Input | Frozen commit | Comparison |
| --- | --- | --- |
| Original PR221 audit | `9d130f375b8a778a55e11de98b57d46ad082c55f` | Original evidence remains unchanged |
| Updated PR221 | `2e9649395dda3ce647b4818c1ec013aba152ecdd` | Actual diff from original PR221 |
| PR222 | `0bddd49a9de3d5f5a695ead7282062bf606fdf8b` | Actual diff from updated PR221 |

This refresh uses the existing frozen independent oracle and reproductions. No new independence claim is made after inspecting the updates. No peer reports, candidate edits, commits, pushes, or additional agents were used. No commits beyond the scope freeze were fetched or reviewed.

## What changed

Updated PR221 changes `_compare` so rows with no finite p-value now receive `insufficient_data`; its readings explain the missing p-value. It improves test-option messages, explicitly rejects non-null step anchors/patterns, and warns that a pre-exposure event is needed before truncating steps. In the frontend diff, graph labels use the tested delta's units and a frame row may be drawn even when graph counts tie. These are useful bounded improvements.

It also changes declared-edge readings from the exploratory confirmation wording to **“declared family … confirmatory, if the edges were declared before the test.”** The pooled binary-support rule and constant-ratio bootstrap calculation remain unchanged. The revised `support=tested` verdict rule only helps when the p-value is missing; a numerically erroneous finite p-value of zero still passes.

PR222 adds six public Eventstream surfaces: `ab_transitions`, `ab_steps`, their `_data` aliases, and `get_ab_transitions_table` / `get_ab_steps_table`. The aliases call the same getters, tables pivot their results, and widgets serialize those results for matrix display. PR222 does not change `tools/ab_transitions.py`, `tools/ab_test.py`, `_bootstrap.py`, or the graph compatibility code relative to updated PR221. It inherits their statistical defects.

The complete changed-Python-function inventory, including new source lines and file hashes, is in `REFRESH_FUNCTION_INVENTORY.{md,json}`. It records 26 changed/added AST function entries for the PR221 refresh and 44 for PR222, including nested/helper entries rather than pretending these are all public functions. Source diffs preserve the selected frontend/documentation changes too. Every changed-file archive copy and every runtime manifest source file matched the corresponding frozen Git blob (`REFRESH_PROVENANCE.json`).

## Actual bounded reruns

The two frozen source trees were imported in separate Python processes. Runtime is the same Python 3.13.17 environment used initially. Full outputs are in `refresh/pr221/`, `refresh/pr222/`, `refresh221.log`, and `refresh222.log`.

| Original finding/check | Updated PR221 | PR222 | Assessment |
| --- | --- | --- | --- |
| F1: identical-distribution null, 2 vs 1,000 units, 50 one-event categories; first 10 saved seed values | 10/10 experiments have discoveries in each getter; 40 edge and 20 step discoveries | Same | Remains unsafe in the accepted sparse-arm domain |
| F2: 30 identical 1/3-ratio paths versus 30 identical 1/2-ratio paths | SE `2.77694638204588e-17`, p `0`, decisive decrease | Same | Numerical degeneracy remains |
| F3: old +1 edge result on a changed stream whose current result is −1 | Public graph constructor accepts, empty error string | Same | No source/population binding added |
| Sparse denominator with excessive invalid bootstrap draws | Missing p-value now receives `insufficient_data` | Same | Initial status ambiguity improved |
| Filtered result frame | Original family/row metadata preserved | Same | Intended filtering compatibility retained |

The ten null seeds are deterministic replays of seeds 51000–51009 from the original generator, not a new independent 200-seed calibration study. The original **200/200** result belongs only to original PR221; it has not been relabeled as a latest-commit result. The 10/10 result confirms continued failure for these representative inputs and is not a new broad error-rate estimate.

The constant-ratio fixture is the original exact construction: treatment path `A,B,A,C,A,C`, control path `A,B,A,C`, 30 units each, `get_ab_transitions(diff, "proba_out", edges=[("A", "B")])`. Exact within-arm resampling cannot vary either ratio. The tiny reported SE still arises from floating-point residual variation. Both new commits now append the **confirmatory** declared-family reading to this defective result. Predeclaration does not validate a numerically degenerate standard error, sparse marginal p-values, post-treatment denominators, or BH dependence assumptions.

## PR222 propagation checks

For the constant fixture, `ab_transitions_data(...)` exactly equaled `get_ab_transitions(...)`. The public table retained its p-value and verdict. The real Python `ab_transitions(...)` constructor emitted a matrix payload with `p_value_adjusted=0`, no construction error, and the confirmatory reading. PR222's Python `cell_status` classified that row as **significant**. Its TypeScript `cellStatus` source likewise classifies finite adjusted p below alpha as significant; it adds no statistical validity guard. Thus this wrapper carries the existing faulty inference directly into a significance-encoded presentation.

For the fixed small-arm table, `ab_steps_data(...)` reported the same two discoveries, `ab_steps(...)` produced a payload without error, and `get_ab_steps_table(...)` produced the corresponding 2×4 table. These are propagation checks, not proof of full matrix functionality. PR222's general widget docstring still calls the matrices exploratory; the specific problematic confirmatory sentence originates in updated PR221's declared-edge row reading and is passed through. This is not a claim that all PR222 views call themselves confirmatory.

As in the initial review, the frozen archives have no built JS resource. The public Python widget/graph constructor checks used an explicit in-memory `_esm` shim pointing at a tiny local JS stub. They exercised Python parameter handling, computation and payload construction. **No rendered browser, layout, static export, JS bundle, wheel or live CI certification was performed.** Full UI certification is unnecessary to justify deferral while the underlying inference demonstrably remains unsafe.

## Release boundary

Retain the original descriptive counting conclusions and the existing non-inferential graph/step tools. Defer the updated `get_ab_transitions` / `get_ab_steps` inference and graph significance overlay; defer PR222's aliases, inferential pivot-table wrappers and matrix widgets with them. The separate PR220 baseline safeguards remain an extraction boundary: neither this source comparison nor the refresh authorizes replacing the baseline engine with the stacked PR221/222 engine.

Before reconsideration, separately repair and cross-reproduce valid sparse binary inference, robust constant-statistic handling, overlay data/population identity, and calibrated dependence/selection claims. Then audit the new matrices against that repaired engine and run their own frontend/export/package checks. No new broad review of all repository functions was attempted here.

## Reproduction and files

`run_refresh.py` accepts a frozen `--source`, fresh `--output`, and `--simulation-runs`. Run each source in a separate process; exact commands are in `REFRESH_COMMANDS.md`. `refresh/*/manifest.json` records source hashes, and `REFRESH_PROVENANCE.json` verifies them against the stated commits. `REFRESH_EVIDENCE.sha256` covers the new evidence. The initial preregistration, oracle, report, raw 200-run results and their hash manifest remain intact.
