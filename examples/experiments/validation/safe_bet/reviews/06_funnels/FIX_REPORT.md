# Role 06 bounded conversion/funnel fixes

2026-10-30. Candidate `.work/ab-safe-bet/repo`. Changes remain uncommitted. No push, publication, peer-source edit, or subagent was used. Initial evidence and reports are preserved.

## Implemented

**Finite conversion windows retain each start's identity.** When a path has multiple selected starts and a finite window, starts are batched by occurrence rank so each resolver call receives at most one start per path. The existing complete end AnchorSpec is resolved after that start. Successful path IDs are unioned only after applying the finite window, and already-converted paths are omitted from later batches.

This fixes both the reported false negative (an early end hiding a later successful attempt) and a related false positive (a pattern's lead-in belonging to an earlier start while its endpoint is paired with a later start's window). It preserves explicit end first/last/all selection, whole-pattern eligibility, integer/time offsets, virtual path boundaries and event order at timestamp ties. Numeric occurrence syntax was already unsupported; its explicit rejection is preserved. The fix does not replace explicit end selection with “any end.”

**Ordinary funnel/conversion inference uses the shared exact policy.** Arm/rate intervals call `exact_binomial_ci`; differences call `exact_difference_ci`; p-values call `fisher_exact_p`. Funnel `ci_basis` is now:

```json
{"rate":"clopper_pearson","delta":"clopper_pearson_bonferroni","p_value":"fisher_exact_two_sided"}
```

Equal constant nonempty arms now have Fisher p=1 rather than an undefined z p-value. Empty-arm/undefined-denominator output remains unavailable. Conditional comparisons still carry no p-value. Observed-path population and independence metadata remain intact. Exact difference intervals are conservative and are not Fisher-test inversions.

Conversion preserves its row-column schema and attaches method metadata through `DataFrame.attrs`: `ci_basis`, `ci_level`, `population_basis='observed_paths'`, and `independence_assumption='independent_observed_paths'` when inference is requested. Point-only conversion retains population metadata. Copy/reset-index preservation is tested. Serializers that convert only rows must explicitly export attrs; the coordinator notified the MCP/UI owner of this integration requirement.

Public Eventstream funnel/conversion docstrings now describe the exact methods, per-window end occurrence selection, conditioning and metadata. No unrelated Eventstream sections were edited by this reviewer. Old numeric tests were updated only where they intentionally asserted Wilson/Newcombe/z defaults; unknown numerical differences were not relabeled as passes.

## Owned files

- `src/retentioneering/tools/conversion.py`
- `src/retentioneering/tools/funnel.py`
- Funnel/conversion docstrings within `src/retentioneering/eventstream/eventstream.py` (a shared file with other reviewers' earlier changes)
- `tests/tools/conversion_ci_test.py`, `tests/tools/funnel_ci_test.py`
- New `tests/tools/conversion_safe_windows_test.py`, `tests/tools/conversion_funnel_exact_test.py`
- `tests/widgets/funnel_ci_test.py` (updated constant-arm expectation; still checks null serialization for a later zero conditional denominator)

## Verification

- Independent cross-review of role 01's exact primitives and role 07's identity guards: **538 checks passed**, described in `CROSS_REVIEW.md`.
- New regression tests on frozen baseline220: **11 failed, 8 passed** (`regressions_before.log`). Failures cover the all-start default end bug, whole-pattern cross-window false positive, offsets, batching and exact defaults/metadata.
- Expanded handwritten match-enumeration oracle: **256 cases, 57 failures before, zero after** (`windows_before.json`, `windows_after.json`). This includes 120 random logs with both event/time windows, seed `6062026`, pattern starts and ends, first/last/all end selection, offsets, anchor position selection, ties, plus explicit later-attempt, whole-pattern, offset and sentinel cases. It imports no implementation matcher or peer oracle for expected values. It is supplemental post-initial-review evidence, not a retroactively blinded oracle.
- Original five-event public repro now passes (`repro_candidate.json`). Coordinator's independent two-path reproduction now returns converted=1, paths_with_start=2, rate=0.5 for both one-event and one-second windows (`conversion_root_after.json`).
- Final focused ordinary conversion/funnel, integration and widget tests: **125 passed** (`focused_postformat.log`). A first run had one obsolete widget z-test expectation (`focused_after.log`); that expectation was updated to exact p=1 and null-serialization coverage was preserved.
- Repository-pinned pre-commit hooks passed on the owned source/test files (`precommit_first.log`), including Ruff, formatting, hygiene and secret scanning. A command launched the final test paths from the workspace root once and found no files (`focused_final.log`); the correctly scoped candidate runs are preserved separately and pass.

The original 100-log oracle adapter is also rerun as `review_candidate.py`. Its only expected-value changes are the truthful labels for **explicit approximate** strict `binary_intervals={'method':'newcombe'}` requests, renamed by role 01 to `wilson_approximate`/`newcombe_approximate`. Neither `review.py` nor the preregistered `oracle.py` was changed. See the final totals appended below.

## Bounded scaling evidence

`scaling_after.json` measures a worst-late-success construction: all users remain unresolved until their final start. Batching uses one resolver call per occurrence rank, not one per user-window.

| Users | Starts per user | Events | Resolver calls | Elapsed seconds |
|---:|---:|---:|---:|---:|
| 10 | 2 | 40 | 2 | 0.066 |
| 100 | 2 | 400 | 2 | 0.046 |
| 1,000 | 2 | 4,000 | 2 | 0.050 |
| 100 | 10 | 2,000 | 10 | 0.163 |
| 1,000 | 10 | 20,000 | 10 | 0.203 |
| 1,000 | 30 | 60,000 | 30 | 0.700 |

These are single-run local timings while other work was active, not a performance guarantee. Query count scales with the maximum number of unresolved starts in a path; extremely long looping paths can still be expensive. A structural regression verifies 30 users with two starts need two resolver calls, with one start per path in each batch.

## Boundaries and remaining integration

The changes make no new causal claim for observed-path or selected-population results. They do not establish complete logging, randomization, independent paths or pre-treatment cohort selection. Full anchor grammar and every synthetic-index transformation were not exhaustively tested. No generic matcher rewrite was attempted. Shared statistical primitives, A/B scorecard, sample-ratio/readings, JS/MCP, guides and release artifacts remain outside this role's edits. Full integration, wheel validation, CI and final release approval belong to the coordinator.

`FIX_COMMANDS.txt` records rerun commands; `FIX_SOURCE_MANIFEST.json` records actual source/test hashes. Shared-source hashes are snapshots of a concurrent working tree, not a claim that all edits in those files are owned by this reviewer.

## Final original-oracle rerun

`candidate_original_suite.json` and `candidate_original_suite.log` record **9,854 assertions, zero failures** for the two original fixed cases and 100 original random logs, using the unchanged preregistered oracle. The old finite-window failures are resolved. Only the explicit approximate strict-interval metadata expectations were amended in the new adapter copy, as stated above. Original baseline220/PR217 results and the original adapter remain available.
