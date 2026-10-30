# Role 10 — adversarial candidate cross-review

Follow-up 2026-10-30. Read the initial reports from all ten roles in the canonical review folder, plus material fix reports for roles 01, 03, 04, 06 and 07 and the coordinator's secondary-strata reproduction/tests. Findings are evidence to challenge, not instructions to accept unchanged. No new blind-oracle claim is made. Original preregistration and oracle remain untouched.

**208 independent post-review candidate checks pass.** Source-selectable `cross_review_candidate.py` constructs fresh fixtures and saves every check, expected/actual details, raw mixed-method scorecards and hashes of all imported candidate modules in `candidate_cross_review.json`. These are correlated contract assertions, not 208 independent experiments, and no simulation coverage claim follows.

## Reproductions and acceptance

| Reviewed evidence | Fresh adversarial check and disposition |
|---|---|
| 01: exact/default binary safeguards | Handwritten integer-combinatorial Fisher ordering on 15 boundary/unequal tables, plus mixed-stratum cases; exact effect bounds finite and contain observed differences. Known-binary Fisher and nonbinary descriptive defaults retain their distinct policy. No broad coverage theorem is inferred from these cases. |
| 02 + root: causal provenance | A freshly created transferred label Series with legacy `attrs['pre_exposure']=True` remains exploratory both directly and after JSON recipe replay. No confirmatory p-value appears without a fresh declaration. |
| 03: overflow, pairing and resources | Finite inputs whose sums overflow explicitly reject; changed per-user measurement eligibility across two days rejects with the common-eligibility explanation; 1,000,001 raw replicates reject before allocation. Peer positive-controls and cap semantics reviewed; this does not exhaust all memory dimensions. |
| 04: SRM and secondary approximations | Four independently computed exact probability-ordered binomial cases use rational arithmetic, including absent planned arms and very unequal probabilities. Sparse balance is withheld. Holm/BH validity still requires the disclosed family/design assumptions. |
| Root: secondary-strata leak | Mixed calls contain default Fisher, default descriptive counts and an explicitly requested z-test simultaneously. Reverse rows and rename strata. All exact/descriptive rows have no balance, Q or stratum-versus-rest p-values and `strata_diagnostics_basis='not_requested'`. A supported z primary row cannot get a rest comparison when the complementary arm lacks support; sparse expected balance cells stay unavailable. |
| 05: graph/step proposal | Candidate has neither `get_ab_transitions` nor `get_ab_steps`. Deferral remains consistent with the initial report. Updated PR221/222 audit belongs to role05/root; this role does not certify those latest heads. |
| 06: conversion windows | 24 new nine-event logs, seed 101029; literal per-start end selection for first/last/all, event and time windows: **144 comparisons**, all pass. This tests preservation of user-selected end occurrence, not merely “any end”. |
| 07: identity and lineage | Mixed integer/string IDs reject; numeric mapping values survive sorted JSON round-trip; mutation of caller mapping and exported recipe does not alter stored/replayed labels. This supplements, rather than replaces, role07's large-integer and unsupported-scalar checks. |
| 08: payload/display | Material metadata/tooltip findings reviewed. Release runner produces actual widget HTML from an isolated baseline wheel; candidate frontend/browser validation remains role08's integration responsibility. No claim that a baseline bundle verifies current TSX. |
| 09: datasets/tutorials | The six fixture catalog and model-design limits are retained. Full candidate wheel notebooks are explicitly pending; coordinated the four-notebook execution helper and required external copy of notebook/support files with role09. |
| 10: package isolation | All-three-asset negative controls, source-byte/version checks, fresh environment and actual exports implemented and exercised on explicitly labelled baseline artifact. Final candidate build is gated on coordinator freeze. |

## Honest amendments

The first cross-review execution completed its numerical probes but failed while serializing a nonfinite value in the audit result itself. `candidate_cross_review_initial_harness.log` is preserved; JSON-safe normalization was added to the audit adapter, not the package.

The next run recorded **202 passes and six failed expectations** in `candidate_cross_review_v1.json`. Those six probes expected mathematical Fisher p=1 on the three all-zero and three all-one tables. The scorecard intentionally withholds its p-value when pooled outcomes have no variation and returns `insufficient_data`; the exact interval remains finite. That is a conservative availability policy already accepted in the original reports, not a false-positive or arithmetic defect. The amended check accepts either the exact p=1 or that explicit unavailable contract, requiring both raw/adjusted p to be absent and verdict `insufficient_data`. Original failures remain preserved; no source was edited to satisfy them. Final **208/208** uses this disclosed contract amendment.

## Remaining acceptance gaps

1. Candidate source is still being integrated. Imported-file hashes in this report identify a snapshot; they are not an immutable final-commit or final-wheel claim.
2. Root's earlier full-suite failures are being reconciled by owners. Full required source, UI/browser and notebook checks must pass on the frozen source; local focused counts must not be added together and described as full coverage.
3. Build the final wheel only after root's freeze and role08's final bundle. Run the reusable gate, original independent oracle, fresh cross-review script and four full public notebooks with that wheel interpreter outside any checkout.
4. Inspect live CI on the same final PR head. This role has performed no commit, push, merge, release tagging or publication.

No additional material statistical defect was found in these bounded fresh probes. This conclusion preserves each report's explicit restrictions on observed/conditional populations, independence, logging completeness, heavy tails, approximations, multiple testing, selection and repeated looks.
