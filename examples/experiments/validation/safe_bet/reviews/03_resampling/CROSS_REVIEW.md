# Independent cross-review of r01 defaults and r04 findings

This phase followed reading r01's fix/cross-review reports, r04's initial report and the coordinator's release decision draft. It is explicitly post-unblinding. The initial preregistration and oracle were not changed, and no peer test/oracle code was imported.

`cross_review_fixed.py` creates new public datasets, derives Fisher probabilities with integer combinations and exact difference bounds by bisection of direct binomial tails, and uses this role's frozen mean/ratio oracle. It asserts that every imported retentioneering module comes from the selected source and records source paths/hashes.

## r01 safe defaults independently confirmed

For control 6 successes/17 assigned units and treatment 7/13, default `get_ab_test` selects `fisher_exact`. Independent integer enumeration gives p **0.46008835181406793**. The candidate matches. Separately inverting direct binomial tails with alpha/4 per one-sided arm bound gives conservative difference CI **[-0.4268667287091964, 0.7113841013903893]**, also matching the candidate. These references call no SciPy Fisher/beta routine. This finite fixture independently checks dispatch and numerical integration; it does not replace r01's mathematical coverage argument.

New mean and ratio fixtures agree with the frozen oracle and return `test='descriptive'`, `verdict='descriptive'`, and no p, CI or MDE. A forbidden-allocation sentinel proves these default calls never reach `_run_bootstrap`. This corroborates the release boundary without requiring all explicit advanced nonbinary inference to be removed.

The first supplemental binary fixture had only seven pooled successes, below the existing default `min_support=10`, so its p-value was intentionally unavailable. The preserved `candidate_cross_review` output records the resulting 11/12 audit check. The final `candidate_cross_review_v2` uses adequate support and passes **12/12** checks; this was a fixture-design correction, not a source change to lower support guards.

## r04 zero-probability SRM finding independently reproduced

Use observed counts **[64,36,0]** and expected shares **[.5,.5,0]**. The zero-probability/zero-observed category contributes no stochastic dimension. The elementary statistic is `2*14²/50 = 7.84`, with one degree of freedom and tail `erfc(sqrt(7.84/2)) = 0.005110260660855862`.

Immutable baseline220 reports `(7.84, 0.01984109474437029, 2)`, independently confirming the wrong-df defect on counts different from the peer's example. The candidate reports `(7.84, 0.005110260660855867, 1)`, agreeing with the independent reference. This role did not edit the SRM primitive or public adapter.

The declared missing-family check `[.01, NaN, .04]` returns Holm `[.03, NaN, .08]`, confirming that unavailable hypotheses do not shrink the family. This passes on baseline and candidate. It is an arithmetic check of the correction; it does not validate arbitrary incoming p-values or dependent/adaptive families.

Evidence: `cross_review_before*/cross_review.json`, `candidate_cross_review_v2/cross_review.json` and corresponding logs. Original and corrected fixture outputs are separately preserved. Root's subsequently identified exact-default secondary-strata diagnostic leak is outside these checks and remains a separate fix; do not extrapolate this passing cross-review to `_strata_columns`.
