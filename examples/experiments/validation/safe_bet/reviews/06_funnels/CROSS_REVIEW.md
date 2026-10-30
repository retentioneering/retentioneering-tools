# Role 06 independent cross-review after initial report

2026-10-30. Cross-review was authorized only after completing the independent initial report. Read role 01 and role 07 fix reports to identify their proposed contracts; imported neither reviewer's oracle. No edits were made to their owned sources.

## Binary policy (role 01)

`cross_review.py` independently enumerates fixed-margin hypergeometric table weights with integer combinations for Fisher p-values. It computes binomial CDFs by direct finite sums and solves equal-tail confidence bounds by bisection, without a beta-distribution inverse or a peer oracle. It then forms the proposed difference interval from two arm intervals with tail alpha/4.

Across 78 marginal interval comparisons and 456 Fisher/difference comparisons, all results agree within absolute tolerance 5e-12. Sizes include unequal and tiny arms (1, 2, 3, 4, 5, 8, 10), all success counts at the tested sizes, and marginal confidence levels 0.5, 0.95 and 0.999. This validates arithmetic on the bounded grid. It is not a new claim of exhaustive coverage calibration.

The policy is coherent for the stated independent binomial model: the separate Clopper-Pearson arm coverages and union bound support a conservative difference interval. Fisher conditions on margins and tests a different construction; its p-value is not the inversion of that difference interval. This distinction remains necessary in the API/docs, even when numerical methods are exact. Correct exact calculations do not establish randomization or prevent post-treatment conditioning. Conversion and funnel adapters now retain conditional-p suppression.

## Identity guard (role 07)

Four independent public checks use this role's funnel/conversion fixture. Mixed integer/string IDs `[1, '1']` and bool/integer IDs `[True, 1]` are rejected with `SchemaConfigError`. Homogeneous object integers `2**63+7` and `2**63+8` remain distinct through a user funnel (reach `[2,1]`) and conversion (0.5). This supports the identity guard on the selected cases; it does not certify arbitrary custom ID objects or repair values rounded before input.

## Evidence

`cross_review_fixed.json`: **538 checks, zero failures**, with source hashes captured at execution. `cross_review.py` is source-selectable and imports only the candidate implementation, its standard runtime dependencies, and the independent calculations above. Initial preregistration and oracle files remain unchanged.
