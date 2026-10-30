# Independent preregistration: role 03, resampling/econometrics

Date: 2026-10-30. Scope: immutable PR220 baseline `38cbe91d5b315584575b132a596890e69eb210aa`, PR217 `7e61d99`, and any declared candidate restrictions. I have read only the shared PROTOCOL.md, task directions and public SciPy bootstrap documentation. I have not inspected implementation, tests, prior reviews or PR218 historical results. This is an oracle-first audit, not a blinded human experiment.

## Estimands fixed before source inspection

1. An arm mean is the arithmetic mean of eligible randomization-unit contributions. Its contrast is treatment minus control.
2. A ratio metric is the ratio of arm sums, equivalently the ratio of arm means. It differs from the mean of unit ratios. A zero unit denominator can be valid if the aggregate denominator is positive; an aggregate zero denominator is undefined. Numerator and denominator must use joint eligibility. Negative denominators require an explicit contract and are outside a safe release unless separately justified.
3. Paired period contrasts preserve each unit's entire vector: both periods, numerator/denominator, and all contributions move together. Arm resampling is independent when randomization arms are independent. Event-row resampling is invalid for clustered user contributions.
4. The literal bootstrap samples n unit indices with replacement and recomputes the statistic. A multinomial count representation is algebraically identical. Independent Poisson(1) counts are a different finite-sample procedure with random total weight; any automatic switch must be labeled and tested. Zero-weight/zero-denominator replicates are invalid and must be counted, not silently interpreted as zero.
5. Percentile CI quantiles and estimate ± normal-quantile × bootstrap SD are different constructions. A normal p-value is not necessarily the inversion of a percentile CI. Neither establishes distribution-free finite-sample coverage for arbitrary heavy tails.
6. Changing numeric units by a positive scale should scale means/contrasts/CI endpoints and preserve standardized evidence. Scaling numerator and denominator together should preserve ratios. Use stable summation and avoid overflow in intermediate sums where the result is representable.

## Planned fixtures and comparisons

- Small exact means, deliberately unequal denominators distinguishing ratio-of-sums from mean-of-ratios, zero numerator, constant outcomes, zero aggregate denominators, singleton/empty arms.
- Explicit joint missingness: numerator-only and denominator-only records, while eligible complete records retain zero contributions.
- Unbalanced arms and heavy tails; correlated repeat events per unit; paired pre/post effects with large shared latent variation.
- Numeric scales from 1e-200 through 1e200, large offsets and cancellation.
- Same seeded literal-index draws versus own count weights; compare public API behavior against independent oracle statistically when RNG algorithms differ, not by expecting arbitrary seeds to share draw sequences.
- Locate/bootstrap availability and any n/B-dependent multinomial-to-Poisson boundary; test above and below with a stated finite-sample distinction.
- Fresh simulations: at least 300 trials per feasible condition, two independent normal arms and an unbalanced heavy-tail condition, a paired contrast condition where supported; bootstrap count 399 or more. Report all counts, seed, failure rates, coverage/rejection and binomial Monte Carlo uncertainty (Wilson 95% intervals); simulations are evidence for tested cases only. If public API runtime makes a planned dimension infeasible, document the deviation.

## Decision rules

Deterministic estimand mismatch, broken joint eligibility, broken pairing, impossible numerical behavior, or hidden inference availability is a defect. Heavy-tail undercoverage despite correct implementation is an estimator limitation. Treat impossible/unsupported contracts as user misuse only if validation/documentation clearly enforce the restriction. Recommend safe-bet inferential retention only for estimands/designs and distribution classes substantiated by fresh evidence; descriptive estimates may be retained with explicit limits. Finite fixture or simulation passes never certify all inputs.

## Public method reference

[SciPy bootstrap documentation](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.html) specifies same-size replacement resampling, shared indices for paired arrays, percentile quantiles and bootstrap sample SD. Its discussion of approximate coverage and normal approximation motivates separate method labeling, not a universal guarantee. All oracle formulas below are derived directly from the estimands, independently of repository code.

## Freeze

`oracle.py` and this preregistration will be SHA256 recorded in `preinspection_sha256.json` before adapter/source inspection. No oracle changes after freeze without a separately dated amendment.
