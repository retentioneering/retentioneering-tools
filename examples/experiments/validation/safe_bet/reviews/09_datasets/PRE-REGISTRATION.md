# Role 09: applied dataset audit preregistration

Date: 2026-10-30. Candidate: PR220 commit `38cbe91d5b315584575b132a596890e69eb210aa`.

Before examining candidate source, API adapter, bundled dataset metadata or other reviews, I read only the common PROTOCOL.md and task instructions. I will freeze `oracle.py` with SHA256 before inspecting those materials. No peer report or checkpoint will be read. This is an oracle-first staged software review, not a blinded human experiment.

## Independent estimands and expectations

1. The assignment roster defines the analysis population, one unique user per arm. A user with no qualifying events contributes zero to sums, counts and binary conversions. Duplicate or conflicting assignments are rejected by the oracle.
2. Event windows are UTC half-open intervals `[start, end)`. Prehistory is excluded when a window begins after it. Cohort or segment filtering selects roster users before aggregation; unknown event users do not expand the population.
3. Per-user event counts count qualifying rows. A binary conversion is whether each assigned user has any qualifying event. A per-user mean averages per-user sums across all selected assigned users, including silent users. Repeated purchases contribute to sums but not additional conversions.
4. Ratios are ratios of total numerator to total denominator contributions. They are undefined when the denominator is zero. This differs from averaging per-user ratios; neither is universally preferable without an explicit estimand.
5. Treatment-minus-control effect signs follow the actual arm-level observations, independent of synthetic data stories. Relative effect is undefined when control equals zero.
6. Two-arm SRM is Pearson's chi-square test against supplied expected allocation; with one degree of freedom the survival probability is `erfc(sqrt(chi_square/2))`. SRM concerns assignment counts, not just event-observed users.

## Planned comparisons

Run every bundled `load_ab_test` dataset (six expected) and frozen Lumeo files, checking schemas and unique/duplicate assignments, missing timestamps, event membership, user/activity counts and prehistory. Compare the public `get_ab_paths`, `get_ab_test`, and `experiment_data` entry points against independently computed aggregates whenever their public contracts match. Exercise segment and time-window flows. Assess narrative/tutorial claims against observed values; a planted synthetic story is not evidence of general coverage.

Held-out variations will include silent treatment users, unknown event users, repeated purchases, zero denominators, exact window boundaries, prehistory, assignment-only segments, and deterministic small hand-computable examples. Seed for any generated variations: 9072026. Preserve raw aggregate tables, schemas, API payloads, environment/source hashes and executable runner. Record unsupported comparisons as deferred rather than passing them.

## Decision rules

Retain a capability only within the tested explicit contract. Restrict when correct use depends on a consequential denominator, cohort, time-window, assignment or inference assumption. Defer unsupported or insufficiently tested metrics/inference. Material numerical disagreement with the matching oracle is an implementation defect; differences in deliberately distinct estimands are documented contract limitations. Inferential outputs require appropriate randomization unit, independent units and fixed observation horizons; dataset aggregate agreement alone cannot validate inference.

## Frozen oracle

The oracle uses pandas and Python, with no imports from the candidate package. Post-freeze corrections, if needed, will be separate and explicitly documented; the frozen file will remain unchanged.
