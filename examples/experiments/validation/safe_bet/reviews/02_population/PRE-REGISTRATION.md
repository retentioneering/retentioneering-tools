# Preregistration — population, windows, independence

Recorded 2026-10-30 before inspecting implementation/source or other reviewer reports. The protocol and assigned review scope are the only material inspected so far. This is staged oracle-first review, not a formally blinded study.

## Expected behavior from experimental design

1. An explicit assignment table defines the randomized population. Assigned units with no observed events remain in denominators with zero contribution. Missing assignment rows, conflicting arms for the same randomization unit, and event/assignment arm contradictions must be rejected or explicitly excluded with auditable counts; never silently multiply observations through joins.
2. A unit has one immutable treatment assignment. Its first exposure/assignment time is the origin for relative time. Repeated events, repeated session identifiers across users, and a varying number of sessions cannot create independent treatment units. The coarsest identifier that received randomized assignment is the independence unit; a session-level analysis cannot substitute for user-level randomization without cluster-aware inference.
3. Analysis events are on/after assignment. Prehistory cannot become a post-treatment outcome, but may define baseline strata. An as-of/cutoff timestamp excludes events exactly at cutoff. A fixed horizon H includes events at assignment and before assignment+H, excluding the endpoint. Only units with assignment+H <= cutoff have fully observed H outcomes. Assigned but immature units must not enter an H estimate as failures.
4. Calendar bins partition absolute time; since-assignment bins partition elapsed time from each unit's assignment. Boundary events belong to the next bin. Any total event contribution must be conserved under partitioning; a unit is counted once in each bin where an outcome is estimated. Varying time at risk must be explicit. Finite nonnegative bin widths and horizons are required.
5. Pretreatment strata may condition a confirmatory comparison only when their values are known/frozen before assignment and they retain the randomized population. Post-exposure strata may induce selection/collider bias; the API must reject their confirmatory use or label their estimates exploratory/conditional without a causal treatment claim. A stratum that only exists on events cannot silently exclude inactive assignees.
6. In event-only designs the observed population is an activity-selected population unless assignment/exposure events enumerate all randomized units. Safe release must make this limitation prominent. Estimator accuracy on observed records cannot establish population validity.

## Planned tests and independent oracle

Build a plain-Python oracle from these definitions, with no package imports: explicit randomized assignments; unique unit identity; half-open event windows; maturity checks; binary event conversion and total event counts; absolute and relative bins; nested-session identity. Run hand fixtures (silent units, arm conflicts, prehistory, exact boundaries, staggered assignment, nested sessions, duplicate assignments, invalid joins) and seeded randomized fixtures where public API permits those semantics. Test exposed strata controls and metadata through the public API. After the oracle is SHA256-frozen, inspect adapter signatures and source, recording any required mapping/amendment without editing the original oracle.

## Decision criteria

An actual/expected population or event-count mismatch is an implementation defect if the API promises the preregistered behavior; otherwise it is a contract/estimator limitation. Post-exposure conditioning, event-only assignment, and unsupported independent units may justify a narrow release restriction even if code is internally consistent. No finite set of tests proves causal validity. Reproductions must record exact arguments, raw results/errors, source locations, and package/source versions.
