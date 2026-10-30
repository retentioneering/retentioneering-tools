# Graph and sequence preregistration

Date: 2026-10-30. Reviewer role 5. Target frozen PR221 commit 9d130f375b8a778a55e11de98b57d46ad082c55f.

## Exposure declaration

Before writing this document and `oracle.py`, I read only the assignment and `PROTOCOL.md`. I have not examined target source, tests, API signatures, peer reviews, or CHECKPOINT. This is staged independent oracle-first work, not a blinded human study.

## Scope and independent definitions

Inputs are an explicit randomized-unit roster and ordered event rows. An empty/silent roster unit belongs in the population denominator. Within each unit, consecutive ordered events form directed transitions. Repeated loops count repeatedly for occurrences; binary reach counts at most once per edge per unit. Unique paths count distinct complete label sequences. Source-normalized transition weight divides edge occurrences by all outgoing transition occurrences from that source. Target-normalized divides by incoming occurrences to that target. Total-normalized divides by all transition occurrences. These are pooled descriptive ratios; the observational grain remains the randomized unit. Step cell occupancy is the number of units with an event label at a zero-based sequence position, divided by all eligible roster units. A missing arm-edge contributes zero rather than being deleted.

The oracle uses no pandas, numpy, scipy, target imports, or implementation-derived formulas. It returns unit-level sufficient statistics so tests can distinguish event pooling, equal unit weighting, conditioning, and clustering. A finite positive observation horizon includes events at `assignment_time <= timestamp < assignment_time + horizon`. An optional common cutoff requires a fully matured horizon when configured. Explicit roster, tie-breaking input index, and this half-open boundary are oracle choices, not claims about the undocumented API contract. Any differences from documented target semantics will be categorized as contract differences rather than automatically called defects.

## Planned public API tests

1. Hand-calculated repeated-edge and loop paths; unequal sequence lengths; unique complete paths.
2. Missing edge in one arm; units with one event and zero transitions; explicit roster units with zero events; empty inputs if allowed.
3. Source, target, total transition denominators and step positions, including optional termination states.
4. Unit versus event grain; unequal contributions; conditional estimands and weights.
5. Enrollment time, horizon boundary, cutoff maturity, timestamp ties, and filtered graph compatibility.
6. Discovery screening/support minimums, family-size accounting, BH dependence/selection, and sparse bootstrap degeneracy. Distinguish a numerical defect from a limited inferential guarantee or missing assumption.
7. Run actual public API reproductions with a configurable source directory, save datasets and raw results, and hash inspected sources.

## Decision rule

Retain descriptive output only when its documented estimand and denominator match independent calculations across tested fixtures. Restrict discovery/inference where randomization-unit clustering, stable denominator, independent hypothesis family, support, horizon, or estimand assumptions are required. Defer claims with demonstrated contradictions or unvalidated selection/resampling behavior. No finite fixture pass is a general safety certificate. Preserve preregistered files unchanged after their SHA256 is recorded; amendments go into separate files.
