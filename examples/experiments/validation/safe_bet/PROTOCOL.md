# A/B safe-bet review — 2026-10-30

Objective: publish a separate reviewable PR containing only justified A/B capabilities, with reproducible independent evidence and explicit limitations. No automatic merge, tag, or package publication.

## Frozen inputs
- PR216: 52615dd93476ef8532487792f26d023e944dfe77
- PR218: ac08a721b98ce017c44d818ec7a6f81a74f65421
- PR220 (candidate baseline): 38cbe91d5b315584575b132a596890e69eb210aa
- PR221: 9d130f375b8a778a55e11de98b57d46ad082c55f
- PR217: freeze from GitHub metadata in provenance.json

## Independence protocol
Ten localized reviewers, fresh contexts, three concurrent slots. Before examining implementation or other reviews each writes a dated PRE-REGISTRATION.md and independent oracle.py using first principles/public statistical references. Record SHA256 before adapter/source inspection. Then run oracle and implementation comparisons, adversarial fixtures, and statistical simulations where relevant. This is staged oracle-first review, not a formal blinded human study: report any exposure/deviation honestly. No copying implementation formulas into the oracle after unblinding without recording amendments.

## Review roles
1. Mathematical statistician — binary intervals, tests, numeric boundaries.
2. Causal inference / experimentation scientist — population, randomization unit, horizons, strata.
3. Resampling / econometrics researcher — means, ratios, heavy tails, dependent contributions.
4. Multiple-testing / power researcher — family definition, adjusted p, MDE, verdicts.
5. Graph / sequence researcher — PR221 edge/step estimands, discovery screens.
6. Product analyst — funnels, conversion, patterns, conditioning.
7. Python API / systems engineer — validation, serialization, determinism, immutability.
8. Frontend / MCP engineer — displayed claims, payload fidelity, state and exports.
9. Applied data scientist — diverse datasets, teaching fixtures, external handwritten aggregates.
10. Adversarial release / reproducibility engineer — package isolation, verification gaps, release boundaries.

## Evidence and decisions
Each role saves runnable code, raw JSON/CSV, command log, MD and HTML report. Findings: severity, exact public reproduction, expected/actual, source location, scope, remedy, regression. Distinguish implementation defect, estimator limitation, contract/user misuse, and untested scope. A passing smoke is not a coverage certificate. Simulation reports show counts, sample sizes, Monte Carlo uncertainty, seed, versions and source hashes. No claiming all inputs safe from finite tests.

Cross-review: another reviewer reproduces material findings and checks fixes; final function matrix includes retained/restricted/deferred and reasons. Preserve original fixtures and prior evidence. Candidate fixes get failing-before tests, targeted regression, full required checks, built wheel verification and live CI. Save final checkpoint for context recovery.
