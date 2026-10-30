# Release/reproducibility acceptance contract

Registered 2026-10-30 before implementation, API, or peer-report inspection. Only PROTOCOL.md and the assigned paths/revision identifiers were read. This is an independent staged oracle-first review, not formal blinding.

## Independent assigned-unit fixture

Eight assigned users, four per arm; control c1 contributes one purchase worth 10, c2 a view, c3/c4 are silent. Treatment t1 contributes two purchases worth 20 and 10, t2 one worth 5, t3 a view, t4 is silent. All eight assignments belong in binary and mean denominators. Purchase binary totals are control 1/4 and treatment 2/4. User revenue totals are control [10,0,0,0] and treatment [30,5,0,0]. Purchase-count ratio denominators are control [1,0,0,0], treatment [2,1,0,0]. Ratios mean sum(revenue)/sum(purchases), not an average of purchaser-level ratios. Handwritten exact aggregate expectations and standard sample-variance calculations are in oracle.py.

Binary expected absolute effect: 1/4. Mean revenue effect: 25/4. Revenue per purchase effect: 5/3. Silent units affect assigned-unit means and binary rates. Reversing event and assignment rows must not change aggregates, inferential outputs given an explicit seed, or serialized semantic results. Zero purchase denominator must yield an explicit unsupported/undefined result or clear validation error; silently converting undefined ratio to zero is unacceptable.

The tiny fixture verifies population and arithmetic only. It cannot validate nominal coverage, asymptotic approximations, bootstrap validity, or power.

## Release gates

1. Exact frozen source revision and dirty-diff hashes must be distinguishable from the eventual candidate commit. An installed existing wheel is never evidence for current uncommitted fixes.
2. Every retained public method has documented estimand, population, assumptions, numerical boundaries, adjustment family, and public-API regression coverage, or is explicitly restricted/deferred.
3. A candidate wheel built from the final commit passes the source-selectable oracle and targeted regressions in a fresh environment outside the checkout, without editable installs, source PYTHONPATH, or hidden local assets.
4. Package metadata and version expose no misleading claim that an old wheel is the new candidate. Wheel contents, dependency constraints, imported file paths, installed metadata, source revision, wheel SHA256, Python and dependency versions are recorded.
5. Documentation and examples accurately describe assignment/silent-unit support, ratio definition, multiplicity/power limits, and randomization-unit independence. Frontend/MCP claims agree with retained backend behavior.
6. Determinism is tested under fixed seeds, row permutation, repeated runs, and clean process execution where the public API promises it. Tests do not depend on a polluted developer environment, cached imports, order, network, mutable global state, or an installed old package.
7. Required complete checks and live CI run on the final commit. A tiny smoke or finite simulation is not a method inventory or safety certificate.
8. A separate reviewable PR contains only justified retained scope, limitations, reproducible evidence, and concrete deferred items. No automatic merge/tag/package publication.

## Planned evidence

Write and hash oracle before API adapter/source inspection; compare baseline public APIs against its expectations; inventory packaging, method/public-documentation boundaries and checks; build and inspect a baseline wheel if feasible. Save raw output/logs, original hashes, report.md and report.html. Report implementation defects, estimator limits, misuse, and untested scope separately. Candidate remains provisional until final commit, wheel, regressions, cross-review and CI gates are met.
