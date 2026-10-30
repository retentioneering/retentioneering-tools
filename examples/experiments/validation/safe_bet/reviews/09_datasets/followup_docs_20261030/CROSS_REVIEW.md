# Role 09 — cross-review of the conservative release contract

Date: 2026-10-30. This review follows the independent data/oracle audit. It uses the release decisions and authorized peer fix reports to reconcile public claims with observed behavior; it does not preserve initial-review blinding.

## Evidence reconciled

The release decisions and role 01 inference review supplied the retained/restricted boundary. Role 03's current scorecard source supplied default methods, interval and MDE metadata, support guards and bootstrap-tail availability. Role 04's SRM fix report and metadata contract supplied exact two-arm allocation assumptions, guarded multi-arm status and tri-state alarms. Role 06's conversion/funnel fixes supplied CP/Fisher labels and window behavior. Role 07's API-integrity review supplied identity, scalar-label and copy semantics. Root's later secondary-diagnostic fix supplied the separate `strata_diagnostics_basis` field and default suppression. The documentation changes use these concrete contracts and retain their limitations.

## Cross-check outcomes

| Claim checked | Observed behavior and documentation decision |
|---|---|
| “Binary defaults use z/Wilson/Newcombe” | Rejected as stale. Default rows report Fisher and conservative CP metadata; explicit z remains an approximate educational option. |
| “Auto chooses Welch/bootstrap for nonbinary rows” | Rejected as stale. Tutorials first display descriptive defaults, then explicitly opt into their advanced illustrative methods. |
| “An interval and the exact test always agree” | Rejected. The Northwind late-window Fisher result has adjusted p ≈ 0.019816 while the conservative difference interval includes zero. Both remain visible and asserted. |
| “A positive conditional funnel step has a positive lower bound” | Rejected for the new interval. Checkout has a positive point estimate but a conservative interval spanning zero; the downstream payment interval remains negative. Assertions preserve the observed directions without claiming checkout significance. |
| “A truncation proves pre-treatment provenance” | Rejected. Transferred labels need a local explicit declaration; undeclared labels remain descriptive. The tutorial explains the substantive exposure assumption. |
| “Exact primary inference makes secondary heterogeneity exact” | Rejected. Default Fisher/descriptive suppress those values; tutorial diagnostic sections explicitly request guarded z and expose `asymptotic_exploratory`. |
| “An SRM non-alarm proves valid randomization” | Rejected. Status, allocation assumptions, conflict alarms and missing-result semantics are explicit. Exact two-arm numerical goldens were updated, without changing counts. |
| “More bootstrap draws fix sparse heavy tails” | Rejected. The A/A example retains its original heavy-tail assertions using explicit approximate methods; the text distinguishes Monte Carlo noise from model/support failure. |
| “Bundled stories demonstrate coverage” | Rejected. Six curated cases and Lumeo provide deterministic workflow checks, not statistical calibration. The preregistered independent oracle remains the aggregate correctness evidence. |

## Golden assertion audit

Before changing showcase expectations, `--audit-assertions` evaluated every existing assertion independently and saved all eight failing claims in `showcase_assertion_audit/assertion_failures.json`. Corrections were limited to consequences of the announced contract: exact SRM rounding, conservative CP endpoints, the guarded support warning, explicit provenance, exact late-bin p-value and conditional checkout uncertainty. Counts, point effects, metric directions, window populations and clustering structure were retained. Additional assertions cover exact default method metadata, withheld relative intervals, descriptive nonbinary defaults and distinct exploratory secondary metadata.

The narrative tests likewise retain the planted numerical stories but opt into their actual approximate methods. An exact equality on MDE's floating-point family alpha was replaced by a 1e-15 tolerance; it still checks the planned correction. Diagnostic runs were never counted as acceptance. Full assertions passed in the four final source runs recorded in the fix report.

## Release handoff

Candidate files were released to root before global formatting/freeze. The owned pinned-hook run changed formatting only in the two A/B notebooks; a Python-AST comparison records semantic equivalence. Root subsequently identified a separate final MCP precision update requiring exact output parity in the showcase. Root reported **2 passed** for its actual notebook test rerun, then froze commit `aac5866c232a48bee3ca071c31c52fa26a818c51`. That rerun and the forthcoming installed-wheel notebook runs supersede the earlier source acceptance for final packaging.

Remaining limits are intentionally public: manual flags cannot prove scientific provenance, schema grain cannot prove independence, finite curated stories cannot certify coverage, and approximate nonbinary procedures remain sensitive to sparsity and tails. No additional inference feature is promoted to the safe default by this documentation work.
