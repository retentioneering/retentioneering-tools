# Amendments and exposure log

2026-10-30, still before any candidate inspection: oracle self-check required a floating Wilson lower endpoint to equal zero exactly. The independent expression produces a harmless roundoff residual. Changed only that self-check to absolute tolerance 1e-15. No inference formula changed. Original SHA256 09bde16341d987de05b2be5d2c8aba32e45be3d03e809701553432f4f7726066 is retained here and in ORACLE_SHA256.txt; frozen executable hash follows in ORACLE_FINAL_SHA256.txt.

Parent supplied immutable baseline path `.work/ab-safe-bet/baseline220` at same PR220 SHA; use that for all baseline evidence.

After unblinding: accepted the documented conservative missing-value policies for degenerate pooled-z and two-constant-arm Welch rather than treating the preregistered natural limit p-values as mandatory. The oracle itself remains unchanged.

Harness corrections, with original failure logs preserved: the huge-count probe n=10^200 exceeds the independent oracle's arithmetic range, so the harness now records that limit; the public experiment adapter was corrected to supply event_catalog as a column-to-events mapping; the accepted-level public AB exception is now captured as a finding instead of stopping enumeration. No source changes or formula-oracle changes were made. `run_initial.log`, `run_adapter_initial.log`, and `run_boundary_initial.log` preserve these earlier runs.

After the initial enumeration, added a public binary-Welch reproduction for the newly quantified sparse-Bernoulli limit. Added expected-five and observed-five guard comparisons to assess candidate restrictions; these are alternatives, not preregistered correctness criteria or exact-validity claims. The initial coverage oracle and grid were not changed after observing results.
