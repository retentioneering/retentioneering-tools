# Role 07: Python API / systems audit

Date: 2026-10-30. Reviewer scope: API validation, repeatability, immutability, serialization, arithmetic and resource bounds. Frozen candidate: PR220, `38cbe91d5b315584575b132a596890e69eb210aa`.

Before source inspection I read only `PROTOCOL.md` and the parent role assignment. I have not read implementation, peer reports, or a checkpoint. The role assignment supplied public names `column_sum`, `column_mean`, `column_last`, `get_ab_paths`, `add_segment`, and `get_path_segments`; exact signatures are not yet known. Tests will use adapters once the public contract is read. This is an oracle-first software audit, not blinded human research.

## Independent expectations

The oracle contains small event rows with IDs that differ by Python type, groups, out-of-order input timestamps, signed values, zero-denominator values, missing measurements and timezone-aware times. It computes sums, means and timestamp-last contributions independently with Python standard library only. Expected missing policies are explicit hypotheses until the documented API is examined. IDs `1` and `'1'` must not silently alias if the API accepts mixed types; typed segment labels similarly require preservation or a clear rejection. Boolean/integer aliasing is a separate explicit Python limitation, not presumed to be a library defect.

For each accepted input: repeat calls should return equal values; dataframes and path data should remain unchanged unless mutation is documented; independent copies should not share unintended mutation. Public operations should either accept supported types with faithful values or reject invalid/unknown parameters clearly. A valid JSON or recipe export should preserve the supported public state to the extent promised by its contract.

The arithmetic oracle uses test minus control, relative change `(test-control)/control` only when control is nonzero, and ratios of aggregate numerators to aggregate denominators. Zero and missing arithmetic is explicitly represented as undefined, not as an invented finite effect. This is a declared reporting convention; source documentation may use a different legitimate convention.

## Planned bounded probes

1. Inspect public names/signatures/docs after hashing this file and `oracle.py`; map independent event-level contributions to supported reducers.
2. Probe `get_ab_paths`, `add_segment`, `get_path_segments`, reducers, and available metric registry/export methods with ordinary and mixed IDs/labels, repeated calls and input snapshots.
3. Probe unknown keyword/metric/column values; bool, NaN, infinity, fractional and integer values in relevant numeric parameters; naive and timezone-aware timestamps; invalid allocation labels and duplicate/colliding labels where meaningful.
4. Exercise ratios and signed/relative effects with small deterministic populations. Do not claim statistical calibration from API probes.
5. Bounded resource check (at most 20,000 events and 1,000 resamples), no deliberate OOM or unbounded jobs. Test deterministic behavior only where a seed or deterministic contract exists.
6. Save actual module paths, source and test hashes, versions, commands, raw results and a function-level MD/HTML matrix. Classify observations as implementation defects, documented limitations, caller misuse, or untested scope.

No source edits, peer review reading, or delegated work in this initial audit. Preserve preregistered oracle; any semantic changes require an amendment rather than silent rewriting.
