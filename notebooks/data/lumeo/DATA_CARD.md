# Lumeo A/B teaching sample 1

Created 2026-10-29 for the Retentioneering A/B tutorial. This is synthetic teaching data from a selected parent realization, not data from real customers or a real randomized product experiment.

## Provenance

- Parent package: **Lumeo v8.1E3.5**.
- Parent receipt `dataset_version`: **v8**.
- Generator seed: **5044**.
- Parent population: 40,000 users, 1,038,662 events, 82,532 sessions.
- Parent event calendar: `[2026-03-02, 2026-05-25)`.
- UI telemetry begins 2026-05-11.
- Time basis: timezone-naive synthetic wall clock for a UK store; DST is not modelled.
- `assigned_at` represents first-visit time floored to a second in the simulation.
- Parent file and build-receipt hashes are recorded in `manifest.json`.

The builder verified the five source files against their receipt and verified their hashes again after extraction. Source files were not edited. The original older v5 runkit was not substituted for the newer corrected package.

## Sample selection

The sample contains 4,000 whole users selected from the complete assignment roster. For every decimal user ID, compute SHA-256 of UTF-8 `salt + '|' + decimal_user_id`; sort by hexadecimal digest, then numeric ID, and take the first 4,000.

Salt: `lumeo-ab-tutorial-2026-10-29-v1`.

No outcome, activity-window, event-count, variant or screen-completeness filter participates in selection. All available rows for each selected user are preserved, including their whole event history, repeated visits, zero-tap and incomplete screens. Layout is preserved in full. Gzip mtime and embedded filename are fixed; file hashes and column schemas are frozen in the manifest.

This outcome-independent subsampling **does not remove selection in the parent case**. Do not use this sample to estimate nominal CI coverage or false-positive rates, and do not select another salt/seed because a result is not significant.

## Tables

| Table | Rows | Key / grain |
|---|---:|---|
| `assignments.csv.gz` | 4,000 | One row per `user_id`; variant and assignment time |
| `events.csv.gz` | 104,916 | Observed event; `session_id` nested within `user_id` |
| `screens.csv.gz` | 3,891 | One `screen_instance_id`, including zero taps |
| `clicks.csv.gz` | 4,897 | One `tap_id`, referencing a screen |
| `layout.csv.gz` | 137 | UI zone geometry and variant/surface metadata |

Events do not contain the assignment variant. Join it from assignments using a many-to-one validated key. A left join from events cannot construct the full experiment population: start with assignments when forming the denominator.

The loader validates checksums, exact columns/counts, assignment uniqueness, event-user membership, session nesting, screen/tap uniqueness, screen references and `n_taps` consistency. These are integrity checks for a frozen fixture, not a proof that arbitrary new data have complete observation or valid randomization.

## Tutorial population and outcome

The tutorial uses assignments before 2026-05-11 and event outcomes in `[2026-05-11, 2026-05-25)`.

| Population | Control | Treatment |
|---|---:|---:|
| Assigned before start | 1,696 | 1,623 |
| Observed in window | 117 | 99 |
| No events in window | 1,579 | 1,524 |
| Observed session paths | 244 | 173 |

This is a late calendar-window outcome among earlier assignments, not conversion within 14 days of each assignment. It is intentionally kept separate from the descriptive population of active sessions reaching a focal event.

For supported event outcomes the simulation contract provides complete logging through the common cutoff. No matching event therefore means zero event count/indicator/sum. A user's last event is not a censoring time. Missing order value on an existing order must not be treated as zero.

Screen recording is a separate coverage layer. An incomplete screen is not a known zero-tap exposure; complete zero-tap screens remain in screen denominators. Filtering screens does not remove assignment units from the event-outcome analysis.

## Intended and unsupported uses

Use this fixture to teach Retentioneering integration, population accounting, metric definitions, clustered resampling and interval diagnostics. Rare outcomes are part of the lesson: there are six buyers in each arm of the tutorial cohort, and some ratio/conditional intervals are unavailable under the explicit bootstrap policy.

Do not treat it as product evidence, tune methods until a desired result appears, use it as an independent statistical calibration benchmark, or infer that all supported real-world assignment/observation designs behave like this simulation.

The small Bernoulli calibration script uses a separate unselected data-generating process. Its output is explicitly a limited smoke check.

## Rebuilding and rights

The ready-to-run package includes the frozen sample; no source dataset is required to execute the tutorial. To rebuild from the parent directory containing `build_receipt.json` and the five source tables:

```bash
python examples/experiments/build_lumeo_sample.py /path/to/v8_1_e3_5/data --output /path/to/empty/new_sample
```

Run the command from the repository root. New manifests record the Python, pandas, NumPy and zlib versions, CSV settings and deterministic gzip settings under `serialization`. The frozen manifest records a `verified_rebuild_serialization` environment that reproduced all five published table hashes; this is a verified rebuild environment, not an assertion about the original build environment. Use those versions and settings when comparing table bytes. The manifest itself is not byte-identical because new builds include their serialization metadata; compare table hashes and the selection, source, licence and schema fields separately. The command rejects a nonempty output directory. The full synthetic generator is not bundled here; the parent receipt and hashes identify its selected output, and this builder reproduces the sampling step.

Redistribution: this synthetic teaching fixture is contributed with the Retentioneering tutorial under the repository's Apache-2.0 license. It contains no real customer records. The selected-parent limitation and the recorded hashes remain part of its interpretation.
