# Role 07 — Python API and systems review

Initial bounded audit, 2026-10-30. Frozen baseline PR220: `38cbe91d5b315584575b132a596890e69eb210aa`. No implementation edits, peer reports, checkpoint reads, or delegated work. Source inspection began only after the preregistration and independent oracle were written and hashed. Post-source adapters and two corrected clock assumptions are recorded in `AMENDMENTS.md`; the oracle is unchanged.

**Decision:** the ordinary numeric API is supported by this audit's evidence, with restrictions on mixed IDs, colliding typed arm labels, and recipe persistence. Five concrete integrity findings require fixes or explicit restrictions before a broad safe-input claim. Parameter validation gaps are reported separately. These tests do not establish statistical calibration or universal input safety.

## Reproduction and provenance

From the workspace root:

```sh
.work/ab-integrated-wheel-env/bin/python output/AB_Safe_Bet_Review/roles/07_api/oracle.py
.work/ab-integrated-wheel-env/bin/python output/AB_Safe_Bet_Review/roles/07_api/audit_api.py --source .work/ab-safe-bet/baseline220 --output output/AB_Safe_Bet_Review/roles/07_api/baseline220
.work/ab-integrated-wheel-env/bin/python output/AB_Safe_Bet_Review/roles/07_api/followup_api.py --source .work/ab-safe-bet/baseline220 --output output/AB_Safe_Bet_Review/roles/07_api/followup
```

Both drivers accept `--source` and insert its `src` before imports. `baseline220/results.json` records actual imported module paths and SHA256 values, Python executable/version, dependency versions and oracle/test hashes. Installed package metadata is recorded but is not used as evidence of the source version. A full source-file manifest is in `source_manifest.json`.

Runtime dependencies: pandas 3.1.0, NumPy 2.5.4, SciPy 1.18.1, DuckDB 2.0.0. Main audit: 83 assertions/probes, 73 pass and 10 fail. Follow-up: 21 assertions/probes, 15 pass and 6 fail. These are raw hypothesis outcomes, **not 16 distinct defects**: two initial failures were clock-format misuse, several failures share one cause, and negative-seed rejection is acceptable validation. Raw errors, warnings, timing and observed tables are preserved in JSON and logs; `checks.csv` is an index.

## Function-level trust matrix

| Public function / capability | Initial disposition | Evidence and boundary |
| --- | --- | --- |
| `get_metrics`: `column_sum`, `column_mean`, `column_last` | Retain for homogeneous path IDs; restrict mixed IDs | Independent finite oracle matches sums, means and timestamp-last values. Documented null behavior passes. Mixed `1`/`'1'` IDs silently merge at SQL boundary (API-01). |
| `get_ab_paths` | Restrict until API-01 and API-02 are fixed | Ordinary per-path exports match scorecard arithmetic, ratios expose correct numerator/denominator, silent paths follow documented 0/NaN behavior, reserved/expanded column-name collisions are rejected. Typed arm labels with identical string form collapse without assignment. |
| `get_ab_test` ordinary means and ratios | Retain within audited API boundary | Independent point estimates and signed absolute effects match. Seeded calls repeat exactly. Negative denominators and infinite values are rejected; missing sum values follow the documented null policy. This role did not test inferential calibration. |
| `get_path_segments` | Retain with reserved-sentinel contract | Typed path IDs survive, missing paths return `<MISSING>`, provenance flag survives ordinary carrying. `<MISSING>`/`<MULTI>` are protocol sentinel strings; avoid them as literal categories. |
| `add_segment(path_values=...)` | Retain in memory; restrict persistence | Original dataframe/stream/Series remain unchanged, ordinary broadcasting and missing mappings work; duplicate/no-match IDs and reserved column names reject. Recipe representation has API-03/04/05. |
| `recipe()` / `from_recipe()` | Restrict to controlled JSON-native labels and noncolliding string IDs; copy nested values defensively | Ordinary string-label JSON round trip matches fingerprints and values. Typed IDs lose information, nested objects alias, and accepted timestamp/NumPy labels are not universally serializable. |
| `to_dataframe()` | Retain tested copy behavior | Mutating the returned dataframe does not mutate the source stream. The public `df` property is direct access, not a copy promise. |
| Core numeric call validation | Retain tested cases | bool, NaN, infinity, fractional count/seed parameters, out-of-range confidence/power, unknown call enums/keywords, unknown path/column, duplicate metrics and unsupported timestamp/bulk metrics reject. NumPy supported scalar cases pass. |
| Metric-config validation | Restrict claim of strictness | Top-level and column-metric unknown keys reject; unknown `has_event` arguments and denominator keys are ignored; falsy non-dict `metric_args` are normalized. See validation observations. |
| Timezone/window inputs | Retain matching-clock restriction | Naive assignment times correctly localize to stream timezone with aware cutoff; assignment table is unchanged. Naive cutoff against aware stream, NaT, invalid cutoff, bare numeric/nonpositive horizons reject in tested configurations. |
| Registry consistency | Retain tested scalar set | All 14 tested scalar metrics yield the same per-path values through `get_metrics` and `get_ab_paths`, including NaN patterns. Four registry entries (three bulk metrics and `first_event_time`) intentionally cannot be scorecard scalar metrics. |
| Scorecard JSON via pandas | Retain tested path | `DataFrame.to_json(orient='records')` round-trips with `json.loads`, including unavailable estimates. No promise that raw dataframe records are strict JSON is inferred. |
| Resource behavior | Bounded evidence only | 4,000 events, 2,000 paths, one ratio, 100 replicates completed in 0.0474 s measured inside the analysis call; process peak RSS about 310 MB including interpreter/imports. No large-scale memory/latency guarantee, OOM test or simulation claimed. |

## Concrete findings

### API-01 — P1: mixed typed path IDs silently move contributions between paths

Classification: implementation defect for accepted inputs. The constructor accepts distinct integer `1` and string `'1'`; `get_path_segments` keeps them distinct. With UTC timestamps, values 2 at `1`, 4 at `'1'`, and −1 at `'01'`, both `get_metrics` and `get_ab_paths` return **0 for integer 1 and 6 for string '1'**, instead of 2 and 4. The mean for string `'1'` is 3 instead of 4, and integer 1 has NaN mean/last despite an event. An explicit-ID split puts these paths in different arms, so this is a cross-arm data-integrity error.

Exact public reproduction: `followup_api.py`, `mixed_ids_utc_get_metrics` and `mixed_ids_utc_get_ab_paths`. The fixture is the frozen `MIXED_ID_ROWS` with equivalent times normalized to UTC; independent values are in `oracle_expected.json`.

Source: `metrics/metric_builder.py:1355–1366` sends the raw mixed-type path column through DuckDB grouping; `:1039–1041` reindexes results to original path IDs and fills unmatched totals with zero. Observed behavior is consistent with string coercion at the SQL boundary; the evidence is the public output, not an assumption that every mixed dtype fails.

Scope: reproduced for the three column reducers and their A/B path export, on integer/string IDs with identical textual representation. Other mixed-type pairs and all registry metrics are untested. Remedy: stable lossless internal path encoding before SQL, or reject heterogeneous/colliding path IDs before analysis. Regression: preserve 2/4/−1 contributions and explicit-ID arm totals, or assert a clear constructor/API rejection.

### API-02 — P1: typed arm labels collapse in the public per-path table

Classification: implementation defect. With string path IDs, group values integer `1` for treatment and string `'1'` for control, `get_ab_test(('group',1,'1'), ...)` correctly returns sum means **7 and 3**. `get_ab_paths` returns string `'1'` for every path. Grouping its exported table by `arm` yields one mean **5**, contradicting the documented aggregation-to-scorecard contract. With an assignment Series, typed labels survive, so the defect is localized to segment-resolved labels.

Exact reproduction: `audit_api.py`, `mixed_arm_labels_without_assignment` (fails) and `mixed_arm_labels_with_assignment` (passes); `followup_api.py`, `typed_arm_label_contradiction` includes scorecard and grouped export together.

Source: `tools/_diff.py:430` converts both selected levels with `str()`; `tools/ab_test.py:1953–1962` consumes those labels, replacing them with typed labels only when assignment is supplied. The arm column's documented string type does not make loss of group identity acceptable.

Remedy: preserve typed arm identity or expose stable distinct arm identifiers separately from display labels; alternatively reject indistinguishable serialized labels. Regression: exported arm grouping must reproduce both scorecard means for integer/string labels, with and without assignment.

### API-03 — P1: recipe serialization drops one of two typed path mappings

Classification: implementation defect violating the documented recipe round-trip. `add_segment('label', path_values=pd.Series(['integer','string','leading'], index=[1,'1','01']))` correctly labels all three paths in memory. Its recipe is `{'1':'string','01':'leading'}`: the integer mapping is already discarded before JSON encoding. Replay gives integer 1 `<MISSING>` rather than `'integer'`.

Exact reproduction: `audit_api.py`, `mixed_id_recipe_json_roundtrip`. This probe deliberately avoids numeric metric calculations, so API-01 is not needed to reproduce it.

Source: `ops.py:151–160` serializes Series indexes with `{str(k): ...}`; `data_processors/add_segment.py:327–343` only falls back to string IDs when **no** key directly matches. Source contract: `eventstream/eventstream.py:259–266` promises JSON-serializable recipes and round trips.

Remedy: typed key/value records with lossless replay, or explicit collision rejection. Regression: round-trip typed Series indexes and ensure an input mapping can never lose a row silently. Merely making replay cast all IDs to strings would merge identities and is insufficient.

### API-04 — P2: mutable parameters and exported recipes alter stored lineage

Classification: implementation defect in reproducible state. After `values={'c1':'a',...}; derived=s.add_segment('stratum',path_values=values)`, changing `values['c1']` changes `derived.recipe()` even though the derived dataframe still has `'a'`. Similarly, changing `derived.recipe()[0]['path_values']['c1']` alters the stream's stored recipe. Replaying that recipe therefore describes different data from the already-created stream.

Exact reproduction: `audit_api.py`, `recipe_input_dict_isolation` and `recipe_nested_copy_isolation`.

Source: `ops.py:135–138` stores mutable parameter values by reference; `eventstream/eventstream.py:273` copies only each outer dict. Remedy: own a deep snapshot of supported recipe values at record time and return defensive copies. Regression: mutate both caller input and exported nested values; stored recipe and reconstructed dataframe must remain identical to the original state.

### API-05 — P2: accepted label values break the JSON-serializable recipe promise

Classification: implementation defect / unimplemented serialization scope. A timestamp-valued `path_values` Series is accepted by `add_segment`, but `json.dumps(derived.recipe(),allow_nan=False)` raises `TypeError: Object of type Timestamp is not JSON serializable`. A dict with NumPy `int64` labels is also accepted but raises `Object of type int64 is not JSON serializable`. These fail even with ordinary string path IDs.

Exact reproduction: `followup_api.py`, `recipe_json_timestamp` and `recipe_json_numpy_dict`.

Source: `ops.py:151–170` normalizes Series NumPy scalars only; dict arguments bypass that conversion and timestamps remain objects. Remedy: recursively encode supported scalar types with matching replay semantics, or reject/document unsupported persistent labels at the API boundary. Regression: JSON encode, decode and reconstruct each accepted persisted scalar type without changing label identity.

## Validation observations and accepted restrictions

- **P3 validation consistency:** `has_event` accepts `metric_args={'event':'pay','typo':1}`. A ratio denominator `{'metric':'length','typo':1}` silently ignores `typo`. Top-level unknown metric fields and unknown column reducer fields reject. Source `tools/ab_test.py:372–375` reconstructs denominators from two recognized fields; the single-event parser reads recognized args without an unknown-key guard. These demonstrate that “all unknown keys reject” is false. The extra inert key itself does not change valid arithmetic; do not inflate this into a measured numerical defect.
- **P3 type validation:** `metric_args=False` and `metric_args=[]` on `length` are accepted because `tools/ab_test.py:355–358` uses `... or {}` before type validation. Both contradict a strict “dict” input interpretation, but are equivalent to empty arguments for this metric. Nonempty malformed args were not exhaustively fuzzed.
- **Accepted contract:** the guide, `docs/guide/ab-testing.md:135`, defines relative delta as NaN for group 2 ≤ 0. Signed absolute effects remain correct. This is a documented restriction, not failure of the oracle's alternative negative-base formula.
- **Accepted rejection:** mixed-offset raw timestamp strings require normalization; a naive cutoff cannot be compared to an aware stream. A corrected timezone-aware-cutoff probe passes with naive assignment localization. The original failures are preserved as exposure/amendment evidence.
- **Seed boundary:** negative `random_state=-1` rejects with NumPy's `ValueError`; 0 and `2**64` are accepted and deterministic behavior is verified separately at seed 17. The API could describe “nonnegative int” more precisely; no incorrect result is observed.
- **Sentinel contract:** literal `<MISSING>` plus actual null categories are indistinguishable in per-path labels, and `get_segment_levels()` lists the sentinel twice. These names have documented sentinel meanings. Avoid using them as literal categories; no additional inferential defect is asserted from this probe.

## Limits and handoff

No statistical simulations, Monte Carlo calibration, full test suite, wheel build, CI, frontend serialization, concurrency/race stress, or memory scaling study were performed by this role. Exact large-integer precision, Decimal/custom objects, DST fold/gap assignment instants and all remaining processor recipes are untested. No fixes or cross-review occurred. Initial evidence is ready for an independent reviewer to reproduce API-01 through API-05 before changes. A passing ordinary fixture supports the stated function boundaries only.
