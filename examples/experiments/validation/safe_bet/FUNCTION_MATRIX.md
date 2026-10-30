# Пoфункциональная матрица кандидата

Инвентаризация: **353 добавленных или измененных Python-функций**, включая внутренние и вложенные функции, относительно master `e597bfb`. Дата: 2026-10-30. AST-инвентаризация включает изменения docstring; количество строк не означает количество независимо доказанных алгоритмов.

Статус относится к указанному контракту. Сохраненный вычислительный метод не получает универсальную гарантию статистической калибровки. Для каждой функции указан пакет независимых свидетельств; внутренние адаптеры часто проверены через публичный API, а не отдельным математическим доказательством. Проверки окончательной сборки и CI перечислены отдельно в итоговом отчете.

## Пакеты свидетельств и границы

| Группа | Ревьюеры | Проверки | Ограничения |
|---|---|---|---|
| binary | 01, 03, 06 | Exact binary enumeration; direct binomial-tail bisection; ab_safe_binary_test, conversion_funnel_exact_test | Independent Bernoulli units; fixed population/window; conservative coverage, no guarantee of CI/p-value equivalence. |
| approx | 01, 03, 04 | Independent arithmetic/oracle and null/coverage experiments; explicit-method regressions | Approximation only. Sparse/unequal/heavy-tail samples can violate nominal calibration; explicit selection and support guards do not prove the model. |
| multiplicity | 04, 03 | 427 initial checks; independent declared-family/NaN cross-checks; MDE/SRM regressions | Holm/Bonferroni require valid input p-values. BH additionally requires independence or suitable positive dependence. No protection for optional stopping or undeclared exploration. |
| resampling | 03, 04, 10 | Frozen multinomial oracle; paired eligibility, overflow/resource/tail regressions; independent heavy-tail simulations | Whole declared unit receives one shared weight. Empirical draws are computationally tested; nominal inferential coverage is not universal. Undefined draws withheld; bounds enforced. |
| population | 02, 07, 09 | Independent assignment/window/strata oracles; collider and legacy-recipe regressions; roster dataset aggregates | Roster, cutoff, independent unit and pretreatment declarations must be true. Observed event logs cannot identify silent assignments or prove complete telemetry. |
| identity | 07, coordinator | 38 identity/recipe regressions; independent mixed-ID and mutation oracles; 87 session/identity compatibility checks | Nonmissing IDs have homogeneous scalar types; integers fit signed64 or uint64. Supported scalar recipe labels only; callable recipes are outside this audit. |
| metrics | 03, 07, 09 | Independent handwritten per-unit aggregates on six generated cases, Lumeo and held-out fixture; registry/ratio regression suites | Defined metric/missingness rules are retained. Synthetic data agreement validates computation, not unbiasedness of an analyst-chosen estimand. |
| funnels | 06, coordinator | 9854 original oracle assertions; 256 expanded cases; 538 cross-checks; exact/fixed-window regression suites | All-start windows are isolated and batched. Ordinary population is observed paths; conditional transitions have no confirmatory p-value. No general cluster/censoring inference. |
| transport | 08, 10 | JSON/payload parity, typed labels, status preservation, actual widget exports and browser checks (see final release record) | Adapters display the engine result. Full browser/package readiness is recorded separately at the final source/artifact revision; static checks alone are insufficient. |
| datasets | 09, 10 | 1107 initial schema/handwritten numerical checks; all six generators, Lumeo and held-out sample; installed package smoke | Teaching fixtures, deliberately planted stories and selected seeds are not evidence of real-world treatment effects or nominal coverage. |
| plumbing | 07, 08, 10 | API/serialization/whole-suite compatibility and package verification; see final release record | Indirect integration coverage; no separate mathematical reliability claim for each helper or every possible input. |

## src/retentioneering/data_processors/add_segment.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `_persistent_scalar` (L308) | Supported lossless JSON scalars for persisted path-label mappings. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `_path_values_series` (L330) | Validate `path_values` and return it as a Series indexed by path id. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `_broadcast_path_values` (L363) | Map per-path values onto rows; ids the mapping does not hold get NaN. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `_check_path_value_ids` (L386) | Raise when no `path_values` id is a path id, warn when under half are. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `AddSegment.__init__` (L434) | Internal adapter/helper; responsibility follows the containing public contract. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `AddSegment.apply` (L487) | Internal adapter/helper; responsibility follows the containing public contract. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |

## src/retentioneering/datasets/ab_cases/_sim.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `check_n_users` (L29) | Internal adapter/helper; responsibility follows the containing public contract. | Retained teaching/verification fixture | datasets; роли 09, 10 |
| `streams` (L39) | ``n`` independent generators: tuning one stage never reshuffles another. | Retained teaching/verification fixture | datasets; роли 09, 10 |
| `arrival_seconds` (L44) | Arrival times (seconds since a Monday start) over ``n_days`` days. | Retained teaching/verification fixture | datasets; роли 09, 10 |
| `sessions` (L63) | First session at ``first_s``, returns as a Poisson process until the end. | Retained teaching/verification fixture | datasets; роли 09, 10 |
| `session_numbers` (L98) | 0-based number of each session within its user (input sorted by user). | Retained teaching/verification fixture | datasets; роли 09, 10 |
| `chain_cdf` (L105) | Cumulative transition matrices ``(kind, state, next_state)``. | Retained teaching/verification fixture | datasets; роли 09, 10 |
| `walk` (L122) | Run one Markov chain per row from ``start`` until a terminal state. | Retained teaching/verification fixture | datasets; роли 09, 10 |
| `flatten` (L161) | Turn a session grid (``-1`` = no event) into events. | Retained teaching/verification fixture | datasets; роли 09, 10 |
| `session_ids` (L187) | ``"<user_id>_<n>"`` with ``n`` counted from 1. | Retained teaching/verification fixture | datasets; роли 09, 10 |
| `money` (L192) | Lognormal amounts rounded to cents. | Retained teaching/verification fixture | datasets; роли 09, 10 |

## src/retentioneering/datasets/ab_cases/aa_calibration.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `generate` (L144) | Return ``(events, assignment)``; ``events`` includes the truth column. | Retained teaching/verification fixture | datasets; роли 09, 10 |

## src/retentioneering/datasets/ab_cases/push_campaign.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `generate` (L139) | Return ``(events, assignment)``; ``events`` includes the truth column. | Retained teaching/verification fixture | datasets; роли 09, 10 |

## src/retentioneering/datasets/ab_cases/session_assignment.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `generate` (L148) | Return ``(events, assignment)``; ``events`` includes the truth column. | Retained teaching/verification fixture | datasets; роли 09, 10 |

## src/retentioneering/datasets/ab_cases/srm_platform_bug.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `generate` (L137) | Return ``(events, assignment)``; ``events`` includes the truth column. | Retained teaching/verification fixture | datasets; роли 09, 10 |

## src/retentioneering/datasets/ab_cases/trial_conversion.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `_late_day` (L136) | Internal adapter/helper; responsibility follows the containing public contract. | Retained teaching/verification fixture | datasets; роли 09, 10 |
| `generate` (L145) | Return ``(events, assignment)``; ``events`` includes the truth column. | Retained teaching/verification fixture | datasets; роли 09, 10 |

## src/retentioneering/datasets/ab_test.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `_transition_cdf` (L329) | Cumulative transition matrices, shape (type, state, next_state). | Retained teaching/verification fixture | datasets; роли 09, 10 |
| `_novelty_mult` (L343) | Internal adapter/helper; responsibility follows the containing public contract. | Retained teaching/verification fixture | datasets; роли 09, 10 |
| `_generate` (L352) | Return ``(events, users)`` in compact form. | Retained teaching/verification fixture | datasets; роли 09, 10 |
| `_user_ids` (L566) | Internal adapter/helper; responsibility follows the containing public contract. | Retained teaching/verification fixture | datasets; роли 09, 10 |
| `_assemble` (L570) | Turn the compact ``(events, users)`` pair into the public events frame. | Retained teaching/verification fixture | datasets; роли 09, 10 |
| `_assignment` (L608) | Internal adapter/helper; responsibility follows the containing public contract. | Retained teaching/verification fixture | datasets; роли 09, 10 |
| `_write_fixture` (L627) | Write the compact output of ``_generate(n_users, seed)`` as three parquet files. | Retained teaching/verification fixture | datasets; роли 09, 10 |
| `_read_fixture` (L689) | Read the files of ``_write_fixture`` back into ``_generate``'s ``(events, users)``. | Retained teaching/verification fixture | datasets; роли 09, 10 |
| `list_ab_tests` (L724) | Return the bundled A/B datasets: name -> one-line description. | Retained teaching/verification fixture | datasets; роли 09, 10 |
| `_resolve` (L749) | Validate the loader arguments; return ``(name, n_users, seed)``. | Retained teaching/verification fixture | datasets; роли 09, 10 |
| `load_ab_test` (L772) | Generate a bundled A/B test dataset (default: "Northwind Home"). | Retained teaching/verification fixture | datasets; роли 09, 10 |
| `load_ab_test_assignment` (L892) | Return the assignment table that goes with :func:`load_ab_test`. | Retained teaching/verification fixture | datasets; роли 09, 10 |

## src/retentioneering/eventstream/eventstream.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `_validate_path_id_types` (L24) | Validate identities and normalize object integers without floating casts. | Retained public identity/recipe adapter | identity; роли 07, coordinator |
| `_numpydoc_split` (L123) | Split a numpy-style docstring into its summary and ``{section: body}``. | Retained integration helper | plumbing; роли 07, 08, 10 |
| `_numpydoc_join` (L148) | Internal adapter/helper; responsibility follows the containing public contract. | Retained integration helper | plumbing; роли 07, 08, 10 |
| `Eventstream._post_init` (L205) | Internal adapter/helper; responsibility follows the containing public contract. | Retained public identity/recipe adapter | identity; роли 07, coordinator |
| `Eventstream._preprocess` (L237) | Internal adapter/helper; responsibility follows the containing public contract. | Retained public identity/recipe adapter | identity; роли 07, coordinator |
| `Eventstream.recipe` (L312) | Return this eventstream's lineage as a JSON-serializable list of op | Retained public identity/recipe adapter | identity; роли 07, coordinator |
| `Eventstream.get_path_segments` (L459) | Return one segment value per path. | Retained public identity/recipe adapter | identity; роли 07, coordinator |
| `Eventstream.get_sample_ratio` (L526) | Check an experiment's arm sizes against the expected split (sample ratio mismatch). | Retained allocation diagnostic under stated design | multiplicity; роли 04, 03 |
| `Eventstream.get_ab_test` (L680) | Compare two arms of an experiment on path metrics: a scorecard. | Retained public adapter; statistical branch restrictions apply | population; роли 02, 07, 09 |
| `Eventstream.get_ab_bootstrap` (L973) | Return joint path-bootstrap replicates of every metric of an experiment. | Explicit advanced/raw resampling surface | resampling; роли 03, 04, 10 |
| `Eventstream.get_ab_paths` (L1096) | Return the per-path table behind an A/B scorecard: one row per compared path. | Retained public identity/recipe adapter | identity; роли 07, coordinator |
| `Eventstream.add_clusters` (L1300) | Cluster paths using ML and add a new segment column with `cluster_0`, `cluster_1`, | Retained integration helper | plumbing; роли 07, 08, 10 |
| `Eventstream.get_metrics` (L1566) | Compute per-path metric values as a feature table. | Retained integration helper | plumbing; роли 07, 08, 10 |
| `Eventstream.add_segment` (L1731) | Add a new categorical segment column to the eventstream. | Retained public identity/recipe adapter | identity; роли 07, coordinator |
| `Eventstream._split_two` (L2431) | Event-level split used by the existing widgets' `diff`. | Retained public identity/recipe adapter | identity; роли 07, coordinator |
| `Eventstream.experiment_data` (L3000) | Estimate fixed-window experiment outcomes from a complete assignment roster. | Retained public adapter; statistical branch restrictions apply | population; роли 02, 07, 09 |
| `Eventstream.experiment` (L3095) | Display a calculated experiment scorecard and funnel in Jupyter. | Retained public adapter; statistical branch restrictions apply | population; роли 02, 07, 09 |
| `Eventstream.funnel` (L3152) | Interactive conversion funnel for Jupyter notebooks. | Retained public conversion/funnel adapter | funnels; роли 06, coordinator |
| `Eventstream.funnel_data` (L3233) | Compute funnel conversion metrics and return a dict (headless). | Retained public conversion/funnel adapter | funnels; роли 06, coordinator |
| `Eventstream.get_conversion_rate` (L3395) | Given that `start_anchor` happened, how often does `end_anchor` follow? | Retained public conversion/funnel adapter | funnels; роли 06, coordinator |
| `Eventstream.ab_test` (L3664) | Internal adapter/helper; responsibility follows the containing public contract. | Retained public adapter; statistical branch restrictions apply | population; роли 02, 07, 09 |
| `Eventstream.ab_test_data` (L3725) | Internal adapter/helper; responsibility follows the containing public contract. | Retained public adapter; statistical branch restrictions apply | population; роли 02, 07, 09 |

## src/retentioneering/exceptions.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `InvalidParameterError.__init__` (L43) | Internal adapter/helper; responsibility follows the containing public contract. | Retained integration helper | plumbing; роли 07, 08, 10 |
| `SegmentLevelNotFoundError.__init__` (L131) | Internal adapter/helper; responsibility follows the containing public contract. | Retained integration helper | plumbing; роли 07, 08, 10 |
| `PathIdNotFoundError.__init__` (L151) | Internal adapter/helper; responsibility follows the containing public contract. | Retained integration helper | plumbing; роли 07, 08, 10 |

## src/retentioneering/mcp/_agent_logic.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `_step_to_code` (L219) | Convert a preprocessor step dict to its Python Eventstream call. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |

## src/retentioneering/mcp/_prompts.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `_static_instructions` (L149) | The stream/context-independent portion of the system prompt: role | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_system_instructions` (L329) | Full system prompt for the notebook `serve()` flow: a small per-stream | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |

## src/retentioneering/mcp/_report_session.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `ReportSession.__init__` (L23) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `ReportSession.load_data` (L45) | Replace the base/active stream with *stream* — used by the | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |

## src/retentioneering/mcp/server.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `serve` (L46) | Start a local MCP server exposing *stream* to Claude (or any MCP client). | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_build_server` (L144) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_build_server.describe_tool` (L272) | Return full parameter documentation for a preprocessor type or internal tool. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_build_server.describe` (L296) | Return schema, event list, unique path counts and available segments. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_build_server.add_ab_test` (L493) | Register an A/B test scorecard tab in the pending report. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_build_server.get_conversion_rate` (L558) | Measure how often one event is followed by another, against its baseline. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_build_server.get_sample_ratio` (L635) | Check an A/B test's arm sizes against the expected split (sample ratio mismatch). | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_build_server.get_ab_test` (L700) | Compare two arms of an experiment on path metrics: delta, interval, verdict, MDE. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_build_server.get_ab_bootstrap` (L822) | Contrast an A/B effect between periods with a joint bootstrap interval (novelty). | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_build_server.export_report` (L920) | Generate the HTML report with all widgets added via the add_* tools | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |

## src/retentioneering/mcp/tools.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `describe_tool` (L173) | Return full parameter documentation for a preprocessor type or internal tool. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_with_table_hint` (L240) | An error about a missing ``assigned_at`` names where MCP gets the table. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_python_only_parameters` (L252) | Parameters of ``Eventstream.<name>`` that the MCP tool of the same name | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `describe` (L263) | Return schema, event list, unique path counts and available segments. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_experiment_facts` (L302) | What an A/B call needs from describe(): segment levels (arm names), value | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_assignment_facts` (L338) | The session's assignment table in a few numbers (never the table). | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `add_ab_test` (L636) | Compute an A/B test scorecard and register it as a tab in the pending report. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `get_conversion_rate` (L775) | Measure how often one event is followed by another, against its baseline. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `get_conversion_rate._num` (L871) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `get_conversion_rate._cell` (L878) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_json_number` (L907) | Preserve finite machine values; formatting belongs to the display layer. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_json_label` (L919) | Preserve supported scalar arm identity across the JSON boundary. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_display_label` (L930) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_ab_metadata` (L937) | Keep method, support and unavailable interval reasons on every MCP row. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `get_sample_ratio` (L969) | Check an A/B test's arm sizes against the expected split (sample ratio mismatch). | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_scope_text` (L1114) | What one row's p_value_adjusted and interval are corrected against. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `get_ab_test` (L1135) | Compare two arms of an experiment on path metrics: delta, interval, verdict, MDE. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `get_ab_test._row` (L1347) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `get_ab_bootstrap` (L1496) | Contrast an A/B effect between periods with a joint bootstrap interval (novelty). | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `export_report` (L1684) | Finalize the report with all widgets added via add_transition_graph / | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |

## src/retentioneering/metrics/composite.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `_composite_ids` (L29) | Factorize ``(path, key)`` pairs in order of first appearance. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `composite_path_stream` (L42) | Derive a stream whose paths are ``(path, key)`` pairs. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |

## src/retentioneering/metrics/condition_ast.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `build_metric_names` (L122) | Builds full metric name(s) from metric type and arguments. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |

## src/retentioneering/metrics/metric_builder.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `compile_scalar_outcome` (L143) | Compile supported finite scalar outcomes through the common parser. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `_normalize_event_col` (L172) | Normalizes the optional 'event_col' metric_args key. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `_normalize_event_value_sum` (L211) | Numeric sum on matching events; missing matching values are errors. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `_normalize_events_bulk` (L228) | Normalizes the 'events' metric_args value for has_event_bulk/event_count_bulk. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `_normalize_events_required` (L249) | Normalizes the 'events' metric_args value for has_all_events/has_any_event: | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `_normalize_time_between` (L260) | Normalizes 'start_event'/'end_event' metric_args for time_between: both required. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `_normalize_pattern` (L271) | Normalizes the 'pattern' metric_args value for matches_pattern: required, non-empty. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `_reject_unknown_in_segment_args` (L281) | Rejects metric_args keys the given in_segment flavour doesn't read. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `_normalize_in_segment` (L307) | Normalizes in_segment's metric_args shape: 'segment_name'/'mode' (required, | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `_normalize_in_segment_bulk` (L368) | Normalizes in_segment_bulk's metric_args shape - the wildcard-friendly | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `_normalize_in_segment_threshold` (L419) | Normalizes the 'threshold' metric_args value for in_segment's 'event_share' | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `_normalize_column` (L434) | Normalizes the 'column' metric_args value for column_sum/mean/last: | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `column_metric_name` (L451) | Column name of a column_sum/column_mean/column_last metric. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `combined_metric_name` (L456) | Canonical column name for has_all_events/has_any_event: the one piece of | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricConfig.__init__` (L472) | Args: | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricConfig.get_enriched_configs` (L490) | Returns original configs enriched with 'cols' key containing column names. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricConfig._parse_configs` (L511) | Parses configuration items into structured format | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricConfig._parse_dict_config` (L526) | Parses a dict-based configuration. Raises InvalidMetricConfigError on invalid config. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder.validate_metric_config` (L767) | Validate a single metric configuration. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder.build_metrics` (L992) | Main method for building metrics | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder._build_metric` (L1053) | Builds specific metric according to configuration | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder._build_event_value_sum` (L1095) | Internal adapter/helper; responsibility follows the containing public contract. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder._resolve_event_names` (L1108) | Resolves a possibly-wildcard (None) 'event_names' config entry to an | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder._build_count_pivot` (L1117) | Shared primitive: per-path count of each event in event_names, pivoted | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder._build_event_count` (L1158) | Builds the event count metric for a single event | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder._build_has` (L1166) | Builds the event presence metric (0/1) for a single event | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder._build_event_count_bulk` (L1175) | Builds event count metrics for one or more events, one column per event | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder._build_has_bulk` (L1183) | Builds event presence metrics (0/1) for one or more events, one column per event | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder._build_has_all_or_any` (L1194) | Builds has_all_events (AND) / has_any_event (OR) - a single 0/1 column | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder._build_time_from_to` (L1210) | Builds time difference metric between two events (in seconds) | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder._build_length` (L1259) | Number of steps (events) per path | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder._build_duration` (L1270) | Duration in seconds between first and last event per path | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder._build_first_event_time` (L1284) | Unix timestamp (seconds) of first event per path. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder._build_active_days` (L1299) | Number of unique days with at least one (matching) event per path. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder._build_column_metric` (L1328) | column_sum / column_mean / column_last over a custom column. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder._build_matches` (L1366) | Builds pattern matching metric (0/1) for each path. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder._build_in_segment` (L1403) | Builds in_segment metric (0/1) for each path. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder._build_in_segment_bulk` (L1424) | Builds in_segment_bulk metrics (0/1) - the same per-level membership | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder._resolve_segment_levels` (L1457) | Every level actually present in a segment column, sorted by string | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `MetricBuilder._build_in_segment_columns` (L1465) | Shared primitive behind in_segment and in_segment_bulk: one 0/1 | Retained descriptive metric construction | metrics; роли 03, 07, 09 |

## src/retentioneering/ops.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `op` (L93) | Decorator for `Eventstream` processor methods: records lineage. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `op.wrapper` (L114) | Internal adapter/helper; responsibility follows the containing public contract. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `_json_safe_series` (L156) | Snapshot a Series/path mapping with native scalars and typed replay keys. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `_native` (L184) | A numpy scalar as the matching Python scalar (JSON knows no numpy types). | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `_adapt_params` (L193) | Shape adapters for ops whose on-the-wire dict doesn't map 1:1 onto the | Retained with identity/serialization restrictions | identity; роли 07, coordinator |

## src/retentioneering/paths/ordered_reach.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `_catalogue` (L10) | Internal adapter/helper; responsibility follows the containing public contract. | Retained with exact/default and conditional-population boundaries | funnels; роли 06, coordinator |
| `_vocabulary` (L29) | Internal adapter/helper; responsibility follows the containing public contract. | Retained with exact/default and conditional-population boundaries | funnels; роли 06, coordinator |
| `ordered_reach` (L35) | Per-path closed ordered reach, preserving Retentioneering index/subindex. | Retained with exact/default and conditional-population boundaries | funnels; роли 06, coordinator |
| `ordered_reach_counts` (L46) | Aggregate ordered-reach counts without materializing per-path flags. | Retained with exact/default and conditional-population boundaries | funnels; роли 06, coordinator |
| `_ordered_reach` (L56) | Internal adapter/helper; responsibility follows the containing public contract. | Retained with exact/default and conditional-population boundaries | funnels; роли 06, coordinator |

## src/retentioneering/stats/experiments.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `_integer` (L18) | Internal adapter/helper; responsibility follows the containing public contract. | Retained guarded strict-adapter helper | resampling; роли 03, 04, 10 |
| `_bootstrap_config` (L28) | Internal adapter/helper; responsibility follows the containing public contract. | Retained guarded strict-adapter helper | resampling; роли 03, 04, 10 |
| `_ratio` (L65) | Internal adapter/helper; responsibility follows the containing public contract. | Retained guarded strict-adapter helper | resampling; роли 03, 04, 10 |
| `_draw_sums` (L70) | Internal adapter/helper; responsibility follows the containing public contract. | Retained guarded strict-adapter helper | resampling; роли 03, 04, 10 |
| `_constant_statistic` (L74) | Internal adapter/helper; responsibility follows the containing public contract. | Retained guarded strict-adapter helper | resampling; роли 03, 04, 10 |
| `_interval` (L78) | Internal adapter/helper; responsibility follows the containing public contract. | Restricted: requested inference; descriptive otherwise | approx; роли 01, 03, 04 |
| `_summary` (L96) | Internal adapter/helper; responsibility follows the containing public contract. | Restricted: requested inference; descriptive otherwise | approx; роли 01, 03, 04 |
| `_binary_interval_config` (L184) | Internal adapter/helper; responsibility follows the containing public contract. | Retained explicit exact or labelled approximate selector | binary; роли 01, 03, 06 |
| `_wilson` (L203) | Internal adapter/helper; responsibility follows the containing public contract. | Retained explicit exact or labelled approximate selector | binary; роли 01, 03, 06 |
| `_binary_intervals` (L207) | Internal adapter/helper; responsibility follows the containing public contract. | Retained explicit exact or labelled approximate selector | binary; роли 01, 03, 06 |
| `adjust_pvalues` (L250) | Holm correction over the full declared family, including unavailable tests. | Retained guarded strict-adapter helper | resampling; роли 03, 04, 10 |
| `_welch` (L255) | Adapter retaining treatment-minus-control orientation. | Restricted: requested inference; descriptive otherwise | approx; роли 01, 03, 04 |
| `_tests` (L261) | Internal adapter/helper; responsibility follows the containing public contract. | Restricted: requested inference; descriptive otherwise | approx; роли 01, 03, 04 |

## src/retentioneering/stats/primitives.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `_check_level` (L51) | Validate a probability-like argument strictly inside (0, 1). | Retained numerical/validation helper | metrics; роли 03, 07, 09 |
| `_z` (L62) | Two-sided standard normal quantile for ``ci_level``. | Retained numerical/validation helper | metrics; роли 03, 07, 09 |
| `_floats` (L67) | Internal adapter/helper; responsibility follows the containing public contract. | Retained numerical/validation helper | metrics; роли 03, 07, 09 |
| `_out` (L71) | Return NumPy scalars for 0-d results, arrays otherwise. | Retained numerical/validation helper | metrics; роли 03, 07, 09 |
| `wilson_ci` (L78) | Wilson score interval for a binomial proportion. | Restricted: explicit approximate method/diagnostic | approx; роли 01, 03, 04 |
| `newcombe_ci` (L116) | Newcombe hybrid-score interval for ``p1 - p2`` (method 10, 1998). | Restricted: explicit approximate method/diagnostic | approx; роли 01, 03, 04 |
| `two_proportion_z` (L151) | Asymptotic pooled two-proportion z-test, not an exact small-sample test. | Restricted: explicit approximate method/diagnostic | approx; роли 01, 03, 04 |
| `_binomial_counts` (L180) | Internal adapter/helper; responsibility follows the containing public contract. | Retained under the declared binary model | binary; роли 01, 03, 06 |
| `fisher_exact_p` (L193) | Two-sided conditional Fisher p-value for independent binary groups. | Retained under the declared binary model | binary; роли 01, 03, 06 |
| `delta_log_ratio_ci` (L218) | Confidence interval for the relative difference ``p1/p2 - 1`` of two proportions. | Restricted: explicit approximate method/diagnostic | approx; роли 01, 03, 04 |
| `welch_t` (L249) | Welch's unequal-variance t-test on summary statistics. | Restricted: explicit approximate method/diagnostic | approx; роли 01, 03, 04 |
| `_mde_factor` (L294) | Internal adapter/helper; responsibility follows the containing public contract. | Restricted: explicit approximate method/diagnostic | approx; роли 01, 03, 04 |
| `mde_proportion` (L302) | Minimum detectable absolute difference for two proportions (normal approximation). | Restricted: explicit approximate method/diagnostic | approx; роли 01, 03, 04 |
| `mde_mean` (L317) | Minimum detectable difference of means for a given pooled standard deviation. | Restricted: explicit approximate method/diagnostic | approx; роли 01, 03, 04 |
| `mde_from_se` (L329) | Minimum detectable effect from a standard error of the delta. | Restricted: explicit approximate method/diagnostic | approx; роли 01, 03, 04 |
| `_validate_pvalues` (L345) | Internal adapter/helper; responsibility follows the containing public contract. | Retained with method-specific family assumptions | multiplicity; роли 04, 03 |
| `holm` (L350) | Holm step-down adjusted p-values. | Retained with method-specific family assumptions | multiplicity; роли 04, 03 |
| `bonferroni` (L376) | Bonferroni adjusted p-values with the same NaN rule as :func:`holm`. | Retained with method-specific family assumptions | multiplicity; роли 04, 03 |
| `benjamini_hochberg` (L386) | Benjamini-Hochberg step-up adjusted p-values (false discovery rate). | Retained with method-specific family assumptions | multiplicity; роли 04, 03 |
| `cochran_q` (L413) | Cochran's Q test of homogeneity of per-stratum deltas. | Restricted: explicit approximate method/diagnostic | approx; роли 01, 03, 04 |
| `chi2_balance` (L466) | Chi-square test of independence on a strata x arm contingency table. | Restricted: explicit approximate method/diagnostic | approx; роли 01, 03, 04 |
| `srm_chi2` (L487) | Chi-square goodness-of-fit for sample ratio mismatch. | Restricted: explicit approximate method/diagnostic | approx; роли 01, 03, 04 |
| `_clopper_pearson_tails` (L530) | Clopper-Pearson endpoints using the tail probability directly. | Retained under the declared binary model | binary; роли 01, 03, 06 |
| `exact_binomial_ci` (L539) | Conservative exact Clopper-Pearson interval for a binomial proportion. | Retained under the declared binary model | binary; роли 01, 03, 06 |
| `exact_difference_ci` (L549) | Conservative exact interval for p1-p2 from Bonferroni CP arm bounds. | Retained under the declared binary model | binary; роли 01, 03, 06 |
| `constant_statistic` (L563) | Whether every defined empirical resample has the same pair-sum ratio. | Retained numerical/validation helper | metrics; роли 03, 07, 09 |
| `paired_constant_statistic` (L588) | Whether a paired change is constant at the resolution of its source values. | Retained numerical/validation helper | metrics; роли 03, 07, 09 |
| `paired_constant_statistic.resolution` (L624) | Internal adapter/helper; responsibility follows the containing public contract. | Retained numerical/validation helper | metrics; роли 03, 07, 09 |
| `stable_sample_summary` (L636) | Mean, sample SD, count and status without squaring raw magnitudes. | Retained numerical/validation helper | metrics; роли 03, 07, 09 |
| `sample_mean_ci` (L672) | Stable t interval for a unit mean; constant/undefined uncertainty is absent. | Restricted: explicit approximate method/diagnostic | approx; роли 01, 03, 04 |
| `welch_samples` (L686) | Stable raw-observation Welch result: t, df, p, SE, availability status. | Restricted: explicit approximate method/diagnostic | approx; роли 01, 03, 04 |

## src/retentioneering/stats/resampling.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `check_bootstrap_size` (L16) | Reject unsupported requests before allocating replicate arrays. | Retained computational kernel; inference restricted | resampling; роли 03, 04, 10 |
| `checked_sum` (L34) | Sum finite contributions or explicitly reject unsupported numeric range. | Retained computational kernel; inference restricted | resampling; роли 03, 04, 10 |
| `checked_ratio` (L46) | Positive-denominator ratios; undefined denominators remain NaN. | Retained computational kernel; inference restricted | resampling; роли 03, 04, 10 |
| `checked_difference` (L60) | Subtract estimates while preserving intentionally undefined values. | Retained computational kernel; inference restricted | resampling; роли 03, 04, 10 |
| `draw_weights` (L69) | Internal adapter/helper; responsibility follows the containing public contract. | Retained computational kernel; inference restricted | resampling; роли 03, 04, 10 |
| `draw_sums` (L77) | Internal adapter/helper; responsibility follows the containing public contract. | Retained computational kernel; inference restricted | resampling; роли 03, 04, 10 |
| `percentile_interval` (L102) | Return low, high, availability status and undefined-draw count. | Retained computational kernel; inference restricted | resampling; роли 03, 04, 10 |

## src/retentioneering/tools/_bootstrap.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `BootstrapResult.delta` (L96) | Replicate deltas ``group1 - group2`` of shape (n_bootstrap, n_cells). | Retained computational kernel; inference restricted | resampling; роли 03, 04, 10 |
| `_draw` (L101) | Compatibility wrapper for the common whole-unit weight kernel. | Retained computational kernel; inference restricted | resampling; роли 03, 04, 10 |
| `bootstrap_paths` (L106) | Resample paths within each arm and evaluate ratio-of-sums cells. | Retained computational kernel; inference restricted | resampling; роли 03, 04, 10 |
| `percentile_ci` (L194) | Pointwise percentile intervals; every requested draw must be defined. | Retained computational kernel; inference restricted | resampling; роли 03, 04, 10 |
| `bootstrap_se` (L215) | Standard deviation (ddof=1) when every requested replica is finite. | Retained computational kernel; inference restricted | resampling; роли 03, 04, 10 |
| `bootstrap_p` (L232) | Two-sided p-value from the bootstrap standard error: ``2 * Phi(- / delta /  / sd*)``. | Retained computational kernel; inference restricted | resampling; роли 03, 04, 10 |
| `contrast_rows` (L247) | Build per-replicate contrast rows. | Retained computational kernel; inference restricted | resampling; роли 03, 04, 10 |

## src/retentioneering/tools/_diff.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `_check_independent_path_grain` (L75) | Path-level intervals must not silently treat nested paths as independent. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `warn_user` (L102) | Emit a warning attributed to the first frame outside the library. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `DiffGroups.n1` (L146) | Number of paths in group 1. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `DiffGroups.n2` (L150) | Number of paths in group 2. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `_sorted_index` (L155) | Internal adapter/helper; responsibility follows the containing public contract. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `coerce_level` (L163) | Match a value that arrived as a string back to a typed segment level. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `path_segment_values` (L185) | Distinct ``(path, level)`` pairs of a segment column. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `per_path_label` (L206) | One value per path: the level, ``<MULTI>`` if not constant, ``<MISSING>`` if null. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `normalize_assignment` (L221) | Validate an assignment table and return it as ``DataFrame(arm[, assigned_at])``. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `align_assignment` (L282) | Match the ids of a normalised assignment table to the stream's path ids. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `diff_repr` (L358) | ``repr(diff)`` with each id collection cut to ``MAX_IDS_SHOWN`` ids. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `_segment_groups` (L384) | Internal adapter/helper; responsibility follows the containing public contract. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `_assignment_groups` (L433) | Internal adapter/helper; responsibility follows the containing public contract. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |
| `resolve_diff` (L516) | Resolve a ``diff`` into two disjoint path-id sets. | Retained with identity/serialization restrictions | identity; роли 07, coordinator |

## src/retentioneering/tools/_stats.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `__getattr__` (L12) | Internal adapter/helper; responsibility follows the containing public contract. | Retained integration helper | plumbing; роли 07, 08, 10 |

## src/retentioneering/tools/ab_test.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `_metric_column` (L244) | Validate one registry config and return its ``get_metrics`` column name. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `_metric_args_hint` (L298) | The fix for keys outside an entry's schema: metric arguments go under | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `_kind_of` (L315) | Internal adapter/helper; responsibility follows the containing public contract. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `ABMetricSpec.from_user` (L355) | Validate a user entry. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `ABMetricSpec.family_key` (L435) | Key of the multiplicity family (the metric name). | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `parse_metrics` (L440) | Validate the ``metrics`` argument (non-empty list, unique names). | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `_config_key` (L457) | Internal adapter/helper; responsibility follows the containing public contract. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `_sorted_ids` (L461) | Internal adapter/helper; responsibility follows the containing public contract. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `_coverage` (L469) | Observed event span, used only as a descriptive calendar display range. | Retained with explicit population/provenance guards | population; роли 02, 07, 09 |
| `_reject_bare_number` (L496) | A duration needs a unit: ``14`` or ``"14"`` would otherwise be read as ns. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `_to_offset` (L506) | A pandas offset from an offset alias, a period alias (``"M"``, ``"Q"``, ``"Y"`` | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `_fixed_length` (L526) | A positive fixed-length duration from an offset alias, Timedelta or offset. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `_calendar_period` (L551) | Resolve a calendar ``period``. | Retained with explicit population/provenance guards | population; роли 02, 07, 09 |
| `_align_assigned_at` (L607) | ``assigned_at`` in the stream's timezone. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `_unit_reason` (L653) | Internal adapter/helper; responsibility follows the containing public contract. | Retained with explicit population/provenance guards | population; роли 02, 07, 09 |
| `_check_unit` (L661) | Refuse a path grain that does not match the assigned unit (Deng et al. 2018). | Retained with explicit population/provenance guards | population; роли 02, 07, 09 |
| `_is_segment_diff` (L730) | Whether ``diff`` has the ``("segment_col", v1, v2)`` form. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `check_segment_unit` (L748) | Refuse a path grain finer than the unit an arm segment was assigned at. | Retained with explicit population/provenance guards | population; роли 02, 07, 09 |
| `prepare_assignment` (L810) | Unit guard, id matching (:func:`~retentioneering.tools._diff.align_assignment`) | Retained with explicit population/provenance guards | population; роли 02, 07, 09 |
| `_strata_provenance` (L828) | Whether ``strata_col`` was fixed before exposure (see :func:`_provenance`). | Retained with explicit population/provenance guards | population; роли 02, 07, 09 |
| `_provenance` (L833) | Read an explicit pretreatment declaration, never infer it from a cut. | Retained with explicit population/provenance guards | population; роли 02, 07, 09 |
| `_strata_flag` (L871) | ``(strata_pre_exposure, strata_pre_exposure_source)``; warns when the auto mode | Retained with explicit population/provenance guards | population; роли 02, 07, 09 |
| `MetricMatrix.build` (L970) | Run the metric registry once over the union of numerators and denominators. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `MetricMatrix.column` (L1293) | ``(num, den, pop)`` of one cell (n_paths each). | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `MetricMatrix.columns` (L1314) | Numerator and denominator matrices ``(n_paths, len(cells))``. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `MetricMatrix.fingerprint` (L1322) | Hash of ``(path_ids, arm, cells)``; the bootstrap cache key. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `_period_keys` (L1333) | Integer period key per row and the period table (sorted chronologically). | Retained with explicit population/provenance guards | population; роли 02, 07, 09 |
| `_metric_values` (L1440) | ``(values, active)``: values ``(n_paths, n_periods, n_configs)`` and the | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `_numeric` (L1481) | Metric frame as floats; a non-numeric column is a config error. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `ABTest.from_stream` (L1530) | Validate the arguments, resolve the groups, build the matrix. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `ABTest.alpha` (L1754) | Internal adapter/helper; responsibility follows the containing public contract. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `ABTest.resolved_n_bootstrap` (L1758) | ``n_bootstrap``, with ``"auto"`` = ``max(1000, ceil(20 * m / alpha))``. | Retained dispatch; exact/default or explicit approximation | resampling; роли 03, 04, 10 |
| `ABTest.fit` (L1773) | Return the scorecard (columns in ``COLUMNS`` order, optional ones dropped). | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `ABTest.bootstrap` (L1805) | Return the long replicate table (``replicate, metric, [stratum], [period  /  | Retained dispatch; exact/default or explicit approximation | resampling; роли 03, 04, 10 |
| `ABTest.contrast_summary` (L1855) | One row per metric (× stratum) × contrast: the point estimate of each | Retained dispatch; exact/default or explicit approximation | resampling; роли 03, 04, 10 |
| `ABTest.paths` (L1981) | The per-path table behind the scorecard (see :func:`ab_paths`). | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `ABTest._run_bootstrap` (L2025) | Bootstrap in column chunks; replicates of a column do not depend on the chunk. | Retained dispatch; exact/default or explicit approximation | resampling; роли 03, 04, 10 |
| `ABTest._replicates` (L2055) | Draw (or reuse) the joint replicates; ``{cell: (group1, group2, valid)}``. | Retained dispatch; exact/default or explicit approximation | approx; роли 01, 03, 04 |
| `ABTest._contrast_columns` (L2072) | Pooled-side columns of every contrast: summed numerators and denominators. | Retained with explicit population/provenance guards | population; роли 02, 07, 09 |
| `ABTest._test_cell` (L2148) | Estimates, CI, SE, p-value and support flags for one cell. | Retained dispatch; exact/default or explicit approximation | approx; роли 01, 03, 04 |
| `ABTest._test_cell.ci_at` (L2308) | Internal adapter/helper; responsibility follows the containing public contract. | Retained dispatch; exact/default or explicit approximation | approx; роли 01, 03, 04 |
| `ABTest._rel_ci` (L2388) | Percentile interval of ``group1 / group2 - 1`` over the joint replicates. | Retained dispatch; exact/default or explicit approximation | approx; роли 01, 03, 04 |
| `ABTest._family_label` (L2398) | Internal adapter/helper; responsibility follows the containing public contract. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `ABTest._adjust` (L2409) | Apply the correction per family, add ``mde_alpha``, ``mde`` at ``mde_alpha`` | Retained dispatch; exact/default or explicit approximation | approx; роли 01, 03, 04 |
| `ABTest._strata_columns` (L2498) | Add shares by arm, chi-square balance p-value, Cochran Q with the support | Restricted: secondary tests suppressed by default | approx; роли 01, 03, 04 |
| `ABTest._window` (L2618) | The window a row reads: the whole period, a horizon, a period row. | Retained with explicit population/provenance guards | population; роли 02, 07, 09 |
| `ABTest._finalise` (L2637) | Internal adapter/helper; responsibility follows the containing public contract. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `ab_paths` (L2693) | One row per compared path with every metric's per-path value; the table | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `_finite` (L2734) | Internal adapter/helper; responsibility follows the containing public contract. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `_decimals` (L2741) | Decimals that show the largest magnitude with three significant digits. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `_signed` (L2749) | ``+1.23`` / ``-1.07`` / ``0.00`` (ASCII minus, so the text parses as numbers). | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `_p_text` (L2755) | Two significant digits, ``< 0.0001`` below. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `_round_down_125` (L2764) | The largest 1, 2 or 5 times a power of ten that is <= ``value``. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `_paths_needed` (L2773) | ``(target, multiplier)``: a round effect at or below half the ``mde`` and the factor | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `_duration_text` (L2786) | ``28 days``, ``1 day``, ``12 hours``; another length as pandas prints it. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `_calendar_text` (L2795) | ``calendar week 2026-03-09``, ``calendar month 2026-03``, ``calendar day ...``. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `row_reading` (L2808) | One sentence that reads a scorecard row from its own numbers. | Retained dispatch; exact/default or explicit approximation | approx; роли 01, 03, 04 |
| `_validity_flagged` (L3037) | Whether a row carries a flag that weakens its interval: few supporting paths | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `_cell_key` (L3047) | A ``(metric, stratum)`` groupby key with a missing stratum as None (NaN keys | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |
| `_validate_on_stream` (L3053) | Check event names, columns and segment levels against the full stream. | Retained shared scorecard computation/validation | metrics; роли 03, 07, 09 |

## src/retentioneering/tools/conversion.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `_check_ci_level` (L76) | ``None`` or a confidence level in [0.5, 1): a lower level would call an | Retained with exact/default and conditional-population boundaries | funnels; роли 06, coordinator |
| `_is_path_start` (L89) | An anchor that every path reaches: the path start, with no offset. | Retained with exact/default and conditional-population boundaries | funnels; роли 06, coordinator |
| `ConversionRate.fit` (L122) | Conversion from `start_anchor` to `end_anchor`, one row per pair. | Retained with exact/default and conditional-population boundaries | funnels; роли 06, coordinator |
| `ConversionRate._metadata` (L160) | Keep method and population claims separate from the row schema. | Retained with exact/default and conditional-population boundaries | funnels; роли 06, coordinator |
| `ConversionRate._fit_diff` (L173) | Path-level comparison of two groups: per-group rates and the delta. | Retained with exact/default and conditional-population boundaries | funnels; роли 06, coordinator |
| `ConversionRate._fit_single` (L234) | One row per pair over this eventstream's paths. | Retained with exact/default and conditional-population boundaries | funnels; роли 06, coordinator |
| `ConversionRate._converted` (L338) | Internal adapter/helper; responsibility follows the containing public contract. | Retained with exact/default and conditional-population boundaries | funnels; роли 06, coordinator |
| `ConversionRate._converted_paths` (L367) | Paths whose end anchor lands after the start one and inside `within`. | Retained with exact/default and conditional-population boundaries | funnels; роли 06, coordinator |

## src/retentioneering/tools/experiment.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `_clean` (L46) | Internal adapter/helper; responsibility follows the containing public contract. | Retained strict contract; inference explicitly requested | population; роли 02, 07, 09 |
| `_snapshot` (L60) | Detached JSON-normalized configuration; never alias caller-owned objects. | Retained strict contract; inference explicitly requested | population; роли 02, 07, 09 |
| `ExperimentResult.__post_init__` (L72) | Internal adapter/helper; responsibility follows the containing public contract. | Retained strict contract; inference explicitly requested | population; роли 02, 07, 09 |
| `ExperimentResult.to_dict` (L75) | JSON-safe aggregates; no row identifiers or bootstrap samples. | Retained strict contract; inference explicitly requested | population; роли 02, 07, 09 |
| `ExperimentResult.to_json` (L86) | Internal adapter/helper; responsibility follows the containing public contract. | Retained strict contract; inference explicitly requested | population; роли 02, 07, 09 |
| `_component` (L94) | Internal adapter/helper; responsibility follows the containing public contract. | Retained strict contract; inference explicitly requested | population; роли 02, 07, 09 |
| `_display` (L133) | Internal adapter/helper; responsibility follows the containing public contract. | Retained strict contract; inference explicitly requested | population; роли 02, 07, 09 |
| `_metric_contributions` (L186) | Internal adapter/helper; responsibility follows the containing public contract. | Retained strict contract; inference explicitly requested | population; роли 02, 07, 09 |
| `_metric_contributions.component` (L192) | Internal adapter/helper; responsibility follows the containing public contract. | Retained strict contract; inference explicitly requested | population; роли 02, 07, 09 |
| `experiment_data` (L292) | Fixed-calendar two-arm analysis with authoritative assignments. | Retained strict contract; inference explicitly requested | population; роли 02, 07, 09 |
| `session_funnel_data` (L469) | Descriptive path funnel; pooled cluster draws preserve group overlap. | Retained strict contract; inference explicitly requested | funnels; роли 06, coordinator |
| `_frame_fingerprint` (L599) | Identity only; raw values and IDs are not included in result metadata. | Retained strict contract; inference explicitly requested | population; роли 02, 07, 09 |

## src/retentioneering/tools/funnel.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `Funnel.fit` (L50) | Funnel counts and rates; see `Eventstream.funnel_data` for the contract. | Retained with exact/default and conditional-population boundaries | funnels; роли 06, coordinator |
| `Funnel._fit_single` (L142) | Internal adapter/helper; responsibility follows the containing public contract. | Retained with exact/default and conditional-population boundaries | funnels; роли 06, coordinator |
| `_counts` (L176) | Per-step successes and the two denominators (overall, previous step). | Retained with exact/default and conditional-population boundaries | funnels; роли 06, coordinator |
| `_put` (L184) | Internal adapter/helper; responsibility follows the containing public contract. | Retained with exact/default and conditional-population boundaries | funnels; роли 06, coordinator |
| `_add_single_ci` (L189) | Internal adapter/helper; responsibility follows the containing public contract. | Retained with exact/default and conditional-population boundaries | funnels; роли 06, coordinator |
| `_add_diff_ci` (L202) | Internal adapter/helper; responsibility follows the containing public contract. | Retained with exact/default and conditional-population boundaries | funnels; роли 06, coordinator |

## src/retentioneering/tools/sample_ratio.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `_p_text` (L64) | Internal adapter/helper; responsibility follows the containing public contract. | Retained: exact two-arm; guarded approximate multiarm SRM | multiplicity; роли 04, 03 |
| `_share_text` (L68) | Internal adapter/helper; responsibility follows the containing public contract. | Retained: exact two-arm; guarded approximate multiarm SRM | multiplicity; роли 04, 03 |
| `sample_ratio_reading` (L76) | One sentence that reads a ``get_sample_ratio`` result from its own numbers. | Retained: exact two-arm; guarded approximate multiarm SRM | multiplicity; роли 04, 03 |
| `_period_name` (L157) | ``"day"``, ``"week"``, ``"month"``; the value itself for another spelling. | Retained: exact two-arm; guarded approximate multiarm SRM | multiplicity; роли 04, 03 |
| `_period_freq` (L165) | The pandas period frequency of one calendar unit (``"D"``, ``"W-SUN"``, ``"M"``). | Retained: exact two-arm; guarded approximate multiarm SRM | multiplicity; роли 04, 03 |
| `_counts_table` (L178) | Internal adapter/helper; responsibility follows the containing public contract. | Retained: exact two-arm; guarded approximate multiarm SRM | multiplicity; роли 04, 03 |
| `_allocation_test` (L194) | Inference under caller-assumed independent allocation probabilities. | Retained: exact two-arm; guarded approximate multiarm SRM | multiplicity; роли 04, 03 |
| `SampleRatio.fit` (L236) | Compare arm sizes against expected shares. | Retained: exact two-arm; guarded approximate multiarm SRM | multiplicity; роли 04, 03 |
| `SampleRatio._levels_and_shares` (L423) | Internal adapter/helper; responsibility follows the containing public contract. | Retained: exact two-arm; guarded approximate multiarm SRM | multiplicity; роли 04, 03 |
| `SampleRatio._periods` (L462) | Internal adapter/helper; responsibility follows the containing public contract. | Retained: exact two-arm; guarded approximate multiarm SRM | multiplicity; роли 04, 03 |
| `SampleRatio._strata` (L479) | Internal adapter/helper; responsibility follows the containing public contract. | Retained: exact two-arm; guarded approximate multiarm SRM | multiplicity; роли 04, 03 |
| `SampleRatio._split_tables` (L502) | Internal adapter/helper; responsibility follows the containing public contract. | Retained: exact two-arm; guarded approximate multiarm SRM | multiplicity; роли 04, 03 |

## src/retentioneering/tools/segment_overview.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `SegmentOverview.fit` (L69) | Build segment overview DataFrame. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |
| `SegmentOverview.get_metric_distribution` (L221) | Calculate distribution statistics for a metric across one or two segment levels. | Retained descriptive metric construction | metrics; роли 03, 07, 09 |

## src/retentioneering/widgets/_base.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `RetentioneeringWidget._encode_result` (L267) | Encode a computed result for a JSON traitlet (``result``, ``dist_result``). | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |

## src/retentioneering/widgets/_utils.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `custom_columns_catalogue` (L101) | JSON `{custom column: is numeric}` for the column_* metrics' dropdown. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |

## src/retentioneering/widgets/ab_test.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `_duration_str` (L61) | A `period` / `horizon` argument as the string the traitlet holds. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_metric_kind` (L75) | `binary`, `count`, `value` or `ratio`, as `get_ab_test` reads the entry. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_cell` (L87) | One scorecard cell as strict JSON: null for NaN / NA, ISO for times. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `scorecard_records` (L103) | The scorecard frame as one dict per row, in its column order. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `ABTestWidget.__init__` (L179) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `ABTestWidget._on_params_change` (L283) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `ABTestWidget._params` (L290) | The data parameters as `ab_test_data` keyword arguments. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `ABTestWidget._tool_ab_test_data` (L324) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `ABTestWidget._recompute` (L332) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `ABTestWidget._compute_raw` (L350) | The three scorecards of the widget, each straight from `ab_test_data`. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `ABTestWidget._export_data` (L441) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_assignment_arms` (L464) | The arms of an assignment table (a Series of arms or a DataFrame with an | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `_assignment_info` (L476) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |

## src/retentioneering/widgets/cluster_analysis.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `ClusterAnalysisWidget.__init__` (L110) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `ClusterAnalysisWidget._recompute` (L346) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `ClusterAnalysisWidget._compute_distribution` (L496) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `ClusterAnalysisWidget._export_data` (L581) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |

## src/retentioneering/widgets/experiment.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `ExperimentWidget.__init__` (L25) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `ExperimentWidget._export_data` (L70) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |

## src/retentioneering/widgets/funnel.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `FunnelWidget.__init__` (L45) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `FunnelWidget._tool_funnel_data` (L128) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `FunnelWidget._recompute` (L145) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `FunnelWidget._compute_raw` (L176) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `FunnelWidget._export_data` (L206) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |

## src/retentioneering/widgets/segment_overview.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `SegmentOverviewWidget.__init__` (L43) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `SegmentOverviewWidget._recompute` (L154) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `SegmentOverviewWidget._export_data` (L190) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |
| `SegmentOverviewWidget._compute_distribution` (L208) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |

## src/retentioneering/widgets/step_matrix.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `StepMatrixWidget._recompute` (L198) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |

## src/retentioneering/widgets/step_sankey.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `StepSankeyWidget._recompute` (L189) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |

## src/retentioneering/widgets/transition_graph.py

| Функция | Назначение | Решение | Свидетельства |
|---|---|---|---|
| `TransitionGraphWidget._recompute` (L288) | Internal adapter/helper; responsibility follows the containing public contract. | Retained adapter; method/status semantics preserved | transport; роли 08, 10 |

## Удалено или не включено

- `_cuts_at_an_event`: удалена эвристика, автоматически объявлявшая поведенческий сегмент предэкспозиционным. Явная декларация остается ответственностью автора анализа.
- PR #221: `get_ab_transitions`, `get_ab_steps`, их сравнительное ядро и inferential graph overlay отложены. Сохранены отдельные oracle-результаты описательных оценок; они не оправдывают выявленные ложные статистические обнаружения.
- PR #222: новые матрицы и табличные оболочки зависят от #221. До исправления и отдельной проверки ядра в этот релиз не включаются. Проверка актуальных ревизий описана в итоговом отчете.
- Универсальная калибровка Welch/percentile bootstrap на тяжелых хвостах, последовательный анализ, произвольные кластерные/парные/crossover-дизайны и автоматическое доказательство причинности не заявляются.
