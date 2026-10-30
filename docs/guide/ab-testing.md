# A/B testing

Compare an experiment on its declared independent assignment units, with a complete
assignment roster and an outcome window chosen before inspecting the result.
`ab_test_data` / `ab_test` provides a comparative scorecard; `get_ab_test` is a
compatible name. [`experiment_data`](/docs/experiments) provides a stricter
fixed-calendar roster report with explicitly requested inference. Their population,
window and default-inference contracts differ; changing adapters requires an
estimand decision. [ADR-0018](https://github.com/retentioneering/retentioneering-tools/blob/master/docs/adr/0018-ab-integration.md)
records the integration and conservative inference decisions.

## Supported inference

| Outcome and method | Result and boundary |
|---|---|
| Known binary outcome, `test="auto"` | Two-sided Fisher exact test; Clopper–Pearson arm intervals and a conservative difference formed by splitting alpha between the arms. Assumes independent units. |
| Nonbinary mean, count or ratio, `test="auto"` | Descriptive point estimates; no p-value, interval or MDE. |
| Binary `test="z_test"` | Explicit approximate pooled z-test and Wilson/Newcombe intervals; every arm must have at least five pooled-null expected successes and failures. This guard is not an exact-coverage guarantee. |
| Nonbinary `test="welch_t_test"` | Explicit approximate Welch inference for means; unsuitable for arbitrary sparse or heavy-tailed outcomes. |
| Nonbinary `test="bootstrap"` | Explicit approximate whole-unit percentile intervals and a bootstrap-SE asymptotic test; ratios preserve joint numerator/denominator contributions. |
| `test="descriptive"` | Point estimates without inferential claims. |

Known-binary Welch and scorecard bootstrap are rejected. Raw `get_ab_bootstrap`
replicates remain an advanced diagnostic, including for binary metrics; they do
not acquire the Fisher method's guarantee. More bootstrap draws reduce simulation
noise, not bias from an unobserved heavy tail or an incorrect independence unit.

The conservative binary difference interval is **not an inversion of Fisher's
test**. Its exclusion of zero and the adjusted test verdict need not agree.
Relative-effect point estimates remain available when defined, but relative
intervals are withheld on the exact Fisher path. Explicit Newcombe intervals in
the strict adapter remain approximate; `binary_intervals=None` requests none.

Default Fisher/descriptive rows do not turn asymptotic heterogeneity or
stratum-versus-rest diagnostics into exact tests. Advanced secondary diagnostics
require an explicit supported approximate method and retain distinct method
metadata and support guards. They are not a correction for discovering a segment
from the same outcomes.

## Unit, roster and outcome

A path is one independent observation. Inferential scorecards, SRM, funnels and
conversion require the coarsest declared path grain. If users were randomized,
keep users as that grain even when descriptive paths display sessions. A schema
cannot prove independence or repair repeated-session, household, reseller,
cluster-randomized, crossover or paired assignment.

The assignment table defines intention-to-treat (ITT): everyone assigned, including
users with no tracked events. It is indexed by path ID, with `arm` and optionally
`assigned_at`. Supported binary outcomes and totals are zero for silent users;
value metrics such as `column_mean` remain undefined where no measurement exists.
Missing telemetry is not a known zero. Events before assignment are excluded.
Without a table, the population consists only of event-observed paths and is
reported as `population="stream"`; the treatment may itself select those people.

```python
from retentioneering import datasets

stream = datasets.load_ab_test()
assignment = datasets.load_ab_test_assignment()
diff = ("exp_group", "treatment", "control")
metrics = [
    {"name": "conversion", "metric": "has_event",
     "metric_args": {"event": "order_success"}},
    {"name": "orders_per_user", "metric": "event_count",
     "metric_args": {"event": "order_success"}},
    {"name": "revenue_per_user", "metric": "event_value_sum",
     "metric_args": {"event": "order_success", "value_col": "order_value"}},
]
score = stream.ab_test_data(diff, metrics, assignment=assignment,
                            observation_end="2026-04-27")
score[["metric", "group1_value", "group2_value", "delta", "test", "ci_basis", "verdict"]]
```

The binary row uses exact inference; the two nonbinary rows are descriptive. For a
predeclared advanced sensitivity analysis, put `"test": "welch_t_test"` on a
nonbinary mean, or `"test": "bootstrap"` on an explicit ratio. State the
approximation and inspect tail/support sensitivity before interpreting it.

`delta` is group1 minus group2. A ratio with `denominator` is a ratio of totals,
not a mean of individual ratios. Use `get_ab_paths` to inspect every assignment's
binary/count/sum contribution or ratio numerator and denominator. Its arm totals
reproduce the point scorecard. `event_value_sum` rejects a missing amount on a
matching event; `column_sum` has broader missing-value semantics.

A complete named roster may legitimately be entirely silent. An unnamed table
with no matching IDs is ambiguous and rejects. Low matching fractions warn; check
ID spelling before interpreting silence. IDs must retain identity before entering
the library: use homogeneous scalar families, with integers fitting signed64 or
uint64 per column. See [Eventstream](/docs/eventstream) for construction and recipe
restrictions.

## Periods, horizon and novelty

`observation_end` is an exclusive cutoff asserting complete logging. Never infer
it from a user's last event. It is required for a fixed follow-up `horizon` and
`period_basis="since_assignment"`. A unit enters the analysis when its entire
requested window has closed; `immature_paths` reports exclusions.

```python
fixed = stream.get_ab_test(diff, metrics, assignment=assignment,
                          horizon="28D", observation_end="2026-04-27")
calendar = stream.get_ab_test(diff, metrics[:1], assignment=assignment,
                             period="W", observation_end="2026-04-27")
age = stream.get_ab_test(diff, metrics[:1], assignment=assignment,
                        period="14D", period_basis="since_assignment",
                        observation_end="2026-04-27")
```

Calendar ITT bins include assignments before each bin's end. Exposure-age bins
compare closed follow-up windows. `period_population="active"` describes survivors
and carries no inferential verdict. Report the window and population with every
number; changing either changes the question.

`get_ab_bootstrap(contrast={"early_minus_late": (["0-13"], ["28-41"])}, ...)`
uses common assigned paths and joint weights. Contrasts also require common
measurement eligibility across the compared periods; a shared roster cannot
repair a missing value in just one period. Such changing eligibility rejects.
Early-minus-late effects are approximate exploratory summaries, not evidence that
novelty is the causal explanation.

Bootstrap requests are bounded at 1,000,000 replicates and 10,000,000
replicate-by-contribution entries. Numeric overflow raises with a rescaling
message. For a Bonferroni bootstrap interval with family size `m`, fewer than ten
expected draws per adjusted tail (`B * (alpha / m) / 2 < 10`) yields
`delta_ci_adjusted_status="insufficient_tail_resamples"`; no normal-SE interval is
substituted. Availability does not certify nominal coverage.

## Segments and exposure provenance

Labels used for inference must be fixed before treatment could affect them.
`strata_pre_exposure=True` or a local `add_segment(..., pre_exposure=True)` is an
explicit analyst declaration. A truncation, recipe, transferred Series attribute
or legacy inferred flag does not establish exposure timing. Reassert the
scientific declaration when transferring labels; otherwise the rows remain
exploratory/descriptive with no p-value or MDE.

```python
before = stream.truncate_paths(
    "path_start", [{"pattern": "product_view", "offset": -1}, "path_end"])
# Compute labels using only this window, after establishing from product design
# that product_view is the first possible exposure. A cut alone cannot prove it.
labels = before.add_segment("entry_size", metric_bins={
    "metric": {"metric": "length"}, "edges": [3], "segment_levels": ["short", "long"]
}).get_path_segments("entry_size")
with_labels = stream.add_segment("entry_size", path_values=labels, pre_exposure=True)
strata = with_labels.get_ab_test(diff, metrics[:1], assignment=assignment,
                               strata_col="entry_size")
```

A true pre-treatment label can still have been selected after scanning outcomes.
Discovery, clustering choices and post-selection inference are not repaired by a
manual flag. Treat discovered effects as hypotheses for a new predeclared test.
The public tutorial uses an explicitly approximate guarded z diagnostic where it
teaches heterogeneity; that diagnostic does not replace the exact primary read.

## Sample ratio mismatch

Check the assignment roster, not an outcome-selected active population. Two-arm
SRM uses `p_value_method="exact_binomial_two_sided"` and assumes independent
Bernoulli allocation at the declared treatment probability. Multiple-arm SRM uses
`pearson_chi_square_asymptotic` under independent multinomial allocation, only when
every planned expected count is at least five. `chi2`/`dof` remain descriptive
Pearson discrepancies; they do not determine the two-arm exact p-value.

```python
srm = stream.get_sample_ratio(assignment=assignment)
print(srm["inference_status"], srm["p_value_method"], srm["mismatch"])
# Inspect overall and split checks separately; do not use `not mismatch` as a pass.
```

Explicit `expected_shares` declares planned arms, including absent arms with zero
observations. `inference_status` is `ok`, `no_data`, or
`insufficient_expected_counts`; `support_warning` and `expected_count_min` explain
availability. `mismatch` is `True`, `False`, or `None`: unavailable inference is
`None`, unless conflicting assignments independently alarm. Require status `ok`
before interpreting a p-value. Fixed-total and blocked designs need a compatible
allocation diagnostic.

`strata_col` or `period` repeats checks within groups. Split Holm families retain
unavailable planned rows. Overall and split alarms, repeated calls and separately
chosen families are not jointly corrected. A good SRM result does not establish
complete logging, independent units or valid causal interpretation.

## Funnels with intervals

An ordinary [funnel](/docs/widgets/funnel) describes event-observed paths. Its
overall binary comparisons use Fisher p-values, Clopper–Pearson arm intervals and
conservative CP-based difference intervals. The intervals are not Fisher-test
inversions. They do not include silent assignments unless those assignments enter
the underlying population; use the roster scorecard for an ITT outcome.

Step-to-step rates condition on reaching the previous step. Treatment can change
who reaches that step, so their intervals are descriptive and have no p-value or
causal verdict. A positive conditional point estimate can have an interval
containing zero. Overall and conditional deltas answer different questions.

## Multiplicity, intervals and decisions

The default Holm family includes every declared metric/stratum/period row in a
call, including rows with unavailable p-values. `correction_scope="metric"`
explicitly separates families and requires a decision plan that justifies that
separation. Separate calls and repeated looks are not corrected automatically.
Bonferroni adds adjusted intervals when available. Benjamini–Hochberg controls FDR
under independence or suitable positive dependence (PRDS); arbitrary dependence
is not covered. No correction repairs invalid input p-values or post-selection.

`mde_basis="normal_approximation"` labels an available planning approximation,
using the observed design and `power` in `[0.5, 1)`. It is not actual Fisher-test
power, an observed-power estimate, or a bound on the true effect. Validate a new
experiment's design with the chosen test and relevant data-generating scenarios.

An inconclusive test does not prove no effect. Report the estimates, method,
interval availability and design assumptions. Confidence intervals are random
procedures with stated repeated-sampling properties, not posterior probability
bounds for this observed parameter. A non-significant harm test does not certify
safety; guardrails require predeclared margins and an appropriate decision rule.
Sequential/always-valid inference, equivalence verdicts, arbitrary clustering and
post-selection guarantees remain outside this surface.

## From scorecard to decision

Declare the independent unit, complete population, exposure mechanism, primary
metric, horizon, read date, correction family and practical threshold before
reading outcomes. Check allocation and logging, then inspect support and method
availability. Use exploratory paths and segments to plan follow-up experiments;
do not replace an inconclusive primary decision by the most favorable secondary
window or discovered subgroup.

## Teaching datasets and tutorials

`load_ab_test()` loads Northwind Home. `list_ab_tests()` names **five more** cases,
six total: `srm_platform_bug`, `session_assignment`, `aa_calibration`,
`trial_conversion`, and `push_campaign`. They teach invalid logging, wrong units,
heavy tails, immature horizons and outcome-selected populations. They are planted,
selected examples, not a calibration certificate. One A/A run cannot estimate
false-positive rates. The frozen Lumeo sample likewise has a selected synthetic
parent and sparse outcomes; its data card preserves that limitation.

- [A/B narrative tutorial](https://github.com/retentioneering/retentioneering-tools/blob/master/notebooks/ab_testing_tutorial.ipynb)
- [A/B API showcase](https://github.com/retentioneering/retentioneering-tools/blob/master/notebooks/ab_testing_showcase.ipynb)
- [Strict experiment tutorial](https://github.com/retentioneering/retentioneering-tools/blob/master/notebooks/experiment_tutorial.ipynb)
- [Lumeo tutorial](https://github.com/retentioneering/retentioneering-tools/blob/master/notebooks/lumeo_experiment_tutorial.ipynb)
- [Scorecard](/docs/tools/get-ab-test), [per-path table](/docs/tools/get-ab-paths), [joint bootstrap](/docs/tools/get-ab-bootstrap), [SRM](/docs/tools/get-sample-ratio)
