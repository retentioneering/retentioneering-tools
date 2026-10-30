# Experiment outcomes from user paths

Experiment analysis combines observed paths with the complete assignment
population, including users who produced no events during the outcome window.

Start with a product question, an independent assignment unit, a declared outcome
and an observation contract. Graphs and step matrices can suggest hypotheses;
an outcome chosen after looking at those graphs remains exploratory.

```python
metrics = [
    {"name": "converted", "metric": "has_event",
     "metric_args": {"event": "purchase"}, "agg": "mean"},
    {"name": "checkout_sequence", "metric": "matches_pattern",
     "metric_args": {"pattern": "cart->.*->purchase"}, "agg": "mean"},
    {"name": "revenue", "metric": "event_value_sum",
     "metric_args": {"event": "purchase", "value_col": "amount"}, "agg": "mean",
     "display": {"label": "Revenue per assigned user", "unit": "GBP / user"}},
]
result = stream.experiment_data(
    assignments=assignments, unit_col="user_id", variant_col="variant",
    control="control", treatment="treatment", metrics=metrics,
    observation={"start": "2026-05-11", "end": "2026-05-25",
                 "assignment_time_col": "assigned_at", "complete": True},
    bootstrap={"n_resamples": 9999, "random_state": 42},
    binary_intervals={"method": "exact", "confidence_level": .95},
    tests={"methods": {"converted": "fisher_exact", "revenue": "welch"},
           "adjustment": "holm"},
)
```

## Population and observation

Assignments must have one non-null unit, variant and parsed assignment timestamp
per row. Both declared arms must be nonempty; conflicting labels and duplicate
assignments are errors. All assignments must precede or equal the common start.
The outcome window includes start and excludes end. Source history may precede
assignment, but does not contribute to outcomes outside this window.

`complete=True` is an explicit assertion that event observation covers the full
window. It is not inferred from a user's last event. Lost logging cannot be
replaced with a structural zero or repaired by dropping affected assigned users.
This version supports independent two-arm units and a fixed calendar window;
individual horizons, censoring, paired or cluster-randomized designs require
their own inference contracts.

## Outcomes and denominators

Use the shared scalar metric configurations for event presence/counts, finite
path patterns and numeric event sums. A missing amount on an existing matching
event is an error. A supported event outcome with no matching observations is
zero. `event_catalog={"event": ["purchase", ...]}` declares valid event names
that may be absent from the observed source; undeclared typos still fail.

`mean` gives each assignment equal weight. `ratio_of_sums` uses total numerator
divided by total denominator, with both contributions kept together during
resampling. `mean_of_ratios` requires explicit denominator-positive eligibility;
it describes eligible individual rates, a different and potentially
treatment-dependent population.

`funnel_steps` adds ordered within-window cumulative and conditional reach. The
same event-position semantics power the existing Funnel, including repeated step
names and synthetic marker order. At user grain a sequence may span sessions;
at session grain it cannot. A conditional-step contrast does not identify a
causal mechanism by itself.

## Uncertainty and tests

Percentile bootstrap resamples whole independent units separately within arms,
with a shared weight across a person's numerator, denominator and outcomes.
Each replica recomputes the statistic and treatment-minus-control effect. The
effect interval is not a subtraction of marginal interval endpoints.

Sparse denominators, nonfinite estimates, insufficient units and degenerate
components retain explicit unavailable statuses. A constant observed control
does not establish that its population probability is known exactly. Explicit `binary_intervals={"method": "exact"}` selects Clopper–Pearson
arm intervals and a conservative difference formed by splitting alpha between
the arms. This difference is not an inversion of the Fisher test. Relative
intervals are withheld on this exact path. `method="newcombe"` remains an
explicit approximate Wilson/Newcombe option; method and level remain visible.
`binary_intervals=None` requests no binary interval; with `bootstrap=None` and
`tests=None`, the strict adapter adds no inference. Relative effects require a
positive control baseline. Intervals are pointwise, not simultaneous bands.

Fisher's two-sided exact test is available for declared binary outcomes.
Known-binary Welch is rejected; Welch is an advanced approximate option for
nonbinary means. Welch's
two-sided test concerns unit-level means, with an approximation that needs care
for sparse or heavy-tailed outcomes. Tests are requested by name before reading
the results, and Holm uses the declared family. Bootstrap tails are not p-values. Requests are capped at 1,000,000 replicates
and 10,000,000 replicate-by-contribution entries. Numeric overflow raises with
a rescaling message. More resamples cannot establish missing tail information.
No automatic winner, observed power or unrequested test selection is reported.

### Nominal confidence is not a coverage guarantee

An `ok` interval status means that the computation is defined, not that its
finite-sample coverage has been certified. Percentile bootstrap can substantially
under-cover with heavy-tailed monetary outcomes, especially with unequal arm
sizes and many zero outcomes. More bootstrap resamples reduce simulation error
in the quantiles; they do not recover tail information absent from the sample.
Do not interpret exclusion of zero by a bootstrap interval as the separately
declared Fisher/Welch test result.

Explicit approximate Wilson/Newcombe intervals have discrete, parameter-dependent coverage;
they are not exact 95% guarantees at every probability and sample size. For
rare conversions, percentile intervals may be unavailable and relative effects
may be undefined. Report availability alongside conditional coverage in method
validation, rather than dropping unsuccessful calculations.

The reproducible independent audit, known-parameter generators, Monte Carlo
uncertainty and exact binary enumeration are in
[`examples/experiments/validation`](https://github.com/retentioneering/retentioneering-tools/tree/codex/pr214-statistical-validation/examples/experiments/validation).
It distinguishes numerical implementation errors from method limitations and
does not certify sequential, clustered or post-selection inference.

`expected_allocation` is a treatment probability for an independent-Bernoulli
allocation check on the assignment roster. It does not test logging coverage or
certify randomization; blocked/fixed-total designs need their own diagnostic.

## Results and display

`experiment_data` returns a versioned JSON-safe aggregate payload: summary rows,
population diagnostics and a normalized specification with resolved seed.
Rows include measurement labels/units, support counts, interval methods and
unavailable states. Assignments and raw bootstrap draws are not sent to the UI.
`experiment` presents the same computation in a native notebook widget.

`display` accepts `label`, `unit`, `effect_unit`, `scale`, `role` and `direction`.
Values and absolute effects use the same display scale, while their unit labels
can differ. For a ratio shown with `unit='%'` and `scale=100`, a change from .20
to .30 displays as 20% to 30%, an absolute difference of +10 pp and a relative
effect of +50%. `effect_unit` overrides the resolved absolute-difference label.
Display units do not make a ratio binary or enable binary-specific inference.
The headless payload retains canonical numeric values and the resolved labels.

Display changes do not resample. The initial view is a fixed result snapshot;
it does not persist/rebind a second input table through `state_file` or expose a
new MCP assignment-data binding. Re-run the explicit analysis with the same
inputs and resolved seed to reproduce the calculation.


## Relationship to the comparative scorecard

`experiment_data` is a strict fixed-calendar adapter: the roster and
`observation={start, end, complete=True, ...}` are mandatory, assignments precede
the window, and interval/test methods are explicitly requested. Its aggregate
JSON snapshot is suitable for a frozen report.

For staggered assignment, fixed follow-up horizons, pre-exposure strata, calendar
or exposure-age periods, and MCP, use [`ab_test_data` and `ab_test`](/docs/ab-testing).
`get_ab_test` is a compatibility name for the same scorecard engine. These
adapters share numerical primitives and whole-unit bootstrap weights, but their
population/window/default-inference contracts differ. Converting an invocation
requires selecting the intended estimand; it is not a method-name substitution.
See the [supported-inference boundary](/docs/ab-testing#supported-inference) and [ADR-0018](https://github.com/retentioneering/retentioneering-tools/blob/master/docs/adr/0018-ab-integration.md)
for the migration and architecture decisions.

An explicit `unit_col` names the coarsest declared independent assignment/resampling
unit. For session randomization, declare sessions as the primary schema grain. The
library validates IDs and memberships but cannot infer independence or identify
cluster randomization from event activity. For user-randomized experiments, keep
users as the unit even when a diagnostic funnel displays sessions.
