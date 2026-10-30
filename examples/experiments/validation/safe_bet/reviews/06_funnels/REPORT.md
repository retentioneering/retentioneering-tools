# Role 06: funnel, conversion and pattern review

**Initial bounded report completed 2026-10-30. One confirmed implementation defect; remaining tested extraction arithmetic passes with explicit interpretation restrictions.** This is an independent staged oracle-first review. No candidate source was edited; no peer report or CHECKPOINT was read; no subagent was used.

## Decision

Retain assignment-roster cumulative funnel reach and binary pattern outcomes for the tested fixed-window, independent-unit contract. Retain observed-path funnels as descriptive path summaries. Restrict start-selected and prior-step-selected contrasts to descriptive comparisons. **Defer or repair `get_conversion_rate` with all start occurrences and a finite window:** it undercounts a later successful window with the default first end occurrence.

Correct fractions and method labels do not prove randomization, independent units, complete observation, valid pre-treatment eligibility, or a causal estimand. A conditional rate can move in the opposite direction from the assignment-level outcome.

## Evidence and independence

- Before inspecting source, wrote `PRE-REGISTRATION.md` and a Python-stdlib oracle for ordered-subsequence witnesses, greedy prefix reach, binary patterns, user/session aggregation, eligibility and cutoff handling. Oracle SHA256: `623860aceda57c5d4eece19aca1dade637cf4d97cd96cccdbe1ff045e8b17cac`. Preregistration SHA256: `3961e2e06f739a15a37931a3a4e6f5f97bfcdf46f8edace9e3c880e8f93ffdba`.
- The oracle is unchanged. Public semantics learned after source inspection are recorded in `AMENDMENTS.md`: overall observed-path denominators, first-stage denominator convention, half-open experiment window, and pattern syntax. Conversion pair enumeration was added to the adapter after unblinding and is identified as supplemental evidence.
- Baseline220 commit: `38cbe91d5b315584575b132a596890e69eb210aa`. PR217 commit: `7e61d997bcbd9bae9f1571ba3521a85fcb59f751`.
- Runtime: Python 3.13.17, pandas 3.1.0, numpy 2.5.4, scipy 1.18.1, DuckDB 2.0.0. Imports were checked to point to the selected source checkout. Actual function-file hashes are in `source_hashes.json` and the raw results.
- Two fixed toy cases and 100 random logs per snapshot, seed `6026030`, 12 assigned users per random log, 0–11 events per user, A/B/C/X labels, repeated funnel steps, loops, 20 tied-timestamp logs, globally unique nested session IDs. Additional public examples cover method labels, blocked conditional tests, finer-grain inference guards, cutoff boundaries, silent assignees, cohort restrictions, reused session IDs, and a descriptive clustered session funnel.
- Counts below are **comparison assertions**, not independent experiments. Randomized property checks are not statistical-calibration simulations; no false-positive/coverage claim or population defect-frequency claim is made.

| Snapshot | Assertions | Failed assertions | Unique randomized logs exposing defect | Experiment API |
|---|---:|---:|---:|---|
| baseline220 | 9,854 | 28 | 9 / 100 | Present |
| PR217 | 5,479 | 28 | 9 / 100 | Absent |

The 28 failures in each snapshot are converted-count and rate comparisons for the same conversion defect: nine event-window cases and five time-window cases. All 4,068 assignment-experiment numerator/denominator/rate assertions and 306 observed-user pattern assertions pass in baseline220. PR217 also passes its 306 pattern assertions. Remaining tests are reach/rates, labels and contract guards. Full inputs, actual outputs, expectations and failures are preserved in `baseline220.json` and `pr217.json`.

## F06-01 — all-start finite windows can miss a valid later conversion

**Severity: P2. Classification: implementation defect. Scope: both frozen snapshots.**

Public reproduction, runnable with either source checkout:

```python
es = Eventstream(pd.DataFrame({
    'user_id': ['u'] * 5,
    'event': ['A', 'X', 'B', 'A', 'B'],
    'timestamp': pd.date_range('2026-01-01', periods=5, freq='s'),
}))
es.get_conversion_rate(
    {'pattern': 'A', 'occurrence': 'all'}, 'B', within=1,
)
```

The first A at position 1 has no B within one event. The second A at position 4 is followed immediately by B at position 5. The documented all-start rule says the path converts if **any** start is followed by the end within the window.

| Quantity | Expected | Actual |
|---|---:|---:|
| paths_with_start | 1 | 1 |
| converted | 1 | 0 |
| conversion_rate | 1.0 | 0.0 |

The same error occurs for `within='1s'`. In the minimal repro, start `first` correctly yields zero; start `last` yields one; an unbounded window yields one. Explicit end `{'pattern':'B','occurrence':'all'}` yields one. These are localized probes, not a general workaround guarantee for arbitrary pattern anchors.

**Cause and source:** baseline220 `src/retentioneering/tools/conversion.py:336` resolves end positions for the full collection of start bounds before applying the window at lines 382–384. `src/retentioneering/paths/anchors.py:783` selects occurrence and lines 795–798 aggregate first/last positions only by path. The default first end after any start is the early B; the later B needed by the later start is discarded before finite-window pairing. The corresponding `_converted` logic is also present in PR217. This is an existing defect rather than a regression introduced by baseline220's shared funnel reach implementation.

**Remedy:** preserve the start-window identity while resolving the requested end occurrence, then collapse successful windows to distinct paths; alternatively explicitly reject this combination until supported. Do not silently change the user's explicit end-occurrence semantics. Add the five-event fixture for both event and duration windows, plus randomized pair-oracle regression with first/all/last start policies. After a fix, rerun `repro_all_start.py` and `review.py` against the changed source. No fix was attempted by this reviewer.

Executable evidence: `repro_all_start.py`, `repro_baseline220.json`, `repro_pr217.json`. Raw random failures: logs 9, 29, 32, 40, 49, 60, 65, 85, 91.

## Interpretation restrictions confirmed with handwritten aggregates

**F06-R1 — observed users and all assignees answer different questions.** A fixture has 10 assigned users per arm. Control: two enter A and both reach B; eight are silent. Treatment: all ten enter A and four reach B. Assignment cumulative B rates are 2/10 and 4/10, giving treatment-minus-control **+0.20**. The A-selected B rates are 2/2 and 4/10, giving **−0.60**. Both fractions are correct. The latter cannot be described as the assignment-based effect on conversion. The observed-path funnel also has only two control paths, so its B contrast is −0.60. Evidence: `supplemental_baseline220.json`.

`funnel_data` explicitly documents its observed-path population and `get_conversion_rate` its start-selected population. Both snapshots reject silent IDs placed in an observed-path diff list; the adapter initially triggered and then recorded this guard. `experiment_data` retains silent assignees as zero outcomes and exports assignment-roster metadata. This distinction must remain visible in any retained interface.

**F06-R2 — conditional experiment rows remain descriptive.** `experiment_data(..., funnel_steps=...)` gives cumulative rows all-assignment denominators and conditional rows prior-reach denominators. It rejects explicit conditional-funnel tests, and binary Wilson/Newcombe intervals apply only to cumulative binary outcomes. The `family='funnel_conditional'` label must be carried into display/export interpretation; the generic `absolute_effect` field alone does not confer a causal interpretation. The first conditional row is an unconditional copy of first-stage reach, but is conservatively typed as a ratio; do not interpret its missing binary interval as a numerical failure.

**F06-R3 — unit choice changes the outcome.** In the fixed ABC toy, user reach is `[7,7,3]` over 8 observed users, while session reach is `[8,6,2]` over 10 observed sessions. User reach legitimately spans sessions; session reach does not. Session IDs must be globally unique within their declared nesting (reused IDs across users are rejected). Finer nested session inference is rejected by public funnel/conversion CI guards and assignment experiments. `session_funnel_data` is descriptive, uses observed session denominators, identifies `pooled_union_clusters`, and rejects independent binary intervals. The sampled clustered interval's calibration was not evaluated here.

**F06-R4 — time, eligibility and ordering remain design inputs.** `experiment_data` applies `[start,end)`: a step exactly at end is excluded, one exactly at start is included, and a pre-window step cannot open a funnel inside the window. The toy and random oracle comparisons verify these rules. Eligible cohorts require explicit roster and stream restriction; an event user omitted from the roster is rejected. The API cannot establish that selection was pre-treatment. It supports a common fixed window, not arbitrary per-user follow-up or a first-session policy. Ties are ordered by event/index order: tied input `[A,B]` reaches B, while `[B,A]` does not. Both outputs follow the supplied sequence but timestamps alone cannot establish a real temporal ordering between tied events.

**F06-R5 — zero previous-step denominator is not an observed zero conversion probability.** Legacy `funnel_data` deliberately returns 0.0 for a zero previous-step denominator, with unavailable CI when requested. The experiment API instead returns null and `undefined_denominator`. Preserve this distinction when displaying outputs; it is a documented compatibility rule, not a failing oracle result.

## Function-level trust / restrict / defect matrix

“Trust” below means the listed numerical contract passed the bounded checks, not unconditional causal safety.

| Public function / path | Decision | Evidence | Required restriction / source |
|---|---|---|---|
| `funnel_data`: ordered unique reach | Trust arithmetic; retain | Loops, repeated labels, missing steps, ties, user/session counts match unchanged oracle in both snapshots | Observed paths only; sequence uses distinct event positions. `tools/funnel.py:138`, `paths/ordered_reach.py` |
| `funnel_data`: cumulative and conditional rates | Trust arithmetic; restrict interpretation | Correct all-observed and prior-reach denominators; fixed user/session aggregate agreement | Silent assignees excluded; conditional selection descriptive; legacy zero-denominator rate is 0. `tools/funnel.py:149` |
| `funnel_data`: CI/diff mode | Restrict | Wilson/Newcombe/pooled-z labels, conditional flags, absence of conditional p-values, nested-grain rejection pass | Independent observed paths assumed; pointwise CI; no calibration audit here. `tools/funnel.py:181`, `tools/funnel.py:195` |
| `get_conversion_rate`: first/last starts; unbounded all starts | Trust tested literal arithmetic; restrict | First/all/last literal anchors, repeats, denominators, base rates, no-window cases | Start-selected population; advanced pattern/offset policy not exhaustively covered. `tools/conversion.py:217` |
| `get_conversion_rate`: all starts + finite window + default end | **Defect — defer/repair** | Minimal 5-event repro and 9 random logs fail in both snapshots | F06-01. `tools/conversion.py:336`, `paths/anchors.py:783` |
| `get_conversion_rate`: diff inference | Restrict | Nontrivial start sets conditional=True and suppresses p-value; nested CI rejected | Suppression does not make conditional differences causal; population still observed. `tools/conversion.py:205` |
| `get_metrics(matches_pattern)` | Trust tested binary extraction | 306 per-user mappings per snapshot match contiguous/subsequence/repeated-token oracle | Ordered gaps differ from contiguous syntax; repeated witnesses do not multiply users. General pattern grammar not exhausted |
| `experiment_data`: cumulative funnel / pattern outcomes | Trust tested extraction; retain with contract | 4,068 arm-level numerator/denominator/rate checks; silent units, half-open window, repeated events, patterns; explicit Fisher/Wilson/Newcombe labels | Valid assignment roster, complete logging assertion, prespecified eligible population and independent declared unit remain external requirements. `tools/experiment.py:258`, `tools/experiment.py:376` |
| `experiment_data`: conditional funnel rows | Restrict descriptive | Prior-reach denominators correct; conditional Fisher request rejected; empty denominator explicit | Post-treatment populations may differ; generic effect field must not be promoted to causal effect. `tools/experiment.py:264`, `stats/experiments.py:266` |
| `session_funnel_data` | Restrict descriptive | Fixed session aggregates and pooled-union label verified; binary intervals rejected | Explicit time filter; observed-session ratio; cluster-bootstrap coverage/overlap behavior not broadly simulated. `tools/experiment.py:469` |

## Reproduction and artifacts

From the workspace root:

```sh
PYTHONPATH=.work/ab-safe-bet/baseline220/src .work/ab-integrated-wheel-env/bin/python output/AB_Safe_Bet_Review/roles/06_funnels/review.py --source .work/ab-safe-bet/baseline220 --output output/AB_Safe_Bet_Review/roles/06_funnels/rerun.json --random-logs 100
PYTHONPATH=.work/ab-safe-bet/baseline220/src .work/ab-integrated-wheel-env/bin/python output/AB_Safe_Bet_Review/roles/06_funnels/repro_all_start.py --source .work/ab-safe-bet/baseline220
```

Replace `--source` and `PYTHONPATH` with any candidate checkout for rerun. The script inserts the selected source ahead of installed packages and records the resolved import. Original raw fixtures are preserved for cross-review. `commands.txt` records the command sequence and adapter correction. `source_hashes.json` records actual source-file hashes; `artifact_hashes.json` records evidence hashes. `REPORT.html` contains the same report as rendered HTML.

Coverage limits: no exhaustive anchor grammar/offset validation, no dynamic-overlap session calibration, no first-session policy implementation, no eventstream transformations involving synthetic positions, no UI validation, no estimator coverage or power simulation. Assertions about independent randomization and causal interpretation are requirements, not verified properties of a dataframe. Independent cross-review of F06-01 and any remedy remains required before promotion.
