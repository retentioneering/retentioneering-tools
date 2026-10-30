# Role 09 — applied dataset and workflow review

**Initial bounded review, 2026-10-30.** Frozen baseline: PR220 `38cbe91d5b315584575b132a596890e69eb210aa`. Numerical point estimates and tested population/window contracts agree with the independent oracle: **1,107 comparisons passed; zero numerical failures; zero final runner errors**. This supports the capabilities below within their tested contracts. It does not certify confidence-interval coverage, Type I error, valid randomization, or arbitrary real-world datasets.

## Independence and reproducibility

I read only PROTOCOL.md before writing PRE-REGISTRATION.md and oracle.py. The frozen oracle SHA256 is `ff98beffd87c78e162f2b39cbf5f9953a8756ee692aac09ef074a19bc0477100`. It uses plain pandas/Python to compute roster-aligned binary conversion, event counts, per-user sums/means, ratios of totals, half-open windows, Pearson two-arm SRM and treatment-minus-control effects. The file was not amended after source exposure. No checkpoint or peer report was read.

The public adapter runner is configurable by `--source` and `--output`; it inserts that source's `src` directory before importing the package. Final results use Python 3.13.17, pandas 3.1.0 and NumPy 2.5.4. `raw_final/environment.json` records every candidate Python source file's SHA256. Full input tables, per-user oracle/API tables, arm summaries, scorecards, segment/period tables, fixed-window JSON payloads, rejected-input messages and original run output are preserved. `COMMANDS.md` records invocation and exposure order.

An initial harness error passed an EventstreamSchema object rather than its dictionary to the Eventstream constructor. It affected only the first-session supplement, was corrected in the adapter, and is preserved in `run.log`/`raw/`. It was not a candidate defect. The corrected full run is `run_final.log`/`raw_final/`. Story spot-checks were written after unblinding and reuse the frozen aggregates; they are explicitly observational checks of a selected teaching realization.

The 1,107 equalities are correlated checks, not 1,107 independent datasets. Bootstrap calls used 100 replicates to exercise execution; their interval endpoints, Monte Carlo error and rejection rates were not validated. No statistical simulation or coverage claim is made by this role.

## Dataset inventory and observed numbers

Every bundled default was loaded with its default seed and size. No assignment duplicates, missing timestamps or event units outside its supplied assignment table were found. “Assignments” means supplied rows, whose scientific meaning differs by case.

| Dataset | Event rows | Supplied assignments | Event-observed users | Silent in full source | Pre-assignment events |
|---|---:|---:|---:|---:|---:|
| northwind_home | 215,677 | 10,000 users | 9,733 | 267 | 0 |
| srm_platform_bug | 119,033 | 9,030 logged users | 9,030 | 0 | 0 |
| session_assignment | 237,645 | 39,659 sessions | 8,000 | 0 sessions | 0 |
| aa_calibration | 120,117 | 10,000 users | 10,000 | 0 | 0 |
| trial_conversion | 197,321 | 10,000 users | 10,000 | 0 | 0 |
| push_campaign | 91,456 | 20,000 users | 6,237 | 13,763 | 14,860 |

The 9,030-row SRM fixture's supplied table is deliberately an incomplete analytics log, not the original 10,000-person assignment population. Correct aggregation over it does not recover the missing people. The session fixture contains 2,454 users with multiple arms; its 39,659 rows cannot be treated as independent users.

| Teaching claim checked | Independently observed result | Interpretation |
|---|---|---|
| Northwind overall purchase conversion | treatment 24.3347%, control 23.7302%, difference +0.6045 pp; Pearson SRM p=0.42371 | Published rounded point estimates and 267 silent users match. |
| Northwind planted types | scanner +12.5359 pp, curator −6.7208 pp, other −0.4307 pp | Signs match among event-observed users with truth labels. Silent users have no exported truth label; this is not a blind clustering validation. |
| SRM platform bug | overall SRM p=2.20e−28; Android p=4.42e−68; apparent overall conversion +3.0576 pp | Android-only imbalance and spurious aggregate lift match the story. The experiment is invalid despite correct arithmetic. |
| Session assignment | original user/session inferential scorecards reject; first-session add conversion +7.2966 pp | First-session analysis has a separate estimand; repeated-session independence remains false. |
| A/A heavy-tail example | conversion +0.0891 pp; six largest revenue users are all treatment, 27,190–34,890 each | A selected A/A run is not a calibration benchmark. The documented shared reseller violates user independence. |
| Trial conversion | unrestricted +3.3240 pp; mature 21-day +0.9189 pp, CI −1.0612 to +2.9007 pp; 4,410 immature users excluded | Point estimates and maturity accounting match. The unrestricted comparison mixes follow-up ages. CI shown is observed package output, not an independently validated interval in this role. |
| Push campaign | ITT +1.9387 pp; stream-only −2.2721 pp | The outcome-selected denominator reverses the sign. All 14,860 pre-assignment rows are excluded by the tested roster API. |

Northwind cluster recovery, all seven planted narrative branches, novelty bootstrap contrasts, the A/A narrative's daily/weekly verdicts, and full notebook execution were not audited here. Verified stories must not be generalized to those deferred claims.

## Frozen Lumeo

All five frozen table SHA256 values, exact column lists and row counts match their manifest. Independent checks also confirm assignment uniqueness, event/screen/click membership, session nesting, screen/tap uniqueness, tap references and screen tap counts. The full tables contain 4,000 assignment rows and 104,916 event rows.

For the documented cohort assigned before 2026-05-11, outcomes in `[2026-05-11, 2026-05-25)` agree across the oracle, `get_ab_paths`, `get_ab_test`, and `experiment_data`:

| Population / outcome | Control | Treatment |
|---|---:|---:|
| Assigned cohort | 1,696 | 1,623 |
| Observed in window | 117 | 99 |
| No event in window | 1,579 | 1,524 |
| Sessions in window | 244 | 173 |
| Buyers | 6 | 6 |
| Purchase / assigned | 0.353774% | 0.369686% |
| Revenue / assigned | 0.891268 | 0.452304 |
| Pooled average order value | 251.931667 | 122.348333 |

The cohort source includes 85,269 events before the analysis start and 5,335 inside the window. The outcome APIs exclude the former. The complete screen table has 856 complete zero-tap screens and 760 incomplete screens; those are different observation states and do not alter the event-outcome assignment roster.

The late fixed-calendar cohort is not “14 days after each assignment.” The sample's outcome-independent subsampling does not remove selection of its parent realization. The data card states these restrictions appropriately. Six buyers per arm are especially weak evidence for ratio inference; agreement of AOV point estimates is not evidence that a percentile interval has nominal coverage.

## Held-out fixtures and workflow checks

The independently constructed 12-user fixture has six assignments per arm, repeated purchases, silent users, prehistory, exact start/end events and an unrelated event. In `[2026-01-01, 2026-01-03)`, control revenue is 6 across six people (mean 1); treatment revenue is 18 across six (mean 3). Both arms have two orders, while buyer counts are one and two. Pooled AOVs are 3 and 9. The APIs match all of these hand computations. Boundary rows at end and prehistory are excluded.

`experiment_data` rejects an event user missing from the roster, a duplicate assignment, and a missing amount on a matching purchase. Declared zero-occurrence purchases produce zero means and undefined AOVs rather than zero AOV. These are public reproductions in `run_review.py`, with exact failure messages/payloads retained under `raw_final/heldout/`.

Segment/period checks cover assignment-only even/odd labels, including silent users, crossed with public 7-day calendar bins. The independent calculation selects assignments before each bin end, excludes pre-assignment events and uses the reported half-open bin boundaries. All 704 cell-value and population-count equalities pass. These checks verify accounting within bins; they do not independently establish correctness of every calendar-offset convention or post-selection inference.

## Findings and release boundaries

**F09-01 — P3 documentation defect, exact catalog count.** `docs/guide/ab-testing.md:86` says `list_ab_tests()` names “six more bundled experiments” after introducing Northwind. Public reproduction: `len(datasets.list_ab_tests())` is 6, containing Northwind plus five other names. Expected: six total, or five more. Actual: wording implies seven total. Remedy: correct the sentence. Regression verification: compare the sentence with the public catalog after editing. No source edit was made by this role.

**R09-01 — estimator/design limitation, not an arithmetic defect.** A validated table can still be an incomplete exposure log (`srm_platform_bug`) or contain dependent units (`aa_calibration`, repeated sessions). Keep the supplied-roster, logging-completeness and independent-unit assumptions explicit. A good SRM result cannot establish either completeness or independence.

**R09-02 — population/contract restriction.** Zero-filling is justified for supported event outcomes only under complete observation. It is not missing-data imputation. Retain distinct names for ITT means, active-population means, ratios of totals, and eligible-user means of ratios. Preserve the current explicit window and assignment contracts; switching between APIs is an estimand choice, not a spelling change.

**R09-03 — untested inferential scope.** This review finds no numerical discrepancy in its tested aggregates. It supplies no independent evidence for auto-selected tests, MDE/verdict logic, bootstrap coverage, multiple-testing control, relative-effect intervals, exact-binomial SRM, post-selection analysis or sequential use. Other independent role evidence is required before retaining those claims.

## Function-level decision matrix

| Public function / capability | Decision from this role | Evidence and precise boundary |
|---|---|---|
| `list_ab_tests`, `load_ab_test` | Retained as teaching fixtures | Six defaults generated; schemas and counts captured. Never a calibration certificate. |
| `load_ab_test_assignment` | Restricted | Roster-aligned arithmetic works; session fixture returns sessions and SRM fixture returns a deliberately incomplete log. |
| `get_ab_paths`: `has_event`, `event_count`, `event_value_sum`, ratio components | Retained for tested point exports | 53 table/count comparisons; silent zero filling and pre-assignment exclusion match oracle. Independent-unit and logging assumptions remain external. |
| `get_ab_test`: tested means/ratios and treatment-minus-control delta | Retained for point estimates; inference restricted | 150 value/count equalities. Test, interval, MDE and verdict correctness deferred to statistical roles. |
| `get_ab_test`: assignment-only strata × calendar periods | Restricted | 704 cell comparisons match. Assumes labels fixed before exposure and declared analysis; offset-edge conventions and selection-adjusted inference not proven. |
| `get_ab_test`: 21-day horizon | Restricted | Trial mature counts and point effects match; horizon fixed in advance and explicit cutoff required. |
| `get_sample_ratio`: two-arm Pearson check | Retained within independent multinomial contract | Six valid user-level workflow comparisons plus held-out/Lumeo cases; chi-square and p match frozen oracle. A/A shared reseller and session dependence remain invalid designs. |
| `experiment_data`: binary/count/sum means and ratio of sums | Retained for tested fixed-window point estimates | 144 value/population checks, strict roster/window, silent users, prehistory, missing amounts and zero denominators. |
| `experiment_data`: expected-allocation exact-binomial SRM | Deferred by this role | Payload inspected; frozen oracle covers Pearson SRM, which is a different procedure. |
| `experiment_data`: mean of ratios, funnels, pattern outcomes | Deferred by this role | Distinct estimands; no independent aggregate comparison here. |
| Bootstrap / analytic intervals, p-values, multiplicity, MDE, novelty contrasts | Deferred by this role | Aggregate agreement and selected stories cannot establish inferential validity. |
| Lumeo loader/data card and frozen sample | Retained as selected teaching artifact | Checksums, schema, membership, counters and reported cohort numbers agree; selected-parent and sparse-outcome restrictions must remain. |

## Evidence files

- `PRE-REGISTRATION.md`, `oracle.py`, `FROZEN_SHA256SUMS`: independent frozen specification and oracle.
- `run_review.py`, `story_audit.py`, `COMMANDS.md`: executable configurable runner and documented supplemental checks.
- `raw_final/checks.csv`, `summary.json`, `errors.json`, `environment.json`: final checks, versions and candidate source hashes.
- `raw_final/<dataset>/`: full raw tables, per-user tables, independent aggregates, API outputs and rejection details.
- `raw_final/story_audit.json`: limited narrative spot-checks and Lumeo integrity checks.
- `run.log`, `raw/`: preserved initial run; `run_final.log`: corrected final run.

No candidate source files were edited. Further tutorial alignment should retain these exact estimands and limitations and correct the catalog count, with larger claims governed by the full independent review.
