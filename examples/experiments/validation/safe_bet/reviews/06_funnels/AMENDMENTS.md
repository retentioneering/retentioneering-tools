# Scope amendments after source inspection

- The registered oracle remains unchanged. Public `funnel_data` explicitly divides cumulative reach by all **observed paths**, not funnel entrants. The adapter therefore compares to oracle `assignment_rates` without an external assignment roster. The public first-step conditional rate also uses all observed paths. This is documented alternative semantics, not a defect.
- `experiment_data` uses [start,end), whereas the independent oracle exposes an inclusive cutoff. The adapter filters events with the documented half-open rule before calling the unchanged oracle. No cutoff semantics are inferred from end timestamps.
- Pattern syntax learned from public source: `A->B` is contiguous; `A->.*->B` is an ordered subsequence. Both are compared to registered pattern modes.
- `get_conversion_rate` supports first/last/all start occurrence and inclusive event/time windows. Adapter contains a separate elementary pair-enumeration expectation for these documented semantics, written after unblinding. These checks are supplemental, not preregistered source-blind oracle evidence.
- No experiment_data exists in PR217. Assignment-roster and session_funnel_data coverage therefore applies to baseline220 only. The two snapshots are still compared on shared public APIs.
- No external statistical reference was used to copy formulas. This role checks denominators, outcomes, contract/method labels and conditioning; numeric CI calibration is assigned to other roles and is not claimed here.
