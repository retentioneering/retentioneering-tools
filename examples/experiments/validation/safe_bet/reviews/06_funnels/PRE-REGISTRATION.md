# Role 06 preregistration: funnels, conversion, and patterns

Date: 2026-10-30. Reviewer: independent product analyst / sequence-method specialist.

Read before registration: only PROTOCOL.md and task instructions. No implementation, adapter, checkpoint, or peer report has been read. Frozen sources to compare: baseline220 and pr217. No source edits authorized for this role.

## Independent expected semantics

1. A funnel is an ordered subsequence of event labels within a unit. A unit reaches step k if its ordered event sequence contains the first k requested labels in order. Repeated events do not multiply users; repeated funnel labels require distinct events. Events are sorted by timestamp and stable input position. Equal-time behavior must be documented because timestamp alone does not identify a strict causal order.
2. A user funnel can combine sessions; a session funnel cannot. Session units are (user, session), not session ID alone. When a first-session or first-entry policy is offered, test it separately from any-session subsequence reach.
3. Cumulative conversion to stage k is reached[k] / entered stage 1. Conditional stage conversion is reached[k] / reached[k-1]. The first stage is 1 when its denominator is positive. An assigned-population outcome instead uses every eligible assignee, including silent assignees, with zero for no reach. Zero denominator is undefined, not automatically a success.
4. The experiment outcome for a funnel must be one binary result per randomization unit. If the API filters to first-step users, the statistical computation can describe those users, but it is generally conditioned on a post-treatment event and is not automatically an assignment-based causal effect.
5. Eligibility and observation cutoffs must be enforced before deriving the outcome, using documented time boundary rules. Ignoring a requested cutoff is a defect. Lack of such controls is a restriction, not automatically an implementation defect.
6. Ordered patterns match subsequences; contiguous patterns match adjacent labels. A unit-level pattern outcome is binary even when the pattern repeats. Count-valued occurrences are distinct and must not be labeled binary conversion.
7. Statistical labels must match the observed metric and unit; correct algebra alone does not establish causal interpretability, randomization, or valid uncertainty for nested/dependent sessions.

## Planned coverage and decision rules

Use exact small logs with repeated events, loops, missing starts/intermediate/end steps, equal timestamps, silent assignees, explicit eligibility, time cutoffs, reused/nested session IDs, and cross-session step completion. Use at least 100 randomized logs (fixed seeds), compare public funnel_data/get_conversion_rate and experimental funnel/pattern extraction with the oracle after learning public signatures. Record unsupported API semantics explicitly rather than guessing them. Check baseline and comparison snapshot on the same fixtures.

Confirmed implementation defects require a public executable reproduction, precise expected/actual result, and source location. Contract ambiguities and deliberate alternative semantics are restrictions unless contradicted by a declared contract. Record every amendment to the oracle or scope after source inspection. Report runtime/library versions, actual source hashes, oracle hash, commands, raw results, and a function-level trust/restrict/defect table in Markdown and HTML. Finite checks are evidence only for exercised inputs.
