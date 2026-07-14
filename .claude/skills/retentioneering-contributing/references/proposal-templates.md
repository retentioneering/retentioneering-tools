# Proposal templates

Copy, fill, delete unused sections. Each template ends with a worked example drawn from
real accepted-style contributions.

## T1 · Bug report (issue)

```markdown
### Expectation
<what you believed would happen; quote the docstring/docs line that says so>

### Reality
<what happened: exact error text or wrong output>

### Minimal reproduction
​```python
# deterministic, synthetic data, one behavior; assert the expectation
​```

### Impact
<time lost / wrong number nearly shipped / workaround required>

### Environment
retentioneering X.Y.Z (pip wheel | source checkout @ <sha>), Python 3.x, OS
```

**Worked example (style reference).** *Expectation:* docstring says `n_clusters` accepts
a range string `"3-8"`. *Reality:* `InvalidParameterError` from sklearn. *Repro:* 6-line
toy stream + `cluster_analysis_data(..., n_clusters="3-8")` with `assert "best_params"
in res`. *Impact:* 2 analysts hit it independently in one day; both fell back to lists.

## T2 · Feature / API-change proposal (issue)

```markdown
### Problem (use-case first)
<the analysis task that cannot be done cleanly today; who hits it and how often>

### Today's workaround and its cost
​```python
# the code users write instead, and what it risks (untested, easy to get wrong)
​```

### Proposed API
​```python
stream.method(arg=..., new_param="...")   # exact signature
​```

### Semantics & edge cases
<what happens on empty input, missing anchors, ties, NaN...>

### Acceptance criteria
<the tests that should pass; the error messages users should see>

### Evidence
<how often this came up; links to journals/issues/threads>

### Non-goals / migration
<what this does NOT change; deprecation path if any>
```

**Worked example.** *Problem:* windowing paths between two events drops paths lacking
the end anchor, but anti-leakage analyses need "truncate completers, keep the rest
whole". *Workaround:* filter_paths split → truncate half → manual concat (3 steps,
easy to desync). *Proposed:* `truncate_paths(..., keep_unmatched="drop"|"keep_whole")`,
default `"drop"` + warning with dropped-path count. *Acceptance:* toy where path
`a→basket` survives with `keep_whole`; warning names counts.

## T3 · "Missing signal" proposal (a special, high-value bug class)

Use when the library computed something it internally knows is unreliable and returned
it without warning (degenerate clustering picked silently; truncated table without a
marker; rows dropped without a count). Frame:

```markdown
### What the library knew and did not surface
### The wrong conclusion this enables   <- the key section: show the bad decision>
### Proposed signal
<warning text / result field / .attrs marker — smallest honest addition>
```

House rule this leans on: silent degradation is an auto-reject; loud-and-helpful is the
style. These proposals historically get accepted fastest.

## T4 · Pull request body

```markdown
## What & why
<1-3 sentences; link the issue: Fixes #NNN>

## Before / after
​```python
# repro from the issue; before: <error/wrong>, after: <correct>
​```

## Tests
<added/changed tests; why they would have caught the bug>

## Sync checklist
- [ ] docstrings updated (+ render_pages regenerated if tracked)
- [ ] MCP layer untouched or updated
- [ ] JS metric editor / widget contract untouched or updated
- [ ] CHANGELOG.md entry (user-visible changes)

## Breaking changes
<none | description + migration note>
```

## T5 · Docs-only PR

Smallest honest fix for "surprising but working as designed": patch the DOCSTRING (the
site renders from it), add a one-line semantics note ("labels are strings 'cluster_0'…";
"conversion_rate is a share of ALL paths — see step_conversion_rate"), and where a
mis-read caused a wrong number, add that as a doctest-style example.

## Triage heuristics (portfolio mode)

Rank a batch of observations by `frequency × silent-failure risk`:
1. silent wrong results → file first, always;
2. loud errors with bad messages → cheap docs/message PRs, good first contributions;
3. missing conveniences → one consolidated feature issue each, evidence-first;
4. style/opinion items → skip unless maintainers signal interest.
