# Role 01: binary inference preregistration

Recorded 2026-10-30 before inspecting candidate implementation or other reviewers' findings.

Exposure: the task described Wilson/Newcombe, Fisher/z/Welch and candidate revision; no candidate source, signatures, tests or findings have been read. This is staged oracle-first review, not a blinded experiment. Only this role directory will be written.

## Independent expectations

Independent Bernoulli trials with fixed group sizes; treatment minus control defines the effect. Wilson intervals invert the one-sample score quadratic. Newcombe method 10 combines separate Wilson bounds in quadrature. Both methods are approximate: nominal 95% does not imply pointwise coverage at least 95%. Do not equate a Newcombe interval excluding zero with a pooled z or Fisher p-value below alpha.

Fisher's two-sided p-value sums conditional hypergeometric probabilities no greater than the observed probability. It uses fixed margins and can be conservative. Pooled z uses the null pooled proportion and is asymptotic; zero pooled variance with equal proportions means no observed difference, p=1. Welch on Bernoulli values uses unbiased sample variances and Satterthwaite degrees of freedom, requires at least two observations in each group, and has small-sample limitations distinct from implementation errors. Constant samples with unequal means require an explicit policy; mathematical zero-variance plug-in limits must not be presented as a universal coverage guarantee.

All ordinary returned p-values must be finite and in [0,1]. Intervals must be ordered, finite and within the estimand's support when that method promises clipping. Swapping groups reverses the difference and interval; complementing binary outcomes does likewise. Two-sided p-values are unchanged under both operations. Duplicating observations keeps rates/effects fixed but changes information and need not preserve p-values or intervals. Numerical tests include zero/all successes, n=1, unbalanced samples, confidence levels near zero/one and feasible very large counts. Invalid counts, nonfinite observations and invalid confidence levels must be rejected or explicitly handled; role 07 owns comprehensive API validation.

## Planned evidence

1. Freeze and SHA256 independent `oracle.py` before any source inspection. It uses Python arithmetic/NormalDist and independent combinatorial Fisher enumeration; SciPy Student-t distribution may supply Welch distribution quantiles only.
2. Exhaustively compare all binary outcomes for several small group sizes, plus handpicked medium/large/sparse inputs. Use tight tolerance (ordinary formulas 1e-12 absolute unless justified).
3. Enumerate exact binomial outcome probabilities for Wilson and Newcombe coverage on a declared parameter grid. This is deterministic enumeration, no Monte Carlo uncertainty or seed; finite grids cannot establish uniform coverage. Preserve pointwise probabilities and worst observed cases.
4. Enumerate null rejection probabilities for Fisher/z/Welch (and whichever automatic policy exists). Quantify conditional/asymptotic limitations without turning them into coding defects.
5. Exercise public API routing, sign invariance and confidence/test labels, preserving minimal reproductions and exact source references.

## References consulted before freezing

- NIST, Wilson score interval: https://www.itl.nist.gov/div898/handbook/prc/section2/prc241.htm
- Newcombe (1998), *Interval estimation for the difference between independent proportions: comparison of eleven methods*, original paper copy: https://misc.biblio.s3.amazonaws.com/Newcombe98abOnWMWprobCI.pdf (method 10; original abstract/title and paper text surfaced by search).
- SciPy `fisher_exact` documented two-sided probability ordering: https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.fisher_exact.html

Any changed assumptions after unblinding will be logged as amendments; oracle formulas will not be silently changed.
