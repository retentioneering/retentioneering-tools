# Binary cross-review — separate from the frozen initial audit

Completed 2026-10-30 after reading role 04 and role 05 reports and the coordinator checkpoint. This phase is explicitly unblinded. The original preregistration, oracle, report, and raw baseline evidence were not changed. The supplemental exact-CI reference is separately identified and does not claim preregistered independence.

## Role 05 reproduced

`cross_review.py` independently constructs the public PR221 fixtures, imports no peer code, and computes the sparse-table reference using this role's frozen integer-combinatorial Fisher oracle. The constant-ratio reference uses Python `Fraction` arithmetic and a direct proof of invariance under every nonempty within-arm resample.

- **Sparse steps:** treatment A/B counts 1/1, control A/B 50/950. Public PR221 yields A p=0.0038208662144492084 and `increase` with no support warning. Independent Fisher gives 0.09925403937379985. Together with this role's own exact null enumeration, this confirms the pooled-support inference defect; it is not merely disagreement between two methods.
- **Constant conditional ratio:** 30 treatment paths `ABACAC`, 30 control paths `ABAC`. Every treatment resample has ratio m/(3m)=1/3 and every control resample m/(2m)=1/2. Exact empirical contrast is always −1/6. PR221 instead reports SE=2.77694638204588e-17, p=0, `decrease`, and zero-width interval. This independently confirms the degeneracy defect under the library's stated no-spread policy.

Evidence: `cross_review/raw_graphs.json`, `cross_graphs.log`. Run `.work/ab-integrated-wheel-env/bin/python output/AB_Safe_Bet_Review/roles/01_binary/cross_review.py --source .work/ab-safe-bet/pr221 --output <new.json>` from the workspace root.

**Decision:** defer PR221's new inferential graph/step surfaces and overlay. A shared scorecard repair alone does not establish those screens' population, family, dependence, conditional estimand or overlay identity contracts. No PR221 extraction was performed.

## Role 04 assessed

The report correctly separates valid Holm/Bonferroni arithmetic from invalid incoming p-values and identifies that Newcombe and pooled-z are not dual. An exact Fisher test and the conservative interval added here are also not dual, so the reading now explicitly says they use different constructions. The old “borderline” explanation is too narrow for conservative intervals.

The coordinator's stable survival-tail quantiles and unrounded positive alpha pass the frozen highest-level public reproducer and focused tail tests. The power>=0.5 restriction is preserved. MDE remains a normal planning approximation, not exact Fisher power; available scorecard rows now carry `mde_basis='normal_approximation'` and the existing corrected prose is preserved.

At role 04's request, fixed `srm_chi2` to compute degrees of freedom from positive expected-probability categories. A zero-probability category with a positive observation returns infinite statistic/p=0. `[68,32,0]` versus `[.5,.5,0]` now equals the two-category result (12.96, df=1); this has a failing-before/passing-after regression. The public SRM adapter belongs to role 04 and was not edited here.

## Safe-policy justification

Known binary outcomes admit exact conditional tests under the fixed independent-Bernoulli design. Default Fisher plus a conservative exact difference interval therefore has a clear mathematical validity argument. For arbitrary nonbinary mean/ratio distributions no observed skewness cutoff or fixed minimum n gives a comparable uniform guarantee. Returning descriptive estimates by default, with explicitly requested Welch/bootstrap marked approximate, is a narrower and more defensible contract than automatically selecting a winner-producing method.

The exact difference interval uses two Clopper–Pearson arm intervals, each with error probability at most alpha/2. By the union bound, `[L1-U2,U1-L2]` covers p1−p2 with probability at least 1−alpha. It is wider than Newcombe and can include zero when Fisher rejects. It is not advertised as a shortest interval or an inversion of Fisher. The displayed individual arm intervals are pointwise CP intervals at the requested level; the difference calculation separately allocates alpha/2 to each arm, using alpha/4 directly in each one-sided tail.

The guarded explicit z-test still has only asymptotic validity. Per-arm expected successes and failures >=5 eliminate the demonstrated tiny-arm misuse but do not establish exact 5% size. Both method metadata and human reading say approximate. Known-binary Welch and scorecard bootstrap are rejected; explicitly requested raw bootstrap draws remain available through `get_ab_bootstrap` without converting them automatically into a winner verdict.
