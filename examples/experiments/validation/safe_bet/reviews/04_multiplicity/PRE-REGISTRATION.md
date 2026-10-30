# Role 4 preregistration — multiple testing and power

Date: 2026-10-30 (local timezone Asia/Nicosia).
Exposure: only the parent task and shared PROTOCOL.md were read before this file and oracle.py. No candidate implementation, adapter, CHECKPOINT, or peer review has been read. This is staged oracle-first review, not a formal blinded study.

## Hypotheses and decision rules frozen before source inspection

1. For any finite family of valid p-values, Bonferroni returns min(1,m*p); Holm returns the cumulative maximum of sorted (m-rank+1)*p, capped at 1; BH returns reverse cumulative minima of sorted m*p/rank, capped at 1. Original ordering and ties must survive. Under valid p-values, Holm/Bonferroni strongly control FWER regardless of dependence. BH is an FDR procedure requiring independence or suitable positive dependence, not general FWER control.
2. Missing/unavailable tests require an explicit family policy. For a prespecified family, the conservative oracle includes unavailable hypotheses in m, substituting p=1 internally while preserving missing output. Silently reducing m after outcome-dependent failures cannot support unconditional family-control claims. A contract deliberately defining an available-test family is a restricted capability, not an arithmetic defect.
3. A correction within one metric, stratum, or time slice does not control the union of claims across separately corrected families. Repeated fixed-horizon looks remain outside ordinary Holm/BH guarantees. Unadjusted confidence intervals remain marginal even when displayed with adjusted p-values.
4. Gaussian mean-difference MDE at two-sided alpha and target power is approximately (z[1-alpha/2]+z[power])*SE; exact power under a fixed-SE normal model is Phi(-z-delta/SE)+1-Phi(z-delta/SE). This approximation is acceptable if labeled. Binary MDE requires specified baseline and alternative variance; null-variance plug-in is only a local approximation. Multiplicity and allocation ratio must be reflected in any promise of family-level power; observed MDE is not post-hoc evidence for no effect.
5. Cochran Q for independent stratum estimates uses inverse-variance weighted centering and chi-square(k-1), an asymptotic diagnostic requiring valid positive variances. Stratum-versus-rest compares independent partitions with variance addition; per-stratum significance alone is not heterogeneity evidence. Multiple stratum-v-rest p-values require a family policy.
6. SRM uses expected allocation probabilities (or must explicitly restrict to equal allocation). Pearson chi-square assumes adequate expected counts; exact binomial/multinomial alternatives are relevant for sparse allocations. A 1:3 randomized design must not be falsely flagged against 1:1 when a configured allocation ratio is available.
7. Verdicts must distinguish evidence of improvement, harm, practical equivalence, and insufficient information. An adjusted p-value crossing alpha must use the relevant metric family; lack of significance and low power do not establish no effect.

## Independent fixtures planned

- Hand vectors [0.01,0.04,0.03,0.002] -> Bonferroni [0.04,0.16,0.12,0.008], Holm [0.03,0.06,0.06,0.008], BH [0.02,0.04,0.04,0.008]. Empty, singleton, tied and 0/1 vectors; permutations.
- Planned missing family [0.03,NaN] -> finite adjusted 0.06 for all three procedures; output missing for unavailable hypothesis. Compare available-family policy separately.
- Gaussian mean fixtures with hand means/variances and several allocation ratios; compare normal MDE/power, reject impossible/invalid input contracts where public interfaces allow them.
- Stratum estimates [0,1,2], variances [1,1,1] -> weighted mean 1, Q=2, df=2. Contrast 0 with mean of other two -> difference -1.5, SE=sqrt(1.5). Unequal and zero variance sensitivity.
- SRM [25,75], allocation [.25,.75] -> statistic 0; equal allocation -> statistic 25.
- Independent synthetic user-level A/B datasets crossing multiple metrics, strata and time slices, with public API expectations derived from raw aggregates.
- Null simulation, if relevant: independent normal z or uniform p-value families, seed 4072026; report Monte Carlo binomial SE and family definition. An illustration of per-family versus union control is not a code defect by itself.

## Analysis constraints

Freeze SHA256 of this preregistration and oracle.py before implementation inspection. Execute public APIs against both frozen baseline220 and PR217 with configurable source path and imported module hashes. No implementation changes. Report exact reproductions, function-level retained/restricted/defect decisions, and limitations. Any oracle amendments after unblinding will be separate files or explicitly logged. Raw JSON/CSV, commands, Markdown and HTML will be preserved under this role directory only.

## Primary references

- Holm (1979), A Simple Sequentially Rejective Multiple Test Procedure, Scandinavian Journal of Statistics 6:65–70. https://www.ime.usp.br/~abe/lista/pdf4R8xPVzCnX.pdf
- Benjamini and Hochberg (1995), Controlling the False Discovery Rate: A Practical and Powerful Approach to Multiple Testing. https://onlinelibrary.wiley.com/doi/10.1111/j.2517-6161.1995.tb02031.x
- Gaussian distribution power calculations and Q/SRM fixtures derive directly from the stated distributional assumptions, not candidate source.
