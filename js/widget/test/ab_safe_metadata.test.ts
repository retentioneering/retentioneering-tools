import test from "node:test";
import assert from "node:assert/strict";
import React from "react";
import { createRequire } from "node:module";
import { ScorecardView, StrataView, type ABRow, type ABResult } from "../src/ab_test_plot";
import { TESTS, armLevelOptions } from "../src/ab_test";
const { renderToStaticMarkup } = createRequire(import.meta.url)("react-dom/server");
const row: ABRow = {
  metric: "conversion", test: "fisher_exact", ci_basis: "clopper_pearson_bonferroni",
  p_basis: "fisher_exact_two_sided", mde_basis: "normal_approximation", mde: .3,
  group1_value: .6, group2_value: .5, delta: .1, rel_delta: .2,
  delta_ci_low: -.4, delta_ci_high: .5, rel_delta_ci_low: null, rel_delta_ci_high: null,
};
const result: ABResult = { overall: [row], metric_kinds: { conversion: "binary" }, correction: "bonferroni" };

test("method selector names exact binary and descriptive nonbinary defaults", () => {
  const choices = new Map(TESTS);
  assert.match(choices.get("auto")!, /binary exact; other outcomes descriptive/);
  assert.match(choices.get("fisher_exact")!, /exact/);
  assert.match(choices.get("descriptive")!, /no inference/);
  for (const key of ["z_test", "welch_t_test", "bootstrap"]) assert.match(choices.get(key)!, /approximate/);
});

test("typed arm options preserve 1 versus string 1 through JSON values", () => {
  const options = armLevelOptions([1, "1", "control"]);
  assert.notEqual(options[0].value, options[1].value);
  assert.notEqual(options[0].label, options[1].label);
  assert.equal(JSON.parse(options[0].value), 1);
  assert.equal(JSON.parse(options[1].value), "1");
  assert.equal(options[2].label, "control");
});

test("exact label does not imply exact MDE or an available relative interval", () => {
  const html = renderToStaticMarkup(React.createElement(ScorecardView, { result }));
  for (const text of ["Fisher exact", "Conservative exact Clopper", "Normal planning approximation", "Relative CI unavailable for Fisher exact", "+10.00 pp", "+20.0%"]) assert.ok(html.includes(text), text);
  assert.ok(!html.includes("never adjusted"));
});

test("unresolved Bonferroni bootstrap tails show status instead of an interval", () => {
  const bootstrap: ABRow = { ...row, test: "bootstrap", ci_basis: "path_bootstrap_approximate", p_basis: "bootstrap_se_asymptotic", delta_ci_low_adj: null, delta_ci_high_adj: null, delta_ci_adjusted_basis: "path_bootstrap_percentile_approximate", delta_ci_adjusted_status: "insufficient_tail_resamples" };
  const html = renderToStaticMarkup(React.createElement(ScorecardView, { result: { ...result, overall: [bootstrap] } }));
  assert.ok(html.includes("Bonferroni CI: unavailable"));
  assert.ok(html.includes("Insufficient resamples in the adjusted tails"));
  assert.ok(html.includes("Percentile bootstrap (approximate)"));
});

test("secondary strata diagnostics are distinguished from primary exact inference", () => {
  const strata = [{ ...row, stratum: "x", strata_diagnostics_basis: "not_requested", heterogeneity_p_value: null, balance_p_value: null, stratum_vs_rest_p_value: null }];
  const html = renderToStaticMarkup(React.createElement(StrataView, { result: { ...result, strata } }));
  assert.ok(html.includes("Secondary diagnostics: Not requested"));
  assert.ok(html.includes("Conservative exact Clopper"));
});
