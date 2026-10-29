import test from "node:test";
import assert from "node:assert/strict";
import React from "react";
import { createRequire } from "node:module";
import { ExperimentContent, ExperimentTable, type ExperimentPayload, type ExperimentRow } from "../src/experiment";

// Avoid bundling Node's server renderer into esbuild's ESM test output.
const { renderToStaticMarkup } = createRequire(import.meta.url)("react-dom/server");

const row: ExperimentRow = {
  metric: "conversion", family: "scorecard", label: "Purchase", unit: "%", scale: 100,
  outcome_kind: "binary", role: "primary", control: .1, treatment: .12,
  control_ci_low: .06, control_ci_high: .15, control_ci_method: "wilson", control_ci_confidence_level: .95,
  treatment_ci_low: .08, treatment_ci_high: .17, treatment_ci_method: "wilson", treatment_ci_confidence_level: .95,
  absolute_effect: .02, absolute_effect_ci_low: -.03, absolute_effect_ci_high: .07,
  absolute_effect_status: "ok", absolute_effect_ci_method: "newcombe", absolute_effect_ci_confidence_level: .95,
  relative_effect: .2, control_numerator: 10, control_denominator: 100,
  treatment_numerator: 12, treatment_denominator: 100, control_units: 100, treatment_units: 100,
  test_method: "fisher_exact_two_sided", p_value: .81, p_value_adjusted: 1, test_status: "ok",
};

const payload: ExperimentPayload = {
  schema_version: "2.0", summary: [row],
  diagnostics: [{ arm: "control", n_assigned: 100, n_observed: 80, n_without_events: 20 }],
  metadata: {
    control: "A", treatment: "B", population_basis: "assignment_roster", unit_col: "user_id",
    observation: { start: "2026-01-01", end: "2026-01-15" }, bootstrap: null,
  },
};

test("experiment view shows arm labels, CI methods and the assignment population", () => {
  const html = renderToStaticMarkup(React.createElement(ExperimentContent, { payload }));
  for (const text of ["Control · A", "Treatment · B", "Purchase", "Wilson", "Newcombe", "Assigned", "Observed", "Silent", "100", "20", "Pointwise".toLowerCase()]) {
    assert.ok(html.includes(text), `Missing ${text}`);
  }
  assert.ok(html.includes("+2"));
  assert.ok(html.includes("pp"));
  assert.ok(!html.includes("compute_request"));
});

test("unavailable intervals retain their point and reason without fabricated bars", () => {
  const sparse: ExperimentRow = {
    ...row, absolute_effect_ci_low: null, absolute_effect_ci_high: null,
    absolute_effect_status: "degenerate_component", absolute_effect_ci_method: "bootstrap_percentile",
    absolute_effect_invalid_resamples: 3,
  };
  const html = renderToStaticMarkup(React.createElement(ExperimentTable, { rows: [sparse], control: "A", treatment: "B" }));
  assert.ok(html.includes("CI unavailable: degenerate component"));
  assert.ok(html.includes("undefined resamples"));
  assert.ok(html.includes("Percentile bootstrap"));
  assert.ok(html.includes("<circle"));
  assert.ok(!html.includes('stroke="#42657d"'));
});

test("repeated funnel steps keep separate ordinal rows", () => {
  const rows = [1, 2].map(step_index => ({
    ...row, metric: `funnel_cumulative_${step_index}`, family: "funnel_cumulative",
    label: undefined, step: "buy", step_index,
  }));
  const html = renderToStaticMarkup(React.createElement(ExperimentTable, { rows, control: "A", treatment: "B", sharedScale: true }));
  assert.ok(html.includes("1. buy"));
  assert.ok(html.includes("2. buy"));
  assert.equal((html.match(/data-metric=/g) ?? []).length, 2);
});

test("labels are escaped and unknown result versions fail visibly", () => {
  const escaped = renderToStaticMarkup(React.createElement(ExperimentContent, {
    payload: { ...payload, summary: [{ ...row, label: "<script>alert(1)</script>" }] },
  }));
  assert.ok(escaped.includes("&lt;script&gt;"));
  assert.ok(!escaped.includes("<script>alert"));
  const unsupported = renderToStaticMarkup(React.createElement(ExperimentContent, { payload: { ...payload, schema_version: "99" } }));
  assert.ok(unsupported.includes("Unsupported experiment result schema"));
});

test("binary scaled values use percent while their absolute effect uses percentage points", () => {
  const percent = renderToStaticMarkup(React.createElement(ExperimentTable, {
    rows: [{ ...row, unit: "probability", scale: 100 }], control: "A", treatment: "B",
  }));
  assert.ok(percent.includes(">10</strong>"));
  assert.ok(percent.includes(">%</span>"));
  assert.ok(percent.includes(">+2</strong>"));
  assert.ok(percent.includes(">pp</span>"));
  const canonical = renderToStaticMarkup(React.createElement(ExperimentTable, {
    rows: [{ ...row, scale: 1 }], control: "A", treatment: "B",
  }));
  assert.ok(canonical.includes(">0.1</strong>"));
  assert.ok(canonical.includes("proportion difference"));
});

test("percentage-valued ratios distinguish absolute pp from relative percent", () => {
  const ratio = {
    ...row, metric: "purchase_rate", outcome_kind: "ratio", effect_unit: "pp",
    control: .2, treatment: .3, absolute_effect: .1, relative_effect: .5,
  };
  const html = renderToStaticMarkup(React.createElement(ExperimentTable, { rows: [ratio], control: "A", treatment: "B" }));
  assert.ok(html.includes(">20</strong>"));
  assert.ok(html.includes(">30</strong>"));
  assert.ok(html.includes(">+10</strong>"));
  assert.ok(html.includes(">pp</span>"));
  assert.ok(html.includes("+50%"));
  assert.ok(!html.includes("Difference: %"));
  const explicit = renderToStaticMarkup(React.createElement(ExperimentTable, {
    rows: [{ ...ratio, effect_unit: "percentage points" }], control: "A", treatment: "B",
  }));
  assert.ok(explicit.includes("Difference: percentage points"));
});
