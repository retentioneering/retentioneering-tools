import test from 'node:test';
import assert from 'node:assert/strict';
import React from 'react';
import { createRequire } from 'node:module';
import { FunnelChart, FunnelTable } from '../src/funnel';
import { ciExcludesZero, formatPValue, relativeDeltaPct } from '../src/widget-utils';

const { renderToStaticMarkup } = createRequire(import.meta.url)('react-dom/server');

// Step 1: everyone reaches it, p undefined (null). Step 2: overall delta CI
// covers 0. Step 3: overall CI excludes 0, and the conditional step delta's CI
// excludes 0 too: it must still stay grey and carry no p-value.
const diffSteps = [
  { step: 'home', funnel1_unique_paths: 100, funnel2_unique_paths: 100,
    funnel1_conversion_rate: 1, funnel2_conversion_rate: 1, funnel1_step_conversion_rate: 1, funnel2_step_conversion_rate: 1,
    delta_conversion_rate: 0, delta_step_conversion_rate: 0,
    funnel1_conversion_rate_ci_low: 0.96, funnel1_conversion_rate_ci_high: 1, funnel2_conversion_rate_ci_low: 0.96, funnel2_conversion_rate_ci_high: 1,
    delta_conversion_rate_ci_low: -0.04, delta_conversion_rate_ci_high: 0.04,
    delta_conversion_rate_p_value: null, delta_conversion_rate_p_value_adjusted: null,
    delta_step_conversion_rate_ci_low: -0.04, delta_step_conversion_rate_ci_high: 0.04,
    step_conditional: false },
  { step: 'cart', funnel1_unique_paths: 30, funnel2_unique_paths: 20,
    funnel1_conversion_rate: 0.3, funnel2_conversion_rate: 0.2, funnel1_step_conversion_rate: 0.3, funnel2_step_conversion_rate: 0.2,
    delta_conversion_rate: 0.1, delta_step_conversion_rate: 0.1,
    funnel1_conversion_rate_ci_low: 0.22, funnel1_conversion_rate_ci_high: 0.4, funnel2_conversion_rate_ci_low: 0.13, funnel2_conversion_rate_ci_high: 0.29,
    delta_conversion_rate_ci_low: -0.02, delta_conversion_rate_ci_high: 0.22,
    delta_conversion_rate_p_value: 0.096, delta_conversion_rate_p_value_adjusted: 0.192,
    delta_step_conversion_rate_ci_low: -0.02, delta_step_conversion_rate_ci_high: 0.22,
    step_conditional: true },
  { step: 'buy', funnel1_unique_paths: 25, funnel2_unique_paths: 10,
    funnel1_conversion_rate: 0.25, funnel2_conversion_rate: 0.1, funnel1_step_conversion_rate: 0.8333, funnel2_step_conversion_rate: 0.5,
    delta_conversion_rate: 0.15, delta_step_conversion_rate: 0.3333,
    funnel1_conversion_rate_ci_low: 0.18, funnel1_conversion_rate_ci_high: 0.34, funnel2_conversion_rate_ci_low: 0.06, funnel2_conversion_rate_ci_high: 0.17,
    delta_conversion_rate_ci_low: 0.05, delta_conversion_rate_ci_high: 0.25,
    delta_conversion_rate_p_value: 0.004, delta_conversion_rate_p_value_adjusted: 0.008,
    delta_step_conversion_rate_ci_low: 0.08, delta_step_conversion_rate_ci_high: 0.56,
    step_conditional: true },
];

const count = (html: string, id: string) => html.split(`data-testid="${id}"`).length - 1;
const table = (props: Record<string, unknown>) =>
  renderToStaticMarkup(React.createElement(FunnelTable,
    { steps: diffSteps, hasDiff: true, label1: 't', label2: 'c', result: { ci_level: 0.95, correction: 'none' }, showCi: true, ...props }));

test('diff funnel draws two whiskers per step only when showCi', () => {
  const props = { steps: diffSteps, hasDiff: true, label1: 't', label2: 'c', chartH: 300 };
  assert.equal(count(renderToStaticMarkup(React.createElement(FunnelChart, { ...props, showCi: true })), 'ci-whisker'), 6);
  assert.equal(count(renderToStaticMarkup(React.createElement(FunnelChart, props)), 'ci-whisker'), 0);
});

test('a missing interval end (null from NaN) draws no whisker instead of NaN coordinates', () => {
  const steps = [{ ...diffSteps[1], funnel2_conversion_rate_ci_low: null }];
  const html = renderToStaticMarkup(React.createElement(FunnelChart,
    { steps, hasDiff: true, label1: 't', label2: 'c', chartH: 300, showCi: true }));
  assert.equal(count(html, 'ci-whisker'), 1);
  assert.ok(!html.includes('NaN'));
});

test('overall delta cells show interval and p; step cells show the interval and † only', () => {
  const html = table({});
  assert.equal(count(html, 'ci-cell'), 6);
  assert.ok(html.includes('[-2.0, +22.0] pp · p=0.096</div>'));
  assert.ok(html.includes('[-2.0, +22.0] pp †'));
  assert.ok(html.includes('p n/a'));          // undefined test on step 1
  assert.equal(html.split('p=').length - 1, 2); // only the two finite overall p-values
  assert.ok(!html.includes('NaN'));
});

test('colour only when the interval excludes 0, never on a conditional step', () => {
  const html = table({});
  // overall: step 1 and 2 cover 0 (grey), step 3 excludes 0 (coloured);
  // step column: step 1 covers 0, steps 2 and 3 are conditional (grey even
  // though step 3's interval excludes 0).
  assert.equal(count(html, 'delta-coloured'), 1);
  assert.equal(count(html, 'delta-grey'), 5);
  assert.ok(html.includes('†'));
});

test('the adjusted p-value is shown with the correction name', () => {
  const html = table({ result: { ci_level: 0.95, correction: 'holm' } });
  assert.ok(html.includes('p=0.004 · Holm p_adj=0.008'));
  assert.ok(html.includes('Holm-adjusted across steps'));
});

test('without intervals the table keeps the magnitude colouring and no CI cells', () => {
  const html = table({ showCi: false });
  assert.equal(count(html, 'ci-cell'), 0);
  assert.equal(count(html, 'delta-coloured') + count(html, 'delta-grey'), 0);
  assert.equal(count(html, 'ci-note'), 0);
});

test('relative delta uses group 2 as the base', () => {
  assert.equal(relativeDeltaPct(0.3, 0.2)?.toFixed(6), (50).toFixed(6));
  assert.equal(relativeDeltaPct(0.2, 0.3)?.toFixed(6), (-100 / 3).toFixed(6));
  assert.equal(relativeDeltaPct(0.1, 0), null);
  assert.equal(relativeDeltaPct(null, 0.2), null);
  const html = table({ showCi: false });
  assert.ok(html.includes('+50.0%'));   // cart: (0.3 - 0.2) / 0.2
  assert.ok(html.includes('+150.0%'));  // buy: (0.25 - 0.1) / 0.1
});

test('helpers treat null as no estimate', () => {
  assert.equal(ciExcludesZero(0.01, 0.2), true);
  assert.equal(ciExcludesZero(-0.2, -0.01), true);
  assert.equal(ciExcludesZero(-0.01, 0.2), false);
  assert.equal(ciExcludesZero(null, 0.2), false);
  assert.equal(formatPValue(null), 'p n/a');
  assert.equal(formatPValue(0.0004), 'p<0.001');
  assert.equal(formatPValue(0.25), 'p=0.250');
});

test('single funnel shows rate intervals and one whisker per step', () => {
  const steps = [
    { step: 'home', unique_paths: 100, conversion_rate: 1, step_conversion_rate: 1,
      conversion_rate_ci_low: 0.96, conversion_rate_ci_high: 1, step_conversion_rate_ci_low: 0.96, step_conversion_rate_ci_high: 1 },
    { step: 'buy', unique_paths: 0, conversion_rate: 0, step_conversion_rate: 0,
      conversion_rate_ci_low: 0, conversion_rate_ci_high: 0.04, step_conversion_rate_ci_low: null, step_conversion_rate_ci_high: null },
  ];
  const chart = renderToStaticMarkup(React.createElement(FunnelChart,
    { steps, hasDiff: false, label1: '', label2: '', chartH: 300, showCi: true }));
  assert.equal(count(chart, 'ci-whisker'), 2);
  const html = renderToStaticMarkup(React.createElement(FunnelTable,
    { steps, hasDiff: false, label1: '', label2: '', result: { ci_level: 0.9, total_paths: 100 }, showCi: true }));
  assert.equal(count(html, 'ci-cell'), 4);
  assert.ok(html.includes('[n/a, n/a]'));
  assert.ok(html.includes('90% intervals'));
});
