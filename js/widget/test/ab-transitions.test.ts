import test from 'node:test';
import assert from 'node:assert/strict';
import React from 'react';
import { createRequire } from 'node:module';
import {
  AB_BLUE, AB_MUTED, AB_RED, abEdgeClass, abEdgeIndex, abEdgeLook, abLegendText, abPayloadApplies,
  abTooltipLines, formatP, parseAbTransitions, type AbTransitionsPayload,
} from '../../viz-core/src/utils/ab-transitions';
import { DiffBreakdownTooltip } from '../../viz-core/src/components/TransitionGraph/DiffBreakdownTooltip';
import { DiffLegendRows } from '../../viz-core/src/components/TransitionGraph/GraphLegend';

const { renderToStaticMarkup } = createRequire(import.meta.url)('react-dom/server');

// Shaped as transitions_payload sends it (null where Python has NaN).
const payload: AbTransitionsPayload = {
  edge_weight: 'unique_paths',
  diff: ['exp_group', 'treatment', 'control'],
  correction: 'bh', alpha: 0.05, ci_level: 0.95, n_tested: 90, population: 'stream',
  group1_label: 'treatment', group2_label: 'control',
  edges: [
    { source: 'cart', target: 'cart', group1_value: 0.049, group2_value: 0, delta: 0.049089, ci_low: 0.043292,
      ci_high: 0.05555, p_adjusted: 1.46e-54, verdict: 'increase', support_warning: 'one_arm_only', conditional: false },
    { source: 'product_view', target: 'add_to_cart', group1_value: 0.287, group2_value: 0.643, delta: -0.355327,
      ci_low: -0.373638, ci_high: -0.336621, p_adjusted: 6.2e-269, verdict: 'decrease', support_warning: '' },
    { source: 'home', target: 'catalog', group1_value: 0.5, group2_value: 0.49, delta: 0.01, ci_low: -0.01,
      ci_high: 0.03, p_adjusted: 0.41, verdict: 'inconclusive', support_warning: '' },
    { source: 'payment', target: 'search', group1_value: 0.001, group2_value: 0, delta: 0.001, ci_low: null,
      ci_high: null, p_adjusted: null, verdict: 'insufficient_data', support_warning: 'one_arm_only' },
  ],
};
const base = { edgeSize: 2, edgeOpacity: 0.4, baseColor: '239, 68, 68', dashed: false };

test('payload parses from the traitlet string and rejects junk', () => {
  assert.equal(parseAbTransitions(JSON.stringify(payload))?.edges.length, 4);
  assert.equal(parseAbTransitions(payload)?.n_tested, 90);
  assert.equal(parseAbTransitions(''), null);
  assert.equal(parseAbTransitions('{not json'), null);
  assert.equal(parseAbTransitions({ edges: [] }), null);
});

test('the overlay applies only to its edge weight and diff', () => {
  const diff = ['exp_group', 'treatment', 'control'];
  assert.equal(abPayloadApplies(payload, 'unique_paths', diff), true);
  assert.equal(abPayloadApplies(payload, 'proba_out', diff), false);
  assert.equal(abPayloadApplies(payload, 'unique_paths', ['exp_group', 'control', 'treatment']), false);
  assert.equal(abPayloadApplies(payload, 'unique_paths', null), false);
  assert.equal(abPayloadApplies(null, 'unique_paths', diff), false);
});

test('edges are classed by verdict; untested and absent edges alike', () => {
  const index = abEdgeIndex(payload);
  assert.equal(abEdgeClass(index.get('cart|cart')), 'increase');
  assert.equal(abEdgeClass(index.get('product_view|add_to_cart')), 'decrease');
  assert.equal(abEdgeClass(index.get('home|catalog')), 'inconclusive');
  assert.equal(abEdgeClass(index.get('payment|search')), 'untested');
  assert.equal(abEdgeClass(index.get('nowhere|else')), 'untested');
});

test('significant edges are emphasised in their sign, others muted', () => {
  const up = abEdgeLook('increase', base);
  assert.equal(up.baseColor, AB_RED);
  assert.ok(up.edgeSize >= 3 && up.edgeOpacity >= 0.85 && !up.dashed);
  const down = abEdgeLook('decrease', { ...base, edgeSize: 8, baseColor: '1, 2, 3' });
  assert.equal(down.baseColor, AB_BLUE);
  assert.equal(down.edgeSize, 8);
  const grey = abEdgeLook('inconclusive', { ...base, edgeSize: 9, edgeOpacity: 1 });
  assert.equal(grey.baseColor, AB_MUTED);
  assert.ok(grey.edgeSize <= 2 && grey.edgeOpacity < 0.3 && !grey.dashed);
  const untested = abEdgeLook('untested', base);
  assert.ok(untested.dashed && untested.edgeOpacity < grey.edgeOpacity);
});

test('tooltip lines carry the delta, the interval, the adjusted p and the verdict', () => {
  const index = abEdgeIndex(payload);
  const lines = abTooltipLines(index.get('product_view|add_to_cart'), payload);
  const byLabel = Object.fromEntries(lines.map((l) => [l.label, l.value]));
  assert.equal(byLabel['delta'], '-35.53 pp');
  assert.equal(byLabel['95% interval'], '[-37.36, -33.66] pp');
  assert.equal(byLabel['adjusted p (bh)'], '< 0.0001');
  assert.equal(byLabel['verdict'], 'decrease');
  const loop = Object.fromEntries(abTooltipLines(index.get('cart|cart'), payload).map((l) => [l.label, l.value]));
  assert.equal(loop['support'], 'one_arm_only');
  const counts = abTooltipLines(index.get('home|catalog'), { ...payload, edge_weight: 'avg_per_path' });
  assert.equal(counts[0].value, '+0.010');
  assert.deepEqual(abTooltipLines(undefined, payload), [{ label: 'test', value: 'not in ab_transitions' }]);
  assert.equal(formatP(0.4123), '0.41');
  assert.equal(formatP(null), '-');
});

test('the diff tooltip renders the overlay rows', () => {
  const lines = abTooltipLines(abEdgeIndex(payload).get('cart|cart'), payload);
  const html = renderToStaticMarkup(React.createElement(DiffBreakdownTooltip, {
    segmentName: 'exp_group', value1Label: 'treatment', value2Label: 'control',
    group1Value: 0.049, group2Value: 0, diffValue: 0.049, extraLines: lines,
  }));
  assert.equal(html.split('data-testid="ab-tooltip-line"').length - 1, lines.length);
  assert.ok(html.includes('increase'));
  const plain = renderToStaticMarkup(React.createElement(DiffBreakdownTooltip, {
    segmentName: null, value1Label: 'a', value2Label: 'b', group1Value: 1, group2Value: 0, diffValue: 1,
  }));
  assert.ok(!plain.includes('ab-tooltip-line'));
});

test('the diff legend names the overlay and the family size', () => {
  const text = abLegendText(payload);
  assert.ok(text.includes('bh') && text.includes('90 tested edges') && text.includes('0.05'));
  const props = { rowStyle: {}, diffLabel1: 'treatment', diffLabel2: 'control' };
  const html = renderToStaticMarkup(React.createElement(DiffLegendRows, { ...props, abText: text }));
  assert.ok(html.includes('data-testid="ab-legend"') && html.includes('significantly more in treatment'));
  const plain = renderToStaticMarkup(React.createElement(DiffLegendRows, props));
  assert.ok(!plain.includes('ab-legend') && plain.includes('more in treatment'));
});
