import test from 'node:test';
import assert from 'node:assert/strict';
import React from 'react';
import { createRequire } from 'node:module';
import {
  AB_BLUE, AB_MUTED, AB_RED, abEdgeClass, abEdgeIndex, abEdgeLook, abIsFiltered, abLegendText, abPayloadApplies,
  abTooltipLines, abTooltipValues, formatAbDelta, formatAbValue, formatP, parseAbTransitions,
  type AbTransitionsPayload,
} from '../../viz-core/src/utils/ab-transitions';
import { DiffBreakdownTooltip } from '../../viz-core/src/components/TransitionGraph/DiffBreakdownTooltip';
import { DiffLegendRows } from '../../viz-core/src/components/TransitionGraph/GraphLegend';

const { renderToStaticMarkup } = createRequire(import.meta.url)('react-dom/server');

// Shaped as transitions_payload sends it (null where Python has NaN).
const payload: AbTransitionsPayload = {
  edge_weight: 'unique_paths',
  diff: ['exp_group', 'treatment', 'control'],
  correction: 'bh', alpha: 0.05, ci_level: 0.95, n_tested: 90, n_rows: 4, population: 'stream',
  horizon: null, path_col: 'user_id', group1_label: 'treatment', group2_label: 'control',
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

test('the overlay applies only to its edge weight, diff and path column', () => {
  const diff = ['exp_group', 'treatment', 'control'];
  assert.equal(abPayloadApplies(payload, 'unique_paths', diff), true);
  assert.equal(abPayloadApplies(payload, 'proba_out', diff), false);
  assert.equal(abPayloadApplies(payload, 'unique_paths', ['exp_group', 'control', 'treatment']), false);
  assert.equal(abPayloadApplies(payload, 'unique_paths', null), false);
  assert.equal(abPayloadApplies(null, 'unique_paths', diff), false);
  // the sidebar's path column
  assert.equal(abPayloadApplies(payload, 'unique_paths', diff, 'user_id'), true);
  assert.equal(abPayloadApplies(payload, 'unique_paths', diff, 'session_id'), false);
  // an id-list diff is compared as the raw lists
  const ids = { ...payload, diff: [[1, 2, 3], [4, 5]] };
  assert.equal(abPayloadApplies(ids, 'unique_paths', [[1, 2, 3], [4, 5]]), true);
  assert.equal(abPayloadApplies(ids, 'unique_paths', [[4, 5], [1, 2, 3]]), false);
  // integer segment values come back from the sidebar as strings
  const ints = { ...payload, diff: ['cohort', 1, 2] };
  assert.equal(abPayloadApplies(ints, 'unique_paths', ['cohort', '1', '2']), true);
  assert.equal(abPayloadApplies(ints, 'unique_paths', ['cohort', '2', '1']), false);
});

test('edges are classed by verdict; untested and absent edges apart', () => {
  const index = abEdgeIndex(payload);
  assert.equal(abEdgeClass(index.get('cart|cart')), 'increase');
  assert.equal(abEdgeClass(index.get('product_view|add_to_cart')), 'decrease');
  assert.equal(abEdgeClass(index.get('home|catalog')), 'inconclusive');
  assert.equal(abEdgeClass(index.get('payment|search')), 'untested');
  assert.equal(abEdgeClass(index.get('nowhere|else')), 'absent');
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
  const absent = abEdgeLook('absent', base);
  assert.ok(!absent.dashed && absent.edgeOpacity < untested.edgeOpacity && absent.baseColor === AB_MUTED);
});

test('tooltip lines carry the delta, the interval, the adjusted p and the verdict', () => {
  const index = abEdgeIndex(payload);
  const lines = abTooltipLines(index.get('product_view|add_to_cart'), payload);
  const byLabel = Object.fromEntries(lines.map((l) => [l.label, l.value]));
  assert.equal(byLabel['delta'], undefined); // the diff row carries it, see abTooltipValues
  assert.equal(byLabel['95% interval'], '[-37.36, -33.66] pp');
  assert.equal(byLabel['adjusted p (bh)'], '< 0.0001');
  assert.equal(byLabel['verdict'], 'decrease');
  const loop = Object.fromEntries(abTooltipLines(index.get('cart|cart'), payload).map((l) => [l.label, l.value]));
  assert.equal(loop['support'], 'one_arm_only');
  const counts = abTooltipLines(index.get('home|catalog'), { ...payload, edge_weight: 'avg_per_path' });
  assert.equal(counts[0].value, '[-0.010, +0.030]');
  assert.equal(abTooltipLines(undefined, payload)[0].label, 'test');
  assert.ok(abTooltipLines(undefined, payload)[0].value.startsWith('no row in ab_transitions'));
  assert.equal(formatP(0.4123), '0.41');
  assert.equal(formatP(null), '-');
});

test('under the overlay the tooltip shows the test values in their units', () => {
  const stat = abEdgeIndex(payload).get('product_view|add_to_cart');
  const values = abTooltipValues(stat, payload);
  assert.deepEqual([values?.group1, values?.group2, values?.delta], [0.287, 0.643, -0.355327]);
  assert.ok(values?.subtitle.includes('share of paths'));
  assert.equal(abTooltipValues(undefined, payload), null);
  assert.equal(formatAbValue(0.287, payload), '28.70 %');
  assert.equal(formatAbDelta(-0.355327, payload), '-35.53 pp');
  const perPath = { ...payload, edge_weight: 'avg_per_path' };
  assert.equal(formatAbValue(1.5, perPath), '1.500');
  assert.equal(formatAbDelta(0.25, perPath), '+0.250');
  const html = renderToStaticMarkup(React.createElement(DiffBreakdownTooltip, {
    segmentName: 'exp_group', value1Label: 'treatment', value2Label: 'control',
    group1Value: values!.group1, group2Value: values!.group2, diffValue: values!.delta,
    formatValue: (v: number | null) => formatAbValue(v, payload),
    formatDiff: (v: number | null) => formatAbDelta(v, payload),
  }));
  assert.ok(html.includes('28.70 %') && html.includes('64.30 %') && html.includes('-35.53 pp'));
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
  assert.ok(text.includes('faint not in the frame') && !text.includes('ITT') && !text.includes('filtered'));
  const itt = abLegendText({ ...payload, population: 'assignment', horizon: '28D' });
  assert.ok(itt.includes('ITT') && itt.includes('horizon 28D'));
  // one tested edge is singular
  const one = abLegendText({ ...payload, n_tested: 1 });
  assert.ok(one.includes('(bh, 1 tested edge)') && !one.includes('1 tested edges'));
  const filtered = { ...payload, n_rows: 92 };
  assert.equal(abIsFiltered(filtered), true);
  assert.equal(abIsFiltered(payload), false);
  assert.ok(abLegendText(filtered).includes('frame filtered to 4 of 92 rows'));
  const props = { rowStyle: {}, diffLabel1: 'treatment', diffLabel2: 'control' };
  const html = renderToStaticMarkup(React.createElement(DiffLegendRows, { ...props, abText: text }));
  assert.ok(html.includes('data-testid="ab-legend"') && html.includes('significantly more in treatment'));
  const plain = renderToStaticMarkup(React.createElement(DiffLegendRows, props));
  assert.ok(!plain.includes('ab-legend') && plain.includes('more in treatment'));
});
