import test from 'node:test';
import assert from 'node:assert/strict';
import React from 'react';
import { createRequire } from 'node:module';
import {
  ForestPlot, PeriodChart, ScorecardTable, ScorecardView, StrataView, SUPPORT_NOTES, familyText,
  fmtDelta, fmtInterval, fmtMagnitude, fmtRel, fmtValue, markerStyle, verdictColor, pointTitle,
  type ABRow, type ABResult,
} from '../src/ab_test_plot';
import { AB_METRIC_NAMES, armColumnChoices, isValidDuration, validateAbMetric } from '../src/ab_test';

const { renderToStaticMarkup } = createRequire(import.meta.url)('react-dom/server');
const count = (html: string, id: string) => html.split(`data-testid="${id}"`).length - 1;

const BIN = { kind: 'binary', unit: '' };
const COUNT = { kind: 'count', unit: '' };
const SECONDS = { kind: 'value', unit: 'seconds' };

// Rows shaped as ab_test_data sends them (null where Python has NaN).
const conversion: ABRow = {
  metric: 'conversion', group1_paths: 4828, group2_paths: 4905,
  group1_value: 0.25, group2_value: 0.243833, delta: 0.006167, delta_ci_low: -0.010964, delta_ci_high: 0.023301,
  rel_delta: 0.025293, rel_delta_ci_low: -0.043445, rel_delta_ci_high: 0.09897,
  p_value: 0.4805, p_value_adjusted: 0.4805, mde: 0.024491, verdict: 'inconclusive', test: 'z_test', support_warning: '',
};
const revenue: ABRow = {
  ...conversion, metric: 'revenue', group1_value: 20.69, group2_value: 19.15, delta: 1.54, delta_ci_low: 0.29,
  delta_ci_high: 3.37, p_value: 0.01, p_value_adjusted: 0.01, verdict: 'increase', test: 'welch_t_test',
};
const loop: ABRow = {
  ...conversion, metric: 'loop', delta: -0.004, delta_ci_low: -0.006, delta_ci_high: -0.002,
  p_value: null, p_value_adjusted: null, mde: null, verdict: 'insufficient_data', support_warning: 'one_arm_only',
};
const result: ABResult = {
  overall: [conversion, revenue, loop],
  metric_kinds: { conversion: 'binary', revenue: 'count', loop: 'binary' },
  metric_units: { conversion: '', revenue: '', loop: '' },
  group1_label: 'treatment', group2_label: 'control', ci_level: 0.95, correction: 'holm',
};

test('verdict colours: increase red, decrease blue (the diff colours), the rest grey', () => {
  assert.equal(verdictColor('increase'), '#ef4444');
  assert.equal(verdictColor('decrease'), '#3b82f6');
  assert.equal(verdictColor('inconclusive'), '#9ca3af');
  assert.equal(verdictColor('insufficient_data'), '#9ca3af');
  assert.equal(verdictColor('descriptive'), '#9ca3af');
  assert.equal(verdictColor(undefined), '#9ca3af');
});

test('hollow markers: insufficient_data and incomplete periods; descriptive rows are squares', () => {
  assert.deepEqual(markerStyle({ verdict: 'increase' }), { fill: '#ef4444', stroke: '#ef4444', hollow: false, shape: 'circle' });
  assert.equal(markerStyle({ verdict: 'inconclusive' }).hollow, false);
  assert.deepEqual(markerStyle({ verdict: 'insufficient_data' }), { fill: '#ffffff', stroke: '#9ca3af', hollow: true, shape: 'circle' });
  const incomplete = markerStyle({ verdict: 'decrease', period_complete: false });
  assert.equal(incomplete.hollow, true);
  assert.equal(incomplete.stroke, '#3b82f6');
  assert.equal(markerStyle({ verdict: 'decrease', period_complete: true }).hollow, false);
  assert.equal(markerStyle({ verdict: 'descriptive' }).shape, 'square');
});

test('0/1 metrics: values in %, deltas and intervals in pp', () => {
  assert.equal(fmtValue(0.25, BIN), '25.0%');
  assert.equal(fmtDelta(0.006167, BIN), '+0.62 pp');
  assert.equal(fmtDelta(-0.0025, BIN), '-0.25 pp');
  assert.equal(fmtInterval(-0.010964, 0.023301, BIN), '[-1.10, +2.33] pp');
  assert.equal(fmtMagnitude(0.024491, BIN), '2.45 pp');
});

test('other metrics stay in their unit; seconds pick one time unit for a delta and its interval', () => {
  assert.equal(fmtValue(20.694886, COUNT), '20.69');
  assert.equal(fmtDelta(1.539942, COUNT), '+1.54');
  assert.equal(fmtInterval(-0.286949, 3.366833, COUNT), '[-0.287, +3.37]');
  assert.equal(fmtValue(2272731.6, SECONDS), '26.30 d');
  // the interval reaches 17.9 h, so the delta is written in hours too
  assert.equal(fmtDelta(-5622.4, SECONDS, -64413.6, 53168.8), '-1.56 h');
  assert.equal(fmtInterval(-64413.6, 53168.8, SECONDS, -5622.4), '[-17.89, +14.77] h');
  assert.equal(fmtDelta(30, SECONDS), '+30.00 s');
});

test('null (NaN in Python) reads n/a, never 0', () => {
  assert.equal(fmtValue(null, BIN), 'n/a');
  assert.equal(fmtDelta(null, COUNT), 'n/a');
  assert.equal(fmtInterval(null, 0.1, BIN), '[n/a, +10.00] pp');
  assert.equal(fmtRel(null), 'n/a');
  assert.equal(fmtMagnitude(undefined, BIN), 'n/a');
});

test('the relative delta is in the hover text of a plotted point', () => {
  const title = pointTitle('conversion', conversion, BIN);
  assert.ok(title.includes('Δ +0.62 pp [-1.10, +2.33] pp'));
  assert.ok(title.includes('relative +2.5% [-4.3%, +9.9%]'));
  assert.ok(title.includes('inconclusive'));
});

test('forest plot: one point and whisker per row, colours by verdict, insufficient_data hollow', () => {
  const html = renderToStaticMarkup(React.createElement(ForestPlot, {
    rows: result.overall, labelOf: (r: ABRow) => r.metric,
    formatOf: (r: ABRow) => ({ kind: result.metric_kinds![r.metric], unit: '' }),
  }));
  assert.equal(count(html, 'forest-row'), 3);
  assert.equal(count(html, 'forest-point'), 3);
  assert.equal(count(html, 'forest-whisker'), 3);
  assert.ok(html.includes('data-verdict="increase" data-hollow="false"'));
  assert.ok(html.includes('data-verdict="insufficient_data" data-hollow="true"'));
  assert.ok(html.includes('fill="#ef4444"'));
  assert.ok(!html.includes('NaN'));
});

test('forest plot: a row without an estimate draws neither point nor whisker', () => {
  const empty: ABRow = { ...conversion, delta: null, delta_ci_low: null, delta_ci_high: null };
  const html = renderToStaticMarkup(React.createElement(ForestPlot, {
    rows: [empty], labelOf: (r: ABRow) => r.metric, formatOf: () => BIN,
  }));
  assert.equal(count(html, 'forest-point'), 0);
  assert.equal(count(html, 'forest-whisker'), 0);
  assert.ok(!html.includes('NaN'));
});

test('scorecard: one row per metric with a data-event anchor; pp for 0/1 metrics', () => {
  const html = renderToStaticMarkup(React.createElement(ScorecardTable, { rows: result.overall, result }));
  assert.equal(count(html, 'scorecard-row'), 3);
  assert.ok(html.includes('data-event="conversion"'));
  assert.ok(html.includes('+0.62 pp'));
  assert.ok(html.includes('[-1.10, +2.33] pp'));
  assert.ok(html.includes('24.4%'));
  assert.equal(count(html, 'support-warning'), 1);
  assert.ok(html.includes('>treatment<') && html.includes('>control<'));
});

test('reading: a column only when the rows carry it, the full sentence on hover', () => {
  const without = renderToStaticMarkup(React.createElement(ScorecardTable, { rows: result.overall, result }));
  assert.equal(count(without, 'reading-cell'), 0);
  assert.ok(!without.includes('>Reading<'));
  const reading = 'interval [-1.10, +2.33] pp includes 0 after holm (p 0.48)';
  const rows = result.overall!.map(r => ({ ...r, reading: r.metric === 'conversion' ? reading : '' }));
  const withReading = renderToStaticMarkup(React.createElement(ScorecardTable, { rows, result }));
  assert.ok(withReading.includes('>Reading<'));
  assert.equal(count(withReading, 'reading-cell'), 1);
  assert.ok(withReading.includes(`title="${reading}"`));
});

test('periods: incomplete periods hollow, interval band over consecutive rows', () => {
  const rows: ABRow[] = [
    { ...conversion, period: '2026-03-02/2026-03-08', period_complete: true },
    { ...revenue, metric: 'conversion', period: '2026-03-09/2026-03-15', period_complete: true },
    { ...conversion, period: '2026-03-16/2026-03-22', period_complete: false },
  ];
  const html = renderToStaticMarkup(React.createElement(PeriodChart, { rows, format: BIN }));
  assert.equal(count(html, 'period-point'), 3);
  assert.equal(html.split('data-hollow="true"').length - 1, 1);
  assert.ok(html.includes('data-complete="false"'));
  assert.equal(count(html, 'period-band'), 1);
  assert.ok(html.includes('2026-03-09'));
  assert.ok(!html.includes('NaN'));
});

test('strata: heterogeneity and balance in the header, exploratory rows marked', () => {
  const strataRows: ABRow[] = ['desktop', 'mobile'].map(stratum => ({
    ...conversion, stratum, group1_share: 0.4, group2_share: 0.41, balance_p_value: 0.45,
    heterogeneity_p_value: 0.94, heterogeneity_strata_used: 2, stratum_vs_rest_p_value: 0.9,
    exploratory: true, strata_pre_exposure_source: 'not_pre_exposure',
  }));
  const html = renderToStaticMarkup(React.createElement(StrataView, {
    result: { ...result, strata: strataRows, strata_col: 'platform' },
  }));
  assert.equal(count(html, 'strata-row'), 2);
  assert.equal(html.split('data-exploratory="true"').length - 1, 2);
  assert.equal(count(html, 'strata-exploratory'), 1);
  assert.ok(html.includes('heterogeneity p 0.940'));
  assert.ok(html.includes('balance p 0.450'));
});

test('horizon must be a duration with a unit', () => {
  for (const ok of ['14D', '28 days', '12h', 'W', '2W', '30min', '1.5h']) assert.ok(isValidDuration(ok), ok);
  for (const bad of ['', '14', 'abc', '14 parsecs', '-3D']) assert.ok(!isValidDuration(bad), bad);
});

test('metric editor offers one-number-per-path metrics and refuses what get_ab_test refuses', () => {
  assert.ok(!AB_METRIC_NAMES.some(m => m.endsWith('_bulk')));
  assert.ok(!AB_METRIC_NAMES.includes('first_event_time'));
  assert.ok(AB_METRIC_NAMES.includes('column_sum'));
  assert.equal(validateAbMetric({ metric: 'has_event', metric_args: { event: 'buy' } }, []), null);
  assert.ok(validateAbMetric({ metric: 'event_count_bulk' }, []));
  assert.ok(validateAbMetric({ metric: 'has_event', metric_args: { event: 'buy' }, denominator: { metric: 'length' } }, []));
  assert.equal(validateAbMetric({ metric: 'column_sum', metric_args: { column: 'v' }, denominator: { metric: 'length' } }, []), null);
  assert.ok(validateAbMetric({ metric: 'column_sum', metric_args: {} }, []));
  assert.ok(validateAbMetric({ name: 'x', metric: 'length' }, ['x', 'x']));
});

test('every support_warning value has a tooltip, no_data included', () => {
  for (const warning of ['no_data', 'one_arm_only', 'invalid_replicates', 'low_support']) {
    assert.ok(SUPPORT_NOTES[warning], warning);
  }
  const html = renderToStaticMarkup(React.createElement(ScorecardTable, {
    rows: [{ ...loop, support_warning: 'no_data' }], result,
  }));
  assert.match(html, /title="No path of either arm has the behaviour/);
});

test('the interval note names the correction family of correction_scope', () => {
  assert.equal(familyText(undefined), "within each metric's rows");
  assert.equal(familyText('all'), 'across every row of every metric');
  const all = renderToStaticMarkup(React.createElement(ScorecardView, { result: { ...result, correction_scope: 'all' } }));
  assert.match(all, /holm across every row of every metric/);
  const metric = renderToStaticMarkup(React.createElement(ScorecardView, { result }));
  assert.match(metric, /holm within each metric&#x27;s rows/);
});

test('an assignment table adds its arm column to the arm choices', () => {
  assert.deepEqual(armColumnChoices(['platform'], ''), ['platform']);
  assert.deepEqual(armColumnChoices(['platform'], 'arm,assigned_at'), ['arm', 'platform']);
  assert.deepEqual(armColumnChoices(['arm', 'platform'], 'arm'), ['arm', 'platform']);
});
