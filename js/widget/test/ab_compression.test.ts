import test from 'node:test';
import assert from 'node:assert/strict';
import React from 'react';
import { createRequire } from 'node:module';
import {
  actionCounts, actionOf, beforeAfter, fmtMde, labelAt, mappingAt, panelGeometry, parseCompression, stepOf, stepText,
  xScale, type CompressionPayload, type PathStep, type Segment,
} from '../src/ab_compression_encoding';
import { CompressionBody, Header, PathPanel, type View } from '../src/ab_compression';

const { renderToStaticMarkup } = createRequire(import.meta.url)('react-dom/server');

// A path as `get_ab_compression` builds it (attrs["path"]), in the payload's shape.
const step = (s: Partial<PathStep>): PathStep => ({
  step: 0, operation: 'start', events: '', label: null, vocabulary: 8, cells: 30, testable: 20, untestable: 10,
  median_mde: 0.03, transitions: 1000, mass_retained: 1, artificial_transitions: 0, artificial_mass: 0,
  reason: 'the stream as it is', ...s,
});
const PATH: PathStep[] = [
  step({}),
  step({ step: 1, operation: 'merge_chain', events: 'checkout, pay', label: 'checkout+pay', vocabulary: 7, cells: 29, testable: 19, untestable: 10, mass_retained: 0.98, reason: 'P(next = pay | checkout) = 1.00' }),
  step({ step: 2, operation: 'other', events: 'r1', label: 'other', vocabulary: 7, cells: 28, testable: 20, untestable: 8, median_mde: 0.025, mass_retained: 0.98, reason: 'in 0.4% of paths' }),
  step({ step: 3, operation: 'drop', events: 'X', label: null, vocabulary: 6, cells: 26, testable: 19, untestable: 7, median_mde: null, mass_retained: 0.97, artificial_transitions: 1, artificial_mass: 3, reason: 'bridges' }),
];
const SEGMENT: Segment = {
  key: 'all', chosen_step: 2, path: PATH,
  events: ['X', 'cart', 'checkout', 'home', 'pay', 'r1'],
  path_share: [0.005, 0.7, 0.4, 1, 0.4, 0.004],
  changes: {
    X: [[0, 'X'], [3, null]], cart: [[0, 'cart']], checkout: [[0, 'checkout'], [1, 'checkout+pay']],
    home: [[0, 'home']], pay: [[0, 'pay'], [1, 'checkout+pay']], r1: [[0, 'r1'], [2, 'other']],
  },
  kept_reason: { cart: 'kept: not rare', home: 'kept: a path boundary event', X: 'kept: not yet' },
  homogeneity: { paths: 2000, events: 6, effective_vocabulary: 4.2, transition_entropy: 1.1, distinct_path_share: 0.01 },
};
const PAYLOAD: CompressionPayload = parseCompression(JSON.stringify({
  tool: 'get_ab_compression', screen: 'transitions', edge_weight: 'unique_paths', max_steps: null, unit: 'pp',
  target: 'testable', other: 'other', keep: null, by: null, by_pre_exposure: null, min_path_share: 0.01,
  chain_threshold: 0.8, synonym_threshold: 0.1, bridge_threshold: 0.9, max_events: null, min_support: 10,
  correction: 'bh', ci_level: 0.95, power: 0.8, population: 'assignment', horizon: null, path_col: 'user_id',
  group1_paths: 1000, group2_paths: 1000, segments: [SEGMENT],
}))!;
const view: View = { selected_step: -1, changed_only: true, segment: '' };

test('the payload parses; an empty or broken one is null', () => {
  assert.equal(PAYLOAD.segments.length, 1);
  assert.deepEqual(PAYLOAD.warnings, []);
  assert.equal(parseCompression('{}'), null);
  assert.equal(parseCompression(''), null);
  assert.equal(parseCompression('{bad'), null);
});

test('the selected step: the chosen one by default, within the path', () => {
  assert.equal(stepOf(SEGMENT, -1), 2);
  assert.equal(stepOf(SEGMENT, 1), 1);
  assert.equal(stepOf(SEGMENT, 99), 3);
});

test('the label at a step is the last change at or before it; the action follows the label', () => {
  assert.deepEqual(labelAt(SEGMENT, 'pay', 0), { label: 'pay', since: 0 });
  assert.deepEqual(labelAt(SEGMENT, 'pay', 2), { label: 'checkout+pay', since: 1 });
  assert.deepEqual(labelAt(SEGMENT, 'X', 2), { label: 'X', since: 0 });
  assert.deepEqual(labelAt(SEGMENT, 'X', 3), { label: null, since: 3 });
  assert.equal(actionOf('X', null, 'other'), 'drop');
  assert.equal(actionOf('r1', 'other', 'other'), 'other');
  assert.equal(actionOf('pay', 'checkout+pay', 'other'), 'merge');
  assert.equal(actionOf('cart', 'cart', 'other'), 'keep');
  assert.deepEqual(actionCounts(SEGMENT, 2, 'other'), { keep: 3, merge: 2, drop: 0, other: 1 });
  assert.deepEqual(actionCounts(SEGMENT, 3, 'other'), { keep: 2, merge: 2, drop: 1, other: 1 });
});

test('the mapping table: the changed events by the step that set them, with that step reason', () => {
  const rows = mappingAt(SEGMENT, 3, 'other', true);
  assert.deepEqual(rows.map((r) => [r.event, r.action, r.label, r.since]), [
    ['checkout', 'merge', 'checkout+pay', 1], ['pay', 'merge', 'checkout+pay', 1], ['r1', 'other', 'other', 2], ['X', 'drop', null, 3],
  ]);
  assert.equal(rows[0].reason, 'P(next = pay | checkout) = 1.00');
  assert.equal(rows[0].operation, 'merge_chain');
  const all = mappingAt(SEGMENT, 0, 'other', false);
  assert.equal(all.length, 6);
  assert.ok(all.every((r) => r.action === 'keep'));
  assert.equal(all.find((r) => r.event === 'home')!.reason, 'kept: a path boundary event');
});

test('the path chart: vocabulary on x (largest on the right), one panel per measure, artificial steps flagged', () => {
  const x = xScale(PATH, 300, 8);
  assert.equal(x(8), 292);
  assert.equal(x(6), 8);
  const testable = panelGeometry(PATH, 'testable', 300, 64, 8);
  assert.equal(testable.points.length, 4);
  assert.deepEqual(testable.points.map((p) => p.artificial), [false, false, false, true]);
  // more testable cells sit higher
  assert.ok((testable.points[0].y as number) < (testable.points[1].y as number));
  assert.ok(testable.path.startsWith('M292.0,'));
  const mde = panelGeometry(PATH, 'median_mde', 300, 64, 8);
  assert.equal(mde.points[3].y, null);  // no MDE at that step: no point
  assert.equal(mde.path.split('L').length, 3);
  const mass = panelGeometry(PATH, 'mass_retained', 300, 64, 8);
  assert.equal(mass.max, 1);
});

test('numbers: the MDE in percentage points, the before / after rows, a step in words', () => {
  assert.equal(fmtMde(0.0312, 'pp'), '3.12 pp');
  assert.equal(fmtMde(0.0312, 'per_path'), '0.031');
  assert.equal(fmtMde(null, 'pp'), 'n/a');
  const rows = beforeAfter(PATH, 2, 'pp');
  assert.deepEqual(rows[0], ['vocabulary', '8', '7']);
  assert.deepEqual(rows[2], ['testable cells', '20', '20']);
  assert.deepEqual(rows[4], ['median MDE', '3.00 pp', '2.50 pp']);
  assert.equal(stepText(PATH[1]), 'step 1: merge a chain checkout, pay → checkout+pay');
  assert.equal(stepText(PATH[0]), 'step 0: the stream as it is');
});

test('the body draws the chart, the slider at the chosen step, the counts and the table', () => {
  const html = renderToStaticMarkup(React.createElement(CompressionBody, { payload: PAYLOAD, segment: SEGMENT, view, onStep: () => {} }));
  for (const id of ['abc-panel-testable', 'abc-panel-median_mde', 'abc-panel-mass_retained', 'abc-slider', 'abc-before-after', 'abc-mapping']) {
    assert.match(html, new RegExp(`data-testid="${id}"`), id);
  }
  assert.match(html, /data-testid="abc-step"[^>]*>2 \/ 3</);
  assert.equal((html.match(/data-testid="abc-mapping-row"/g) ?? []).length, 3);
  assert.match(html, /data-event="r1" data-action="other"/);
  // one point per panel; the MDE panel has none at step 3 (no MDE there)
  assert.equal((html.match(/data-artificial="true"/g) ?? []).length, 2);
  assert.match(html, /step=2\)\)/);
  assert.match(html, /3 kept, 2 merged, 0 dropped, 1 relabelled other/);
  // another step, every event listed
  const all = renderToStaticMarkup(React.createElement(CompressionBody, {
    payload: PAYLOAD, segment: SEGMENT, view: { ...view, selected_step: 3, changed_only: false }, onStep: () => {},
  }));
  assert.equal((all.match(/data-testid="abc-mapping-row"/g) ?? []).length, 6);
  assert.match(all, /data-event="X" data-action="drop"/);
  // step 0: nothing changes
  const none = renderToStaticMarkup(React.createElement(CompressionBody, {
    payload: PAYLOAD, segment: SEGMENT, view: { ...view, selected_step: 0 }, onStep: () => {},
  }));
  assert.match(none, /data-testid="abc-mapping-empty"/);
});

test('the header: the screen, the population, the chosen step, arm-blindness and homogeneity', () => {
  const html = renderToStaticMarkup(React.createElement(Header, { payload: PAYLOAD, segment: SEGMENT }));
  assert.match(html, /transition screen, unique_paths/);
  assert.match(html, /ITT: every path of the assignment table/);
  assert.match(html, /chosen step 2 of 3/);
  assert.match(html, /Arm-blind/);
  assert.match(html, /effective vocabulary 4\.2 of 6 events/);
  const panel = renderToStaticMarkup(React.createElement(PathPanel, {
    path: PATH, measure: 'testable', title: 'Testable cells', unit: 'pp', selected: 1, chosen: 2, onSelect: () => {},
  }));
  assert.equal((panel.match(/data-testid="abc-chosen"/g) ?? []).length, 1);
  assert.match(panel, /19 of 29/);
});

test('a path of one step (nothing qualifies) draws without NaN', () => {
  const changes: Segment['changes'] = Object.fromEntries(SEGMENT.events.map((e) => [e, [[0, e]] as [number, string | null][]]));
  const one: Segment = { ...SEGMENT, chosen_step: 0, path: [PATH[0]], changes };
  const html = renderToStaticMarkup(React.createElement(CompressionBody, {
    payload: { ...PAYLOAD, segments: [one] }, segment: one, view, onStep: () => {},
  }));
  assert.doesNotMatch(html, /NaN/);
  assert.match(html, /data-testid="abc-mapping-empty"/);
  assert.match(html, /data-testid="abc-step"[^>]*>0 \/ 0</);
});
