import test from 'node:test';
import assert from 'node:assert/strict';
import React from 'react';
import { createRequire } from 'node:module';
import {
  MATRIX_BLUE, MATRIX_RED, MAX_ALPHA, OUTLINE_BLACK, OUTLINE_GREY, TEXT_MUTED,
  axisStats, backgroundAlpha, cellBackground, cellId, cellLook, cellStatus, cellText, crossImage, decimalsFor,
  decodeColumn, decodeFrame, deltaSign, familyText, fmtArmValue, fmtP, fmtSigned, indexRows, intervalNote, intervalOf, keepsSignificance,
  orderKeys, parsePayload, populationText, readingHint, scaleMax, unitInfo,
  type EncodedColumn, type EncodedFrame, type MatrixPayload, type MatrixRow,
} from '../src/ab_matrix_encoding';
import { Legend, Matrix, type View } from '../src/ab_matrix';
import { AB_BLUE, AB_RED, abEdgeClass } from '../../viz-core/src/utils/ab-transitions';

const { renderToStaticMarkup } = createRequire(import.meta.url)('react-dom/server');

// Rows shaped as the widget sends them (scorecard_records: null where Python has NaN).
const sig: MatrixRow = {
  source: 'add_to_cart', target: 'cart', group1_value: 0.2131, group2_value: 0.5339, delta: -0.32079,
  delta_ci_low: -0.33877, delta_ci_high: -0.30249, p_value: 1e-200, p_value_adjusted: 1e-198,
  verdict: 'decrease', group1_edge_paths: 1029, group2_edge_paths: 2619, group1_paths: 4828, group2_paths: 4905,
  reading: 'over the whole observation period: ...',
};
const up: MatrixRow = {
  source: 'cart', target: 'cart', delta: 0.0491, delta_ci_low: 0.0433, delta_ci_high: 0.0555,
  p_value_adjusted: 2e-50, verdict: 'increase',
};
const ns: MatrixRow = {
  source: 'checkout_start', target: 'payment', delta: 0.0006, delta_ci_low: -0.0176, delta_ci_high: 0.0187,
  p_value: 0.9, p_value_adjusted: 0.95, verdict: 'inconclusive',
};
const untested: MatrixRow = {
  source: 'checkout_start', target: 'search', delta: 0.0008, delta_ci_low: -0.0004, delta_ci_high: 0.0023,
  p_value: null, p_value_adjusted: null, verdict: 'insufficient_data', support_warning: 'one_arm_only',
};
// a constant row: tested by the rule, no spread, so no p-value (read off the column, not the verdict)
const constant: MatrixRow = {
  source: 'path_start', target: 'home', delta: 0, delta_ci_low: 0, delta_ci_high: 0,
  p_value: null, p_value_adjusted: null, verdict: 'inconclusive',
};

/** `encode_column` of widgets/_ab_matrix.py: levels and codes when at most half
 *  the values are distinct, the levels alone for one value, else the values. */
function encodeColumn(values: unknown[]): EncodedColumn {
  const levels: unknown[] = [];
  const codes = values.map((v) => {
    const i = levels.findIndex((l) => JSON.stringify(l) === JSON.stringify(v));
    return i >= 0 ? i : levels.push(v) - 1;
  });
  if (!values.length || 2 * levels.length > values.length) return { values };
  return levels.length === 1 ? { levels } : { levels, codes };
}

/** `encode_frame`: every key of the rows, null where a row lacks it. */
function encodeFrame(rows: MatrixRow[]): EncodedFrame {
  const names = [...new Set(rows.flatMap((r) => Object.keys(r)))];
  return {
    length: rows.length,
    columns: names.map((name) => ({ name, ...encodeColumn(rows.map((r) => r[name] ?? null)) })),
  };
}

const ROWS = [sig, up, ns, untested, constant];
const REASONS = [null, null, null, 'pooled support 8 < min_support 10', 'no p-value: no spread'];
const payload: MatrixPayload = parsePayload(JSON.stringify({
  tool: 'get_ab_transitions', frame: encodeFrame(ROWS), untested_reason: encodeColumn(REASONS),
  row_keys: ['path_start', 'add_to_cart', 'cart', 'checkout_start'],
  col_keys: ['cart', 'home', 'payment', 'search'],
  group1_label: 'treatment', group2_label: 'control', alpha: 0.05, ci_level: 0.95, correction: 'bh',
  n_tested: 3, n_rows: 5, population: 'stream', horizon: null, path_col: 'user_id', min_support: 10,
  simultaneous: false, warnings: [], unit: 'pp', conditional: false, edge_weight: 'unique_paths', edges: 'all',
}))!;

test('the frame travels column by column and decodes to its rows', () => {
  // as widgets/_ab_matrix.py's encode_frame sends it: plain, dictionary, constant
  const frame: EncodedFrame = {
    length: 4,
    columns: [
      { name: 'source', levels: ['home', 'cart'], codes: [0, 0, 1, 1] },
      { name: 'delta', values: [0.1, null, -0.0, 0.25] },
      { name: 'edge_weight', levels: ['unique_paths'] },
    ],
  };
  assert.deepEqual(decodeFrame(frame), [
    { source: 'home', delta: 0.1, edge_weight: 'unique_paths' },
    { source: 'home', delta: null, edge_weight: 'unique_paths' },
    { source: 'cart', delta: -0.0, edge_weight: 'unique_paths' },
    { source: 'cart', delta: 0.25, edge_weight: 'unique_paths' },
  ]);
  assert.deepEqual(Object.keys(decodeFrame(frame)[0]), ['source', 'delta', 'edge_weight']);
  assert.deepEqual(decodeColumn({ levels: [null] }, 3), [null, null, null]);
  assert.deepEqual(decodeColumn(undefined, 2), [null, null]);
  assert.deepEqual(decodeFrame({ length: 0, columns: [{ name: 'delta', values: [] }] }), []);
  // the payload's rows and reasons are the rows and reasons encoded
  assert.equal(payload.rows.length, ROWS.length);
  assert.equal(payload.rows[0].reading, sig.reading);
  assert.equal(payload.rows[3].support_warning, 'one_arm_only');
  assert.equal(payload.rows[2].group1_value, null);
  assert.deepEqual(payload.untested_reason, REASONS);
  // a result without a frame (the "{}" of an error) is no payload
  assert.equal(parsePayload('{}'), null);
  assert.equal(parsePayload(JSON.stringify({ rows: ROWS, row_keys: [], col_keys: [] })), null);
});

test('the hues are the diff palette of the graph overlay and the step matrix', () => {
  assert.equal(MATRIX_RED, AB_RED);
  assert.equal(MATRIX_BLUE, AB_BLUE);
});

test('outline class: black when adjusted p < alpha, grey + cross when tested, dashed + cross when not, none when empty', () => {
  assert.equal(cellStatus(sig, 0.05), 'significant');
  assert.equal(cellStatus(up, 0.05), 'significant');
  assert.equal(cellStatus(ns, 0.05), 'not_significant');
  assert.equal(cellStatus(untested, 0.05), 'untested');
  assert.equal(cellStatus(constant, 0.05), 'untested');
  assert.equal(cellStatus(undefined, 0.05), 'empty');
  assert.equal(cellStatus({ ...ns, p_value_adjusted: 0.05 }, 0.05), 'not_significant');
  assert.equal(cellStatus({ ...ns, p_value_adjusted: 0.049 }, 0.05), 'significant');

  assert.deepEqual(cellLook('significant'), { outlineColor: OUTLINE_BLACK, outlineStyle: 'solid', cross: false, textColor: '#111827' });
  assert.deepEqual(cellLook('not_significant'), { outlineColor: OUTLINE_GREY, outlineStyle: 'solid', cross: true, textColor: '#111827' });
  assert.deepEqual(cellLook('untested'), { outlineColor: OUTLINE_GREY, outlineStyle: 'dashed', cross: true, textColor: TEXT_MUTED });
  const empty = cellLook('empty');
  assert.equal(empty.outlineColor, null);
  assert.equal(empty.cross, false);
  assert.match(crossImage(), /to top right.*to bottom right/);
});

test('the outline agrees with the graph overlay on every edge', () => {
  for (const row of payload.rows) {
    const edge = {
      source: row.source!, target: row.target!, group1_value: null, group2_value: null, delta: row.delta ?? null,
      ci_low: null, ci_high: null, p_adjusted: row.p_value_adjusted ?? null, verdict: row.verdict as any,
    };
    const cls = abEdgeClass(edge as any);
    const status = cellStatus(row, 0.05);
    const expected = status === 'significant' ? ((row.delta as number) > 0 ? 'increase' : 'decrease')
      : status === 'not_significant' ? 'inconclusive' : 'untested';
    assert.equal(cls, expected, `${row.source}->${row.target}`);
  }
  assert.equal(abEdgeClass(undefined), 'absent');
  assert.equal(cellStatus(undefined, 0.05), 'empty');
});

test('background: red for a positive delta, blue for a negative one, opacity proportional to |delta|', () => {
  assert.equal(deltaSign(0.1), 'positive');
  assert.equal(deltaSign(-0.1), 'negative');
  assert.equal(deltaSign(0), 'zero');
  assert.equal(deltaSign(null), 'none');
  assert.equal(backgroundAlpha(0.2, 0.4), MAX_ALPHA * 0.5);
  assert.equal(backgroundAlpha(-0.4, 0.4), MAX_ALPHA);
  assert.equal(backgroundAlpha(0.9, 0.4), MAX_ALPHA); // clipped
  assert.equal(backgroundAlpha(0, 0.4), 0);
  assert.equal(backgroundAlpha(null, 0.4), 0);
  assert.equal(backgroundAlpha(0.1, 0), 0);
  assert.equal(cellBackground(0.2, 0.4), `rgba(${MATRIX_RED}, ${(MAX_ALPHA * 0.5).toFixed(3)})`);
  assert.equal(cellBackground(-0.4, 0.4), `rgba(${MATRIX_BLUE}, ${MAX_ALPHA.toFixed(3)})`);
  assert.equal(cellBackground(null, 0.4), 'transparent');
});

test('scale maximum: the largest |delta| shown, or the clip', () => {
  assert.equal(scaleMax(payload.rows, 0), Math.abs(sig.delta as number));
  assert.equal(scaleMax([ns, untested, undefined], null), untested.delta);
  assert.equal(scaleMax(payload.rows, 0.05), 0.05);
  assert.equal(scaleMax([], 0), 0);
});

test('cell text: percentage points, one decimal from 10 pp, ASCII minus; simultaneous intervals when asked', () => {
  const unit = unitInfo(payload);
  assert.deepEqual(unit, { scale: 100, suffix: 'pp', label: "percentage points of the arm's paths" });
  assert.deepEqual(cellText(sig, unit, 'per_cell'), { delta: '-32.1', interval: '[-33.9, -30.2]' });
  assert.deepEqual(cellText(ns, unit, 'per_cell'), { delta: '+0.06', interval: '[-1.76, +1.87]' });
  assert.deepEqual(cellText(constant, unit, 'per_cell'), { delta: '0.00', interval: '[0.00, 0.00]' });
  assert.equal(cellText({ ...ns, delta_ci_low: null, delta_ci_high: null }, unit, 'per_cell').interval, null);
  const adj = { ...ns, delta_ci_low_adj: -0.03, delta_ci_high_adj: 0.031 };
  assert.deepEqual(intervalOf(adj, 'simultaneous'), [-0.03, 0.031]);
  assert.deepEqual(intervalOf(ns, 'simultaneous'), [ns.delta_ci_low, ns.delta_ci_high]);
  assert.deepEqual(intervalOf(adj, 'per_cell'), [ns.delta_ci_low, ns.delta_ci_high]);

  const perPath = unitInfo({ unit: 'per_path', tool: 'get_ab_transitions', edge_weight: 'avg_per_path' });
  assert.equal(perPath.suffix, '');
  assert.equal(decimalsFor(perPath, 0.0123), 4);
  assert.equal(decimalsFor(perPath, 1.5), 2);
  assert.equal(fmtSigned(-0.01234, 4), '-0.0123');
  assert.equal(fmtSigned(-0.00001, 2), '0.00');
  assert.equal(fmtArmValue(0.2131, unit), '21.31 %');
  assert.equal(fmtArmValue(null, unit), 'n/a');
  assert.equal(fmtP(1e-9), '< 0.0001');
  assert.equal(fmtP(0.0312), '0.031');
  assert.equal(fmtP(null), 'n/a');
});

test('axes: sort by largest |delta| or by significant cells, pinned first; the filter keeps pinned keys', () => {
  const index = indexRows(payload);
  assert.equal(index.get(cellId('add_to_cart', 'cart')), 0);
  assert.equal(index.get(cellId('checkout_start', 'search')), 3);
  const cells = payload.rows.map((row) => ({ r: row.source!, c: row.target!, row }));
  const stats = axisStats(cells, payload.alpha);
  assert.deepEqual(stats.rows.get('checkout_start'), { maxAbsDelta: untested.delta, nSignificant: 0 });
  assert.equal(stats.cols.get('cart')!.nSignificant, 2);
  const keys = payload.row_keys;
  assert.deepEqual(orderKeys(keys, 'name', stats.rows), keys);
  assert.deepEqual(orderKeys(keys, 'max_abs_delta', stats.rows), ['add_to_cart', 'cart', 'checkout_start', 'path_start']);
  assert.deepEqual(orderKeys(keys, 'n_significant', stats.rows, new Set(['checkout_start'])),
    ['checkout_start', 'add_to_cart', 'cart', 'path_start']);
  assert.equal(keepsSignificance('cart', stats.rows, new Set()), true);
  assert.equal(keepsSignificance('checkout_start', stats.rows, new Set()), false);
  assert.equal(keepsSignificance('checkout_start', stats.rows, new Set(['checkout_start'])), true);
});

test('words: the family, the population, the interval note and the reading hints', () => {
  assert.equal(familyText(payload), 'bh over 3 tested of 5 edges');
  assert.equal(familyText({ ...payload, correction: 'none' }), 'no correction, 3 of 5 edges tested');
  assert.equal(populationText(payload), "the stream's paths");
  assert.match(populationText({ population: 'assignment', horizon: '7D' }), /^ITT.*horizon 7D$/);
  assert.match(intervalNote(0.95, 'per_cell', false), /95% per cell, not simultaneous/);
  assert.match(intervalNote(0.95, 'simultaneous', true), /Bonferroni simultaneous/);
  assert.match(readingHint('proba_out')!, /row-normalised/);
  assert.match(readingHint('proba_in')!, /column-normalised/);
  assert.match(readingHint('share_of_total')!, /composition/);
  assert.equal(readingHint('unique_paths'), null);
  assert.equal(parsePayload(''), null);
  assert.equal(parsePayload('{"rows": 1}'), null);
});

const view: View = {
  show_ci: true, interval_kind: 'per_cell', significance_filter: 'all', color_clip: 0, row_sort: 'name',
  col_sort: 'name', cell_size: 'auto', visibility: {}, reading_hint: true,
};

function renderMatrix(v: View = view) {
  const index = indexRows(payload);
  return renderToStaticMarkup(React.createElement(Matrix, {
    payload, kind: 'transitions', rowKeys: payload.row_keys, colKeys: payload.col_keys.map(String), index,
    dimRows: new Set<string>(), dimCols: new Set<string>(), scale: scaleMax(payload.rows, v.color_clip), view: v,
    pinned: new Set<string>(), onTogglePin: () => {}, onToggleHidden: () => {},
    tipRef: { current: null },
  }));
}

function cellHtml(html: string, row: string, col: string): string {
  const re = new RegExp(`<td[^>]*data-row="${row}" data-col="${col}"[^>]*>`);
  const hit = html.match(re);
  assert.ok(hit, `${row} -> ${col}`);
  return hit![0];
}

test('the matrix draws one cell per (row, column), with the status, sign and outline of its row', () => {
  const html = renderMatrix();
  assert.equal((html.match(/data-testid="ab-cell"/g) ?? []).length, 16);
  const s = cellHtml(html, 'add_to_cart', 'cart');
  assert.match(s, /data-status="significant"/);
  assert.match(s, /data-sign="negative"/);
  assert.match(s, new RegExp(`outline:2.5px solid ${OUTLINE_BLACK}`));
  assert.match(s, new RegExp(`background-color:rgba\\(${MATRIX_BLUE}, ${MAX_ALPHA.toFixed(3)}\\)`));
  assert.doesNotMatch(s, /background-image/);
  const n = cellHtml(html, 'checkout_start', 'payment');
  assert.match(n, /data-status="not_significant"/);
  assert.match(n, /data-cross="true"/);
  assert.match(n, new RegExp(`outline:2.5px solid ${OUTLINE_GREY}`));
  assert.match(n, /background-image:linear-gradient/);
  const u = cellHtml(html, 'checkout_start', 'search');
  assert.match(u, /data-status="untested"/);
  assert.match(u, new RegExp(`outline:2.5px dashed ${OUTLINE_GREY}`));
  assert.match(u, /color:#9ca3af/);
  const e = cellHtml(html, 'path_start', 'cart');
  assert.match(e, /data-status="empty"/);
  assert.match(e, /data-cross="false"/);
  assert.doesNotMatch(e, /outline:/);
  assert.match(html, /-32\.1/);
  assert.match(html, /\[-33\.9, -30\.2\]/);
  // intervals off: the delta only
  assert.doesNotMatch(renderMatrix({ ...view, show_ci: false }), /ab-cell-interval/);
  assert.doesNotMatch(renderMatrix({ ...view, cell_size: 'compact' }), /ab-cell-interval/);
});

test('the legend names the arms, the scale and every encoding', () => {
  const html = renderToStaticMarkup(React.createElement(Legend, { payload, kind: 'transitions', scale: 0.32079, view }));
  assert.match(html, /treatment/);
  assert.match(html, /control/);
  assert.match(html, /full at ±32\.1 pp/);
  for (const status of ['significant', 'not_significant', 'untested', 'empty']) {
    assert.match(html, new RegExp(`data-testid="ab-legend-${status}"`));
  }
  assert.match(html, /bh over 3 tested of 5 edges/);
  assert.match(html, /not simultaneous/);
});

test('the header names a declared family of edges and its size; steps name their range', () => {
  const declared = { ...payload, edges: 'declared', n_tested: 2, n_rows: 2, correction: 'holm' };
  const html = renderToStaticMarkup(React.createElement(Legend, { payload: declared, kind: 'transitions', scale: 0.1, view }));
  assert.match(html, /edge weight unique_paths \(declared edges\)/);
  assert.match(html, /holm over 2 tested of 2 edges/);
  const screen = renderToStaticMarkup(React.createElement(Legend, { payload, kind: 'transitions', scale: 0.1, view }));
  assert.doesNotMatch(screen, /declared/);
  const steps = { ...payload, tool: 'get_ab_steps', max_steps: 8, n_tested: 40, n_rows: 52, edges: undefined };
  const stepsHtml = renderToStaticMarkup(React.createElement(Legend, { payload: steps, kind: 'steps', scale: 0.1, view }));
  assert.match(stepsHtml, /steps 1–8/);
  assert.match(stepsHtml, /bh over 40 tested of 52 cells/);
});
