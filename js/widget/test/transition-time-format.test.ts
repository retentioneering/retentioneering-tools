import test from 'node:test';
import assert from 'node:assert/strict';
import { formatTime } from '../../viz-core/src/utils/format-time';

test('transition duration labels preserve milliseconds and negative diff', () => {
  assert.equal(formatTime(0.125), '125ms');
  assert.equal(formatTime(-0.125), '-125ms');
  assert.equal(formatTime(1.25), '1.25s');
});

import React from 'react';
import { createRequire } from 'node:module';
// Keep the Node renderer external to esbuild's ESM test bundle.
const { renderToStaticMarkup } = createRequire(import.meta.url)('react-dom/server');
import { DiffBreakdownTooltip } from '../../viz-core/src/components/TransitionGraph/DiffBreakdownTooltip';

function breakdown(group1Value: number | null, group2Value: number | null, diffValue: number | null, isTime = true) {
  return renderToStaticMarkup(React.createElement(DiffBreakdownTooltip, {
    segmentName: null, value1Label: 'fast', value2Label: 'slow',
    group1Value, group2Value, diffValue, isTime,
  }));
}

test('time diff tooltip renders units for both groups and signed differences', () => {
  const negative = breakdown(.125, .375, -.25);
  for (const value of ['125ms', '375ms', '-250ms']) assert.ok(negative.includes(`>${value}</span>`));
  const positive = breakdown(1.25, .125, 1.125);
  assert.ok(positive.includes('>+1.13s</span>'));
  // No difference is an observed zero, not a missing value.
  assert.ok(breakdown(.125, .125, 0).includes('>0ms</span>'));
  assert.ok(breakdown(null, .125, null).includes('>-</span>'));
  assert.ok(!breakdown(Infinity, null, Infinity).includes('>+-</span>'));
  assert.ok(breakdown(NaN, null, NaN).includes('>-</span>'));
});

test('count/probability and node-share tooltips retain exact numeric formatting', () => {
  const numeric = breakdown(.125, .375, -.25, false);
  assert.ok(numeric.includes('>0.125</span>'));
  assert.ok(numeric.includes('>-0.25</span>'));
  assert.ok(!numeric.includes('ms</span>'));
});
