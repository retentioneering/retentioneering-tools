import test from 'node:test';
import assert from 'node:assert/strict';
import { formatTime } from '../../viz-core/src/utils/format-time';

test('transition duration labels preserve milliseconds and negative diff', () => {
  assert.equal(formatTime(0.125), '125ms');
  assert.equal(formatTime(-0.125), '-125ms');
  assert.equal(formatTime(1.25), '1.25s');
});
