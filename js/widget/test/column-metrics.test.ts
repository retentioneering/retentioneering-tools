import test from 'node:test';
import assert from 'node:assert/strict';
import {
  COLUMN_METRICS, METRIC_NAMES, columnOptions, validateMetricCfg,
} from '../src/metric_config_row';
import { metricArgsFromName, metricBaseName } from '../src/segment_overview_table';

const customCols = { revenue: true, items: true, plan: false };

test('the metric editor offers the column metrics from the exported schema', () => {
  for (const m of COLUMN_METRICS) assert.ok(METRIC_NAMES.includes(m), m);
});

test('sum and mean list numeric custom columns only; last lists every one', () => {
  assert.deepEqual(columnOptions('column_sum', customCols), ['items', 'revenue']);
  assert.deepEqual(columnOptions('column_mean', customCols), ['items', 'revenue']);
  assert.deepEqual(columnOptions('column_last', customCols), ['items', 'plan', 'revenue']);
  assert.deepEqual(columnOptions('column_sum', {}), []);
});

test('a column metric without a column is invalid', () => {
  assert.equal(validateMetricCfg({ metric: 'column_sum' }), 'Select a column');
  assert.equal(validateMetricCfg({ metric: 'column_last', metric_args: { column: 'plan' } }), null);
});

test('segment overview cells of column metrics can be drilled into', () => {
  assert.equal(metricBaseName('column_sum_revenue_mean'), 'column_sum');
  assert.deepEqual(metricArgsFromName('column_sum_revenue_mean', []), { column: 'revenue' });
  assert.deepEqual(metricArgsFromName('column_last_order_total_median', []), { column: 'order_total' });
});
