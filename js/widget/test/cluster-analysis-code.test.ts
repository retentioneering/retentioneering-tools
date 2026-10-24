import assert from "node:assert/strict";
import { test } from "node:test";
import { buildAddClustersCode } from "../src/cluster_analysis_code";

const render = (params: Record<string, any>, scaler = "std", pathCol = "edited_path") =>
  buildAddClustersCode("stream", "saved", [{ metric: "duration" }], "hdbscan", scaler,
    params, pathCol, {});

test("copied code uses the successful pipeline after controls are edited", () => {
  const code = render({
    features: [{ metric: "length" }], path_col: "session_id",
    method: "kmeans", method_args: { n_clusters: 3, n_init: 10, random_state: 7 },
    scaler: "minmax", nmf_components: 2,
  });
  assert.match(code, /features=\[\{'metric': 'length'\}\]/);
  assert.match(code, /path_col='session_id'/);
  assert.match(code, /method='kmeans'/);
  assert.match(code, /'n_clusters': 3, 'n_init': 10, 'random_state': 7/);
  assert.match(code, /scaler='minmax'/);
  assert.match(code, /nmf_components=2/);
  assert.doesNotMatch(code, /duration|edited_path|hdbscan/);
});

test("explicit None scaler and path override edited nonempty controls", () => {
  const code = render({ scaler: null, path_col: null });
  assert.match(code, /scaler=None,/);
  assert.match(code, /path_col=None,/);
  assert.doesNotMatch(code, /scaler='std'|edited_path/);
});

test("legacy snapshots still use fallback fields and preserve disabled scaling", () => {
  const code = render({ method_args: { min_cluster_size: 2 } }, "", "user_id");
  assert.match(code, /features=\[\{'metric': 'duration'\}\]/);
  assert.match(code, /method='hdbscan'/);
  assert.match(code, /'min_cluster_size': 2/);
  assert.match(code, /scaler=None,/);
  assert.match(code, /path_col='user_id'/);
});

test("snapshot None values and optional renames render as valid Python literals", () => {
  const code = buildAddClustersCode("events", "a'b", [], "kmeans", "minmax", {
    features: [{ metric: "has_event", metric_args: { event: "a'b\\c" } }],
    method_args: { n_clusters: 2, random_state: null }, scaler: null,
  }, "", { cluster_0: "can't\\stop" });
  assert.match(code, /events_new = events\.add_clusters/);
  assert.ok(code.includes("name='a\\'b'"));
  assert.ok(code.includes("'event': 'a\\'b\\\\c'"));
  assert.match(code, /'random_state': None/);
  assert.ok(code.includes(".rename_segment_levels(\n    'a\\'b',\n    {'cluster_0': 'can\\'t\\\\stop'},\n)"));
});
