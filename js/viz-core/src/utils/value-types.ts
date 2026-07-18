export const MATRIX_VALUE_TYPES = [
  "unique_paths",
  "count",
  "share_of_total",
  "avg_per_path",
  "proba_out",
  "proba_in",
  "time_median",
  "time_q95",
] as const;

export type MatrixValueType = (typeof MATRIX_VALUE_TYPES)[number];

// Keep in sync with the Python-side default (`edge_weight` traitlet in
// widgets/transition_graph.py) — this is only a fallback for hosts that
// don't provide an edge_weight value at all.
export const DEFAULT_VALUE_TYPE: MatrixValueType = "proba_out";

export const isTimeValueType = (value?: MatrixValueType | null) =>
  value === "time_median" || value === "time_q95";

export const isProbabilityValueType = (value?: MatrixValueType | null) =>
  value === "proba_out" || value === "proba_in";
