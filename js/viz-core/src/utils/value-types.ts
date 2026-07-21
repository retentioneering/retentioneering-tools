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

// Shared with the Edge Weight Type dropdown's hover tooltip (SettingsSidebar)
// and the graph legend's "edge width = ..." line — one wording, two spots.
export const VALUE_TYPE_DESCRIPTIONS: Record<MatrixValueType, string> = {
  unique_paths: "Number of unique paths that have an A→B transition.",
  count: "Total number of A→B transitions.",
  share_of_total: "Count divided by all transitions: #(A→B) / #(*→*).",
  avg_per_path: "Count divided by total paths: #(A→B) / total paths.",
  proba_out: "P(A→B) = #(A→B) / #(A→*). Markov transition probabilities.",
  proba_in: "P(A→B) = #(A→B) / #(*→B).",
  time_median: "Median time the A→B transition takes.",
  time_q95: "95th percentile of time the A→B transition takes.",
};
