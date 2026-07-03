export const MATRIX_VALUE_TYPES = [
  "unique_paths",
  "count",
  "transition_rate",
  "per_path",
  "proba_out",
  "proba_in",
  "time_median",
  "time_q95",
] as const;

export type MatrixValueType = (typeof MATRIX_VALUE_TYPES)[number];

export const DEFAULT_VALUE_TYPE: MatrixValueType = "unique_paths";

export const isTimeValueType = (value?: MatrixValueType | null) =>
  value === "time_median" || value === "time_q95";

export const isProbabilityValueType = (value?: MatrixValueType | null) =>
  value === "proba_out" || value === "proba_in";
