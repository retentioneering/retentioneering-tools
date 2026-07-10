// `DataProvider` and `WidgetHost` now live in ./WidgetHost.ts (re-exported
// from index.ts). DataProvider used to be defined here as a standalone
// compute-only interface; it's now a derived subset of WidgetHost so the two
// can never drift apart. See WidgetHost.ts for the rationale.

// ── Shared data shapes ────────────────────────────────────────────────────────

export interface RawMatrixGroupData {
  events: string[];
  values: number[][];
}

export interface RawMatrixData {
  events: string[];
  values: number[][];
  group1?: RawMatrixGroupData | null;
  group2?: RawMatrixGroupData | null;
}

export interface GraphLayoutPosition {
  x: number;
  y: number;
}

export interface GraphLayoutResponse {
  result: Record<string, GraphLayoutPosition>;
}
