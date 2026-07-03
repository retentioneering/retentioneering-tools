// ── DataProvider ─────────────────────────────────────────────────────────────

/**
 * Single abstraction over all transport layers (HTTP, Jupyter comm, mock).
 * The tool name mirrors the be-app endpoint name (e.g. "transition_matrix").
 */
export interface DataProvider {
  compute<T = unknown>(tool: string, params: Record<string, unknown>): Promise<T>;
}

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
