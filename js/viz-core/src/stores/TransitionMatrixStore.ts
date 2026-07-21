import { makeAutoObservable } from "mobx";

export type HeatmapType = "overall" | "row-wise" | "column-wise";
export type TransitionViewMode = "graph" | "table";

interface EventState {
  id: string;
  isPinned: boolean;
  isHidden: boolean;
  population: number;
}

export interface RawMatrixData {
  events: string[];
  values: number[][];
  group1?: { events: string[]; values: number[][] } | null;
  group2?: { events: string[]; values: number[][] } | null;
  // Sparse raw transition counts ({source: {target: n}}), independent of the
  // displayed weight type; used by the ego view to derive exact
  // incoming/outgoing shares. Absent in diff mode and in old exports.
  counts?: Record<string, Record<string, number>> | null;
}

interface MatrixBounds {
  min: number;
  max: number;
}

export class TransitionMatrixStore {
  events = new Map<string, EventState>();
  values: number[][] = [];
  matrixType: "differential" | "proba_in" | "proba_out" | undefined;
  transitionCounts: Record<string, Record<string, number>> | null = null;

  private _eventIds: string[] = [];
  private _indexById = new Map<string, number>();
  private _group1Values: number[][] | null = null;
  private _group2Values: number[][] | null = null;
  private _group1IndexById = new Map<string, number>();
  private _group2IndexById = new Map<string, number>();

  heatmapType: HeatmapType = "overall";
  private _heatmapTypeSetByUser = false;

  populationBounds = { min: 0, max: 0 };
  matrixValueBounds = { min: 0, max: 0 };

  viewMode: TransitionViewMode = "graph";
  private _viewModeSetByUser = false;
  isUpdating = false;
  isLoading = true;
  dataVersion = 0;

  private _rowBounds: MatrixBounds[] = [];
  private _columnBounds: MatrixBounds[] = [];

  filters = {
    population: { min: 0, max: 0 },
    matrixValue: { min: 0, max: 0, reverse: false },
  };

  collapseHidden = false;
  // How strongly non-focused nodes/edges fade when a node/edge/path is
  // focused: 0 = no dimming, 1 = fully invisible. Feeds the focus
  // animation's easing (dimOpacity = 1 - progress * focusDimStrength) in
  // TransitionGraph.tsx.
  focusDimStrength = 0.9;
  setFocusDimStrength = (value: number) => {
    this.focusDimStrength = Math.min(1, Math.max(0, value));
  };
  private shouldResetOnData = false;
  // True once the user has moved the Event Count slider manually.
  // When false, applyEventCounts freely syncs the filter to real bounds.
  populationCustomized = false;

  markFiltersForReset = () => { this.shouldResetOnData = true; };

  get hasData(): boolean { return this.values.length > 0; }

  search = {
    query: "",
    highlightedEventId: null as string | null,
    matchedEvents: new Set<string>(),
  };

  constructor() { makeAutoObservable(this); }

  get visibleEvents(): EventState[] {
    const visible = Array.from(this.events.values()).filter((event) => {
      if (!event.id.trim()) return false;
      if (event.isPinned) return true;
      if (event.isHidden) return false;
      const inPop = event.population >= this.filters.population.min && event.population <= this.filters.population.max;
      if (!inPop) return false;
      if (this.viewMode === "table") {
        const idx = this.getEventIndex(event.id);
        const rowMax = this._rowBounds[idx]?.max ?? Number.NEGATIVE_INFINITY;
        const colMax = this._columnBounds[idx]?.max ?? Number.NEGATIVE_INFINITY;
        const maxValue = Math.max(rowMax, colMax);
        const inRange = this.filters.matrixValue.reverse
          ? maxValue <= this.filters.matrixValue.min || maxValue >= this.filters.matrixValue.max
          : maxValue >= this.filters.matrixValue.min && maxValue <= this.filters.matrixValue.max;
        if (!inRange) return false;
      }
      return true;
    });
    const idx = visible.findIndex((e) => e.id === "path_end");
    if (idx >= 0 && idx !== visible.length - 1) {
      const [pe] = visible.splice(idx, 1);
      visible.push(pe);
    }
    return visible;
  }

  get displayedEvents(): EventState[] {
    return this.collapseHidden ? this.visibleEvents : Array.from(this.events.values());
  }

  get hiddenEvents(): EventState[] {
    if (this.collapseHidden) return [];
    return Array.from(this.events.values()).filter((event) => {
      if (event.isPinned) return false;
      if (event.isHidden) return true;
      if (event.population < this.filters.population.min || event.population > this.filters.population.max) return true;
      if (this.viewMode === "table") {
        const idx = this.getEventIndex(event.id);
        const rowMax = this._rowBounds[idx]?.max ?? Number.NEGATIVE_INFINITY;
        const colMax = this._columnBounds[idx]?.max ?? Number.NEGATIVE_INFINITY;
        const maxValue = Math.max(rowMax, colMax);
        const isOut = this.filters.matrixValue.reverse
          ? maxValue > this.filters.matrixValue.min && maxValue < this.filters.matrixValue.max
          : maxValue < this.filters.matrixValue.min || maxValue > this.filters.matrixValue.max;
        if (isOut) return true;
      }
      return false;
    });
  }

  getEventIndex(eventId: string): number {
    const idx = this._indexById.get(eventId);
    return idx == null ? -1 : idx;
  }

  getMatrixValue(rowEventId: string, colEventId: string): number {
    const r = this.getEventIndex(rowEventId);
    const c = this.getEventIndex(colEventId);
    if (r < 0 || c < 0) return 0;
    return this.values[r]?.[c] ?? 0;
  }

  getDiffGroupValue(row: string, col: string, group: "group1" | "group2"): number | null {
    const rIdx = group === "group1" ? this._group1IndexById.get(row) : this._group2IndexById.get(row);
    const cIdx = group === "group1" ? this._group1IndexById.get(col) : this._group2IndexById.get(col);
    const vals = group === "group1" ? this._group1Values : this._group2Values;
    if (rIdx == null || cIdx == null || !vals) return null;
    const v = vals[rIdx]?.[cIdx];
    return typeof v === "number" && Number.isFinite(v) ? v : null;
  }

  /** Returns {group1Value, group2Value, diffValue} as proportions for a node in diff mode. */
  getNodeShareBreakdown(eventId: string): { group1Value: number | null; group2Value: number | null; diffValue: number | null } {
    const null3 = { group1Value: null, group2Value: null, diffValue: null };
    if (!this._group1Values || !this._group2Values) return null3;
    const idx1 = this._group1IndexById.get(eventId);
    const idx2 = this._group2IndexById.get(eventId);
    if (idx1 == null || idx2 == null) return null3;

    const rowColSum = (vals: number[][], idx: number): number => {
      const row = vals[idx];
      const rowS = row ? row.reduce((s, v) => s + (Number.isFinite(v) && v > 0 ? v : 0), 0) : 0;
      const colS = vals.reduce((s, r) => { const v = r[idx]; return s + (Number.isFinite(v) && v > 0 ? v : 0); }, 0);
      return rowS + colS;
    };
    const total = (vals: number[][]): number =>
      vals.reduce((s, row) => s + row.reduce((a, v) => a + (Number.isFinite(v) && v > 0 ? v : 0), 0), 0);

    const t1 = total(this._group1Values);
    const t2 = total(this._group2Values);
    if (t1 === 0 || t2 === 0) return null3;

    const share1 = rowColSum(this._group1Values, idx1) / (2 * t1);
    const share2 = rowColSum(this._group2Values, idx2) / (2 * t2);
    return { group1Value: share1, group2Value: share2, diffValue: share1 - share2 };
  }

  /** share_group1(E) − share_group2(E), where share = (rowSum + colSum) / (2 * totalMatrixSum).
   *  Positive → event is proportionally more common in group 1 (red).
   *  Negative → more common in group 2 (blue).
   *  Returns null when not in differential mode or event is missing. */
  getNodeShareDiff(eventId: string): number | null {
    if (!this._group1Values || !this._group2Values) return null;
    const idx1 = this._group1IndexById.get(eventId);
    const idx2 = this._group2IndexById.get(eventId);
    if (idx1 == null || idx2 == null) return null;

    const rowColSum = (vals: number[][], idx: number): number => {
      const row = vals[idx];
      const rowS = row ? row.reduce((s, v) => s + (Number.isFinite(v) && v > 0 ? v : 0), 0) : 0;
      const colS = vals.reduce((s, r) => { const v = r[idx]; return s + (Number.isFinite(v) && v > 0 ? v : 0); }, 0);
      return rowS + colS;
    };
    const total = (vals: number[][]): number =>
      vals.reduce((s, row) => s + row.reduce((a, v) => a + (Number.isFinite(v) && v > 0 ? v : 0), 0), 0);

    const t1 = total(this._group1Values);
    const t2 = total(this._group2Values);
    if (t1 === 0 || t2 === 0) return null;

    const share1 = rowColSum(this._group1Values, idx1) / (2 * t1);
    const share2 = rowColSum(this._group2Values, idx2) / (2 * t2);
    return share1 - share2;
  }

  getDiffCellBreakdown(row: string, col: string) {
    const r = this.getEventIndex(row);
    const c = this.getEventIndex(col);
    const diffValue = r >= 0 && c >= 0 ? (this.values[r]?.[c] ?? null) : null;
    const normalizedDiff = typeof diffValue === "number" && Number.isFinite(diffValue) ? diffValue : null;
    return {
      group1Value: this.getDiffGroupValue(row, col, "group1"),
      group2Value: this.getDiffGroupValue(row, col, "group2"),
      diffValue: normalizedDiff,
    };
  }

  private determineMatrixType(values: number[][]): "differential" | "proba_in" | "proba_out" {
    if (values.some((row) => row.some((v) => v < 0))) return "differential";
    const rowSums = values.map((row) => row.reduce((s, v) => s + v, 0));
    const colSums = values[0].map((_, ci) => values.reduce((s, row) => s + row[ci], 0));
    const rowDev = rowSums.reduce((s, v) => s + Math.abs(v - 1), 0) / rowSums.length;
    const colDev = colSums.reduce((s, v) => s + Math.abs(v - 1), 0) / colSums.length;
    return rowDev < colDev ? "proba_out" : "proba_in";
  }

  get matrixBounds(): MatrixBounds { return { ...this.matrixValueBounds }; }
  get rowBounds(): MatrixBounds[] { return this._rowBounds; }
  get columnBounds(): MatrixBounds[] { return this._columnBounds; }

  getBoundsForCell(rowEventId: string, colEventId: string): MatrixBounds {
    const r = this.getEventIndex(rowEventId);
    const c = this.getEventIndex(colEventId);
    switch (this.heatmapType) {
      case "row-wise": return this.rowBounds[r];
      case "column-wise": return this.columnBounds[c];
      default: return this.matrixBounds;
    }
  }

  setData = (data: RawMatrixData) => {
    this.isLoading = false;
    this.values = data.values;
    this.dataVersion += 1;
    this._eventIds = data.events.slice();
    this._indexById.clear();
    this._eventIds.forEach((id, i) => this._indexById.set(id, i));
    this._group1Values = data.group1?.values ?? null;
    this._group2Values = data.group2?.values ?? null;
    this.transitionCounts = data.counts ?? null;
    this._group1IndexById.clear();
    data.group1?.events.forEach((id, i) => this._group1IndexById.set(id, i));
    this._group2IndexById.clear();
    data.group2?.events.forEach((id, i) => this._group2IndexById.set(id, i));
    const prevMatrixType = this.matrixType;
    this.matrixType = this.determineMatrixType(data.values);
    // Any transition between differential ↔ non-differential changes the population
    // scale entirely (diff rows sum to 0, non-diff rows sum to 1), so the old filter
    // range becomes invalid and must be reset.
    const matrixTypeChanged = prevMatrixType !== undefined && prevMatrixType !== this.matrixType;
    if (!this._heatmapTypeSetByUser) {
      this.heatmapType = this.matrixType === "proba_out" ? "row-wise" : this.matrixType === "proba_in" ? "column-wise" : "overall";
    }
    const newEvents = new Map<string, EventState>();
    data.events.forEach((id, index) => {
      const old = this.events.get(id);
      const rowSum = data.values[index].reduce((s, v) => s + v, 0);
      const colSum = data.values.reduce((s, row) => s + row[index], 0);
      newEvents.set(id, { id, isPinned: old?.isPinned || false, isHidden: old?.isHidden || false, population: rowSum + colSum });
    });
    this.events = newEvents;
    const populations = Array.from(newEvents.values()).map((e) => e.population);
    let pMin = Infinity, pMax = -Infinity;
    for (const p of populations) { if (p < pMin) pMin = p; if (p > pMax) pMax = p; }
    this.populationBounds = { min: pMin, max: pMax };
    let vMin = Infinity, vMax = -Infinity;
    for (const row of data.values) for (const v of row) { if (v == null) continue; if (v < vMin) vMin = v; if (v > vMax) vMax = v; }
    this.matrixValueBounds = { min: vMin, max: vMax };
    this._rowBounds = data.values.map((row) => ({ min: Math.min(...row), max: Math.max(...row) }));
    this._columnBounds = data.values[0].map((_, ci) => { const col = data.values.map((row) => row[ci]); return { min: Math.min(...col), max: Math.max(...col) }; });
    if (this.shouldResetOnData || matrixTypeChanged) {
      // Only reset matrixValue — Event Count (population) filter must not change
      // when switching edge weights, diff mode, or path column.
      this.filters.matrixValue = { min: this.matrixValueBounds.min, max: this.matrixValueBounds.max, reverse: false };
      this.shouldResetOnData = false;
      return;
    }
    // First-load initialisation (filters still at their zero defaults)
    if (this.filters.population.min === 0 && this.filters.population.max === 0) {
      this.filters.population = { ...this.populationBounds };
    }
    if (this.filters.matrixValue.min === 0 && this.filters.matrixValue.max === 0) {
      this.filters.matrixValue = { min: this.matrixValueBounds.min, max: this.matrixValueBounds.max, reverse: false };
    }
  };

  setViewMode = (mode: TransitionViewMode) => { this.viewMode = mode; this._viewModeSetByUser = true; };
  setUserRole = (_isAdmin: boolean) => {};
  setUpdating = (v: boolean) => { this.isUpdating = v; };

  applyEventCounts = (counts: Record<string, number> | undefined | null) => {
    if (!counts || this.events.size === 0) return;
    const prev = { ...this.populationBounds };
    const wasAtBounds = this.filters.population.min === prev.min && this.filters.population.max === prev.max;
    for (const [id, state] of this.events) {
      const c = counts[id];
      if (typeof c === "number" && Number.isFinite(c)) this.events.set(id, { ...state, population: c });
    }
    let pMin = Infinity, pMax = -Infinity;
    for (const { population } of this.events.values()) { if (population < pMin) pMin = population; if (population > pMax) pMax = population; }
    if (!Number.isFinite(pMin) || !Number.isFinite(pMax)) return;
    this.populationBounds = { min: pMin, max: pMax };
    if (this.shouldResetOnData) {
      this.filters.population = { ...this.populationBounds };
      this.shouldResetOnData = false;
      this.populationCustomized = false;
      return;
    }
    // Sync to real counts only when the user hasn't touched the slider yet.
    if (!this.populationCustomized) {
      this.filters.population = { ...this.populationBounds };
    }
  };

  setPopulationRange = (min: number, max: number) => {
    this.filters.population = { min, max };
    this.populationCustomized = true;
  };
  setMatrixValueRange = (min: number, max: number) => { this.filters.matrixValue = { ...this.filters.matrixValue, min, max }; };
  toggleMatrixValueReverse = () => { this.filters.matrixValue.reverse = !this.filters.matrixValue.reverse; };
  toggleCollapseHidden = () => { this.collapseHidden = !this.collapseHidden; };

  toggleEventVisibility = (id: string) => {
    if (this.isImmutable(id)) return;
    const e = this.events.get(id);
    if (e) this.events.set(id, { ...e, isHidden: !e.isHidden });
  };

  toggleEventPin = (id: string) => {
    if (this.isImmutable(id)) return;
    const e = this.events.get(id);
    if (e) this.events.set(id, { ...e, isPinned: !e.isPinned, isHidden: false });
  };

  setSearchQuery = (query: string) => { this.search.query = query; this._updateMatchedEvents(); };
  setHighlightedEvent = (id: string | null) => { this.search.highlightedEventId = id; };

  private _updateMatchedEvents = () => {
    this.search.matchedEvents.clear();
    if (!this.search.query) return;
    const q = this.search.query.toLowerCase();
    for (const e of this.displayedEvents) {
      if (e.id.toLowerCase().includes(q)) this.search.matchedEvents.add(e.id);
    }
  };

  setHeatmapType = (type: HeatmapType) => { this.heatmapType = type; this._heatmapTypeSetByUser = true; };

  resetFiltersToDefaults = () => {
    this.filters.population = { ...this.populationBounds };
    this.filters.matrixValue = { min: this.matrixValueBounds.min, max: this.matrixValueBounds.max, reverse: false };
    this.collapseHidden = false;
    this.populationCustomized = false;
    this._heatmapTypeSetByUser = false;
    this.heatmapType = this.matrixType === "proba_out" ? "row-wise" : this.matrixType === "proba_in" ? "column-wise" : "overall";
  };

  private isImmutable(_id: string) { return false; }

  // AI overlay (kept for fe compatibility)
  overlayHighlightedCells = new Set<string>();
  overlayHighlightedRows = new Set<string>();
  overlayHighlightedColumns = new Set<string>();
  overlayPathEdges = new Set<string>();
  aiFilters: Array<string | { eventId: string; action: "include" | "exclude"; note?: string }> = [];
  aiSorts: Array<{ eventId: string; direction: "asc" | "desc"; criteria: "value" | "frequency" | "anomaly" }> = [];
  aiSegments: Array<{ name: string; criteria: Array<{ eventId: string; condition: "above" | "below" | "equals"; value: number }> }> = [];

  applyOverlay = (cells: Array<{ rowEventId: string; colEventId: string }>) => {
    const next = new Set<string>();
    for (const c of cells) { if (c?.rowEventId && c?.colEventId) next.add(`${c.rowEventId}→${c.colEventId}`); }
    this.overlayHighlightedCells = next;
  };
  clearOverlay = () => {
    this.overlayHighlightedCells = new Set<string>();
    this.overlayHighlightedRows = new Set<string>();
    this.overlayHighlightedColumns = new Set<string>();
    this.overlayPathEdges = new Set<string>();
    this.aiFilters = [];
    this.aiSorts = [];
    this.aiSegments = [];
  };
  applyRowOverlay = (rows: string[]) => { this.overlayHighlightedRows = new Set(rows.filter(Boolean)); };
  applyPathEdges = (edges: Array<{ from: string; to: string }>) => {
    this.overlayPathEdges = new Set(edges.filter((e) => e?.from && e?.to).map((e) => `${e.from}|${e.to}`));
  };
  applyColumnOverlay = (cols: string[]) => { this.overlayHighlightedColumns = new Set(cols.filter(Boolean)); };
  applyFilters = (filters: typeof this.aiFilters) => { this.aiFilters = filters.map((f) => typeof f === "string" ? { eventId: f, action: "include" as const } : f); };
  applySorts = (sorts: typeof this.aiSorts) => { this.aiSorts = sorts; };
  createSegment = (seg: typeof this.aiSegments[0]) => { this.aiSegments.push(seg); };
}
