import { makeAutoObservable } from "mobx";

export interface Event {
  id: string;
  isPinned: boolean;
  isHidden: boolean;
  population: number;
}

export type HeatmapType = "overall" | "row-wise" | "column-wise";

interface RawStepMatrixGroupData {
  events: string[];
  values: number[][];
  columns: number[];
}

export interface RawMatrixData {
  events: string[];
  values: number[][];
  columns: number[];
  group1?: { events: string[]; values: number[][]; columns: number[] } | null;
  group2?: { events: string[]; values: number[][]; columns: number[] } | null;
}

export type { RawMatrixData as RawStepMatrixData };

export class StepMatrixStore {
  // Event states
  events = new Map<string, Event>();

  // Fast lookup caches
  private _eventIds: string[] = [];
  private _indexById = new Map<string, number>();

  // Matrix data (can be multiple matrices)
  matrices: {
    values: number[][];
    columns: number[]; // Step numbers
  }[] = [];
  group1Matrices: Array<RawStepMatrixGroupData | null> = [];
  group2Matrices: Array<RawStepMatrixGroupData | null> = [];

  // Current active matrix index
  activeMatrixIndex = 0;

  // Heatmap type
  heatmapType: HeatmapType = "overall";

  // Value bounds - will be set from actual data
  populationBounds = {
    min: 0,
    max: 0,
  };

  matrixValueBounds = {
    min: 0,
    max: 0,
  };

  // Current filters - will be set to match bounds after data loads
  filters = {
    population: {
      min: 0,
      max: 0,
    },
    matrixValue: {
      min: 0,
      max: 0,
      reverse: false,
    },
  };

  // Flag indicating all data (including event counts) has been applied
  private _countsApplied = false;

  // Flag to force resetting filters on next data load (used when switching value type or diff mode)
  private shouldResetOnData = false;

  // True once the user has manually moved the Event Count slider
  private _populationCustomized = false;

  collapseHidden = false;

  // Search state
  search = {
    query: "",
    highlightedEventId: null as string | null,
    matchedEvents: new Set<string>(),
  };

  // Step Sankey UI State
  expandedColumns = new Set<number>();
  viewMode: "table" | "sankey" = "sankey";
  isLoadingPreview = false;
  private _viewModeSetByUser = false;
  private _isAdmin = false;

  constructor() {
    makeAutoObservable(this);
  }

  /** Call to ensure filters reset when next setData is invoked */
  markFiltersForReset = () => {
    this.shouldResetOnData = true;
  };

  setViewMode(mode: "table" | "sankey") {
    this.viewMode = mode;
    this._viewModeSetByUser = true;
  }

  setUserRole(isAdmin: boolean) {
    this._isAdmin = isAdmin;
  }

  // Computed values
  get activeMatrix() {
    return this.matrices[this.activeMatrixIndex] || { values: [], columns: [] };
  }

  get hasData(): boolean {
    return this.matrices.length > 0;
  }

  // Returns true when all data including event counts has been applied
  get isFullyLoaded(): boolean {
    return this.hasData && this._countsApplied;
  }

  get allEventIds(): string[] {
    return this._eventIds;
  }

  get visibleEvents(): Event[] {
    if (this.isLoadingPreview) {
      return Array.from(this.events.values());
    }

    return Array.from(this.events.values()).filter((event) => {
      // path_start / path_end are always visible
      if (this.isImmutable(event.id)) return true;
      if (event.isPinned) return true;
      if (event.isHidden) return false;

      // Population check
      const isInPopulationRange =
        event.population >= this.filters.population.min &&
        event.population <= this.filters.population.max;
      if (!isInPopulationRange) return false;

      // Matrix value check
      const eventIndex = this.getEventIndex(event.id);
      const rowValues = this.activeMatrix.values[eventIndex] || [];
      const maxValue = Math.max(...rowValues);

      const isInValueRange = this.filters.matrixValue.reverse
        ? maxValue <= this.filters.matrixValue.min ||
          maxValue >= this.filters.matrixValue.max
        : maxValue >= this.filters.matrixValue.min &&
          maxValue <= this.filters.matrixValue.max;

      return isInValueRange;
    });
  }

  get displayedEvents(): Event[] {
    if (this.collapseHidden) {
      return this.visibleEvents;
    }
    return Array.from(this.events.values());
  }

  get hiddenEvents(): Event[] {
    if (this.collapseHidden) return [];
    return Array.from(this.events.values()).filter((event) => {
      // path_start / path_end are never hidden
      if (this.isImmutable(event.id)) return false;
      if (event.isPinned) return false;

      return (
        event.isHidden ||
        event.population < this.filters.population.min ||
        event.population > this.filters.population.max ||
        (() => {
          const eventIndex = this.getEventIndex(event.id);
          const rowValues = this.activeMatrix.values[eventIndex] || [];
          const maxValue = Math.max(...rowValues);

          return this.filters.matrixValue.reverse
            ? maxValue > this.filters.matrixValue.min &&
                maxValue < this.filters.matrixValue.max
            : maxValue < this.filters.matrixValue.min ||
                maxValue > this.filters.matrixValue.max;
        })()
      );
    });
  }

  // Helpers
  getEventIndex(eventId: string): number {
    const idx = this._indexById.get(eventId);
    return idx == null ? -1 : idx;
  }

  getMatrixValue(
    eventId: string,
    stepIndex: number,
    matrixIndex: number = this.activeMatrixIndex,
  ): number {
    const rowIndex = this.getEventIndex(eventId);
    const matrix = this.matrices[matrixIndex];
    if (!matrix) return 0;

    // Find the column index for this step
    const colIndex = matrix.columns.indexOf(stepIndex);
    if (colIndex === -1) return 0;

    return matrix.values[rowIndex]?.[colIndex] || 0;
  }

  private getGroupMatrix(
    matrixIndex: number,
    group: "group1" | "group2",
  ): RawStepMatrixGroupData | null {
    const groups =
      group === "group1" ? this.group1Matrices : this.group2Matrices;
    return groups[matrixIndex] ?? null;
  }

  getDiffGroupValueByColumnIndex(
    eventId: string,
    matrixIndex: number,
    columnIndex: number,
    group: "group1" | "group2",
  ): number | null {
    const groupMatrix = this.getGroupMatrix(matrixIndex, group);
    if (!groupMatrix) return null;

    const rowIndex = groupMatrix.events.indexOf(eventId);
    if (
      rowIndex < 0 ||
      columnIndex < 0 ||
      columnIndex >= groupMatrix.columns.length
    ) {
      return null;
    }

    const value = groupMatrix.values[rowIndex]?.[columnIndex];
    return typeof value === "number" && Number.isFinite(value) ? value : null;
  }

  getDiffGroupValueByStep(
    eventId: string,
    stepIndex: number,
    matrixIndex: number,
    group: "group1" | "group2",
  ): number | null {
    const groupMatrix = this.getGroupMatrix(matrixIndex, group);
    if (!groupMatrix) return null;

    const rowIndex = groupMatrix.events.indexOf(eventId);
    if (rowIndex < 0) return null;

    const colIndex = groupMatrix.columns.indexOf(stepIndex);
    if (colIndex < 0) return null;

    const value = groupMatrix.values[rowIndex]?.[colIndex];
    return typeof value === "number" && Number.isFinite(value) ? value : null;
  }

  getDiffBreakdownByColumnIndex(
    eventId: string,
    matrixIndex: number,
    columnIndex: number,
  ): {
    group1Value: number | null;
    group2Value: number | null;
    diffValue: number | null;
  } {
    const rowIndex = this.getEventIndex(eventId);
    const diffValue =
      rowIndex >= 0 && matrixIndex >= 0
        ? (this.matrices[matrixIndex]?.values[rowIndex]?.[columnIndex] ?? null)
        : null;

    return {
      group1Value: this.getDiffGroupValueByColumnIndex(
        eventId,
        matrixIndex,
        columnIndex,
        "group1",
      ),
      group2Value: this.getDiffGroupValueByColumnIndex(
        eventId,
        matrixIndex,
        columnIndex,
        "group2",
      ),
      diffValue:
        typeof diffValue === "number" && Number.isFinite(diffValue)
          ? diffValue
          : null,
    };
  }

  getDiffBreakdownByStep(
    eventId: string,
    stepIndex: number,
    matrixIndex: number,
  ): {
    group1Value: number | null;
    group2Value: number | null;
    diffValue: number | null;
  } {
    const diffValue = this.getMatrixValue(eventId, stepIndex, matrixIndex);

    return {
      group1Value: this.getDiffGroupValueByStep(
        eventId,
        stepIndex,
        matrixIndex,
        "group1",
      ),
      group2Value: this.getDiffGroupValueByStep(
        eventId,
        stepIndex,
        matrixIndex,
        "group2",
      ),
      diffValue:
        typeof diffValue === "number" && Number.isFinite(diffValue)
          ? diffValue
          : null,
    };
  }

  private updateMatrixValueBounds = () => {
    // Recalculate bounds based only on currently visible (non-hidden or pinned) events
    const values: number[] = [];
    const visibleIds = Array.from(this.events.values())
      .filter((e) => !e.isHidden || e.isPinned)
      .map((e) => e.id);

    // Aggregate values across all stored matrices
    for (const matrix of this.matrices) {
      for (const eventId of visibleIds) {
        const rowIdx = this.getEventIndex(eventId);
        if (rowIdx === -1) continue;
        values.push(...matrix.values[rowIdx]);
      }
    }

    if (values.length === 0) {
      this.matrixValueBounds = { min: 0, max: 0 };
      return;
    }

    // Compute bounds without spreading to avoid exceeding argument limits
    let vMin = Number.POSITIVE_INFINITY;
    let vMax = Number.NEGATIVE_INFINITY;
    for (const v of values) {
      if (v < vMin) vMin = v;
      if (v > vMax) vMax = v;
    }
    this.matrixValueBounds = { min: vMin, max: vMax };
  };

  // Actions
  setData = (data: RawMatrixData[]) => {
    this.isLoadingPreview = false;
    // Reset counts applied flag - will be set after applyEventCounts
    this._countsApplied = false;

    // Store all matrices
    this.matrices = data.map((matrix) => ({
      values: matrix.values,
      columns: matrix.columns,
    }));
    this.group1Matrices = data.map((matrix) => matrix.group1 ?? null);
    this.group2Matrices = data.map((matrix) => matrix.group2 ?? null);

    // Reset active matrix to first one
    this.activeMatrixIndex = 0;

    // Create events Map from first matrix (events are same across all matrices)
    const newEvents = new Map<string, Event>();

    // Build fast index tables once
    this._eventIds = data[0].events.slice();
    this._indexById.clear();
    this._eventIds.forEach((id, i) => this._indexById.set(id, i));

    data[0].events.forEach((id, index) => {
      const oldState = this.events.get(id);
      const rowValues = data[0].values[index];
      const population = rowValues.reduce((sum, val) => sum + Math.abs(val), 0);

      newEvents.set(id, {
        id,
        isPinned: oldState?.isPinned || false,
        isHidden: oldState?.isHidden || false,
        population,
      });
    });

    this.events = newEvents;

    // Update bounds
    const populations = Array.from(newEvents.values()).map((e) => e.population);
    const allValues: number[] = [];
    for (const m of data) {
      for (const row of m.values) {
        allValues.push(...row);
      }
    }

    // Safe min/max calculation to prevent "Maximum call stack size exceeded" for large datasets
    let pMin = Number.POSITIVE_INFINITY;
    let pMax = Number.NEGATIVE_INFINITY;
    for (const p of populations) {
      if (p < pMin) pMin = p;
      if (p > pMax) pMax = p;
    }
    this.populationBounds = { min: pMin, max: pMax };

    let vMin = Number.POSITIVE_INFINITY;
    let vMax = Number.NEGATIVE_INFINITY;
    for (const v of allValues) {
      if (v < vMin) vMin = v;
      if (v > vMax) vMax = v;
    }

    // Detect switch between differential (has negatives) and non-differential.
    // Population scale and value scale change completely; old filter range is invalid.
    const prevVMin = this.matrixValueBounds.min;
    const prevHadData = prevVMin !== 0 || this.matrixValueBounds.max !== 0;
    const matrixTypeChanged = prevHadData && (vMin < 0) !== (prevVMin < 0);

    this.matrixValueBounds = { min: vMin, max: vMax };

    if (this.shouldResetOnData || matrixTypeChanged) {
      this.filters.population = { ...this.populationBounds };
      this.filters.matrixValue = {
        min: this.matrixValueBounds.min,
        max: this.matrixValueBounds.max,
        reverse: false,
      };
      this.shouldResetOnData = false;
      this.updateMatrixValueBounds();
      return;
    }

    // Set filters to data bounds only if they are not yet initialized
    if (
      this.filters.population.min === 0 &&
      this.filters.population.max === 0
    ) {
      this.filters.population = { ...this.populationBounds };
    }
    if (
      this.filters.matrixValue.min === 0 &&
      this.filters.matrixValue.max === 0
    ) {
      this.filters.matrixValue = {
        ...this.filters.matrixValue,
        min: this.matrixValueBounds.min,
        max: this.matrixValueBounds.max,
      };
    }

    // Recalculate bounds based on current visibility state (hidden/pinned)
    this.updateMatrixValueBounds();
  };

  /**
   * Apply server-provided event counts to override computed populations and bounds.
   */
  applyEventCounts = (counts: Record<string, number> | undefined | null) => {
    // Mark as counts applied even if counts is empty/null - this means the API responded
    this._countsApplied = true;

    if (!counts || this.events.size === 0) return;

    for (const [eventId, state] of this.events) {
      const apiCount = counts[eventId];
      if (typeof apiCount === "number" && Number.isFinite(apiCount)) {
        this.events.set(eventId, { ...state, population: apiCount });
      }
    }

    // Recompute population bounds strictly from current events map
    let pMin = Number.POSITIVE_INFINITY;
    let pMax = Number.NEGATIVE_INFINITY;
    for (const { population } of this.events.values()) {
      if (population < pMin) pMin = population;
      if (population > pMax) pMax = population;
    }
    if (!Number.isFinite(pMin) || !Number.isFinite(pMax)) return;
    this.populationBounds = { min: pMin, max: pMax };

    // If reset was requested (e.g. value type changed or diff mode toggled), reset population filter now
    // that we have correct bounds from API
    if (this.shouldResetOnData || !this._populationCustomized) {
      this.filters.population = { ...this.populationBounds };
      this.shouldResetOnData = false;
    }
    // If _populationCustomized, keep the user's current filter range intact
  };

  setActiveMatrixIndex = (index: number) => {
    if (index >= 0 && index < this.matrices.length) {
      this.activeMatrixIndex = index;
    }
  };

  // Rest of the actions remain same
  setPopulationRange = (min: number, max: number) => {
    this.filters.population = { min, max };
    this._populationCustomized = true;
  };

  setMatrixValueRange = (min: number, max: number) => {
    this.filters.matrixValue = { ...this.filters.matrixValue, min, max };
  };

  toggleMatrixValueReverse = () => {
    this.filters.matrixValue.reverse = !this.filters.matrixValue.reverse;
  };

  toggleCollapseHidden = () => {
    this.collapseHidden = !this.collapseHidden;
    this.updateMatrixValueBounds();
  };

  toggleEventVisibility = (eventId: string) => {
    if (this.isImmutable(eventId)) {
      return; // ignore for path_start / path_end
    }
    const event = this.events.get(eventId);
    if (!event) return;

    this.events.set(eventId, {
      ...event,
      isHidden: !event.isHidden,
    });

    this.updateMatrixValueBounds();
  };

  toggleEventPin = (eventId: string) => {
    if (this.isImmutable(eventId)) {
      return; // ignore for path_start / path_end
    }
    const event = this.events.get(eventId);
    if (!event) return;

    this.events.set(eventId, {
      ...event,
      isPinned: !event.isPinned,
      isHidden: false,
    });

    this.updateMatrixValueBounds();
  };

  setSearchQuery = (query: string) => {
    this.search.query = query;
    this.updateMatchedEvents();
  };

  setHighlightedEvent = (eventId: string | null) => {
    this.search.highlightedEventId = eventId;
  };

  private updateMatchedEvents = () => {
    this.search.matchedEvents.clear();
    if (!this.search.query) return;

    const query = this.search.query.toLowerCase();
    for (const event of this.displayedEvents) {
      if (event.id.toLowerCase().includes(query)) {
        this.search.matchedEvents.add(event.id);
      }
    }
  };

  setHeatmapType = (type: HeatmapType) => {
    this.heatmapType = type;
  };

  // Reset filters to default bounds (real data bounds)
  resetFiltersToDefaults = () => {
    this.filters.population = { ...this.populationBounds };
    this.filters.matrixValue = {
      min: this.matrixValueBounds.min,
      max: this.matrixValueBounds.max,
      reverse: false,
    };
    this.collapseHidden = false;
    this._populationCustomized = false;
  };

  // Helpers
  private isImmutable(eventId: string) {
    return eventId === "path_start" || eventId === "path_end";
  }

  // -------- Overlay (AI highlights) --------
  // We keep a set of highlighted cell keys: `${eventId}→${matrixIndex}→${stepIndex}`
  overlayHighlightedCells = new Set<string>();

  applyOverlay = (
    cells: Array<{ eventId: string; matrixIndex: number; stepIndex: number }>,
  ) => {
    const next = new Set<string>();
    for (const c of cells) {
      if (
        !c ||
        !c.eventId ||
        typeof c.matrixIndex !== "number" ||
        typeof c.stepIndex !== "number"
      )
        continue;
      next.add(`${c.eventId}→${c.matrixIndex}→${c.stepIndex}`);
    }
    this.overlayHighlightedCells = next;
  };

  clearOverlay = () => {
    this.overlayHighlightedCells = new Set<string>();
  };

  // -------- Step Sankey Helpers --------

  toggleColumnExpansion = (stepIndex: number) => {
    if (this.expandedColumns.has(stepIndex)) {
      this.expandedColumns.delete(stepIndex);
    } else {
      this.expandedColumns.add(stepIndex);
    }
  };

  getTopEvents = (
    stepIndex: number,
    limit: number = 6,
    isDiff: boolean = false,
    matrixIndex: number = this.activeMatrixIndex,
  ): { top: Event[]; rest: Event[] } => {
    const eventsWithValues = this.visibleEvents
      .filter((e) => !this.isImmutable(e.id)) // path_start/path_end only as fixed anchors
      .map((event) => {
        const value = this.getMatrixValue(event.id, stepIndex, matrixIndex);
        return { event, value };
      });

    // Filter out events with 0 value
    // Actually, we usually want to show non-zero events.
    const activeEvents = eventsWithValues.filter(
      (item) => Math.abs(item.value) > 0,
    );

    if (isDiff) {
      // Diff Mode: Top 3 Positive, Top 3 Negative
      const positive = activeEvents
        .filter((e) => e.value > 0)
        .sort((a, b) => b.value - a.value);
      const negative = activeEvents
        .filter((e) => e.value < 0)
        .sort((a, b) => a.value - b.value); // Ascending for negative (most negative first)

      const topPositive = positive.slice(0, 3);
      const topNegative = negative.slice(0, 3);

      const top = [...topPositive, ...topNegative];

      // Rest are the ones not in top
      const topIds = new Set(top.map((i) => i.event.id));
      const rest = activeEvents
        .filter((i) => !topIds.has(i.event.id))
        .map((i) => i.event);

      return { top: top.map((i) => i.event), rest };
    } else {
      // Standard Mode: Top K by absolute value
      activeEvents.sort((a, b) => Math.abs(b.value) - Math.abs(a.value));

      const top = activeEvents.slice(0, limit).map((i) => i.event);
      const rest = activeEvents.slice(limit).map((i) => i.event);

      return { top, rest };
    }
  };
}
