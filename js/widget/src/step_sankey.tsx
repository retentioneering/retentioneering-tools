import * as React from "react";
import { createRoot } from "react-dom/client";
import { reaction } from "mobx";
import { parseJson, ComputingSpinner, RetentioneeringSpinKeyframes, useHostSubscriptions, type RenderContext } from "./widget-utils";
import {
  StepSankey,
  StepMatrixStore,
  SettingsSidebar,
  type MatrixValueType,
  type StoredPosition,
  DEFAULT_VALUE_TYPE,
} from "@retentioneering/viz-core";

function SidebarToggle({ onClick }: { onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      title="Toggle settings"
      style={{
        position: "absolute", top: 10, right: 16, zIndex: 25,
        display: "flex", alignItems: "center", justifyContent: "center",
        width: 32, height: 32, borderRadius: 6, cursor: "pointer",
        background: "#f3f4f6", border: "1px solid #d1d5db", color: "#6b7280",
      }}
    >
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
        <rect x="1" y="1" width="14" height="14" rx="2" stroke="currentColor" strokeWidth="1.5" />
        <line x1="10" y1="1" x2="10" y2="15" stroke="currentColor" strokeWidth="1.5" />
      </svg>
    </button>
  );
}

export function render({ host, el, isStatic = false }: RenderContext) {
  const store = new StepMatrixStore();

  function syncResultToStore() {
    const raw = host.get("result") as string;
    if (!raw || raw === "{}") return;
    try {
      const d = parseJson<{ matrices?: unknown[]; event_counts?: Record<string, number> }>(raw, {});
      if (d?.matrices) store.setData(d.matrices as any);
      store.applyEventCounts(d?.event_counts ?? null);
    } catch {}
  }
  syncResultToStore();

  // Restore the Event count filter. setPopulationRange marks the filter as
  // user-customized, so applyEventCounts won't overwrite it on later syncs.
  {
    const f = parseJson<number[]>(host.get("event_count_filter") || "", []);
    if (f.length === 2) store.setPopulationRange(f[0], f[1]);
  }

  function App() {
    const [maxSteps, setMaxSteps]         = React.useState<number>(() => (host.get("max_steps") as number) ?? 10);
    const [pathPattern, setPathPattern]   = React.useState<string>(() => (host.get("path_pattern") as string) || "");
    // Initialize diff from host immediately so isDiff is correct on first render
    const [diffSegment, setDiffSegment] = React.useState<string | null>(() => {
      const d = parseJson<string[]>(host.get("diff") || "[]", []);
      return d[0] ?? null;
    });
    const [diffValue1,  setDiffValue1]  = React.useState<string | null>(() => {
      const d = parseJson<string[]>(host.get("diff") || "[]", []);
      return d[1] ?? null;
    });
    const [diffValue2,  setDiffValue2]  = React.useState<string | null>(() => {
      const d = parseJson<string[]>(host.get("diff") || "[]", []);
      return d[2] ?? null;
    });
    const [pathCols, setPathCols]     = React.useState<string[]>(() => parseJson(host.get("path_cols"), []));
    const [pathIdCol, setPathIdCol]   = React.useState<string>(() => (host.get("path_col") as string) || "");
    const [segmentLevels, setSegLvls] = React.useState<Record<string, string[]>>(() => parseJson(host.get("segment_levels"), {}));
    const [height, setHeight]         = React.useState<number>(() => (host.get("height") as number) ?? 500);
    const [isLoading, setIsLoading]   = React.useState<boolean>(() => (host.get("is_loading") as boolean) ?? false);
    const [sidebarOpen, setSidebarOpen] = React.useState<boolean>(() => (host.get("sidebar_open") as boolean) ?? true);
    const [stepWindow,  setStepWindow]  = React.useState<number>(() => (host.get("step_window") as number) || 3);
    const [initialPositions, setInitPos] = React.useState<Record<string, StoredPosition>>(
      () => parseJson(host.get("node_positions"), {}),
    );

    React.useEffect(() => { syncResultToStore(); }, []);

    // Sync the Event count filter to Python. Only a user-customized filter is
    // persisted; "" means "follow the data bounds" (the default behaviour).
    React.useEffect(() => {
      const dispose = reaction(
        () => ({
          min: store.filters.population.min,
          max: store.filters.population.max,
          customized: store.populationCustomized,
        }),
        ({ min, max, customized }) => {
          host.set("event_count_filter", customized ? JSON.stringify([min, max]) : "");
        },
        { fireImmediately: false, delay: 150 }
      );
      return dispose;
    }, []);

    const handleScrollXChange = React.useCallback((x: number) => {
      host.set("scroll_x", x);
    }, []);

    useHostSubscriptions(host, [
      ["result",         () => syncResultToStore()],
      ["is_loading",     () => setIsLoading((host.get("is_loading") as boolean) ?? false)],
      ["max_steps",      () => setMaxSteps((host.get("max_steps") as number) ?? 10)],
      ["path_pattern",   () => setPathPattern((host.get("path_pattern") as string) || "")],
      ["height",         () => setHeight((host.get("height") as number) ?? 500)],
      ["sidebar_open",   () => setSidebarOpen((host.get("sidebar_open") as boolean) ?? true)],
      ["step_window",    () => setStepWindow((host.get("step_window") as number) || 3)],
      ["path_cols",      () => setPathCols(parseJson(host.get("path_cols"), []))],
      ["path_col",    () => setPathIdCol((host.get("path_col") as string) || "")],
      ["segment_levels", () => setSegLvls(parseJson(host.get("segment_levels"), {}))],
      ["node_positions", () => {
        const p = parseJson<Record<string, StoredPosition>>(host.get("node_positions"), {});
        if (Object.keys(p).length > 0) setInitPos(p);
      }],
      ["diff", () => {
        const d = parseJson<string[]>(host.get("diff") || "[]", []);
        setDiffSegment(d[0] ?? null); setDiffValue1(d[1] ?? null); setDiffValue2(d[2] ?? null);
      }],
    ]);

    const handleDiffChange = React.useCallback((seg: string | null, v1: string | null, v2: string | null) => {
      setDiffSegment(seg); setDiffValue1(v1); setDiffValue2(v2);
      host.set("diff", seg != null && v1 != null && v2 != null && seg !== "" && v1 !== "" && v2 !== ""
        ? JSON.stringify([seg, v1, v2])
        : "");
    }, []);

    const handlePathIdColChange = React.useCallback((col: string) => {
      setPathIdCol(col); host.set("path_col", col);
    }, []);

    const handleToggleSidebar = React.useCallback(() => {
      setSidebarOpen((prev) => { const next = !prev; host.set("sidebar_open", next); return next; });
    }, []);

    return (
      <div style={{ display: "flex", flexDirection: "row", height, background: "#ffffff", borderRadius: 8, overflow: "hidden", border: "1px solid #e2e8f0", fontFamily: "system-ui,-apple-system,sans-serif" }}>
        {/* Wrapper: position:relative so the toggle button anchors correctly */}
        <div style={{ flex: 1, position: "relative", overflow: "hidden", minWidth: 0 }}>
          {/* Toggle rendered here — positioned relative to this wrapper, z-index above StepSankey */}
          <SidebarToggle onClick={handleToggleSidebar} />

          <div style={{ width: "100%", height: "100%" }}>
            <div style={{ width: "100%", height: "100%", display: "flex", flexDirection: "column" }}>
              <StepSankey
                store={store}
                maxSteps={maxSteps}
                stepWindow={stepWindow}
                pathPattern={pathPattern}
                onPatternChange={isStatic ? undefined : (p) => {
                  setPathPattern(p);
                  host.set("path_pattern", p);
                }}
                diffSegment={diffSegment}
                diffValue1={diffValue1}
                diffValue2={diffValue2}
                theme="light"
                initialScrollX={(host.get("scroll_x") as number) || 0}
                onScrollXChange={handleScrollXChange}
              />
            </div>
          </div>

          {isLoading && <ComputingSpinner opacity={0.55} />}
        </div>

        {sidebarOpen && (
          <SettingsSidebar
            store={store as any}
            valuesType={DEFAULT_VALUE_TYPE}
            onValuesTypeChange={() => {}}
            showValueType={false}
            pathCols={pathCols}
            pathIdCol={pathIdCol}
            onPathIdColChange={handlePathIdColChange}
            segmentLevels={segmentLevels}
            diffSegment={diffSegment}
            diffValue1={diffValue1}
            diffValue2={diffValue2}
            onDiffChange={handleDiffChange}
            isLoading={isLoading}
            onResetFilters={() => store.resetFiltersToDefaults()}
            theme="light"
            stepWindow={stepWindow}
            maxSteps={maxSteps}
            onStepWindowChange={(w) => {
              setStepWindow(w);
              host.set("step_window", w);
            }}
            isStatic={isStatic}
          />
        )}

        <RetentioneeringSpinKeyframes />
      </div>
    );
  }

  const root = createRoot(el);
  root.render(<App />);
  return () => root.unmount();
}
