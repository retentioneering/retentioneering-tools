import * as React from "react";
import { createRoot } from "react-dom/client";
import { parseJson, ComputingSpinner, RetentioneeringSpinKeyframes } from "./widget-utils";
import {
  StepSankey,
  StepMatrixStore,
  SettingsSidebar,
  type MatrixValueType,
  type StoredPosition,
  DEFAULT_VALUE_TYPE,
} from "@retentioneering/viz-core";
import { JupyterDataProvider } from "./JupyterDataProvider";
import { AuthGate, loadSession, clearSession, type AuthSession } from "./AuthGate";

interface AnyWidgetModel {
  get(key: string): unknown;
  set(key: string, value: unknown): void;
  save_changes(): void;
  on(event: string, cb: () => void): void;
  off(event: string, cb: () => void): void;
}

interface RenderContext {
  model: AnyWidgetModel;
  el: HTMLElement;
  isStatic?: boolean;
}

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

export function render({ model, el, isStatic = false }: RenderContext) {
  const store    = new StepMatrixStore();
  const provider = new JupyterDataProvider(model);

  function syncResultToStore() {
    const raw = model.get("result") as string;
    if (!raw || raw === "{}") return;
    try {
      const d = parseJson<{ matrices?: unknown[]; event_counts?: Record<string, number> }>(raw, {});
      if (d?.matrices) store.setData(d.matrices as any);
      store.applyEventCounts(d?.event_counts ?? null);
    } catch {}
  }
  syncResultToStore();

  function App() {
    const [maxSteps, setMaxSteps]         = React.useState<number>(() => (model.get("max_steps") as number) ?? 10);
    const [pathPattern, setPathPattern]   = React.useState<string>(() => (model.get("path_pattern") as string) || "");
    // Initialize diff from model immediately so isDiff is correct on first render
    const [diffSegment, setDiffSegment] = React.useState<string | null>(() => {
      const d = parseJson<string[]>(model.get("diff") || "[]", []);
      return d[0] ?? null;
    });
    const [diffValue1,  setDiffValue1]  = React.useState<string | null>(() => {
      const d = parseJson<string[]>(model.get("diff") || "[]", []);
      return d[1] ?? null;
    });
    const [diffValue2,  setDiffValue2]  = React.useState<string | null>(() => {
      const d = parseJson<string[]>(model.get("diff") || "[]", []);
      return d[2] ?? null;
    });
    const [pathCols, setPathCols]     = React.useState<string[]>(() => parseJson(model.get("path_cols"), []));
    const [pathIdCol, setPathIdCol]   = React.useState<string>(() => (model.get("path_col") as string) || "");
    const [segmentLevels, setSegLvls] = React.useState<Record<string, string[]>>(() => parseJson(model.get("segment_levels"), {}));
    const [height, setHeight]         = React.useState<number>(() => (model.get("height") as number) ?? 500);
    const [isLoading, setIsLoading]   = React.useState<boolean>(() => (model.get("is_loading") as boolean) ?? false);
    const [sidebarOpen, setSidebarOpen] = React.useState<boolean>(() => (model.get("sidebar_open") as boolean) ?? true);
    const [stepWindow,  setStepWindow]  = React.useState<number>(() => (model.get("step_window") as number) || 3);
    const [session, setSession] = React.useState<AuthSession | null>(() => loadSession());
    const [initialPositions, setInitPos] = React.useState<Record<string, StoredPosition>>(
      () => parseJson(model.get("node_positions"), {}),
    );

    React.useEffect(() => { syncResultToStore(); }, []);

    React.useEffect(() => {
      const subs: Array<[string, () => void]> = [
        ["result",         () => syncResultToStore()],
        ["is_loading",     () => setIsLoading((model.get("is_loading") as boolean) ?? false)],
        ["max_steps",      () => setMaxSteps((model.get("max_steps") as number) ?? 10)],
        ["path_pattern",   () => setPathPattern((model.get("path_pattern") as string) || "")],
        ["height",         () => setHeight((model.get("height") as number) ?? 500)],
        ["sidebar_open",   () => setSidebarOpen((model.get("sidebar_open") as boolean) ?? true)],
        ["step_window",    () => setStepWindow((model.get("step_window") as number) || 3)],
        ["path_cols",      () => setPathCols(parseJson(model.get("path_cols"), []))],
        ["path_col",    () => setPathIdCol((model.get("path_col") as string) || "")],
        ["segment_levels", () => setSegLvls(parseJson(model.get("segment_levels"), {}))],
        ["node_positions", () => {
          const p = parseJson<Record<string, StoredPosition>>(model.get("node_positions"), {});
          if (Object.keys(p).length > 0) setInitPos(p);
        }],
        ["diff", () => {
          const d = parseJson<string[]>(model.get("diff") || "[]", []);
          setDiffSegment(d[0] ?? null); setDiffValue1(d[1] ?? null); setDiffValue2(d[2] ?? null);
        }],
      ];
      subs.forEach(([key, cb]) => model.on(`change:${key}`, cb));
      return () => subs.forEach(([key, cb]) => model.off(`change:${key}`, cb));
    }, []);

    const handleDiffChange = React.useCallback((seg: string | null, v1: string | null, v2: string | null) => {
      setDiffSegment(seg); setDiffValue1(v1); setDiffValue2(v2);
        model.set("diff", seg != null && v1 != null && v2 != null && seg !== "" && v1 !== "" && v2 !== ""
          ? JSON.stringify([seg, v1, v2])
          : "");
      model.save_changes();
    }, []);

    const handlePathIdColChange = React.useCallback((col: string) => {
      setPathIdCol(col); model.set("path_col", col); model.save_changes();
    }, []);

    const handleToggleSidebar = React.useCallback(() => {
      setSidebarOpen((prev) => { const next = !prev; model.set("sidebar_open", next); model.save_changes(); return next; });
    }, []);

    return (
      <div style={{ display: "flex", flexDirection: "row", height, background: "#ffffff", borderRadius: 8, overflow: "hidden", border: "1px solid #e2e8f0", fontFamily: "system-ui,-apple-system,sans-serif" }}>
        {/* Wrapper: position:relative so the toggle button anchors correctly */}
        <div style={{ flex: 1, position: "relative", overflow: "hidden", minWidth: 0 }}>
          {/* Toggle rendered here — positioned relative to this wrapper, z-index above StepSankey */}
          <SidebarToggle onClick={handleToggleSidebar} />

          <AuthGate session={session} onLogin={setSession} disabled={true} style={{ width: "100%", height: "100%" }}>
            <div style={{ width: "100%", height: "100%", display: "flex", flexDirection: "column" }}>
              <StepSankey
                store={store}
                maxSteps={maxSteps}
                stepWindow={stepWindow}
                pathPattern={pathPattern}
                onPatternChange={isStatic ? undefined : (p) => {
                  setPathPattern(p);
                  model.set("path_pattern", p);
                  model.save_changes();
                }}
                diffSegment={diffSegment}
                diffValue1={diffValue1}
                diffValue2={diffValue2}
                theme="light"
              />
            </div>
          </AuthGate>

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
            authEmail={session?.user.email ?? null}
            onLogout={() => { clearSession(); setSession(null); }}
            theme="light"
            stepWindow={stepWindow}
            maxSteps={maxSteps}
            onStepWindowChange={(w) => {
              setStepWindow(w);
              model.set("step_window", w);
              model.save_changes();
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
