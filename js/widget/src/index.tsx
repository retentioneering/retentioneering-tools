import * as React from "react";
import { createRoot } from "react-dom/client";
import { parseJson, ComputingSpinner, RetentioneeringSpinKeyframes, useHostSubscriptions, type RenderContext } from "./widget-utils";
import { reaction } from "mobx";
import {
  TransitionGraph,
  TransitionMatrixStore,
  SettingsSidebar,
  type MatrixValueType,
  type StoredPosition,
  type StoredViewport,
  type EdgeFilterSpec,
  type GraphView,
  parseGraphView,
  decodeGraphView,
  DEFAULT_VALUE_TYPE,
} from "@retentioneering/viz-core";

function SidebarToggle({ onClick }: { onClick: () => void }) {
  return (
    <button onClick={onClick} aria-label="Toggle settings sidebar" data-rete-tooltip="Toggle settings sidebar" style={{
      position: "absolute", top: 10, right: 16, zIndex: 25,
      display: "flex", alignItems: "center", justifyContent: "center",
      width: 32, height: 32, borderRadius: 6, cursor: "pointer",
      background: "#f3f4f6", border: "1px solid #d1d5db", color: "#6b7280",
    }}>
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
        <rect x="1" y="1" width="14" height="14" rx="2" stroke="currentColor" strokeWidth="1.5" />
        <line x1="10" y1="1" x2="10" y2="15" stroke="currentColor" strokeWidth="1.5" />
      </svg>
    </button>
  );
}

export function render({ host, el, isStatic = false }: RenderContext) {
  const store = new TransitionMatrixStore();

  function applyEventVisibility() {
    const raw = host.get("event_visibility") as string;
    if (!raw || raw === "{}") return;
    try {
      const vis = JSON.parse(raw) as Record<string, { isHidden: boolean; isPinned: boolean }>;
      store.events.forEach((e, id) => {
        const v = vis[id];
        if (v !== undefined) store.events.set(id, { ...e, isHidden: !!v.isHidden, isPinned: !!v.isPinned });
      });
    } catch {}
  }

  function syncResultToStore() {
    const raw = host.get("result") as string;
    if (!raw || raw === "{}") return;
    try {
      const d = JSON.parse(raw);
      if (d?.events && d?.values) {
        store.setData({ events: d.events, values: d.values, group1: d.group1 ?? null, group2: d.group2 ?? null });
        applyEventVisibility();
      }
    } catch {}
  }

  function syncEventCounts() {
    const raw = host.get("event_counts") as string;
    if (!raw || raw === "{}") return;
    try { store.applyEventCounts(JSON.parse(raw)); } catch {}
  }

  function parseCountsMap(key: string): Record<string, number> {
    try { return JSON.parse((host.get(key) as string) || "{}"); } catch { return {}; }
  }

  syncResultToStore();
  syncEventCounts();

  // Restore the Event count filter. setPopulationRange marks the filter as
  // user-customized, so applyEventCounts won't overwrite it on later syncs.
  {
    const f = parseJson<number[]>(host.get("event_count_filter") || "", []);
    if (f.length === 2) store.setPopulationRange(f[0], f[1]);
  }

  function App() {
    const [valuesType, setValuesType] = React.useState<MatrixValueType>(
      () => (host.get("edge_weight") as MatrixValueType) ?? DEFAULT_VALUE_TYPE,
    );
    const initDiff = parseJson<string[]>(host.get("diff") || "[]", []);
    const [diffSegment, setDiffSegment] = React.useState<string | null>(initDiff[0] ?? null);
    const [diffValue1,  setDiffValue1]  = React.useState<string | null>(initDiff[1] ?? null);
    const [diffValue2,  setDiffValue2]  = React.useState<string | null>(initDiff[2] ?? null);
    const [pathCols, setPathCols]       = React.useState<string[]>(() => parseJson(host.get("path_cols"), []));
    const [pathIdCol, setPathIdCol]     = React.useState<string>(() => (host.get("path_col") as string) || "");
    const [segmentLevels, setSegLvls]   = React.useState<Record<string, string[]>>(() => parseJson(host.get("segment_levels"), {}));
    const [height, setHeight]           = React.useState<number>(() => (host.get("height") as number) ?? 500);
    const [isLoading, setIsLoading]     = React.useState<boolean>(() => (host.get("is_loading") as boolean) ?? false);
    const [sidebarOpen, setSidebarOpen] = React.useState<boolean>(() => (host.get("sidebar_open") as boolean) ?? true);
    const fitRef = React.useRef<(() => void) | undefined>(undefined);

    // Sync event visibility (hidden/pinned) to Python whenever store changes
    React.useEffect(() => {
      const dispose = reaction(
        () => Array.from(store.events.entries())
          .map(([id, e]) => `${id}:${e.isHidden ? 1 : 0}:${e.isPinned ? 1 : 0}`)
          .join(","),
        () => {
          const vis: Record<string, { isHidden: boolean; isPinned: boolean }> = {};
          store.events.forEach((e, id) => {
            if (e.isHidden || e.isPinned) vis[id] = { isHidden: e.isHidden, isPinned: e.isPinned };
          });
          host.set("event_visibility", JSON.stringify(vis));
        },
        { fireImmediately: false, delay: 100 }
      );
      return dispose;
    }, []);

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

    const [eventCounts,  setEventCounts]  = React.useState<Record<string, number>>(() => parseCountsMap("event_counts"));
    const [eventCountsG1, setCountsG1]   = React.useState<Record<string, number>>(() => parseCountsMap("event_counts_g1"));
    const [eventCountsG2, setCountsG2]   = React.useState<Record<string, number>>(() => parseCountsMap("event_counts_g2"));
    const [initialPositions, setInitPos] = React.useState<Record<string, StoredPosition>>(
      () => parseJson(host.get("node_positions"), {}),
    );

    React.useEffect(() => { syncResultToStore(); syncEventCounts(); }, []);

    useHostSubscriptions(host, [
      ["result",         () => { syncResultToStore(); syncEventCounts(); }],
      ["event_counts",   () => { syncEventCounts(); setEventCounts(parseCountsMap("event_counts")); }],
      ["event_counts_g1",() => setCountsG1(parseCountsMap("event_counts_g1"))],
      ["event_counts_g2",() => setCountsG2(parseCountsMap("event_counts_g2"))],
      ["is_loading",     () => setIsLoading((host.get("is_loading") as boolean) ?? false)],
      ["edge_weight",    () => setValuesType((host.get("edge_weight") as MatrixValueType) ?? DEFAULT_VALUE_TYPE)],
      ["height",         () => setHeight((host.get("height") as number) ?? 500)],
      ["sidebar_open",   () => setSidebarOpen((host.get("sidebar_open") as boolean) ?? true)],
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

    const handleValuesChange = React.useCallback((v: MatrixValueType) => {
      setValuesType(v); host.set("edge_weight", v);
    }, []);
    const handlePathIdColChange = React.useCallback((col: string) => {
      setPathIdCol(col); host.set("path_col", col);
    }, []);
    const handleDiffChange = React.useCallback(
      (seg: string | null, v1: string | null, v2: string | null) => {
        setDiffSegment(seg); setDiffValue1(v1); setDiffValue2(v2);
        // Use != null (not truthy) so segment values like false/0 are handled correctly
        host.set("diff", seg != null && v1 != null && v2 != null && seg !== "" && v1 !== "" && v2 !== ""
          ? JSON.stringify([seg, v1, v2])
          : "");
      }, [],
    );
    const handleToggleSidebar = React.useCallback(() => {
      setSidebarOpen((prev) => { const next = !prev; host.set("sidebar_open", next); return next; });
    }, []);
    const handlePositionsChange = React.useCallback(
      (positions: Record<string, StoredPosition>) => {
        host.set("node_positions", JSON.stringify(positions));
      }, [],
    );
    // edge_filter accepts two serialized shapes: the legacy "[min, max]"
    // array (old saved states and old exported HTML → manual range mode) and
    // the newer '{"mode": "topk", "k": n}' object. Range mode is written
    // back in the legacy array form so old readers keep working.
    const initialEdgeFilter = React.useMemo<EdgeFilterSpec | null>(() => {
      const parsed = parseJson<unknown>((host.get("edge_filter") as string) || "", null);
      if (Array.isArray(parsed) && parsed.length === 2) {
        return { mode: "range", range: [Number(parsed[0]), Number(parsed[1])] };
      }
      if (parsed && typeof parsed === "object") {
        const spec = parsed as { mode?: string; k?: number; range?: number[] };
        if (spec.mode === "topk" && Number.isFinite(spec.k)) {
          return { mode: "topk", k: Number(spec.k) };
        }
        if (spec.mode === "range" && Array.isArray(spec.range) && spec.range.length === 2) {
          return { mode: "range", range: [Number(spec.range[0]), Number(spec.range[1])] };
        }
      }
      return null;
    }, []);
    const handleEdgeFilterChange = React.useCallback((filter: EdgeFilterSpec) => {
      host.set(
        "edge_filter",
        filter.mode === "range" ? JSON.stringify(filter.range) : JSON.stringify(filter),
      );
    }, []);
    const initialViewport = React.useMemo<StoredViewport | null>(
      () => parseJson<StoredViewport | null>(host.get("viewport") || "", null),
      [],
    );
    const handleViewportChange = React.useCallback((viewport: StoredViewport) => {
      host.set("viewport", JSON.stringify(viewport));
    }, []);

    // GraphView: named pills from the `views` traitlet; the initial view is
    // a JSON object, a name from `views`, or (in static exports) a base64url
    // string injected from the #view= URL fragment.
    const [graphViews, setGraphViews] = React.useState<GraphView[]>(() =>
      parseJson<unknown[]>(host.get("views") || "[]", [])
        .map((raw) => parseGraphView(raw))
        .filter((v): v is GraphView => v !== null),
    );
    useHostSubscriptions(host, [
      ["views", () =>
        setGraphViews(
          parseJson<unknown[]>(host.get("views") || "[]", [])
            .map((raw) => parseGraphView(raw))
            .filter((v): v is GraphView => v !== null),
        ),
      ],
    ]);
    const initialGraphView = React.useMemo<GraphView | string | null>(() => {
      const raw = (host.get("view") as string) || "";
      if (!raw) return null;
      const parsed = parseGraphView(raw);
      if (parsed) return parsed;
      const decoded = decodeGraphView(raw);
      if (decoded) return decoded;
      return raw; // plain name referencing an entry in `views`
    }, []);
    const applyViewRef = React.useRef<
      ((view: GraphView | string) => void) | undefined
    >(undefined);
    // Expose per-root so analysis links (focusLink) can apply views/focus
    // through the full GraphView pipeline.
    React.useEffect(() => {
      (el as HTMLElement & { __applyGraphView?: unknown }).__applyGraphView = (
        view: GraphView | string,
      ) => applyViewRef.current?.(view);
      return () => {
        delete (el as HTMLElement & { __applyGraphView?: unknown })
          .__applyGraphView;
      };
    }, []);

    const graphArea = (
      <div style={{ flex: 1, position: "relative", overflow: "hidden", minWidth: 0 }}>
        <TransitionGraph
          store={store}
          host={isStatic ? null : host}
          widgetId={(host.get("widget_id") as string) || undefined}
          valuesType={valuesType}
          onValuesTypeChange={handleValuesChange}
          diffSegment={diffSegment}
          diffValue1={diffValue1}
          diffValue2={diffValue2}
          theme="light"
          eventCounts={Object.keys(eventCounts).length > 0 ? eventCounts : undefined}
          eventCountsG1={Object.keys(eventCountsG1).length > 0 ? eventCountsG1 : undefined}
          eventCountsG2={Object.keys(eventCountsG2).length > 0 ? eventCountsG2 : undefined}
          initialPositions={initialPositions}
          onPositionsChange={handlePositionsChange}
          initialEdgeFilter={initialEdgeFilter}
          onEdgeFilterChange={handleEdgeFilterChange}
          initialViewport={initialViewport}
          onViewportChange={handleViewportChange}
          fitRef={fitRef}
          views={graphViews}
          initialView={initialGraphView}
          applyViewRef={applyViewRef}
        />
        <SidebarToggle onClick={handleToggleSidebar} />
        {isLoading && <ComputingSpinner opacity={0.55} />}
      </div>
    );

    return (
      <div style={{ display: "flex", flexDirection: "row", height, background: "#ffffff", borderRadius: 8, overflow: "hidden", border: "1px solid #e2e8f0", fontFamily: "system-ui,-apple-system,sans-serif", position: "relative" }}>
        {graphArea}

        {sidebarOpen && (
          <SettingsSidebar
            store={store}
            valuesType={valuesType}
            onValuesTypeChange={handleValuesChange}
            pathCols={pathCols}
            pathIdCol={pathIdCol}
            onPathIdColChange={handlePathIdColChange}
            segmentLevels={segmentLevels}
            diffSegment={diffSegment}
            diffValue1={diffValue1}
            diffValue2={diffValue2}
            onDiffChange={handleDiffChange}
            isLoading={isLoading}
            onFitGraph={() => fitRef.current?.()}
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
