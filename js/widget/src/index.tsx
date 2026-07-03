import * as React from "react";
import { createRoot } from "react-dom/client";
import { parseJson, ComputingSpinner, RetentioneeringSpinKeyframes } from "./widget-utils";
import { reaction } from "mobx";
import {
  TransitionGraph,
  TransitionMatrixStore,
  SettingsSidebar,
  type MatrixValueType,
  type StoredPosition,
  DEFAULT_VALUE_TYPE,
} from "@retentioneering/viz-core";
import { JupyterDataProvider } from "./JupyterDataProvider";
import { AuthGate, loadSession, clearSession, refreshSession, type AuthSession } from "./AuthGate";
import { CloudSection, CloudModal, CloudErrorModal } from "./CloudSection";

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
    <button onClick={onClick} title="Toggle settings" style={{
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

export function render({ model, el, isStatic = false }: RenderContext) {
  const store    = new TransitionMatrixStore();
  const provider = new JupyterDataProvider(model);

  function applyEventVisibility() {
    const raw = model.get("event_visibility") as string;
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
    const raw = model.get("result") as string;
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
    const raw = model.get("event_counts") as string;
    if (!raw || raw === "{}") return;
    try { store.applyEventCounts(JSON.parse(raw)); } catch {}
  }

  function parseCountsMap(key: string): Record<string, number> {
    try { return JSON.parse((model.get(key) as string) || "{}"); } catch { return {}; }
  }

  syncResultToStore();
  syncEventCounts();

  function App() {
    const [valuesType, setValuesType] = React.useState<MatrixValueType>(
      () => (model.get("edge_weight") as MatrixValueType) ?? DEFAULT_VALUE_TYPE,
    );
    const initDiff = parseJson<string[]>(model.get("diff") || "[]", []);
    const [diffSegment, setDiffSegment] = React.useState<string | null>(initDiff[0] ?? null);
    const [diffValue1,  setDiffValue1]  = React.useState<string | null>(initDiff[1] ?? null);
    const [diffValue2,  setDiffValue2]  = React.useState<string | null>(initDiff[2] ?? null);
    const [pathCols, setPathCols]       = React.useState<string[]>(() => parseJson(model.get("path_cols"), []));
    const [pathIdCol, setPathIdCol]     = React.useState<string>(() => (model.get("path_id_col") as string) || "");
    const [segmentLevels, setSegLvls]   = React.useState<Record<string, string[]>>(() => parseJson(model.get("segment_levels"), {}));
    const [height, setHeight]           = React.useState<number>(() => (model.get("height") as number) ?? 500);
    const [isLoading, setIsLoading]     = React.useState<boolean>(() => (model.get("is_loading") as boolean) ?? false);
    const [sidebarOpen, setSidebarOpen] = React.useState<boolean>(() => (model.get("sidebar_open") as boolean) ?? true);
    const fitRef = React.useRef<(() => void) | undefined>(undefined);
    const [session, setSession]         = React.useState<AuthSession | null>(() => loadSession());
    const [widgetId, setWidgetId] = React.useState<string>(() => (model.get("widget_id") as string) || "");
    const [cloudStatus, setCloudStatus] = React.useState<string>(() => (model.get("cloud_status") as string) || "idle");
    const cloudEnabled = (model.get("cloud_enabled") as boolean) ?? false;
    const cloudManageUrl = (model.get("cloud_manage_url") as string) || "";
    const [showCloudAuth, setShowCloudAuth] = React.useState(() => !!widgetId && !loadSession());
    const [pendingAuthAction, setPendingAuthAction] = React.useState<(() => void) | null>(null);
    const [cloudModalOpen, setCloudModalOpen] = React.useState(false);
    const [cloudNameExists, setCloudNameExists] = React.useState(false);
    const [cloudError, setCloudError] = React.useState<string | null>(null);
    const [cloudWarning, setCloudWarning] = React.useState<string | null>(null);

    // Sync session token to Python on init; refresh if expired; auto-load if object_name is set
    React.useEffect(() => {
      const syncSession = async () => {
        let s = loadSession();
        if (!s) s = await refreshSession();
        if (s) {
          setSession(s);
          model.set("auth_token", s.access_token);
          if (widgetId) model.set("cloud_load_trigger", 1);
          model.save_changes();
        }
      };
      syncSession();
    }, []);
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
          model.set("event_visibility", JSON.stringify(vis));
          model.save_changes();
        },
        { fireImmediately: false, delay: 100 }
      );
      return dispose;
    }, []);

    const [eventCounts,  setEventCounts]  = React.useState<Record<string, number>>(() => parseCountsMap("event_counts"));
    const [eventCountsG1, setCountsG1]   = React.useState<Record<string, number>>(() => parseCountsMap("event_counts_g1"));
    const [eventCountsG2, setCountsG2]   = React.useState<Record<string, number>>(() => parseCountsMap("event_counts_g2"));
    const [initialPositions, setInitPos] = React.useState<Record<string, StoredPosition>>(
      () => parseJson(model.get("node_positions"), {}),
    );

    React.useEffect(() => { syncResultToStore(); syncEventCounts(); }, []);

    React.useEffect(() => {
      const subs: Array<[string, () => void]> = [
        ["result",         () => { syncResultToStore(); syncEventCounts(); }],
        ["event_counts",   () => { syncEventCounts(); setEventCounts(parseCountsMap("event_counts")); }],
        ["event_counts_g1",() => setCountsG1(parseCountsMap("event_counts_g1"))],
        ["event_counts_g2",() => setCountsG2(parseCountsMap("event_counts_g2"))],
        ["is_loading",     () => setIsLoading((model.get("is_loading") as boolean) ?? false)],
        ["cloud_status", () => {
          const s = (model.get("cloud_status") as string) || "idle";
          setCloudStatus(s);
          if (s.startsWith("error:")) setCloudError(s.slice(6));
        }],
        ["widget_id",         () => setWidgetId((model.get("widget_id") as string) || "")],
        ["cloud_name_exists",  () => setCloudNameExists((model.get("cloud_name_exists") as boolean) ?? false)],
        ["cloud_load_warning", () => setCloudWarning((model.get("cloud_load_warning") as string) || null)],
        ["edge_weight",    () => setValuesType((model.get("edge_weight") as MatrixValueType) ?? DEFAULT_VALUE_TYPE)],
        ["height",         () => setHeight((model.get("height") as number) ?? 500)],
        ["sidebar_open",   () => setSidebarOpen((model.get("sidebar_open") as boolean) ?? true)],
        ["path_cols",      () => setPathCols(parseJson(model.get("path_cols"), []))],
        ["path_id_col",    () => setPathIdCol((model.get("path_id_col") as string) || "")],
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

    const handleValuesChange = React.useCallback((v: MatrixValueType) => {
      setValuesType(v); model.set("edge_weight", v); model.save_changes();
    }, []);
    const handlePathIdColChange = React.useCallback((col: string) => {
      setPathIdCol(col); model.set("path_id_col", col); model.save_changes();
    }, []);
    const handleDiffChange = React.useCallback(
      (seg: string | null, v1: string | null, v2: string | null) => {
        setDiffSegment(seg); setDiffValue1(v1); setDiffValue2(v2);
        // Use != null (not truthy) so segment values like false/0 are handled correctly
        model.set("diff", seg != null && v1 != null && v2 != null && seg !== "" && v1 !== "" && v2 !== ""
          ? JSON.stringify([seg, v1, v2])
          : "");
        model.save_changes();
      }, [],
    );
    const handleToggleSidebar = React.useCallback(() => {
      setSidebarOpen((prev) => { const next = !prev; model.set("sidebar_open", next); model.save_changes(); return next; });
    }, []);
    const handlePositionsChange = React.useCallback(
      (positions: Record<string, StoredPosition>) => {
        model.set("node_positions", JSON.stringify(positions)); model.save_changes();
      }, [],
    );

    // graph area: flex: 1 — same as before AuthGate was introduced
    const graphArea = (
      <div style={{ flex: 1, position: "relative", overflow: "hidden", minWidth: 0 }}>
        <TransitionGraph
          store={store}
          dataProvider={null}
          widgetId={widgetId}
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
          fitRef={fitRef}
        />
        <SidebarToggle onClick={handleToggleSidebar} />
        {cloudWarning && (
          <div style={{ position: "absolute", top: 0, left: 0, right: 0, zIndex: 40, background: "#fffbeb", borderBottom: "1px solid #fde68a", padding: "6px 12px 6px 14px", display: "flex", alignItems: "center", gap: 8, fontSize: 11, color: "#92400e" }}>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#d97706" strokeWidth="2" style={{ flexShrink: 0 }}><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
            <span style={{ flex: 1 }}>{cloudWarning}</span>
            <button onClick={() => setCloudWarning(null)} style={{ background: "none", border: "none", cursor: "pointer", color: "#d97706", padding: 2, lineHeight: 1, flexShrink: 0 }}>
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M18 6 6 18M6 6l12 12"/></svg>
            </button>
          </div>
        )}
        {isLoading && <ComputingSpinner opacity={0.55} />}
        {/* Cloud loading overlay */}
        {cloudStatus === "loading" && !isLoading && <ComputingSpinner opacity={0.7} label="Loading…" />}
        {/* No data overlay — shown when cloud_file_name set but no data loaded yet */}
        {widgetId && !store.hasData && !isLoading && cloudStatus !== "loading" && (
          <div style={{ position: "absolute", inset: 0, background: "rgba(255,255,255,0.85)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 29 }}>
            <span style={{ color: "#9ca3af", fontSize: 13 }}>No data</span>
          </div>
        )}
      </div>
    );

    return (
      <div style={{ display: "flex", flexDirection: "row", height, background: "#ffffff", borderRadius: 8, overflow: "hidden", border: "1px solid #e2e8f0", fontFamily: "system-ui,-apple-system,sans-serif", position: "relative" }}>
        {/* AuthGate: only shown for cloud auth when user clicks Load */}
        <AuthGate
          title="Unlock Cloud features"
          description={<>Save and restore widget configurations — including node positions — across sessions and machines.<br /><br />Cloud sync is invite-only for now. To request early access, message me on <a href="https://www.linkedin.com/in/vladimir-kukushkin" target="_blank" rel="noopener noreferrer" style={{ color: "#0a66c2" }}>LinkedIn</a>.</>}
          session={session}
          onLogin={(s) => {
            setSession(s);
            setShowCloudAuth(false);
            model.set("auth_token", s.access_token);
            model.save_changes();
            if (pendingAuthAction) {
              pendingAuthAction();
              setPendingAuthAction(null);
            } else if (widgetId) {
              const cur = (model.get("cloud_load_trigger") as number) || 0;
              model.set("cloud_load_trigger", cur + 1);
              model.save_changes();
            }
          }}
          disabled={!showCloudAuth}
          onClose={() => setShowCloudAuth(false)}
          style={{ flex: 1, position: "relative", overflow: "hidden", minWidth: 0 }}
        >
          {graphArea}
        </AuthGate>

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
            authEmail={session?.user.email ?? null}
            onLogout={() => { clearSession(); setSession(null); }}
            isStatic={isStatic}
            headerRight={
              <CloudSection
                widgetId={widgetId}
                cloudStatus={cloudStatus}
                session={session}
                enabled={cloudEnabled}
                onOpen={() => setCloudModalOpen(true)}
                onAuthNeeded={action => {
                  setPendingAuthAction(() => action);
                  setShowCloudAuth(true);
                  model.set("cloud_auth_shown", ((model.get("cloud_auth_shown") as number) || 0) + 1);
                  model.save_changes();
                }}
              />
            }
          />
        )}

        {cloudError && (
          <CloudErrorModal message={cloudError} onClose={() => setCloudError(null)} />
        )}
        {cloudModalOpen && (
          <CloudModal
            widgetId={widgetId}
            nameExists={cloudNameExists}
            manageUrl={cloudManageUrl}
            onCheckName={name => { model.set("cloud_name_check", name); model.save_changes(); }}
            onSave={name => { model.set("cloud_save_request", name); model.save_changes(); }}
            onLoad={() => { const cur = (model.get("cloud_load_trigger") as number) || 0; model.set("cloud_load_trigger", cur + 1); model.save_changes(); }}
            onClose={() => setCloudModalOpen(false)} />
        )}
        <RetentioneeringSpinKeyframes />
      </div>
    );
  }

  const root = createRoot(el);
  root.render(<App />);
  return () => root.unmount();
}
