import * as React from "react";
import { createPortal } from "react-dom";
import { formatNumber } from "../../utils/format-number";

// ── PatternNodeMenu ────────────────────────────────────────────────────────

type InsertView = "main" | "before" | "after" | "replace";
type InsertMode = "event" | "gap";

interface PatternMenuProps {
  pos: { top: number; left: number };
  menuQuery: string;
  setMenuQuery: (q: string) => void;
  menuInputRef: React.RefObject<HTMLInputElement | null>;
  menuProps: {
    eventOptions: string[];
    onAddPrevious?: (ev: string) => void;
    onAddNext?: (ev: string) => void;
    onReplace?: (ev: string) => void;
    onAddWildcardPrevious?: (ev: string) => void;
    onAddWildcardNext?: (ev: string) => void;
    onDelete?: () => void;
  };
  eventId: string;
  onClose: () => void;
}

const S = {
  menu:    { background: "#fff", border: "1px solid #e5e7eb", borderRadius: 8, boxShadow: "0 4px 16px rgba(0,0,0,0.12)", width: 280, overflow: "hidden" } as React.CSSProperties,
  header:  { padding: "8px 12px", borderBottom: "1px solid #f3f4f6", fontSize: 11, color: "#6b7280", fontWeight: 600 } as React.CSSProperties,
  tabRow:  { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 4, padding: "6px 10px", borderBottom: "1px solid #f3f4f6", background: "#f9fafb" } as React.CSSProperties,
  tabBase: { padding: "4px 0", borderRadius: 4, border: "none", cursor: "pointer", fontSize: 12, fontWeight: 500, transition: "all 0.1s" } as React.CSSProperties,
  list:    { overflowY: "auto" as const, maxHeight: 200 },
  item:    { padding: "7px 14px", fontSize: 13, cursor: "pointer", color: "#111827", display: "block", width: "100%", background: "none", border: "none", textAlign: "left" as const },
  search:  { width: "100%", boxSizing: "border-box" as const, border: "1px solid #e5e7eb", borderRadius: 6, padding: "6px 10px", fontSize: 13, outline: "none" },
  back:    { padding: "6px 14px", fontSize: 12, color: "#6b7280", cursor: "pointer", display: "flex", alignItems: "center", gap: 4, background: "none", border: "none", borderBottom: "1px solid #f3f4f6", width: "100%" } as React.CSSProperties,
};

const PatternNodeMenu = React.forwardRef<HTMLDivElement, PatternMenuProps>(
  ({ pos, menuQuery, setMenuQuery, menuInputRef, menuProps, eventId, onClose }, ref) => {
    const isPathStart = eventId === "path_start";
    const isPathEnd   = eventId === "path_end";
    const isInternal  = !isPathStart && !isPathEnd;

    // Determine initial view: simple nodes go straight to insert panel
    const initialView: InsertView = isPathStart ? "after" : isPathEnd ? "before" : "main";
    const [view, setView] = React.useState<InsertView>(initialView);
    const [mode, setMode] = React.useState<InsertMode>("event");

    React.useEffect(() => {
      setTimeout(() => menuInputRef.current?.focus(), 60);
    }, [view, menuInputRef]);

    const filtered = menuProps.eventOptions
      .filter(e => !menuQuery || e.toLowerCase().includes(menuQuery.toLowerCase()));

    const commit = (ev: string) => {
      if (view === "after")   { mode === "gap" ? menuProps.onAddWildcardNext?.(ev) : menuProps.onAddNext?.(ev); }
      if (view === "before")  { mode === "gap" ? menuProps.onAddWildcardPrevious?.(ev) : menuProps.onAddPrevious?.(ev); }
      if (view === "replace") { menuProps.onReplace?.(ev); }
      onClose();
    };

    const directionLabel =
      view === "after"   ? `Insert After ${eventId}`  :
      view === "before"  ? `Insert Before ${eventId}` :
      view === "replace" ? `Replace ${eventId}` : "";

    const hasGap = view === "after" ? !!menuProps.onAddWildcardNext : !!menuProps.onAddWildcardPrevious;

    return (
      <div ref={ref} style={{ position: "fixed", top: pos.top, left: pos.left, zIndex: 9999, ...S.menu }}
           onClick={e => e.stopPropagation()}>

        {/* Main menu for internal events */}
        {view === "main" && (
          <>
            {[
              { label: "Insert Before", action: () => { setView("before"); setMode("event"); setMenuQuery(""); } },
              { label: "Insert After",  action: () => { setView("after");  setMode("event"); setMenuQuery(""); } },
              { label: "Replace",       action: () => { setView("replace"); setMenuQuery(""); } },
            ].map(({ label, action }) => (
              <button key={label} onClick={action} style={S.item}
                onMouseEnter={e => (e.currentTarget.style.background = "#f3f4f6")}
                onMouseLeave={e => (e.currentTarget.style.background = "")}>
                {label}
              </button>
            ))}
            {menuProps.onDelete && (
              <>
                <div style={{ height: 1, background: "#f3f4f6", margin: "4px 0" }} />
                <button onClick={() => { menuProps.onDelete!(); onClose(); }} style={{ ...S.item, color: "#ef4444" }}
                  onMouseEnter={e => (e.currentTarget.style.background = "#fff5f5")}
                  onMouseLeave={e => (e.currentTarget.style.background = "")}>
                  Delete
                </button>
              </>
            )}
          </>
        )}

        {/* Insert / Replace panel */}
        {view !== "main" && (
          <>
            {isInternal && (
              <button onClick={() => { setView("main"); setMenuQuery(""); }} style={S.back}>
                ← {directionLabel}
              </button>
            )}
            {!isInternal && (
              <div style={S.header}>{directionLabel}</div>
            )}

            {/* Event / Gap + Event tabs */}
            {view !== "replace" && hasGap && (
              <div style={S.tabRow}>
                {([["event", "Event"], ["gap", "Gap + Event"]] as [InsertMode, string][]).map(([m, label]) => (
                  <button key={m} onClick={() => setMode(m)} style={{
                    ...S.tabBase,
                    background: mode === m ? "#fff" : "transparent",
                    color: mode === m ? "#111827" : "#6b7280",
                    boxShadow: mode === m ? "0 1px 3px rgba(0,0,0,0.1)" : "none",
                  }}>
                    {label}
                  </button>
                ))}
              </div>
            )}

            <div style={{ padding: "8px 10px", borderBottom: "1px solid #f3f4f6" }}>
              <input ref={menuInputRef} value={menuQuery} onChange={e => setMenuQuery(e.target.value)}
                placeholder="Search event…" style={S.search} />
            </div>
            <div style={S.list}>
              {filtered.map(ev => (
                <button key={ev} onClick={() => commit(ev)} style={S.item}
                  onMouseEnter={e => (e.currentTarget.style.background = "#f3f4f6")}
                  onMouseLeave={e => (e.currentTarget.style.background = "")}>
                  {ev}
                </button>
              ))}
              {filtered.length === 0 && <div style={{ padding: "10px 14px", fontSize: 12, color: "#9ca3af" }}>No events found</div>}
            </div>
          </>
        )}
      </div>
    );
  }
);

// ── StepNode ───────────────────────────────────────────────────────────────

export interface Event {
  id: string;
  isPinned: boolean;
  isHidden: boolean;
  population: number;
}

// Node size constant - uniform squares
export const NODE_SIZE = 40;

// Minimum value to display - anything below this is filtered out
export const MIN_DISPLAY_VALUE = 0.005;

// Helper to calculate node height based on content
export const calculateNodeHeight = (
  eventId: string,
  isCenter: boolean,
): number => {
  if (!isCenter) return NODE_SIZE;
  // Base height 80px, approx 8px per char for vertical text (safer estimate)
  // We want to fit the text with equal padding.
  const charHeight = 8;
  const padding = 32; // 16px top + 16px bottom
  return Math.max(80, eventId.length * charHeight + padding);
};

// Export color function for use in connection-layer
export const getHeatmapColor = (
  value: number,
  maxValue: number,
  isDiff: boolean,
  isCenter: boolean,
): string => {
  // Center uses black
  if (isCenter) return "rgba(0, 0, 0, 1)";

  const absValue = Math.abs(value);
  const ratio = maxValue > 0 ? absValue / maxValue : 0;
  const clampedRatio = Math.min(1, Math.max(0, ratio));

  if (absValue === 0) {
    return "rgba(128, 128, 128, 0.1)";
  }

  if (isDiff) {
    if (value > 0) {
      return `rgba(239, 68, 68, ${0.2 + clampedRatio * 0.8})`; // Red-500
    } else {
      return `rgba(59, 130, 246, ${0.2 + clampedRatio * 0.8})`; // Blue-500
    }
  } else {
    return `rgba(249, 115, 22, ${0.2 + clampedRatio * 0.8})`; // Orange-500
  }
};

// Get RGB values for gradient
export const getHeatmapRGB = (
  value: number,
  maxValue: number,
  isDiff: boolean,
  isCenter?: boolean,
): string => {
  if (isCenter) return "0, 0, 0";

  const absValue = Math.abs(value);

  if (absValue === 0) {
    return "128, 128, 128";
  }

  if (isDiff) {
    if (value > 0) {
      return "239, 68, 68"; // Red
    } else {
      return "59, 130, 246"; // Blue
    }
  } else {
    return "249, 115, 22"; // Orange
  }
};

interface StepNodeProps {
  event: Event;
  value: number;
  maxValue: number;
  isDiff: boolean;
  isCenter?: boolean;
  flatSide?: "left" | "right" | "both" | "none";
  proximity?: number;
  onContextMenu?: (e: React.MouseEvent) => void;
  menuProps?: {
    eventOptions: string[];
    onAddPrevious?: (event: string) => void;
    onAddNext?: (event: string) => void;
    onReplace?: (event: string) => void;
    onAddWildcardPrevious?: (event: string) => void;
    onAddWildcardNext?: (event: string) => void;
    onDelete?: () => void;
  };
  isLoadingPreview?: boolean;
  isHighlighted?: boolean;
  onHover?: (payload: {
    eventId: string;
    value: number;
    anchorRect: DOMRect;
  }) => void;
  onHoverEnd?: () => void;
}

export const StepNode = ({
  event,
  value,
  maxValue,
  isDiff,
  isCenter,
  flatSide = "none",
  proximity = 0,
  onContextMenu,
  menuProps,
  isLoadingPreview = false,
  isHighlighted = false,
  onHover,
  onHoverEnd,
}: StepNodeProps) => {
  const [menuOpen, setMenuOpen] = React.useState(false);
  const [menuQuery, setMenuQuery] = React.useState("");
  const [menuPos, setMenuPos] = React.useState<{ top: number; left: number }>({ top: 0, left: 0 });
  const nodeWrapRef = React.useRef<HTMLDivElement>(null);
  const menuRef = React.useRef<HTMLDivElement>(null);
  const menuInputRef = React.useRef<HTMLInputElement>(null);

  React.useEffect(() => {
    if (!menuOpen) return;
    const handle = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) setMenuOpen(false);
    };
    document.addEventListener("mousedown", handle);
    return () => document.removeEventListener("mousedown", handle);
  }, [menuOpen]);

  React.useEffect(() => {
    if (menuOpen) { setMenuQuery(""); setTimeout(() => menuInputRef.current?.focus(), 50); }
  }, [menuOpen]);
  const bgColor = getHeatmapColor(value, maxValue, isDiff, isCenter ?? false);

  // Text shadow for label outline (matches light background)
  const textOutlineShadow =
    "-1px -1px 0 #ffffff, 1px -1px 0 #ffffff, -1px 1px 0 #ffffff, 1px 1px 0 #ffffff";

  const nodeRef = React.useRef<HTMLDivElement>(null);

  React.useEffect(() => {
    if (isHighlighted && nodeRef.current) {
      nodeRef.current.scrollIntoView({
        behavior: "smooth",
        block: "nearest",
        inline: "center",
      });
    }
  }, [isHighlighted]);

  // Fisheye scale logic - smoother interpolation
  const isActive = proximity > 0.05;
  const scale = 1 + proximity * 0.5; // Max scale 1.5x

  // Dynamic truncate: interpolate max characters based on proximity
  // At 10px font, ~6px per char average. 60px → ~10 chars, 200px → ~33 chars
  const maxChars = Math.round(10 + proximity * 23);

  // Middle truncate: keep start and end, ellipsis in middle
  const middleTruncate = (str: string, max: number): string => {
    if (str.length <= max) return str;
    const ellipsis = "…";
    const charsToShow = max - 1;
    const frontChars = Math.ceil(charsToShow / 2);
    const backChars = Math.floor(charsToShow / 2);
    return str.slice(0, frontChars) + ellipsis + str.slice(-backChars);
  };

  const displayLabel = isCenter ? event.id : middleTruncate(event.id, maxChars);
  const nodeHeight = calculateNodeHeight(event.id, isCenter ?? false);

  // The inner node content
  const nodeContent = (
    <div
      style={{
        display: "flex",
        flexShrink: 0,
        alignItems: "center",
        justifyContent: "center",
        fontFamily: "monospace",
        fontSize: 10,
        fontWeight: "bold",
        transition: "all 150ms",
        width: isCenter ? 30 : NODE_SIZE,
        height: nodeHeight,
        backgroundColor: bgColor,
        color: isCenter && !isHighlighted ? "white" : "#111827",
        borderRadius: 0,
        ...(isHighlighted ? { transform: "scale(1.5)", boxShadow: "0 4px 16px rgba(0,0,0,0.5)" } : {}),
      }}
      data-flat-side={flatSide}
    >
      {!isCenter && !isLoadingPreview && (
        <>
          {isDiff && value > 0 ? "+" : ""}
          {formatNumber(value)}
        </>
      )}

      {/* Vertical label inside center node */}
      {isCenter && (!isLoadingPreview || event.id === "path_start") && (
        <div
          style={{
            position: "absolute",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            writingMode: "vertical-rl",
            textOrientation: "mixed",
            transform: "rotate(180deg)",
          }}
        >
          <span
            style={{
              display: "block",
              whiteSpace: "nowrap",
              fontSize: 10,
              fontWeight: "bold",
              color: "white",
            }}
          >
            {event.id}
          </span>
        </div>
      )}
    </div>
  );

  return (
    <div
      ref={nodeRef}
      style={{
        position: "relative",
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        userSelect: "none",
        height: nodeHeight + 16,
        zIndex: isHighlighted || isActive ? 50 : 0,
      }}
      onContextMenu={onContextMenu}
      onMouseEnter={(mouseEvent) =>
        onHover?.({
          eventId: event.id,
          value,
          anchorRect: mouseEvent.currentTarget.getBoundingClientRect(),
        })
      }
      onMouseLeave={() => onHoverEnd?.()}
    >
      {/* Square node — clickable when has menu */}
      <div
        ref={nodeWrapRef}
        style={{ position: "relative", cursor: menuProps ? "pointer" : undefined }}
        onClick={menuProps ? (e) => {
          e.stopPropagation();
          if (!menuOpen && nodeWrapRef.current) {
            const r = nodeWrapRef.current.getBoundingClientRect();
            setMenuPos({ top: r.top, left: r.right + 6 });
          }
          setMenuOpen(v => !v);
        } : undefined}
      >
        {nodeContent}

        {/* Pattern-edit menu — portal to document.body to escape all stacking contexts */}
        {menuOpen && menuProps && typeof document !== "undefined" && createPortal(
          <PatternNodeMenu
            ref={menuRef}
            pos={menuPos}
            menuQuery={menuQuery}
            setMenuQuery={setMenuQuery}
            menuInputRef={menuInputRef}
            menuProps={menuProps}
            eventId={event.id}
            onClose={() => setMenuOpen(false)}
          />,
          document.body
        )}
      </div>

      {/* Label BELOW the square with text outline - hide in loading preview */}
      {!isCenter && !isLoadingPreview && (
        <div
          style={{
            pointerEvents: "none",
            position: "absolute",
            left: "50%",
            top: isHighlighted ? NODE_SIZE + 14 : NODE_SIZE + 2,
            transform: `translateX(-50%) scale(${isHighlighted ? 1.8 : scale})`,
            transformOrigin: "center top",
            width: "max-content",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            transition: "top 150ms ease-out, transform 150ms ease-out",
          }}
        >
          <span
            style={{
              textAlign: "center",
              fontSize: 10,
              fontWeight: isHighlighted ? "bold" : "500",
              color: isHighlighted ? "#111827" : "#374151",
              textShadow: textOutlineShadow,
              whiteSpace: "nowrap",
            }}
          >
            {displayLabel}
          </span>
        </div>
      )}
    </div>
  );
};
