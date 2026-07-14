import * as React from "react";
import { createPortal } from "react-dom";

interface GapSeparatorProps {
  eventOptions?: string[];
  onInsert?: (event: string) => void;
  onDelete?: () => void;
}

const BAR_WIDTH = 28;
const SEP_WIDTH = 56;
const SERR_H = 10;
const SERR_D = 6;

function serratedPath(h: number): string {
  let d = `M 0 0 H ${BAR_WIDTH} V 0`;
  for (let y = 0; y < h; y += SERR_H) {
    const ny = Math.min(y + SERR_H, h);
    const mid = y + (ny - y) / 2;
    d += ` L ${BAR_WIDTH - SERR_D} ${mid} L ${BAR_WIDTH} ${ny}`;
  }
  d += ` V ${h} H 0 V ${h}`;
  for (let y = h; y > 0; y -= SERR_H) {
    const ny = Math.max(y - SERR_H, 0);
    const mid = y - (y - ny) / 2;
    d += ` L ${SERR_D} ${mid} L 0 ${ny}`;
  }
  d += " Z";
  return d;
}

export function GapSeparator({ eventOptions = [], onInsert, onDelete }: GapSeparatorProps) {
  const [menuOpen, setMenuOpen] = React.useState(false);
  const [query, setQuery] = React.useState("");
  const [menuPos, setMenuPos] = React.useState<{ top: number; left: number }>({ top: 0, left: 0 });
  const barRef = React.useRef<HTMLDivElement>(null);
  const ref = React.useRef<HTMLDivElement>(null);
  const inputRef = React.useRef<HTMLInputElement>(null);

  const hasMenu = !!(onInsert || onDelete);

  React.useEffect(() => {
    if (!menuOpen) return;
    const handle = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setMenuOpen(false);
    };
    document.addEventListener("mousedown", handle);
    return () => document.removeEventListener("mousedown", handle);
  }, [menuOpen]);

  React.useEffect(() => {
    if (menuOpen) { setQuery(""); setTimeout(() => inputRef.current?.focus(), 50); }
  }, [menuOpen]);

  const filtered = React.useMemo(() => {
    const q = query.toLowerCase();
    return eventOptions
      .filter(e => !q || e.toLowerCase().includes(q));
  }, [query, eventOptions]);

  return (
    <div
      style={{ width: SEP_WIDTH, height: "100%", position: "relative", flexShrink: 0, display: "flex", alignItems: "stretch" }}
    >
      {/* Serrated bar */}
      <div
        ref={barRef}
        style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", cursor: hasMenu ? "pointer" : "default" }}
        onClick={() => {
          if (!hasMenu) return;
          if (!menuOpen && barRef.current) {
            const r = barRef.current.getBoundingClientRect();
            setMenuPos({ top: r.top, left: r.right + 6 });
          }
          setMenuOpen(v => !v);
        }}
        title={hasMenu ? "Click to add/remove event" : undefined}
      >
        <svg
          width={BAR_WIDTH}
          style={{ height: "100%", overflow: "visible" }}
          viewBox={`0 0 ${BAR_WIDTH} 100`}
          preserveAspectRatio="none"
        >
          <path
            d={serratedPath(100)}
            fill="#e5e7eb"
            stroke="#d1d5db"
            strokeWidth="1"
            strokeLinejoin="round"
            style={{ transition: "fill 0.15s" }}
            onMouseEnter={e => hasMenu && ((e.target as SVGElement).style.fill = "#d1d5db")}
            onMouseLeave={e => ((e.target as SVGElement).style.fill = "#e5e7eb")}
          />
        </svg>
      </div>

      {/* Popup menu */}
      {menuOpen && typeof document !== "undefined" && createPortal(
        <div
          ref={ref}
          style={{
          position: "fixed", top: menuPos.top, left: menuPos.left, zIndex: 9999,
          background: "#fff", border: "1px solid #e5e7eb", borderRadius: 8,
          boxShadow: "0 4px 12px rgba(0,0,0,0.1)", width: 220, maxHeight: 320,
          display: "flex", flexDirection: "column", overflow: "hidden",
        }}>
          {onInsert && (
            <>
              <div style={{ padding: "8px 10px", borderBottom: "1px solid #f3f4f6" }}>
                <input
                  ref={inputRef}
                  value={query}
                  onChange={e => setQuery(e.target.value)}
                  placeholder="Add event…"
                  style={{ width: "100%", boxSizing: "border-box", border: "1px solid #e5e7eb", borderRadius: 6, padding: "5px 8px", fontSize: 13, outline: "none" }}
                />
              </div>
              <div style={{ overflowY: "auto", maxHeight: 220 }}>
                {filtered.map(ev => (
                  <div
                    key={ev}
                    onClick={() => { onInsert(ev); setMenuOpen(false); }}
                    style={{ padding: "6px 12px", fontSize: 13, cursor: "pointer", color: "#111827" }}
                    onMouseEnter={e => (e.currentTarget.style.background = "#f3f4f6")}
                    onMouseLeave={e => (e.currentTarget.style.background = "")}
                  >
                    {ev}
                  </div>
                ))}
                {filtered.length === 0 && (
                  <div style={{ padding: "8px 12px", fontSize: 12, color: "#9ca3af" }}>No events found</div>
                )}
              </div>
            </>
          )}
          {onDelete && (
            <button
              onClick={() => { onDelete(); setMenuOpen(false); }}
              style={{ margin: 8, padding: "6px 12px", background: "transparent", border: "1px solid #fca5a5", borderRadius: 6, color: "#ef4444", fontSize: 13, cursor: "pointer", textAlign: "left" }}
            >
              Delete Gap
            </button>
          )}
        </div>,
        document.body
      )}
    </div>
  );
}
