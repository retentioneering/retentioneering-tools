import * as React from "react";

interface EventItemState {
  isHidden: boolean;
  isPinned: boolean;
}

interface SearchBarProps {
  onSearch: (q: string) => void;
  onClear: () => void;
  onClose?: () => void;
  onSelect?: (event: string) => void;
  events?: Array<{ id: string; name: string }>;
  showAllOnOpen?: boolean;
  isDark?: boolean;
  onToggleVisibility?: (eventId: string) => void;
  onTogglePin?: (eventId: string) => void;
  getEventState?: (eventId: string) => EventItemState;
  eventCounts?: Record<string, number>;
  eventCountsG1?: Record<string, number>;
  eventCountsG2?: Record<string, number>;
}

// ── SVG icons ─────────────────────────────────────────────────────────────

function EyeIcon({ hidden }: { hidden: boolean }) {
  return hidden ? (
    // Eye with slash — hidden
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M9.88 9.88a3 3 0 1 0 4.24 4.24"/>
      <path d="M10.73 5.08A10.43 10.43 0 0 1 12 5c7 0 10 7 10 7a13.16 13.16 0 0 1-1.67 2.68"/>
      <path d="M6.61 6.61A13.526 13.526 0 0 0 2 12s3 7 10 7a9.74 9.74 0 0 0 5.39-1.61"/>
      <line x1="2" y1="2" x2="22" y2="22"/>
    </svg>
  ) : (
    // Open eye — visible
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M2 12s3-7 10-7 10 7 10 7-3 7-10 7-10-7-10-7Z"/>
      <circle cx="12" cy="12" r="3"/>
    </svg>
  );
}

function PinIcon({ pinned }: { pinned: boolean }) {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill={pinned ? "currentColor" : "none"} stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="12" y1="17" x2="12" y2="22"/>
      <path d="M5 17h14v-1.76a2 2 0 0 0-1.11-1.79l-1.78-.9A2 2 0 0 1 15 10.76V6h1a2 2 0 0 0 0-4H8a2 2 0 0 0 0 4h1v4.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24Z"/>
    </svg>
  );
}

// ── Component ──────────────────────────────────────────────────────────────

function fmtCount(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000)     return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}

export function SearchBar({
  onSearch, onClear, onClose, onSelect,
  events = [], showAllOnOpen = false,
  isDark = true,
  onToggleVisibility, onTogglePin, getEventState,
  eventCounts, eventCountsG1, eventCountsG2,
}: SearchBarProps) {
  const [query, setQuery] = React.useState("");
  const [selectedIdx, setSelectedIdx] = React.useState(showAllOnOpen ? 0 : -1);
  const inputRef = React.useRef<HTMLInputElement>(null);
  const timerRef = React.useRef<ReturnType<typeof setTimeout> | null>(null);
  const containerRef = React.useRef<HTMLDivElement>(null);

  const filtered = React.useMemo(() => {
    if (!query && !showAllOnOpen) return [];
    if (!query) return events;
    const q = query.toLowerCase();
    return events.filter((e) => e.name.toLowerCase().includes(q));
  }, [query, events, showAllOnOpen]);

  React.useEffect(() => { inputRef.current?.focus(); }, []);

  React.useEffect(() => {
    const handle = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) onClose?.();
    };
    document.addEventListener("mousedown", handle);
    return () => document.removeEventListener("mousedown", handle);
  }, [onClose]);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const v = e.target.value;
    setQuery(v);
    setSelectedIdx(0);
    if (timerRef.current) clearTimeout(timerRef.current);
    timerRef.current = setTimeout(() => onSearch(v), 300);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Escape") { onClose?.(); return; }
    if (e.key === "ArrowDown") { e.preventDefault(); setSelectedIdx((p) => Math.min(p + 1, filtered.length - 1)); }
    if (e.key === "ArrowUp") { e.preventDefault(); setSelectedIdx((p) => Math.max(p - 1, 0)); }
    if (e.key === "Enter" && selectedIdx >= 0 && filtered[selectedIdx]) { select(filtered[selectedIdx].name); }
  };

  const select = (name: string) => { setQuery(name); onSelect?.(name); onClose?.(); };

  const bg     = isDark ? "#1f2937" : "#fff";
  const border = isDark ? "#374151" : "#e5e7eb";
  const text   = isDark ? "#f3f4f6" : "#111827";
  const muted  = isDark ? "#9ca3af" : "#6b7280";

  const iconBtnStyle = (active: boolean, activeColor: string): React.CSSProperties => ({
    display: "flex", alignItems: "center", justifyContent: "center",
    width: 22, height: 22, borderRadius: 4,
    background: "transparent", border: "none", cursor: "pointer",
    color: active ? activeColor : muted,
    flexShrink: 0, padding: 0,
  });

  return (
    <div ref={containerRef} style={{ background: bg, border: `1px solid ${border}`, borderRadius: 8, overflow: "hidden" }}>
      <div style={{ padding: "12px 16px" }}>
        <div style={{ fontSize: 11, color: muted, marginBottom: 8 }}>Search and navigate to events</div>
        <div style={{ position: "relative" }}>
          <svg style={{ position: "absolute", left: 8, top: "50%", transform: "translateY(-50%)", opacity: 0.5 }} width="14" height="14" viewBox="0 0 24 24" fill="none" stroke={text} strokeWidth="2">
            <circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/>
          </svg>
          <input
            ref={inputRef}
            value={query}
            onChange={handleChange}
            onKeyDown={handleKeyDown}
            placeholder="Start typing…"
            style={{ width: "100%", boxSizing: "border-box", background: isDark ? "#111827" : "#f9fafb", border: `1px solid ${border}`, borderRadius: 6, padding: "6px 28px", color: text, fontSize: 13, outline: "none" }}
          />
          {query && (
            <button onClick={() => { setQuery(""); onClear(); inputRef.current?.focus(); }} style={{ position: "absolute", right: 6, top: "50%", transform: "translateY(-50%)", background: "none", border: "none", cursor: "pointer", padding: 4, color: muted }}>
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M18 6 6 18M6 6l12 12"/></svg>
            </button>
          )}
        </div>
      </div>

      {filtered.length > 0 && (
        <div style={{ borderTop: `1px solid ${border}`, maxHeight: 280, overflowY: "auto" }}>
          {filtered.map((e, i) => {
            const state = getEventState?.(e.id);
            const isHidden = state?.isHidden ?? false;
            const isPinned = state?.isPinned ?? false;
            const isSelected = i === selectedIdx;

            return (
              <div
                key={e.id}
                onMouseEnter={() => setSelectedIdx(i)}
                style={{
                  display: "flex", alignItems: "center", gap: 4,
                  padding: "4px 8px 4px 16px",
                  background: isSelected ? (isDark ? "#374151" : "#f3f4f6") : "transparent",
                  opacity: isHidden ? 0.45 : 1,
                }}
              >
                {/* Event name — click to focus */}
                <span
                  onClick={() => select(e.name)}
                  style={{ flex: 1, fontSize: 13, cursor: "pointer", color: text,
                    textDecoration: isHidden ? "line-through" : "none",
                    padding: "2px 0" }}
                >
                  {e.name}
                </span>

                {/* Frequency badges */}
                {eventCountsG1 && eventCountsG2 ? (
                  // Diff mode: blue (g1) and red (g2)
                  <span style={{ display: "flex", alignItems: "center", gap: 3, flexShrink: 0 }}>
                    <span style={{ fontSize: 11, color: "#3b82f6", fontVariantNumeric: "tabular-nums" }}>
                      {fmtCount(eventCountsG1[e.id] ?? 0)}
                    </span>
                    <span style={{ fontSize: 10, color: muted }}>/</span>
                    <span style={{ fontSize: 11, color: "#ef4444", fontVariantNumeric: "tabular-nums" }}>
                      {fmtCount(eventCountsG2[e.id] ?? 0)}
                    </span>
                  </span>
                ) : eventCounts && eventCounts[e.id] !== undefined ? (
                  // Normal mode: single gray count
                  <span style={{ fontSize: 11, color: muted, flexShrink: 0, fontVariantNumeric: "tabular-nums" }}>
                    {fmtCount(eventCounts[e.id])}
                  </span>
                ) : null}

                {/* Eye — toggle visibility */}
                {onToggleVisibility && (
                  <button
                    title={isHidden ? "Show event" : "Hide event"}
                    onClick={(ev) => { ev.stopPropagation(); onToggleVisibility(e.id); }}
                    style={iconBtnStyle(isHidden, "#ef4444")}
                  >
                    <EyeIcon hidden={isHidden} />
                  </button>
                )}

                {/* Pin — toggle pin */}
                {onTogglePin && (
                  <button
                    title={isPinned ? "Unpin event" : "Pin event (immune to filters)"}
                    onClick={(ev) => { ev.stopPropagation(); onTogglePin(e.id); }}
                    style={iconBtnStyle(isPinned, "#f59e0b")}
                  >
                    <PinIcon pinned={isPinned} />
                  </button>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
