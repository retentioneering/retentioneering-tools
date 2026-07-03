import * as React from "react";
import { createPortal } from "react-dom";

// ── constants ──────────────────────────────────────────────────────────────

export const METRIC_NAMES = ["active_days","belongs_to","duration","event_count","first_event_dt","has","length","matches","time_between"];
export const AGG_OPTIONS  = ["mean","median","q5","q25","q75","q95","complement_diff"];
export const AGG_LABELS: Record<string, string> = {
  mean: "Mean", median: "Median", q5: "Q5", q25: "Q25", q75: "Q75", q95: "Q95",
  complement_diff: "Complement diff",
};

const METRIC_TIPS: Record<string, string> = {
  active_days:    "Number of unique calendar days with at least one event.",
  belongs_to:     "Path membership check for a segment value.\n• any: ≥1 event has the value\n• all: all events have the value\n• event_share: ≥ threshold share of events have the value\nIf multiple segment values are selected, a separate metric is created for each value.",
  duration:       "Time (seconds) between the first and last event.",
  event_count:    "Number of times the selected event(s) occurred.",
  first_event_dt: "Unix timestamp of the first event.",
  has:            "1 if the selected event(s) occurred at least once, 0 otherwise.",
  length:         "Total number of events in the path.",
  matches:        "1 if the path matches the pattern, 0 otherwise.\nEvents separated by ->, .* matches any sequence.\nExample: login->.*->purchase",
  time_between:   "Time (seconds) between the first occurrences of two events.\nNull if either event is missing.",
};

const AGG_TIPS: Record<string, string> = {
  mean:            "Mean value across all paths in the segment.",
  median:          "Median value (50th percentile).",
  q5:              "5th percentile.",
  q25:             "25th percentile.",
  q75:             "75th percentile.",
  q95:             "95th percentile.",
  complement_diff: "Wasserstein distance between this segment's distribution and all others combined. Higher = more distinctive from the rest.",
};

const BELONGS_TO_MODES = ["any", "all", "event_share"];

const mkSel = (): React.CSSProperties => ({
  boxSizing: "border-box", border: "1px solid #d1d5db", borderRadius: 5,
  fontSize: 11, padding: "4px 24px 4px 8px", cursor: "pointer", outline: "none",
  appearance: "none",
  backgroundImage: `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='10' viewBox='0 0 24 24' fill='none' stroke='%236b7280' stroke-width='2'%3E%3Cpath d='m6 9 6 6 6-6'/%3E%3C/svg%3E")`,
  backgroundRepeat: "no-repeat", backgroundPosition: "right 6px center", background: "#f9fafb",
});

// ── InfoTip ────────────────────────────────────────────────────────────────

export function InfoTip({ text }: { text: string }) {
  const [pos, setPos] = React.useState<{x: number; y: number} | null>(null);
  if (!text) return null;
  return (
    <span style={{ display: "inline-flex", alignItems: "center", flexShrink: 0, cursor: "help" }}
      onMouseEnter={e => setPos({ x: e.clientX, y: e.clientY })}
      onMouseMove={e => setPos({ x: e.clientX, y: e.clientY })}
      onMouseLeave={() => setPos(null)}>
      <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="#9ca3af" strokeWidth="2">
        <circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/>
      </svg>
      {pos && createPortal(
        <div style={{ position: "fixed", top: pos.y - 8, left: pos.x + 14, zIndex: 99999,
          background: "#fff", color: "#374151", border: "1px solid #e5e7eb", fontSize: 11, padding: "6px 10px",
          borderRadius: 6, maxWidth: 250, lineHeight: 1.5, whiteSpace: "pre-line",
          boxShadow: "0 4px 12px rgba(0,0,0,0.25)", pointerEvents: "none",
          transform: "translateY(-100%)" }}>
          {text}
        </div>,
        document.body
      )}
    </span>
  );
}

// ── MultiSelect — multi-value dropdown with search + select all / clear all ─

export function MultiSelect({ selected, options, onChange, placeholder = "— select —" }: {
  selected: string[]; options: string[]; onChange: (v: string[]) => void; placeholder?: string;
}) {
  const [open, setOpen] = React.useState(false);
  const [query, setQuery] = React.useState("");
  const btnRef = React.useRef<HTMLButtonElement>(null);
  const dropRef = React.useRef<HTMLDivElement>(null);
  const [pos, setPos] = React.useState({ top: 0, left: 0, width: 0 });

  const filtered = query ? options.filter(o => o.toLowerCase().includes(query.toLowerCase())) : options;
  const allSel = options.length > 0 && options.every(o => selected.includes(o));
  const toggle = (v: string) => onChange(selected.includes(v) ? selected.filter(s => s !== v) : [...selected, v]);

  const openDrop = () => {
    if (!btnRef.current) return;
    const r = btnRef.current.getBoundingClientRect();
    setPos({ top: r.bottom + 4, left: r.left, width: r.width });
    setQuery(""); setOpen(true);
  };

  React.useEffect(() => {
    if (!open) return;
    const h = (e: MouseEvent) => {
      if (dropRef.current && !dropRef.current.contains(e.target as Node) &&
          btnRef.current && !btnRef.current.contains(e.target as Node)) setOpen(false);
    };
    const k = (e: KeyboardEvent) => { if (e.key === "Escape") setOpen(false); };
    document.addEventListener("mousedown", h);
    document.addEventListener("keydown", k);
    return () => { document.removeEventListener("mousedown", h); document.removeEventListener("keydown", k); };
  }, [open]);

  const label = selected.length === 0 ? placeholder
    : selected.length === options.length ? `All ${options.length}`
    : `${selected.length} selected`;

  return (
    <div style={{ flex: 1, minWidth: 0 }}>
      <button ref={btnRef} onClick={open ? () => setOpen(false) : openDrop}
        style={{ width: "100%", padding: "4px 8px", border: "1px solid #d1d5db", borderRadius: 5, background: "#f9fafb", cursor: "pointer", textAlign: "left", fontSize: 11, color: selected.length > 0 ? "#111827" : "#9ca3af", display: "flex", justifyContent: "space-between", alignItems: "center", gap: 4 }}>
        <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 }}>{label}</span>
        <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="#6b7280" strokeWidth="2" style={{ flexShrink: 0 }}><path d="m6 9 6 6 6-6"/></svg>
      </button>
      {open && typeof document !== "undefined" && createPortal(
        <div ref={dropRef} style={{ position: "fixed", top: pos.top, left: pos.left, width: Math.max(pos.width, 240), zIndex: 9999, background: "#fff", border: "1px solid #e5e7eb", borderRadius: 8, boxShadow: "0 4px 16px rgba(0,0,0,0.12)", maxHeight: 260, overflow: "hidden", display: "flex", flexDirection: "column" }}>
          <div style={{ padding: "6px 8px", borderBottom: "1px solid #f3f4f6" }}>
            <input autoFocus value={query} onChange={e => setQuery(e.target.value)} placeholder="Search…"
              style={{ width: "100%", boxSizing: "border-box", border: "1px solid #e5e7eb", borderRadius: 5, padding: "4px 7px", fontSize: 11, outline: "none" }} />
          </div>
          <div style={{ padding: "4px 10px", borderBottom: "1px solid #f3f4f6", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <span style={{ fontSize: 10, color: "#6b7280" }}>{selected.length > 0 ? `${selected.length} selected` : "Select values"}</span>
            <div style={{ display: "flex", gap: 8 }}>
              {!allSel && <span onClick={() => onChange([...options])} style={{ fontSize: 10, color: "#6b7280", cursor: "pointer", textDecoration: "underline" }}>Select all</span>}
              {selected.length > 0 && <span onClick={() => onChange([])} style={{ fontSize: 10, color: "#6b7280", cursor: "pointer", textDecoration: "underline" }}>Clear all</span>}
            </div>
          </div>
          <div style={{ overflowY: "auto", flex: 1 }}>
            {filtered.map(v => (
              <div key={v} onClick={() => toggle(v)}
                style={{ padding: "5px 10px", fontSize: 11, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "space-between", color: "#111827" }}
                onMouseEnter={e => (e.currentTarget.style.background = "#f9fafb")}
                onMouseLeave={e => (e.currentTarget.style.background = "")}>
                <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{v}</span>
                {selected.includes(v) && <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="#7c3aed" strokeWidth="2.5" style={{ flexShrink: 0, marginLeft: 6 }}><polyline points="20 6 9 17 4 12"/></svg>}
              </div>
            ))}
            {filtered.length === 0 && <div style={{ padding: "8px 10px", fontSize: 11, color: "#9ca3af" }}>No values found</div>}
          </div>
        </div>,
        document.body
      )}
    </div>
  );
}

// ── SingleSelect — single-value dropdown with search ───────────────────────

export function SingleSelect({ value, options, placeholder, onChange }: {
  value: string; options: string[]; placeholder: string; onChange: (v: string) => void;
}) {
  const [open, setOpen] = React.useState(false);
  const [query, setQuery] = React.useState("");
  const btnRef = React.useRef<HTMLButtonElement>(null);
  const dropRef = React.useRef<HTMLDivElement>(null);
  const [pos, setPos] = React.useState({ top: 0, left: 0, width: 0 });

  const filtered = query ? options.filter(e => e.toLowerCase().includes(query.toLowerCase())) : options;

  const openDrop = () => {
    if (!btnRef.current) return;
    const r = btnRef.current.getBoundingClientRect();
    setPos({ top: r.bottom + 4, left: r.left, width: r.width });
    setQuery(""); setOpen(true);
  };

  React.useEffect(() => {
    if (!open) return;
    const h = (e: MouseEvent) => {
      if (dropRef.current && !dropRef.current.contains(e.target as Node) &&
          btnRef.current && !btnRef.current.contains(e.target as Node)) setOpen(false);
    };
    const k = (e: KeyboardEvent) => { if (e.key === "Escape") setOpen(false); };
    document.addEventListener("mousedown", h);
    document.addEventListener("keydown", k);
    return () => { document.removeEventListener("mousedown", h); document.removeEventListener("keydown", k); };
  }, [open]);

  return (
    <div style={{ flex: 1, minWidth: 0 }}>
      <button ref={btnRef} onClick={open ? () => setOpen(false) : openDrop}
        style={{ width: "100%", padding: "4px 8px", border: "1px solid #d1d5db", borderRadius: 5, background: "#f9fafb", cursor: "pointer", textAlign: "left", fontSize: 11, color: value ? "#111827" : "#9ca3af", display: "flex", justifyContent: "space-between", alignItems: "center", gap: 4 }}>
        <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 }}>{value || placeholder}</span>
        <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="#6b7280" strokeWidth="2" style={{ flexShrink: 0 }}><path d="m6 9 6 6 6-6"/></svg>
      </button>
      {open && createPortal(
        <div ref={dropRef} style={{ position: "fixed", top: pos.top, left: pos.left, width: Math.max(pos.width, 200), zIndex: 9999, background: "#fff", border: "1px solid #e5e7eb", borderRadius: 8, boxShadow: "0 4px 16px rgba(0,0,0,0.12)", maxHeight: 240, overflow: "hidden", display: "flex", flexDirection: "column" }}>
          <div style={{ padding: "6px 8px", borderBottom: "1px solid #f3f4f6" }}>
            <input autoFocus value={query} onChange={e => setQuery(e.target.value)} placeholder="Search…"
              style={{ width: "100%", boxSizing: "border-box", border: "1px solid #e5e7eb", borderRadius: 5, padding: "4px 7px", fontSize: 11, outline: "none" }} />
          </div>
          <div style={{ overflowY: "auto", flex: 1 }}>
            {filtered.map(ev => (
              <div key={ev} onClick={() => { onChange(ev); setOpen(false); }}
                style={{ padding: "5px 10px", fontSize: 11, cursor: "pointer", color: ev === value ? "#7c3aed" : "#111827", fontWeight: ev === value ? 600 : 400, display: "flex", alignItems: "center", justifyContent: "space-between" }}
                onMouseEnter={e => (e.currentTarget.style.background = "#f9fafb")}
                onMouseLeave={e => (e.currentTarget.style.background = "")}>
                {ev}
                {ev === value && <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="#7c3aed" strokeWidth="2.5"><polyline points="20 6 9 17 4 12"/></svg>}
              </div>
            ))}
            {filtered.length === 0 && <div style={{ padding: "8px 10px", fontSize: 11, color: "#9ca3af" }}>No values found</div>}
          </div>
        </div>,
        document.body
      )}
    </div>
  );
}

// ── validateMetricCfg ──────────────────────────────────────────────────────

export function validateMetricCfg(cfg: any): string | null {
  const m = cfg?.metric;
  const a = cfg?.metric_args ?? {};
  if (m === "event_count") {
    const ev = a.event;
    if (!ev || (Array.isArray(ev) ? ev.length === 0 : !ev)) return "Select at least one event";
  }
  if (m === "has") {
    const ev = a.events;
    if (!ev || (Array.isArray(ev) ? ev.length === 0 : !ev)) return "Select at least one event";
  }
  if (m === "time_between") {
    if (!a.event_from) return "Select From event";
    if (!a.event_to)   return "Select To event";
    if (a.event_from === a.event_to) return "From and To events must differ";
  }
  if (m === "matches") {
    if (!a.pattern?.trim()) return "Enter a pattern, e.g. login->.*->purchase";
  }
  if (m === "belongs_to") {
    if (!a.segment_name) return "Select segment column";
    const sv = a.segment_value;
    if (!sv || (Array.isArray(sv) ? sv.length === 0 : !sv)) return "Select at least one segment value";
    if (a.mode === "event_share") {
      const t = parseFloat(a.threshold);
      if (isNaN(t) || t < 0 || t > 1) return "Threshold must be between 0 and 1";
    }
  }
  return null;
}

// ── MetricRow ──────────────────────────────────────────────────────────────
// showAgg=true  → Configure Metrics (with aggregation dropdown)
// showAgg=false → Configure Features (no aggregation dropdown)

export function MetricRow({ cfg, events, segmentCols, segmentLevels, showErrors, showAgg = true, onChange, onRemove }: {
  cfg: any;
  events: string[];
  segmentCols: string[];
  segmentLevels: Record<string, string[]>;
  showErrors: boolean;
  showAgg?: boolean;
  onChange: (c: any) => void;
  onRemove: () => void;
}) {
  const sel = mkSel();
  const needsEvent      = ["event_count", "has"].includes(cfg.metric);
  const needsRange      = cfg.metric === "time_between";
  const needsBelongsTo  = cfg.metric === "belongs_to";
  const needsMatches    = cfg.metric === "matches";
  const needsActiveDays = cfg.metric === "active_days";
  const hasSecondRow    = needsEvent || needsRange || needsBelongsTo || needsMatches || needsActiveDays;

  const err = showErrors ? validateMetricCfg(cfg) : null;

  return (
    <div style={{ border: `1px solid ${err ? "#fca5a5" : "#e5e7eb"}`, borderRadius: 8, padding: "10px 12px", background: err ? "#fff5f5" : "#fff" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: hasSecondRow ? 8 : 0 }}>
        <select value={cfg.metric} onChange={e => onChange(showAgg
          ? { metric: e.target.value, agg: cfg.agg, metric_args: undefined }
          : { metric: e.target.value, metric_args: undefined })}
          style={{ ...sel, flex: "0 0 130px" }}>
          {METRIC_NAMES.map(m => <option key={m} value={m}>{m}</option>)}
        </select>
        <InfoTip text={METRIC_TIPS[cfg.metric] ?? ""} />
        {showAgg && (
          <>
            <select value={cfg.agg || "mean"} onChange={e => onChange({ ...cfg, agg: e.target.value })} style={{ ...sel, flex: "0 0 110px" }}>
              {AGG_OPTIONS.map(a => <option key={a} value={a}>{AGG_LABELS[a] ?? a}</option>)}
            </select>
            <InfoTip text={AGG_TIPS[cfg.agg || "mean"] ?? ""} />
          </>
        )}
        <div style={{ flex: 1 }} />
        <button onClick={onRemove} style={{ background: "none", border: "none", cursor: "pointer", color: "#9ca3af", padding: 0, lineHeight: 1, marginLeft: "auto" }}>
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14H6L5 6"/><path d="M10 11v6"/><path d="M14 11v6"/><path d="M9 6V4h6v2"/></svg>
        </button>
      </div>

      {needsEvent && (() => {
        const key = cfg.metric === "event_count" ? "event" : "events";
        const raw = cfg.metric_args?.event ?? cfg.metric_args?.events;
        const sel2: string[] = !raw ? [] : Array.isArray(raw) ? raw.map(String) : [String(raw)];
        return <MultiSelect selected={sel2} options={events} onChange={vals => onChange({ ...cfg, metric_args: { [key]: vals } })} placeholder="Events…" />;
      })()}

      {needsActiveDays && (() => {
        const raw = cfg.metric_args?.active_events;
        const sel2: string[] = !raw ? [] : Array.isArray(raw) ? raw.map(String) : [String(raw)];
        return (
          <div>
            <div style={{ fontSize: 10, color: "#6b7280", marginBottom: 3 }}>Active events (optional — leave empty to count all days)</div>
            <MultiSelect selected={sel2} options={events} onChange={vals => onChange({ ...cfg, metric_args: { active_events: vals.length ? vals : undefined } })} placeholder="Events…" />
          </div>
        );
      })()}

      {needsRange && (() => {
        const tbEvents = ["path_start", ...events, "path_end"];
        return (
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <SingleSelect value={cfg.metric_args?.event_from ?? ""} options={tbEvents} placeholder="From…" onChange={v => onChange({ ...cfg, metric_args: { ...cfg.metric_args, event_from: v } })} />
            <span style={{ color: "#9ca3af", fontSize: 11, flexShrink: 0 }}>→</span>
            <SingleSelect value={cfg.metric_args?.event_to ?? ""} options={tbEvents} placeholder="To…" onChange={v => onChange({ ...cfg, metric_args: { ...cfg.metric_args, event_to: v } })} />
          </div>
        );
      })()}

      {needsMatches && (
        <input value={cfg.metric_args?.pattern ?? ""} onChange={e => onChange({ ...cfg, metric_args: { pattern: e.target.value } })}
          placeholder="Pattern, e.g. login->.*->purchase"
          style={{ width: "100%", boxSizing: "border-box", border: "1px solid #d1d5db", borderRadius: 5, padding: "4px 7px", fontSize: 11, outline: "none" }} />
      )}

      {needsBelongsTo && (() => {
        const segName    = cfg.metric_args?.segment_name ?? "";
        const segValues  = (segmentLevels[segName] ?? []).map(String);
        const curVal     = cfg.metric_args?.segment_value;
        const selectedVals: string[] = Array.isArray(curVal) ? curVal.map(String) : (curVal ? [String(curVal)] : []);
        const mode       = cfg.metric_args?.mode ?? "any";
        return (
          <div style={{ display: "flex", gap: 6, alignItems: "center", flexWrap: "nowrap" }}>
            <select value={segName} onChange={e => onChange({ ...cfg, metric_args: { ...cfg.metric_args, segment_name: e.target.value, segment_value: undefined } })} style={{ ...sel, flex: 1, minWidth: 0 }}>
              <option value="">Segment column…</option>
              {segmentCols.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
            {segValues.length > 0 ? (
              <MultiSelect selected={selectedVals} options={segValues} onChange={vals => onChange({ ...cfg, metric_args: { ...cfg.metric_args, segment_value: vals } })} placeholder="Segment value…" />
            ) : (
              <input value={selectedVals.join(", ")} onChange={e => { const vals = e.target.value.split(",").map((s: string) => s.trim()).filter(Boolean); onChange({ ...cfg, metric_args: { ...cfg.metric_args, segment_value: vals } }); }}
                placeholder="Segment value…"
                style={{ flex: 1, minWidth: 0, boxSizing: "border-box", border: "1px solid #d1d5db", borderRadius: 5, padding: "4px 7px", fontSize: 11, outline: "none" }} />
            )}
            <select value={mode} onChange={e => onChange({ ...cfg, metric_args: { ...cfg.metric_args, mode: e.target.value } })} style={{ ...sel, flex: "0 0 100px" }}>
              {BELONGS_TO_MODES.map(m => <option key={m} value={m}>{m}</option>)}
            </select>
            {mode === "event_share" && (
              <input type="text" inputMode="decimal" value={cfg.metric_args?.threshold ?? 0.5}
                onChange={e => { const v = e.target.value.replace(",", "."); if (v === "" || /^[0-9]*\.?[0-9]*$/.test(v)) onChange({ ...cfg, metric_args: { ...cfg.metric_args, threshold: v === "" ? 0 : parseFloat(v) || 0 } }); }}
                style={{ width: 52, border: "1px solid #d1d5db", borderRadius: 5, padding: "4px 6px", fontSize: 11, outline: "none", flexShrink: 0 }} />
            )}
          </div>
        );
      })()}

      {err && <div style={{ fontSize: 10, color: "#dc2626", marginTop: 6 }}>⚠ {err}</div>}
    </div>
  );
}
