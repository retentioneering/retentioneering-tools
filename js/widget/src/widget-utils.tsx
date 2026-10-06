/**
 * Shared utilities for all widget components.
 */
import * as React from "react";
import type { WidgetHost } from "@retentioneering/viz-core";

// ── JSON helpers ──────────────────────────────────────────────────────────────

export function parseJson<T>(raw: unknown, fallback: T): T {
  // JSON.parse(null) doesn't throw — it happily parses the literal `null`
  // and returns JS `null`, silently bypassing the fallback below. Every
  // anywidget traitlet always has a real string default (e.g. "{}"), so
  // `model.get()` never surfaces this; a host backed by a plain data dict
  // (staticHost/restHost) can return `null`/`undefined` for an absent key,
  // which is exactly when this bites.
  if (raw === null || raw === undefined) return fallback;
  try { return JSON.parse(raw as string) as T; } catch { return fallback; }
}

// ── Link focus on table cells ────────────────────────────────────────────────

const FOCUS_RING = "2px solid #f59e0b";
const FOCUS_FLASH = "#fde68a";

type FocusState = { restore: () => void };

/** Mark the cells an external link points at (a report's analysis text,
 *  `scrollToEvent`). A heatmap already colours every cell, so a short pale
 *  flash alone is easy to miss: the cells get an amber ring that stays until
 *  the next link focus or a click inside `root`, plus a brief stronger flash
 *  to draw the eye. Styles are set on the DOM nodes directly — React only
 *  rewrites the properties it renders, so a re-render keeps the ring. */
export function focusCells(root: HTMLElement, cells: HTMLElement[]) {
  clearCellFocus(root);
  if (!cells.length) return;
  const saved = cells.map(c => ({
    c, outline: c.style.outline, offset: c.style.outlineOffset, bg: c.style.background,
  }));
  for (const c of cells) {
    c.style.outline = FOCUS_RING;
    c.style.outlineOffset = "-2px";
    c.style.background = FOCUS_FLASH;
  }
  const flash = setTimeout(() => saved.forEach(s => { s.c.style.background = s.bg; }), 700);
  const onDown = () => clearCellFocus(root);
  root.addEventListener("mousedown", onDown, true);
  (root as HTMLElement & { __cellFocus?: FocusState }).__cellFocus = {
    restore: () => {
      clearTimeout(flash);
      root.removeEventListener("mousedown", onDown, true);
      saved.forEach(s => {
        s.c.style.outline = s.outline;
        s.c.style.outlineOffset = s.offset;
        s.c.style.background = s.bg;
      });
    },
  };
}

export function clearCellFocus(root: HTMLElement) {
  const holder = root as HTMLElement & { __cellFocus?: FocusState };
  holder.__cellFocus?.restore();
  holder.__cellFocus = undefined;
}

/** Shape every widget entry file's `render()` receives — see main.tsx. */
export interface RenderContext {
  host: WidgetHost;
  el: HTMLElement;
  isStatic?: boolean;
}

// ── Host subscription hook ────────────────────────────────────────────────────

/** Subscribe to a list of host param changes. Cleans up on unmount. */
export function useHostSubscriptions(
  host: WidgetHost,
  subs: Array<[string, () => void]>,
): void {
  React.useEffect(() => {
    const disposers = subs.map(([key, cb]) => host.onChange(key, cb));
    return () => disposers.forEach((dispose) => dispose());
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
}

// ── Computing spinner ─────────────────────────────────────────────────────────

interface SpinnerProps {
  /** Overlay background opacity (default 0.6) */
  opacity?: number;
  /** Label shown below spinner (default "Computing…") */
  label?: string;
  /** z-index (default 30) */
  zIndex?: number;
}

/**
 * Full-canvas loading overlay with a yellow spinner.
 * Place inside a `position: relative` container.
 */
export function ComputingSpinner({ opacity = 0.6, label = "Computing…", zIndex = 30 }: SpinnerProps) {
  return (
    <div style={{
      position: "absolute", inset: 0,
      background: `rgba(255,255,255,${opacity})`,
      backdropFilter: "blur(3px)",
      display: "flex", alignItems: "center", justifyContent: "center",
      zIndex,
    }}>
      <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 8 }}>
        <div style={{
          width: 28, height: 28,
          border: "2px solid #e5e7eb",
          borderTop: "2px solid var(--retentioneering-yellow)",
          borderRadius: "50%",
          animation: "retentioneering-spin 0.8s linear infinite",
        }} />
        <span style={{ color: "#6b7280", fontSize: 11 }}>{label}</span>
      </div>
    </div>
  );
}

/** @keyframes retentioneering-spin style tag — include once per widget root. */
export const RetentioneeringSpinKeyframes = () => (
  <style>{`@keyframes retentioneering-spin { to { transform: rotate(360deg); } }`}</style>
);
