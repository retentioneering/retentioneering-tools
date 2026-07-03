/**
 * Shared utilities for all widget components.
 */
import * as React from "react";

// ── JSON helpers ──────────────────────────────────────────────────────────────

export function parseJson<T>(raw: unknown, fallback: T): T {
  try { return JSON.parse(raw as string) as T; } catch { return fallback; }
}

// ── Model subscription hook ───────────────────────────────────────────────────

interface AnyWidgetModel {
  get(key: string): unknown;
  on(event: string, cb: () => void): void;
  off(event: string, cb: () => void): void;
}

/** Subscribe to a list of model traitlet changes. Cleans up on unmount. */
export function useModelSubscriptions(
  model: AnyWidgetModel,
  subs: Array<[string, () => void]>,
): void {
  React.useEffect(() => {
    subs.forEach(([k, cb]) => model.on(`change:${k}`, cb));
    return () => subs.forEach(([k, cb]) => model.off(`change:${k}`, cb));
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
