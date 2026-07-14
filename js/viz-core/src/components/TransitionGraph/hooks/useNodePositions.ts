import { useState, useEffect, useCallback } from "react";

interface Position { x: number; y: number; }

function parse(raw: string | null): Record<string, Position> | null {
  if (!raw) return null;
  try {
    const p = JSON.parse(raw);
    if (!p || typeof p !== "object") return null;
    const next: Record<string, Position> = {};
    for (const [id, v] of Object.entries(p)) {
      if (!v || typeof v !== "object") continue;
      const { x, y } = v as { x?: unknown; y?: unknown };
      if (!Number.isFinite(x) || !Number.isFinite(y)) continue;
      next[id] = { x: Number(x), y: Number(y) };
    }
    return Object.keys(next).length > 0 ? next : null;
  } catch { return null; }
}

export function useNodePositions(widgetId: string) {
  const key = widgetId ? `transition-graph-positions:${widgetId}` : null;
  const [positions, setPositions] = useState<Record<string, Position>>({});
  const [hasSavedPositions, setHasSavedPositions] = useState(false);

  useEffect(() => {
    if (!key) return;
    try {
      const p = parse(window.localStorage.getItem(key));
      if (p) { setPositions(p); setHasSavedPositions(true); }
    } catch {}
  }, [key]);

  const savePositions = useCallback((p: Record<string, Position>) => {
    setPositions(p);
    setHasSavedPositions(true);
    if (key) try { window.localStorage.setItem(key, JSON.stringify(p)); } catch {}
  }, [key]);

  const resetPositions = useCallback(() => {
    setPositions({});
    setHasSavedPositions(false);
    if (key) try { window.localStorage.removeItem(key); } catch {}
  }, [key]);

  return { positions, savePositions, resetPositions, hasSavedPositions };
}
