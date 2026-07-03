import { useState, useEffect, useCallback } from "react";

export function useNodeColors(widgetId: string) {
  const key = `transition-node-colors:${widgetId}`;
  const [colors, setColors] = useState<Record<string, string>>({});

  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(key);
      if (raw) setColors(JSON.parse(raw));
    } catch {}
  }, [key]);

  const setNodeColor = useCallback((nodeId: string, color: string) => {
    setColors((prev) => {
      const next = { ...prev, [nodeId]: color };
      try { window.localStorage.setItem(key, JSON.stringify(next)); } catch {}
      return next;
    });
  }, [key]);

  const removeNodeColor = useCallback((nodeId: string) => {
    setColors((prev) => {
      const next = { ...prev };
      delete next[nodeId];
      try { window.localStorage.setItem(key, JSON.stringify(next)); } catch {}
      return next;
    });
  }, [key]);

  const getNodeColor = useCallback((nodeId: string) => colors[nodeId], [colors]);
  return { colors, setNodeColor, removeNodeColor, getNodeColor };
}
