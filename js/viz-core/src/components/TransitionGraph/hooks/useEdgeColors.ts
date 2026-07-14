import { useState, useEffect, useCallback } from "react";

export function useEdgeColors(widgetId: string) {
  const key = `transition-edge-colors:${widgetId}`;
  const [colors, setColors] = useState<Record<string, string>>({});

  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(key);
      if (raw) setColors(JSON.parse(raw));
    } catch {}
  }, [key]);

  const setEdgeColor = useCallback((src: string, tgt: string, color: string) => {
    setColors((prev) => {
      const next = { ...prev, [`${src}|${tgt}`]: color };
      try { window.localStorage.setItem(key, JSON.stringify(next)); } catch {}
      return next;
    });
  }, [key]);

  const removeEdgeColor = useCallback((src: string, tgt: string) => {
    setColors((prev) => {
      const next = { ...prev };
      delete next[`${src}|${tgt}`];
      try { window.localStorage.setItem(key, JSON.stringify(next)); } catch {}
      return next;
    });
  }, [key]);

  const getEdgeColor = useCallback((src: string, tgt: string) => colors[`${src}|${tgt}`], [colors]);
  return { colors, setEdgeColor, removeEdgeColor, getEdgeColor };
}
