import type { WidgetHost } from "@retentioneering/viz-core";

/**
 * Read-only `WidgetHost` over a plain data blob — the host for the
 * static-HTML-export path (ADR-0010). `data` is `window.__HS_DATA__`
 * (or one entry of `window.__HS_WIDGETS__`), produced once by Python at
 * export time; there is no kernel behind it, so writes/subscriptions/compute
 * are all no-ops (matching the previous fake read-only model in main.tsx,
 * which this replaces).
 *
 * anywidget traitlets always deliver JSON-encoded strings for
 * objects/arrays via model.get(); static-export data is plain parsed JSON,
 * so get() re-serializes object/array values to match what widget code
 * expects (it JSON.parse()s them right back) — same behavior as before.
 */
export function staticHost(data: Record<string, unknown>): WidgetHost {
  return {
    get(key: string) {
      const v = data[key];
      if (v !== null && v !== undefined && typeof v === "object") {
        return JSON.stringify(v);
      }
      return v ?? null;
    },

    set() {
      // Static export has no kernel to persist to.
    },

    setMany() {
      // Static export has no kernel to persist to.
    },

    onChange() {
      // Data never changes after export; nothing to subscribe to.
      return () => {};
    },

    compute() {
      // No backend behind a static export. Widget code must guard compute-
      // dependent interactions behind `isStatic` (as it already does), so
      // this should never actually be called; reject loudly if it is.
      return Promise.reject(new Error("compute() is not available in static (exported HTML) widgets"));
    },
  };
}
