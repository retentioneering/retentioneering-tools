import type { WidgetHost } from "@retentioneering/viz-core";

/**
 * Minimal demonstration `WidgetHost` over a plain HTTP backend — proof that
 * `WidgetHost` isn't anywidget-shaped, it's transport-agnostic. This is
 * intentionally small: an in-memory param store (same idea as
 * `staticHost`'s data blob, but mutable) plus a single POST endpoint for
 * compute(). It is NOT a production client for the future web platform
 * backend — just enough to exercise get/set/onChange/compute end to end
 * against a real (if toy) server. See demo/rest-host-demo.ts for a working
 * example against a local Node http server.
 */
export function restHost(baseUrl: string, initialData: Record<string, unknown> = {}): WidgetHost {
  const store = new Map<string, unknown>(Object.entries(initialData));
  const listeners = new Map<string, Set<() => void>>();

  const notify = (key: string) => {
    listeners.get(key)?.forEach((cb) => cb());
  };

  return {
    get(key: string) {
      return store.get(key) ?? null;
    },

    set(key: string, value: unknown) {
      store.set(key, value);
      notify(key);
    },

    setMany(values: Record<string, unknown>) {
      for (const [key, value] of Object.entries(values)) {
        store.set(key, value);
      }
      for (const key of Object.keys(values)) notify(key);
    },

    onChange(key: string, cb: () => void) {
      if (!listeners.has(key)) listeners.set(key, new Set());
      listeners.get(key)!.add(cb);
      return () => listeners.get(key)?.delete(cb);
    },

    async compute<T = unknown>(tool: string, params: Record<string, unknown>): Promise<T> {
      const res = await fetch(`${baseUrl}/compute`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tool, params }),
      });
      if (!res.ok) {
        throw new Error(`restHost.compute(${tool}) failed: ${res.status} ${res.statusText}`);
      }
      return (await res.json()) as T;
    },
  };
}
