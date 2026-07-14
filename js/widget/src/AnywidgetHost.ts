import type { WidgetHost } from "@retentioneering/viz-core";

/**
 * Minimal shape of anywidget's `AnyModel` that we depend on. Kept local
 * (rather than importing anywidget's types) since this is the *only* file
 * in the whole JS workspace allowed to know about anywidget's model.get/set
 * shape — every other file (viz-core components/stores, the six widget
 * entry files) talks to a `WidgetHost` instead.
 */
export interface AnyWidgetModel {
  get(key: string): unknown;
  set(key: string, value: unknown): void;
  save_changes(): void;
  on(event: string, cb: () => void): void;
  off(event: string, cb: () => void): void;
}

/**
 * Wraps a live anywidget model as a `WidgetHost`:
 *  - get/set go straight through model.get/model.set (set() also flushes
 *    via save_changes(), matching the old hand-written
 *    `model.set(k, v); model.save_changes();` pairs).
 *  - setMany() flushes once after writing every key — the batched-apply
 *    equivalent of the old `model.set(...) × N; model.save_changes();`
 *    blocks (one comm message, not N).
 *  - onChange() subscribes to `change:<key>` and returns an unsubscribe fn.
 *  - compute() routes through the compute_request/compute_response
 *    traitlet RPC: Python's `_on_compute_request` observes `compute_request`,
 *    runs `_dispatch(tool, params)`, and writes the result to
 *    `compute_response`. This subsumes what `JupyterDataProvider` used to do.
 *
 * This is the ONLY place in the widget package (and the only place outside
 * viz-core) that touches `model.get`/`model.set`/`model.on`/`model.off`.
 */
export function anywidgetHost(model: AnyWidgetModel): WidgetHost {
  return {
    get(key: string) {
      return model.get(key);
    },

    set(key: string, value: unknown) {
      model.set(key, value);
      model.save_changes();
    },

    setMany(values: Record<string, unknown>) {
      for (const [key, value] of Object.entries(values)) {
        model.set(key, value);
      }
      model.save_changes();
    },

    onChange(key: string, cb: () => void) {
      const event = `change:${key}`;
      model.on(event, cb);
      return () => model.off(event, cb);
    },

    compute<T = unknown>(tool: string, params: Record<string, unknown>): Promise<T> {
      return new Promise<T>((resolve, reject) => {
        const requestId = Math.random().toString(36).slice(2);

        const handler = () => {
          const raw = model.get("compute_response") as string;
          try {
            const resp = JSON.parse(raw || "{}") as { id?: string; result?: T; error?: string };
            if (resp.id !== requestId) return; // different request, ignore
            model.off("change:compute_response", handler);
            if (resp.error) {
              reject(new Error(resp.error));
            } else {
              resolve(resp.result as T);
            }
          } catch (e) {
            model.off("change:compute_response", handler);
            reject(e);
          }
        };

        model.on("change:compute_response", handler);
        model.set("compute_request", JSON.stringify({ id: requestId, tool, params }));
        model.save_changes();
      });
    },
  };
}
