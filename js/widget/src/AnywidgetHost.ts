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

/** Safety net only: generous enough never to cut off a slow but healthy computation. */
export const COMPUTE_TIMEOUT_MS = 10 * 60 * 1000;

type PendingCompute = {
  resolve: (result: unknown) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
};

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
 *    Every view of a widget shares that one `compute_response` traitlet, so
 *    a single listener per host parses each response once and settles only
 *    the pending call whose id it carries. A response that fails to parse
 *    is logged and dropped without touching other calls; a call that never
 *    gets an answer (e.g. the kernel restarted) rejects after
 *    `computeTimeoutMs`.
 *
 * This is the ONLY place in the widget package (and the only place outside
 * viz-core) that touches `model.get`/`model.set`/`model.on`/`model.off`.
 */
export function anywidgetHost(
  model: AnyWidgetModel,
  computeTimeoutMs: number = COMPUTE_TIMEOUT_MS,
): WidgetHost {
  const pending = new Map<string, PendingCompute>();

  const onComputeResponse = () => {
    const raw = model.get("compute_response") as string;
    if (!raw) return;
    let resp: { id?: unknown; result?: unknown; error?: string } | null;
    try {
      resp = JSON.parse(raw);
    } catch (e) {
      // The id is inside the unparseable payload, so we can't tell whose
      // response this was. Leave every pending call alone; the one it was
      // meant for still rejects on its timeout.
      console.error("anywidgetHost: unparseable compute_response", e);
      return;
    }
    if (typeof resp?.id !== "string") return;
    const call = pending.get(resp.id);
    if (!call) return; // another view's request, or one that already timed out
    settle(resp.id);
    if (resp.error) call.reject(new Error(resp.error));
    else call.resolve(resp.result);
  };

  // Forget a pending call; detach the listener once nothing is pending.
  const settle = (requestId: string) => {
    clearTimeout(pending.get(requestId)?.timer);
    pending.delete(requestId);
    if (pending.size === 0) model.off("change:compute_response", onComputeResponse);
  };

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
        const timer = setTimeout(() => {
          settle(requestId);
          reject(new Error(`compute("${tool}") got no response in ${computeTimeoutMs / 1000}s`));
        }, computeTimeoutMs);

        if (pending.size === 0) model.on("change:compute_response", onComputeResponse);
        pending.set(requestId, { resolve: resolve as (result: unknown) => void, reject, timer });
        model.set("compute_request", JSON.stringify({ id: requestId, tool, params }));
        model.save_changes();
      });
    },
  };
}
