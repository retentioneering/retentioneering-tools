import type { DataProvider } from "@retentioneering/viz-core";

interface AnyWidgetModel {
  get(key: string): unknown;
  set(key: string, value: unknown): void;
  save_changes(): void;
  on(event: string, cb: () => void): void;
  off(event: string, cb: () => void): void;
}

/**
 * Routes compute() calls through the anywidget comm channel to the Python kernel.
 * Python side must handle `compute_request` traitlet and respond via `compute_response`.
 */
export class JupyterDataProvider implements DataProvider {
  constructor(private readonly model: AnyWidgetModel) {}

  compute<T = unknown>(tool: string, params: Record<string, unknown>): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const requestId = Math.random().toString(36).slice(2);

      const handler = () => {
        const raw = this.model.get("compute_response") as string;
        try {
          const resp = JSON.parse(raw || "{}") as { id?: string; result?: T; error?: string };
          if (resp.id !== requestId) return; // different request, ignore
          this.model.off("change:compute_response", handler);
          if (resp.error) {
            reject(new Error(resp.error));
          } else {
            resolve(resp.result as T);
          }
        } catch (e) {
          this.model.off("change:compute_response", handler);
          reject(e);
        }
      };

      this.model.on("change:compute_response", handler);
      this.model.set("compute_request", JSON.stringify({ id: requestId, tool, params }));
      this.model.save_changes();
    });
  }
}
