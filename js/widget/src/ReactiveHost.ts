import type { WidgetHost } from "@retentioneering/viz-core";

/**
 * Wraps a `WidgetHost` so that setting specific keys triggers a `compute()`
 * call and writes the response back — replicating, over an explicit RPC
 * call, what anywidget gets for free from Python's `@observe` on a
 * traitlet (e.g. changing `edge_weight` server-side recomputes `result`
 * and pushes it back down). Stateless REST backends have no persistent
 * widget instance to observe trait changes on, so the reactive mapping has
 * to be explicit here instead.
 *
 * `reactions` is keyed by the traitlet name to watch; each entry names the
 * tool to call and how to build its params from the current value (and,
 * if needed, other current host state).
 */
export function reactiveHost(
  host: WidgetHost,
  reactions: Record<
    string,
    {
      tool: string;
      params: (host: WidgetHost, value: unknown) => Record<string, unknown>;
      resultKey?: string; // defaults to "result"
    }
  >,
): WidgetHost {
  return {
    ...host,
    set(key: string, value: unknown) {
      host.set(key, value);
      const reaction = reactions[key];
      if (!reaction) return;
      Promise.resolve(host.compute(reaction.tool, reaction.params(host, value)))
        .then((result) => host.set(reaction.resultKey ?? "result", JSON.stringify(result)))
        .catch((err) => console.error(`reactiveHost: compute("${reaction.tool}") failed`, err));
    },
  };
}
