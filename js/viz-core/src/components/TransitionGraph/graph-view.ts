import type { EdgeFilterSpec } from "./TransitionGraph";
import type { StoredViewport } from "./TransitionGraph";

/**
 * GraphView — a serializable description of a transition graph's VISUAL
 * state on top of an already-computed matrix: focus, filters, hidden
 * events, viewport. Deliberately contains no data parameters
 * (edge_weight / diff / path_col): those require a Python recompute, which
 * a static HTML export cannot do — a view link must work identically in a
 * live notebook, an exported report, and an MCP tab.
 *
 * Entry points that all speak this format:
 *   - `views=` / `view=` kwargs on the Python widget (named pills),
 *   - the pills row in the widget UI,
 *   - the "Copy view link" toolbar button,
 *   - `#view=<base64url>` fragment on a bare HTML export,
 *   - `[tab:view=Name]` links in MCP report analysis text.
 */
export interface GraphView {
  v?: 1;
  /** Pill label; also the handle for `[tab:view=Name]` links. */
  name?: string;
  focus?:
    | { type: "node"; id: string }
    | { type: "edge"; source: string; target: string }
    | { type: "path"; nodes: string[] };
  edgeFilter?: EdgeFilterSpec;
  /** Absolute event-count (population) range. */
  eventCountFilter?: [number, number] | null;
  hiddenEvents?: string[];
  /** "fit" = whole graph, "fit-focus" = fit the focused elements (the
   *  default when focus is present), explicit {zoom, pan} otherwise. */
  viewport?: "fit" | "fit-focus" | StoredViewport;
}

/** Loose runtime validation for a view arriving from JSON (traitlet, URL
 *  hash, analysis link). Returns null when the shape is unusable. */
export function parseGraphView(raw: unknown): GraphView | null {
  if (!raw) return null;
  let value: unknown = raw;
  if (typeof raw === "string") {
    try {
      value = JSON.parse(raw);
    } catch {
      return null;
    }
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const view = value as Record<string, unknown>;
  const out: GraphView = {};

  if (typeof view.name === "string" && view.name) out.name = view.name;

  const focus = view.focus as Record<string, unknown> | undefined;
  if (focus && typeof focus === "object") {
    if (focus.type === "node" && typeof focus.id === "string") {
      out.focus = { type: "node", id: focus.id };
    } else if (
      focus.type === "edge" &&
      typeof focus.source === "string" &&
      typeof focus.target === "string"
    ) {
      out.focus = { type: "edge", source: focus.source, target: focus.target };
    } else if (
      focus.type === "path" &&
      Array.isArray(focus.nodes) &&
      focus.nodes.length >= 2 &&
      focus.nodes.every((n) => typeof n === "string")
    ) {
      out.focus = { type: "path", nodes: focus.nodes as string[] };
    }
  }

  const filter = view.edgeFilter as Record<string, unknown> | undefined;
  if (filter && typeof filter === "object") {
    if (filter.mode === "topk" && Number.isFinite(filter.k)) {
      out.edgeFilter = { mode: "topk", k: Number(filter.k) };
    } else if (
      filter.mode === "range" &&
      Array.isArray(filter.range) &&
      filter.range.length === 2 &&
      filter.range.every((n) => Number.isFinite(n))
    ) {
      out.edgeFilter = {
        mode: "range",
        range: [Number(filter.range[0]), Number(filter.range[1])],
      };
    }
  }

  if (
    Array.isArray(view.eventCountFilter) &&
    view.eventCountFilter.length === 2 &&
    view.eventCountFilter.every((n) => Number.isFinite(n))
  ) {
    out.eventCountFilter = [
      Number(view.eventCountFilter[0]),
      Number(view.eventCountFilter[1]),
    ];
  }

  if (
    Array.isArray(view.hiddenEvents) &&
    view.hiddenEvents.every((e) => typeof e === "string")
  ) {
    out.hiddenEvents = view.hiddenEvents as string[];
  }

  if (view.viewport === "fit" || view.viewport === "fit-focus") {
    out.viewport = view.viewport;
  } else {
    const vp = view.viewport as Record<string, unknown> | undefined;
    if (
      vp &&
      typeof vp === "object" &&
      Number.isFinite(vp.zoom) &&
      vp.pan &&
      typeof vp.pan === "object" &&
      Number.isFinite((vp.pan as Record<string, unknown>).x) &&
      Number.isFinite((vp.pan as Record<string, unknown>).y)
    ) {
      out.viewport = {
        zoom: Number(vp.zoom),
        pan: {
          x: Number((vp.pan as Record<string, unknown>).x),
          y: Number((vp.pan as Record<string, unknown>).y),
        },
      };
    }
  }

  return Object.keys(out).length > 0 ? out : null;
}

/** base64url encoding for the `#view=` URL fragment (unicode-safe). */
export function encodeGraphView(view: GraphView): string {
  const json = JSON.stringify(view);
  const bytes = new TextEncoder().encode(json);
  let binary = "";
  bytes.forEach((b) => {
    binary += String.fromCharCode(b);
  });
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export function decodeGraphView(encoded: string): GraphView | null {
  try {
    const base64 = encoded.replace(/-/g, "+").replace(/_/g, "/");
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    const json = new TextDecoder().decode(bytes);
    return parseGraphView(json);
  } catch {
    return null;
  }
}
