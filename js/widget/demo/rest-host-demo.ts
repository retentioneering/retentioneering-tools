/**
 * Demonstration: `restHost` proves `WidgetHost` is transport-agnostic — the
 * same interface anywidget and static-export use, but here driven entirely
 * by `fetch()` against a plain Node `http` server. This is NOT a production
 * client for a future web-platform backend; it's a minimal, throwaway proof
 * that a real viz-core component (StepSankey, via its StepMatrixStore) can
 * be driven end to end through a WidgetHost's get/set/onChange/compute
 * cycle without anywidget anywhere in the picture.
 *
 * Run with (from js/widget):
 *   npm run demo:rest-host
 * which bundles this file with esbuild (platform=node) and runs it with
 * plain `node` — no test framework, no browser, no jsdom. StepSankey is
 * rendered with react-dom/server's renderToStaticMarkup, which only needs
 * `document`/`window` inside effects (never invoked during a static
 * server render) — see the `typeof document !== "undefined"` guards in
 * viz-core's StepSankey internals.
 */
import * as http from "node:http";
import * as React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { StepSankey, StepMatrixStore, type RawStepMatrixData } from "@retentioneering/viz-core";
import { restHost } from "../src/RestHost";

// ── canned server-side response for one tool call ───────────────────────────

const CANNED_MATRICES: RawStepMatrixData[] = [
  {
    events: ["path_start", "catalog", "cart", "purchase"],
    columns: [0, 1, 2, 3],
    values: [
      [1.0, 0.0, 0.0, 0.0],
      [0.0, 0.6, 0.1, 0.0],
      [0.0, 0.3, 0.5, 0.1],
      [0.0, 0.1, 0.4, 0.9],
    ],
  },
];

function startToyServer(): Promise<{ url: string; close: () => Promise<void> }> {
  return new Promise((resolve) => {
    const server = http.createServer((req, res) => {
      if (req.method === "POST" && req.url === "/compute") {
        let body = "";
        req.on("data", (chunk) => (body += chunk));
        req.on("end", () => {
          const { tool } = JSON.parse(body || "{}");
          res.setHeader("Content-Type", "application/json");
          if (tool === "step_sankey_data") {
            res.writeHead(200);
            res.end(JSON.stringify({ matrices: CANNED_MATRICES, event_counts: { catalog: 100, cart: 40, purchase: 20 } }));
          } else {
            res.writeHead(404);
            res.end(JSON.stringify({ error: `unknown tool ${tool}` }));
          }
        });
        return;
      }
      res.writeHead(404);
      res.end();
    });
    server.listen(0, "127.0.0.1", () => {
      const port = (server.address() as { port: number }).port;
      resolve({
        url: `http://127.0.0.1:${port}`,
        close: () => new Promise((r) => server.close(() => r())),
      });
    });
  });
}

async function main() {
  const { url, close } = await startToyServer();
  console.log(`[demo] toy server listening at ${url}`);

  try {
    // 1. Build a WidgetHost against the toy server, seeded with a couple of
    //    "widget parameters" — exactly what a widget entry file would do
    //    with any WidgetHost (anywidgetHost, staticHost, or this one).
    const host = restHost(url, { max_steps: 10, step_window: 3 });

    // 2. get/set/onChange — same calls the six widget entry files make.
    let sawChange = false;
    const unsubscribe = host.onChange("step_window", () => { sawChange = true; });
    console.assert(host.get("max_steps") === 10, "get() should return seeded param");
    host.set("step_window", 2);
    console.assert(sawChange, "onChange() callback should fire after set()");
    console.assert(host.get("step_window") === 2, "get() should reflect the value set()");
    unsubscribe();

    // 3. compute() — a real HTTP round trip to the toy server.
    const result = await host.compute<{ matrices: RawStepMatrixData[] }>("step_sankey_data", {
      max_steps: host.get("max_steps"),
    });
    console.assert(result.matrices.length === 1, "compute() should return the canned matrices");

    // 4. Feed the computed data into a real viz-core store + component, and
    //    render the component to a string — proving the whole pipeline
    //    (HTTP compute -> store -> React component) works with no anywidget
    //    and no browser involved.
    const store = new StepMatrixStore();
    store.setData(result.matrices);

    const html = renderToStaticMarkup(
      React.createElement(StepSankey, {
        store,
        maxSteps: Number(host.get("step_window")) || 3,
        stepWindow: 3,
        pathPattern: "",
        theme: "light",
      }),
    );

    console.assert(html.includes("catalog"), "rendered HTML should include an event label from the computed data");
    console.assert(html.includes("purchase"), "rendered HTML should include the terminal event label");

    console.log(`[demo] rendered StepSankey via restHost: ${html.length} chars of HTML, contains "catalog" and "purchase": OK`);
    console.log("[demo] restHost get/set/onChange/compute cycle: PASS");
  } finally {
    await close();
  }
}

main().catch((err) => {
  console.error("[demo] FAILED:", err);
  process.exitCode = 1;
});
