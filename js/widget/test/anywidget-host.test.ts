// Run with `npm test` (bundled by esbuild, executed by node:test).
// Covers the compute() RPC over the shared compute_response traitlet
// (https://github.com/retentioneering/retentioneering-tools/issues/175).
import assert from "node:assert/strict";
import { test } from "node:test";
import { anywidgetHost, type AnyWidgetModel } from "../src/AnywidgetHost";

class FakeModel implements AnyWidgetModel {
  state = new Map<string, unknown>();
  listeners = new Map<string, Set<() => void>>();

  get(key: string) {
    return this.state.get(key);
  }
  set(key: string, value: unknown) {
    this.state.set(key, value);
  }
  save_changes() {}
  on(event: string, cb: () => void) {
    if (!this.listeners.has(event)) this.listeners.set(event, new Set());
    this.listeners.get(event)!.add(cb);
  }
  off(event: string, cb: () => void) {
    this.listeners.get(event)?.delete(cb);
  }

  responseListeners() {
    return this.listeners.get("change:compute_response")?.size ?? 0;
  }
  lastRequestId(): string {
    return JSON.parse(this.state.get("compute_request") as string).id;
  }
  respond(raw: string) {
    this.state.set("compute_response", raw);
    for (const cb of [...(this.listeners.get("change:compute_response") ?? [])]) cb();
  }
}

test("resolves each call with the response carrying its id", async () => {
  const model = new FakeModel();
  const host = anywidgetHost(model);
  const p1 = host.compute("a", {});
  const id1 = model.lastRequestId();
  const p2 = host.compute("b", {});
  const id2 = model.lastRequestId();

  model.respond(JSON.stringify({ id: id2, result: "two" }));
  model.respond(JSON.stringify({ id: id1, result: "one" }));

  assert.deepEqual(await Promise.all([p1, p2]), ["one", "two"]);
  assert.equal(model.responseListeners(), 0);
});

test("rejects with the error message from Python", async () => {
  const model = new FakeModel();
  const host = anywidgetHost(model);
  const p = host.compute("a", {});
  model.respond(JSON.stringify({ id: model.lastRequestId(), error: "Unknown tool: 'a'" }));
  await assert.rejects(p, /Unknown tool/);
});

test("an unparseable response leaves other pending calls alone", async () => {
  const model = new FakeModel();
  const host = anywidgetHost(model, 50);
  const originalError = console.error;
  console.error = () => {};
  try {
    const p1 = host.compute("a", {});
    const id1 = model.lastRequestId();
    const p2 = host.compute("b", {});
    const id2 = model.lastRequestId();

    model.respond(`{"id": "${id1}", "result": NaN}`);
    model.respond(JSON.stringify({ id: id2, result: "ok" }));

    assert.equal(await p2, "ok");
    await assert.rejects(p1, /no response/);
  } finally {
    console.error = originalError;
  }
});

test("ignores responses to requests it did not send", async () => {
  const model = new FakeModel();
  const host = anywidgetHost(model);
  const p = host.compute("a", {});
  const id = model.lastRequestId();
  model.respond(JSON.stringify({ id: "another-view", result: "theirs" }));
  model.respond("null");
  model.respond(JSON.stringify({ id, result: "mine" }));
  assert.equal(await p, "mine");
});

test("an unanswered call times out and removes the listener", async () => {
  const model = new FakeModel();
  const host = anywidgetHost(model, 20);
  const p = host.compute("a", {});
  assert.equal(model.responseListeners(), 1);
  await assert.rejects(p, /compute\("a"\) got no response/);
  assert.equal(model.responseListeners(), 0);
});

test("one listener serves any number of pending calls", () => {
  const model = new FakeModel();
  const host = anywidgetHost(model, 20);
  const calls = [host.compute("a", {}), host.compute("b", {}), host.compute("c", {})];
  assert.equal(model.responseListeners(), 1);
  return Promise.allSettled(calls);
});
