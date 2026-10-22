import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import * as NodeURL from "node:url";
import { loadScenario } from "./scenario-api.mjs";
import { assertBackendPinState } from "./scenarios/sidebar-pin-reload.mjs";

const fixture = { projectId: "project", threadId: "target", controlThreadId: "control" };
const pinnedAt = "2026-10-21T12:00:00.000Z";
function snapshot(targetPinned, controlPinned = false) {
  return {
    threads: [
      { id: fixture.threadId, pinned: targetPinned },
      { id: fixture.controlThreadId, pinned: controlPinned },
    ].map(({ id, pinned }) => ({
      id,
      projectId: fixture.projectId,
      pinnedAt: pinned ? pinnedAt : null,
      pinOrderKey: pinned ? "n" : null,
    })),
  };
}

NodeTest.test("standalone scenario loads through the native harness contract", async () => {
  const loaded = await loadScenario(
    NodeURL.fileURLToPath(new URL("./scenarios/sidebar-pin-reload.mjs", import.meta.url)),
  );
  NodeAssert.equal(loaded.run.constructor.name, "AsyncFunction");
  NodeAssert.match(loaded.identity.sha256, /^[a-f0-9]{64}$/);
});

NodeTest.test("pin oracle accepts pin and unpin only for the selected thread", () => {
  NodeAssert.deepEqual(assertBackendPinState(snapshot(true), fixture, true), {
    pinnedAt,
    pinOrderKey: "n",
  });
  NodeAssert.deepEqual(assertBackendPinState(snapshot(false), fixture, false), {
    pinnedAt: null,
    pinOrderKey: null,
  });
});

NodeTest.test("negative control: a no-op or wrong-thread pin fails the pin assertion", () => {
  for (const state of [snapshot(false), snapshot(false, true)]) {
    NodeAssert.throws(() => assertBackendPinState(state, fixture, true), /Backend target is pinned/);
  }
  NodeAssert.throws(
    () => assertBackendPinState(snapshot(true, true), fixture, true),
    /Backend control thread remains unpinned/,
  );
});

NodeTest.test("negative control: unpin must clear both pin fields", () => {
  const staleOrder = snapshot(false);
  staleOrder.threads[0].pinOrderKey = "n";
  for (const state of [snapshot(true), staleOrder]) {
    NodeAssert.throws(
      () => assertBackendPinState(state, fixture, false),
      /Backend target has no pin metadata/,
    );
  }
});

NodeTest.test("negative control: current-server pin must include an order key", () => {
  for (const pinOrderKey of [undefined, null, "", " ", " n "]) {
    const state = snapshot(true);
    state.threads[0].pinOrderKey = pinOrderKey;
    NodeAssert.throws(
      () => assertBackendPinState(state, fixture, true),
      /Backend target has a pin order key/,
    );
  }
});

NodeTest.test("missing target cannot pass the unpinned oracle", () => {
  const state = snapshot(false);
  state.threads.shift();
  NodeAssert.throws(
    () => assertBackendPinState(state, fixture, false),
    /Backend contains exactly one target thread/,
  );
});
