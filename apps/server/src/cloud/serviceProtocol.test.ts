import { expect, it } from "vite-plus/test";
import {
  decodeServiceLauncherContext,
  decodeServiceLauncherParentMessage,
  decodeServiceState,
} from "./serviceProtocol.ts";

it("retains durable pending phases and refuses unbound qualified metadata", () => {
  const update = {
    id: "update-1",
    fromVersion: "0.1.7",
    targetVersion: "0.1.8",
    dbPath: "/fixture/state.sqlite",
    status: "pending",
    phase: "accepted",
  };
  const state = { protocol: 4, activeVersion: "0.1.7", update };
  expect(decodeServiceState(state)?.update).toEqual(update);
  expect(decodeServiceState({ ...state, update: { ...update, phase: "trial-ready" } })?.update)
    .toMatchObject({ phase: "trial-ready" });
  for (const metadata of [
    { phase: "unknown" },
    { qualified: {} },
    { startupReceipt: {} },
    { migrationPlan: {} },
    { stateRecovery: "clone-or-copy" },
  ]) {
    expect(decodeServiceState({ ...state, update: { ...update, ...metadata } })).toBeUndefined();
  }
});

it("does not infer qualified capabilities from a protocol number", () => {
  expect(decodeServiceLauncherContext(JSON.stringify({ protocol: 4, childVersion: "0.1.8" })))
    .not.toHaveProperty("qualifiedUpdatesProtocol");
  expect(decodeServiceLauncherContext(JSON.stringify({
    protocol: 3,
    childVersion: "0.1.8",
    launcherHandoffProtocol: 1,
    startupGateProtocol: 1,
  }))).toBeUndefined();
  expect(decodeServiceLauncherContext(JSON.stringify({
    protocol: 4,
    childVersion: "0.1.8",
    qualifiedRecoveryProtocol: 2,
  }))).toBeUndefined();
});

it("requires a bound qualified commit grant while retaining ordinary stop IPC", () => {
  expect(decodeServiceLauncherParentMessage({ type: "stop" })).toEqual({ type: "stop" });
  expect(decodeServiceLauncherParentMessage({
    type: "committed",
    updateId: "update-1",
    startupGateProtocol: 1,
  })).toBeUndefined();
  expect(decodeServiceLauncherParentMessage({
    type: "committed",
    updateId: "update-1",
  })).toEqual({ type: "committed", updateId: "update-1" });
});
