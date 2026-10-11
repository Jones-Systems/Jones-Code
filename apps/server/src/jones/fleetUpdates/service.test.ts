import { describe, expect, it, vi } from "vitest";
import * as Effect from "effect/Effect";
import { EnvironmentId, type JonesUpdateState } from "@t3tools/contracts";
import type { FleetEnrollment, FleetHostOperation, FleetStageInput } from "@t3tools/contracts/jones/fleet-updates";
import { makeFleetUpdates, type FleetHostUpdater } from "./service.ts";
import type { FleetHostStore } from "./store.ts";

const environmentId = EnvironmentId.make("fleet-host");
const enrollment: FleetEnrollment = { environmentId, enrollmentId: "11111111-1111-4111-8111-111111111111", enabled: true, continueRunningThreads: false };
const input: FleetStageInput = { operationId: "22222222-2222-4222-8222-222222222222", enrollmentId: enrollment.enrollmentId, environmentId, expectedInstalledSource: "a".repeat(40), targetSource: "b".repeat(40) };
async function fixture() {
  let savedEnrollment: FleetEnrollment | null = null;
  const operations = new Map<string, FleetHostOperation>();
  const store: FleetHostStore = {
    readEnrollment: async () => savedEnrollment,
    editEnrollment: async (change) => savedEnrollment = change(savedEnrollment),
    readOperation: async (id) => operations.get(id) ?? null,
    editOperation: async (id, change) => { const next = change(operations.get(id) ?? null); operations.set(id, next); return next; },
  };
  const update: JonesUpdateState = { source: "jones-actions", channel: "jones-main", phase: "available", capability: { check: true, download: true, install: true }, currentVersion: "old-version", environmentId, installedSource: input.expectedInstalledSource };
  let native: Effect.Success<ReturnType<FleetHostUpdater["reconcileOperation"]>> = { state: "absent", operationId: input.operationId };
  const install = vi.fn<FleetHostUpdater["installForOperation"]>((request) => Effect.sync(() => {
    expect(operations.get(input.operationId)?.phase).toBe("dispatching");
    native = { state: "pending", operationId: request.operationId, binding: request };
    return { ...update, phase: "installing" };
  }));
  const updates: FleetHostUpdater = {
    fleetOperationsSupported: true,
    state: () => Effect.succeed(update),
    stageExact: ({ targetSource }) => Effect.succeed({ ...update, phase: "staged", stagedHandle: "staged-handle", provenance: { repository: "Jones-Systems/Jones-Code", sourceSha: targetSource, sourceTree: "c".repeat(40), workflow: "main", runId: 1, runAttempt: 1, artifactId: 2, artifactDigest: "digest", platform: "linux", architecture: "x64" } }),
    installForOperation: install,
    reconcileOperation: () => Effect.sync(() => native),
  };
  const create = () => Effect.runPromise(makeFleetUpdates({ environmentId, serviceMode: true, store, updates }));
  return { service: await create(), create, install, operations, setNative: (value: typeof native) => { native = value; } };
}

describe("fleet host coordination", () => {
  it("requires explicit enrollment and rejects changed immutable operation bindings", async () => {
    const f = await fixture();
    await expect(Effect.runPromise(f.service.stage(input))).rejects.toThrow("not accepted");
    await Effect.runPromise(f.service.enroll({ enrollment, expectedEnrollmentId: null }));
    await Effect.runPromise(f.service.stage(input));
    await expect(Effect.runPromise(f.service.stage({ ...input, targetSource: "d".repeat(40) }))).rejects.toThrow("another update");
    expect(f.install).not.toHaveBeenCalled();
  });

  it("persists dispatch before IPC and reconciles duplicates across coordinator restart", async () => {
    const f = await fixture();
    await Effect.runPromise(f.service.enroll({ enrollment, expectedEnrollmentId: null }));
    await Effect.runPromise(f.service.stage(input));
    const activate = { environmentId, enrollmentId: enrollment.enrollmentId, operationId: input.operationId };
    const result = await Effect.runPromise(f.service.activate(activate));
    expect(result.operation?.phase).toBe("pending");
    const restarted = await f.create();
    await Effect.runPromise(restarted.activate(activate));
    expect(f.install).toHaveBeenCalledTimes(1);
  });

  it("replays authoritative absence but stops on a mismatched native receipt", async () => {
    for (const changed of [false, true]) {
      const f = await fixture();
      await Effect.runPromise(f.service.enroll({ enrollment, expectedEnrollmentId: null }));
      await Effect.runPromise(f.service.stage(input));
      f.operations.set(input.operationId, { ...f.operations.get(input.operationId)!, phase: "dispatching" });
      if (changed) f.setNative({ state: "pending", operationId: input.operationId, binding: { environmentId, currentVersion: "wrong", expectedInstalledSource: input.expectedInstalledSource, targetSource: input.targetSource, stagedHandle: "staged-handle" } });
      const result = await Effect.runPromise(f.service.activate({ environmentId, enrollmentId: enrollment.enrollmentId, operationId: input.operationId }));
      expect(result.operation?.phase).toBe(changed ? "blocked" : "pending");
      expect(f.install).toHaveBeenCalledTimes(changed ? 0 : 1);
    }
  });
});
