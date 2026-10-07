import { expect, it } from "@effect/vitest";
import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Duration from "effect/Duration";
import * as Schema from "effect/Schema";
import { TestClock } from "effect/testing";
import * as NetAddress from "effect/unstable/net/NetAddress";
import {
  decodeServiceLauncherContext,
  decodeServiceLauncherChildMessage,
  decodeServiceLauncherParentMessage,
} from "./serviceProtocol.ts";
import type {
  QualifiedTrialReceipt,
  QualifiedTrialRuntimeWitness,
} from "../jones/cloud/qualifiedStartup.ts";

import {
  LEGACY_SERVICE_LAUNCHER_PROTOCOL,
  SERVICE_LAUNCHER_CONTEXT_ENV,
  SERVICE_LAUNCHER_PROTOCOL,
  type ServiceLauncherChildMessage,
  type ServiceLauncherParentMessage,
} from "./serviceProtocol.ts";
import * as ServiceLauncherClient from "./serviceLauncherClient.ts";

const encodeContextFixture = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

class FakeLauncherProcess {
  readonly connected = true;
  readonly env: Record<string, string | undefined>;
  readonly sent: ServiceLauncherChildMessage[] = [];
  readonly sentSignal = Promise.withResolvers<void>();
  readonly #listeners = new Map<string, Set<(...args: ReadonlyArray<unknown>) => void>>();

  constructor(context: unknown) {
    this.env = { [SERVICE_LAUNCHER_CONTEXT_ENV]: JSON.stringify(context) };
  }

  send = (message: ServiceLauncherChildMessage, callback?: (error: Error | null) => void) => {
    this.sent.push(message);
    this.sentSignal.resolve();
    callback?.(null);
    return true;
  };

  on = (event: "message" | "disconnect", listener: (...args: ReadonlyArray<unknown>) => void) => {
    const listeners = this.#listeners.get(event) ?? new Set();
    listeners.add(listener);
    this.#listeners.set(event, listeners);
  };

  off = (event: "message" | "disconnect", listener: (...args: ReadonlyArray<unknown>) => void) => {
    this.#listeners.get(event)?.delete(listener);
  };

  listenerCount() {
    return [...this.#listeners.values()].reduce((count, listeners) => count + listeners.size, 0);
  }

  disconnect() {
    for (const listener of this.#listeners.get("disconnect") ?? []) listener();
  }

  emitUnknown(message: unknown) {
    for (const listener of this.#listeners.get("message") ?? []) listener(message);
  }

  emit(message: ServiceLauncherParentMessage) {
    for (const listener of this.#listeners.get("message") ?? []) listener(message);
  }
}

const makeClient = (host: FakeLauncherProcess, currentVersion: string) =>
  ServiceLauncherClient.make({ currentVersion }).pipe(
    Effect.provideService(ServiceLauncherClient.ServiceLauncherHostProcess, host),
    Effect.provideService(HostProcessEnvironment, host.env),
  );

it.effect("waits for the launcher to durably commit the trial update ID", () =>
  Effect.gen(function* () {
    const pending = {
      id: "update-1",
      fromVersion: "1.0.0",
      targetVersion: "1.1.0",
      dbPath: "/tmp/state.sqlite",
      status: "pending" as const,
      phase: "trial-ready" as const,
    };
    const host = new FakeLauncherProcess({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      childVersion: "1.1.0",
      update: pending,
    });
    const client = yield* makeClient(host, "1.1.0");
    const prepared = yield* Effect.forkChild(client.prepareTrial, { startImmediately: true });
    yield* Effect.yieldNow;
    expect(host.sent).toEqual([{ type: "prepared", updateId: "update-1" }]);

    const committed = {
      id: pending.id,
      fromVersion: pending.fromVersion,
      targetVersion: pending.targetVersion,
      status: "committed" as const,
    };
    host.emit({ type: "committed", updateId: committed.id });
    expect(yield* Fiber.join(prepared)).toEqual(committed);
  }),
);

it.effect(
  "completes a trial started by the legacy launcher without authorizing another update",
  () =>
    Effect.gen(function* () {
      const host = new FakeLauncherProcess({
        protocol: LEGACY_SERVICE_LAUNCHER_PROTOCOL,
        childVersion: "1.1.0",
        update: {
          id: "legacy-update",
          fromVersion: "1.0.0",
          targetVersion: "1.1.0",
          dbPath: "/tmp/state.sqlite",
          status: "pending",
        },
      });
      const client = yield* makeClient(host, "1.1.0");
      const prepared = yield* Effect.forkChild(client.prepareTrial, { startImmediately: true });
      yield* Effect.yieldNow;
      expect(host.sent).toEqual([{ type: "prepared", updateId: "legacy-update" }]);
      host.emit({ type: "committed", updateId: "legacy-update" });
      expect(yield* Fiber.join(prepared)).toMatchObject({ status: "committed" });
    }),
);

it.effect(
  "requires a launcher upgrade before a legacy-managed server can request another update",
  () =>
    Effect.gen(function* () {
      const host = new FakeLauncherProcess({
        protocol: LEGACY_SERVICE_LAUNCHER_PROTOCOL,
        childVersion: "1.1.0",
      });
      const client = yield* makeClient(host, "1.1.0");
      const error = yield* client
        .requestUpdate({ targetVersion: "1.2.0", dbPath: "/tmp/state.sqlite" })
        .pipe(Effect.flip);
      expect(error).toMatchObject({
        _tag: "ServiceLauncherRejectedError",
        reason: "The installed service launcher must be upgraded before another remote update.",
      });
      expect(host.sent).toEqual([]);
    }),
);

it.effect("returns the launcher-generated ID only after update acceptance", () =>
  Effect.gen(function* () {
    const host = new FakeLauncherProcess({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      childVersion: "1.0.0",
    });
    const client = yield* makeClient(host, "1.0.0");
    const requested = yield* Effect.forkChild(
      client.requestUpdate({ targetVersion: "1.1.0", dbPath: "/tmp/state.sqlite" }),
      { startImmediately: true },
    );
    yield* Effect.yieldNow;
    host.emit({
      type: "update-accepted",
      updateId: "launcher-id",
    });
    expect(yield* Fiber.join(requested)).toBe("launcher-id");
  }),
);

it.effect("preserves a launcher rejection as a distinct error", () =>
  Effect.gen(function* () {
    const host = new FakeLauncherProcess({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      childVersion: "1.0.0",
    });
    const client = yield* makeClient(host, "1.0.0");
    const requested = yield* Effect.forkChild(
      client.requestUpdate({ targetVersion: "1.1.0", dbPath: "/tmp/state.sqlite" }),
      { startImmediately: true },
    );
    yield* Effect.yieldNow;
    host.emit({ type: "update-rejected", reason: "requires local update" });
    expect(yield* Fiber.join(requested).pipe(Effect.flip)).toMatchObject({
      _tag: "ServiceLauncherRejectedError",
      targetVersion: "1.1.0",
      reason: "requires local update",
    });
  }),
);

it.effect("rejects contradictory trial context instead of leaving activation closed", () =>
  Effect.gen(function* () {
    const host = new FakeLauncherProcess({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      childVersion: "1.1.0",
      update: {
        id: "update-1",
        fromVersion: "1.0.0",
        targetVersion: "1.2.0",
        dbPath: "/tmp/state.sqlite",
        status: "pending",
        phase: "trial-ready",
      },
    });
    const error = yield* makeClient(host, "1.1.0").pipe(Effect.flip);
    expect(error.message).toBe("The service launcher supplied invalid startup context.");
  }),
);

it.effect("does not send a qualified Install to a launcher without Jones capability", () =>
  Effect.gen(function* () {
    const host = new FakeLauncherProcess({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      childVersion: "1.0.0",
    });
    const client = yield* makeClient(host, "1.0.0");
    expect(client.qualifiedUpdates).toBe(false);
    const error = yield* client
      .requestUpdate({
        targetVersion: "0.0.0-preview.20261002.101.1",
        dbPath: "/fixture/userdata/state.sqlite",
        stagedHandle: "fixed-handle",
      })
      .pipe(Effect.flip);
    expect(error.message).toContain("bootstrap-required");
    expect(host.sent).toEqual([]);
  }),
);

it.effect("permits qualified staging but sends no Install without the startup gate", () =>
  Effect.gen(function* () {
    const host = new FakeLauncherProcess({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      childVersion: "1.0.0",
      qualifiedUpdatesProtocol: 1,
    });
    const client = yield* makeClient(host, "1.0.0");
    expect(client.qualifiedStaging).toBe(true);
    expect(client.qualifiedUpdates).toBe(false);
    const error = yield* client
      .requestUpdate({
        targetVersion: "0.0.0-preview.20261002.101.1",
        dbPath: "/fixture/userdata/state.sqlite",
        stagedHandle: "11111111-1111-4111-8111-111111111111",
      })
      .pipe(Effect.flip);
    expect(error.message).toContain("bootstrap-required");
    expect(host.sent).toEqual([]);
  }),
);

it.effect("keeps a qualified staged handle fixed through launcher acceptance", () =>
  Effect.gen(function* () {
    const host = new FakeLauncherProcess({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      childVersion: "1.0.0",
      qualifiedUpdatesProtocol: 1,
      startupGateProtocol: 1,
    });
    const client = yield* makeClient(host, "1.0.0");
    expect(client.qualifiedUpdates).toBe(true);
    const install = {
      targetVersion: "0.0.0-preview.20261002.101.1",
      dbPath: "/fixture/userdata/state.sqlite",
      stagedHandle: "fixed-handle",
    };
    const requested = yield* Effect.forkChild(client.requestUpdate(install), {
      startImmediately: true,
    });
    yield* Effect.yieldNow;
    expect(host.sent).toEqual([{ type: "request-update", ...install }]);
    host.emit({ type: "update-accepted", updateId: "qualified-id" });
    expect(yield* Fiber.join(requested)).toBe("qualified-id");
  }),
);

it.effect("excludes legacy release outcomes from readonly Jones terminal state", () =>
  Effect.gen(function* () {
    const outcome = {
      id: "retained-transaction",
      fromVersion: "1.0.0",
      targetVersion: "1.1.0",
      status: "rolled-back" as const,
      reason: "candidate-exited",
    };
    const host = new FakeLauncherProcess({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      childVersion: "1.0.0",
      qualifiedUpdatesProtocol: 1,
      startupGateProtocol: 1,
      update: outcome,
    });
    const client = yield* makeClient(host, "1.0.0");
    expect(client.qualifiedStartupOutcome).toBeUndefined();
    expect(client.qualifiedStartupOutcome).toBeUndefined();
    expect(host.sent).toEqual([]);
  }),
);

function qualifiedFixture(protocol: 3 | 4 = 4, startupGateProtocol?: 1) {
  const fromVersion = "0.0.0-preview.20261002.100";
  const targetVersion = "0.0.0-preview.20261002.101.1";
  const receipt: QualifiedTrialReceipt = {
    protocol: 4,
    startupGateProtocol: 1,
    updateId: "qualified-trial",
    stagedHandle: "11111111-1111-4111-8111-111111111111",
    home: "/fixture",
    databasePath: "/fixture/userdata/statev2.sqlite",
    serviceUserdata: "/fixture/userdata",
    environmentId: "native-fixture",
    version: targetVersion,
    sourceSha: "a".repeat(40),
    sourceTree: "b".repeat(40),
    listener: { family: "IPv4", address: "127.0.0.1", port: 43123, scopeId: 0 },
    processId: 123,
    resumeHeld: true,
  };
  const qualified = {
    protocol: 1,
    stagedHandle: receipt.stagedHandle,
    binding: {
      baseDir: receipt.home,
      dbPath: receipt.databasePath,
      environmentId: receipt.environmentId,
      activeVersion: fromVersion,
      activeSourceSha: "c".repeat(40),
    },
    receipt: {
      protocol: 1,
      repository: "Jones-Systems/Jones-Code",
      channel: "jones-main",
      version: targetVersion,
      sourceSha: receipt.sourceSha,
      sourceTree: receipt.sourceTree,
      installedSourceSha: "c".repeat(40),
      runId: 101,
      runAttempt: 1,
      artifactId: 102,
      workflow: ".github/workflows/artifact-cli-linux.yml",
      artifactDigest: `sha256:${"d".repeat(64)}`,
      archiveSha256: "e".repeat(64),
      payloadSha256: "f".repeat(64),
      platform: "linux",
      architecture: "x64",
    },
  };
  const pending = {
    id: receipt.updateId,
    fromVersion,
    targetVersion,
    dbPath: receipt.databasePath,
    status: "pending",
    phase: "trial-ready",
    qualified,
  };
  const context = {
    protocol,
    childVersion: targetVersion,
    qualifiedUpdatesProtocol: 1,
    ...(startupGateProtocol === undefined ? {} : { startupGateProtocol }),
    update: pending,
  };
  const host = new FakeLauncherProcess(context);
  const events: string[] = [];
  const operations = {
    receipt: async () => {
      events.push("receipt");
      return receipt;
    },
    assertUnreserved: async () => {
      events.push("unreserved");
    },
    reserve: async () => {
      events.push("reserved");
    },
  };
  const witness: QualifiedTrialRuntimeWitness = {
    home: receipt.home,
    databasePath: receipt.databasePath,
    serviceUserdata: receipt.serviceUserdata,
    environmentId: receipt.environmentId,
    version: targetVersion,
    processId: receipt.processId,
    buildMetadata: {
      jonesSource: {
        repository: "Jones-Systems/Jones-Code",
        sha: receipt.sourceSha,
        tree: receipt.sourceTree,
      },
    },
    listener: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 43123),
  };
  const make = () =>
    makeClient(host, targetVersion).pipe(
      Effect.provideService(ServiceLauncherClient.QualifiedTrialOperations, operations),
    );
  return { host, context, pending, receipt, events, operations, witness, make };
}

it.effect("establishes the qualified outcome only after exact grant and durable reservation", () =>
  Effect.gen(function* () {
    const fixture = qualifiedFixture(4, 1);
    const client = yield* fixture.make();
    expect(client.requiresQualifiedTrialGate).toBe(true);
    yield* client.prepareTrial.pipe(Effect.flip);
    expect(fixture.host.sent).toEqual([]);
    const prepared = yield* Effect.forkChild(client.prepareQualifiedTrial(fixture.witness), {
      startImmediately: true,
    });
    yield* Effect.promise(() => fixture.host.sentSignal.promise);
    expect(fixture.host.sent).toEqual([
      {
        type: "prepared",
        updateId: fixture.pending.id,
        startupGateProtocol: 1,
        qualified: fixture.receipt,
      },
    ]);
    expect(fixture.events).toEqual(["receipt", "unreserved"]);
    expect(client.qualifiedStartupOutcome).toBeUndefined();
    fixture.host.emit({
      type: "committed",
      updateId: fixture.pending.id,
      startupGateProtocol: 1,
      qualified: { ...fixture.receipt, generation: fixture.pending.id },
    });
    const outcome = yield* Fiber.join(prepared);
    expect(fixture.events).toEqual(["receipt", "unreserved", "reserved"]);
    expect(client.qualifiedStartupOutcome).toEqual(outcome);
    expect(yield* client.prepareTrial).toEqual(outcome);
    expect((yield* client.prepareQualifiedTrial(fixture.witness).pipe(Effect.flip)).operation).toBe(
      "qualified-replay",
    );
    expect(fixture.host.sent).toHaveLength(1);
    expect(fixture.host.listenerCount()).toBe(0);
  }),
);

it.effect("holds qualified pending context without a startup gate before sending", () =>
  Effect.gen(function* () {
    const fixture = qualifiedFixture(4);
    expect(
      decodeServiceLauncherContext(encodeContextFixture(fixture.context))?.update,
    ).toHaveProperty("qualified");
    const client = yield* fixture.make();
    expect(client.requiresQualifiedTrialGate).toBe(true);
    expect((yield* client.prepareTrial.pipe(Effect.flip)).operation).toBe("qualified-proof");
    expect((yield* client.prepareQualifiedTrial(fixture.witness).pipe(Effect.flip)).operation).toBe(
      "qualified-proof",
    );
    expect(fixture.events).toEqual([]);
    expect(fixture.host.sent).toEqual([]);
  }),
);

it.effect("rejects legacy qualified context instead of stripping its identity", () =>
  Effect.gen(function* () {
    const fixture = qualifiedFixture(3);
    expect(decodeServiceLauncherContext(encodeContextFixture(fixture.context))).toBeUndefined();
    expect((yield* fixture.make().pipe(Effect.flip)).operation).toBe("decode-context");
    expect(fixture.host.sent).toEqual([]);
  }),
);

it.effect.each(["legacy", "mismatch", "malformed"] as const)(
  "refuses %s committed proof and fences another exchange",
  (mode) =>
    Effect.gen(function* () {
      const fixture = qualifiedFixture(4, 1);
      const client = yield* fixture.make();
      const prepared = yield* Effect.forkChild(
        client.prepareQualifiedTrial(fixture.witness).pipe(Effect.flip),
        { startImmediately: true },
      );
      yield* Effect.promise(() => fixture.host.sentSignal.promise);
      const grant = { ...fixture.receipt, generation: fixture.pending.id };
      fixture.host.emitUnknown({
        type: "committed",
        updateId: fixture.pending.id,
        ...(mode === "legacy"
          ? {}
          : {
              startupGateProtocol: 1,
              qualified:
                mode === "malformed"
                  ? { ...grant, processId: undefined }
                  : { ...grant, processId: 999 },
            }),
      });
      expect((yield* Fiber.join(prepared)).operation).toBe("qualified-proof");
      expect(fixture.events).toEqual(["receipt", "unreserved"]);
      expect(
        (yield* client.prepareQualifiedTrial(fixture.witness).pipe(Effect.flip)).operation,
      ).toBe("qualified-replay");
      expect(fixture.host.sent).toHaveLength(1);
      expect(fixture.host.listenerCount()).toBe(0);
    }),
);

it.effect.each(["cancel", "timeout", "disconnect"] as const)(
  "cleans listeners and never retries after %s",
  (mode) =>
    Effect.gen(function* () {
      const fixture = qualifiedFixture(4, 1);
      const client = yield* fixture.make();
      const prepared = yield* Effect.forkChild(
        client.prepareQualifiedTrial(fixture.witness).pipe(Effect.flip),
        { startImmediately: true },
      );
      yield* Effect.promise(() => fixture.host.sentSignal.promise);
      if (mode === "cancel") yield* Fiber.interrupt(prepared);
      else {
        if (mode === "timeout") yield* TestClock.adjust(Duration.seconds(30));
        else fixture.host.disconnect();
        expect((yield* Fiber.join(prepared)).operation).toBe(mode);
      }
      expect(fixture.host.listenerCount()).toBe(0);
      expect(fixture.events).toEqual(["receipt", "unreserved"]);
      expect(
        (yield* client.prepareQualifiedTrial(fixture.witness).pipe(Effect.flip)).operation,
      ).toBe("qualified-replay");
      expect(fixture.host.sent).toHaveLength(1);
    }),
);

it.effect("keeps a grant uncommitted to startup if durable reservation fails", () =>
  Effect.gen(function* () {
    const fixture = qualifiedFixture(4, 1);
    fixture.operations.reserve = async () => {
      throw new Error("synthetic fsync uncertainty");
    };
    const client = yield* fixture.make();
    const prepared = yield* Effect.forkChild(
      client.prepareQualifiedTrial(fixture.witness).pipe(Effect.flip),
      { startImmediately: true },
    );
    yield* Effect.promise(() => fixture.host.sentSignal.promise);
    fixture.host.emit({
      type: "committed",
      updateId: fixture.pending.id,
      startupGateProtocol: 1,
      qualified: { ...fixture.receipt, generation: fixture.pending.id },
    });
    expect((yield* Fiber.join(prepared)).operation).toBe("qualified-reservation");
    expect(client.qualifiedStartupOutcome).toBeUndefined();
    expect((yield* client.prepareTrial.pipe(Effect.flip)).operation).toBe("qualified-proof");
  }),
);

it.effect("rejects an occupied resume marker before exchange and never replays it", () =>
  Effect.gen(function* () {
    const fixture = qualifiedFixture(4, 1);
    fixture.operations.assertUnreserved = async () => {
      throw new Error("occupied marker");
    };
    const client = yield* fixture.make();
    expect((yield* client.prepareQualifiedTrial(fixture.witness).pipe(Effect.flip)).operation).toBe(
      "qualified-proof",
    );
    expect(fixture.host.sent).toEqual([]);
    expect((yield* client.prepareQualifiedTrial(fixture.witness).pipe(Effect.flip)).operation).toBe(
      "qualified-replay",
    );
  }),
);

it.effect(
  "drains cancelled reservation I/O before startup finalization without opening recovery",
  () =>
    Effect.gen(function* () {
      const fixture = qualifiedFixture(4, 1);
      const started = Promise.withResolvers<void>();
      const release = Promise.withResolvers<void>();
      fixture.operations.reserve = async () => {
        started.resolve();
        await release.promise;
        fixture.events.push("reservation-drained");
      };
      const client = yield* fixture.make();
      const prepared = yield* Effect.forkChild(client.prepareQualifiedTrial(fixture.witness), {
        startImmediately: true,
      });
      yield* Effect.promise(() => fixture.host.sentSignal.promise);
      fixture.host.emit({
        type: "committed",
        updateId: fixture.pending.id,
        startupGateProtocol: 1,
        qualified: { ...fixture.receipt, generation: fixture.pending.id },
      });
      yield* Effect.promise(() => started.promise);
      const cancelled = yield* Effect.forkChild(Fiber.interrupt(prepared), {
        startImmediately: true,
      });
      expect(fixture.events).not.toContain("reservation-drained");
      release.resolve();
      yield* Fiber.join(cancelled);
      expect(fixture.events).toContain("reservation-drained");
      expect(client.qualifiedStartupOutcome).toBeUndefined();
      expect(fixture.host.listenerCount()).toBe(0);
      expect((yield* client.prepareTrial.pipe(Effect.flip)).operation).toBe("qualified-proof");
    }),
);

it("rejects malformed supplied IPC proofs without converting them into ID-only messages", () => {
  const fixture = qualifiedFixture(4, 1);
  expect(
    decodeServiceLauncherChildMessage({
      type: "prepared",
      updateId: fixture.pending.id,
      qualified: fixture.receipt,
    }),
  ).toBeUndefined();
  expect(
    decodeServiceLauncherParentMessage({
      type: "committed",
      updateId: fixture.pending.id,
      startupGateProtocol: 1,
    }),
  ).toBeUndefined();
});
