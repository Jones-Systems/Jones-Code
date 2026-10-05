import { expect, it } from "@effect/vitest";
import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";

import {
  SERVICE_LAUNCHER_CONTEXT_ENV,
  SERVICE_LAUNCHER_PROTOCOL,
  type ServiceLauncherChildMessage,
  type ServiceLauncherParentMessage,
} from "./serviceProtocol.ts";
import * as ServiceLauncherClient from "./serviceLauncherClient.ts";

class FakeLauncherProcess {
  readonly connected = true;
  readonly env: Record<string, string | undefined>;
  readonly sent: ServiceLauncherChildMessage[] = [];
  readonly #listeners = new Map<string, Set<(...args: ReadonlyArray<unknown>) => void>>();

  constructor(context: unknown) {
    this.env = { [SERVICE_LAUNCHER_CONTEXT_ENV]: JSON.stringify(context) };
  }

  send = (message: ServiceLauncherChildMessage, callback?: (error: Error | null) => void) => {
    this.sent.push(message);
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

it.effect("keeps a qualified staged handle fixed through launcher acceptance", () =>
  Effect.gen(function* () {
    const host = new FakeLauncherProcess({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      childVersion: "1.0.0",
      qualifiedUpdatesProtocol: 1,
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
      update: outcome,
    });
    const client = yield* makeClient(host, "1.0.0");
    expect(client.qualifiedStartupOutcome).toBeUndefined();
    expect(client.qualifiedStartupOutcome).toBeUndefined();
    expect(host.sent).toEqual([]);
  }),
);

it.effect("updates readonly Jones outcome after one committed trial exchange", () =>
  Effect.gen(function* () {
    const fromVersion = "0.0.0-preview.20261002.100";
    const targetVersion = "0.0.0-preview.20261002.101.1";
    const qualified = {
      protocol: 1,
      stagedHandle: "11111111-1111-4111-8111-111111111111",
      binding: {
        baseDir: "/fixture",
        dbPath: "/fixture/userdata/state.sqlite",
        environmentId: "native-fixture",
        activeVersion: fromVersion,
        activeSourceSha: "c".repeat(40),
      },
      receipt: {
        protocol: 1,
        repository: "Jones-Systems/Jones-Code",
        channel: "jones-main",
        version: targetVersion,
        sourceSha: "a".repeat(40),
        sourceTree: "b".repeat(40),
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
      id: "qualified-trial",
      fromVersion,
      targetVersion,
      dbPath: qualified.binding.dbPath,
      status: "pending",
      phase: "trial-ready",
      qualified,
    };
    const host = new FakeLauncherProcess({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      childVersion: targetVersion,
      qualifiedUpdatesProtocol: 1,
      update: pending,
    });
    const client = yield* makeClient(host, targetVersion);
    expect(client.qualifiedStartupOutcome).toBeUndefined();
    const prepared = yield* Effect.forkChild(client.prepareTrial, { startImmediately: true });
    yield* Effect.yieldNow;
    expect(host.sent).toEqual([{ type: "prepared", updateId: pending.id }]);
    host.emit({ type: "committed", updateId: pending.id });
    const committed = yield* Fiber.join(prepared);
    expect(client.qualifiedStartupOutcome).toEqual(committed);
    expect(yield* client.prepareTrial).toEqual(committed);
    expect(host.sent).toEqual([{ type: "prepared", updateId: pending.id }]);
  }),
);
