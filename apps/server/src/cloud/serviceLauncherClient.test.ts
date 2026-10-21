import { expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Duration from "effect/Duration";
import * as Schema from "effect/Schema";
import { TestClock } from "effect/testing";
import * as NetAddress from "effect/net/NetAddress";
import {
  decodeServiceLauncherContext,
  decodeServiceLauncherChildMessage,
  decodeServiceLauncherParentMessage,
} from "./serviceProtocol.ts";
import type {
  QualifiedTrialReceipt,
  QualifiedTrialRuntimeWitness,
} from "../jones/cloud/qualifiedStartup.ts";
import { decodeQualifiedTrialGrant } from "../jones/cloud/qualifiedStartup.ts";

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
  readonly connected: boolean;
  readonly env: Record<string, string | undefined>;
  readonly sent: ServiceLauncherChildMessage[] = [];
  readonly sentSignal = Promise.withResolvers<void>();
  readonly #listeners = new Map<string, Set<(...args: ReadonlyArray<unknown>) => void>>();

  constructor(context: unknown, connected = true) {
    this.connected = connected;
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
    Effect.provideService(HostProcess.Environment, host.env),
  );

it.effect(
  "requires explicit caller-operation support and correlates acceptance to the exact ID",
  () =>
    Effect.gen(function* () {
      const context = {
        protocol: SERVICE_LAUNCHER_PROTOCOL,
        childVersion: "1.0.0",
        qualifiedUpdatesProtocol: 1,
        startupGateProtocol: 1,
      };
      const input = {
        targetVersion: "1.1.0",
        dbPath: "/synthetic/statev2.sqlite",
        stagedHandle: "22345678-1234-4234-8234-123456789abc",
        operationId: "12345678-1234-4234-8234-123456789abc",
      };
      const oldHost = new FakeLauncherProcess(context);
      const oldClient = yield* makeClient(oldHost, "1.0.0");
      expect((yield* oldClient.requestUpdate(input).pipe(Effect.flip))._tag).toBe(
        "ServiceLauncherRejectedError",
      );
      expect(oldHost.sent).toEqual([]);
      const host = new FakeLauncherProcess({ ...context, updateOperationsProtocol: 1 });
      const client = yield* makeClient(host, "1.0.0");
      const request = yield* Effect.forkChild(client.requestUpdate(input), {
        startImmediately: true,
      });
      yield* Effect.promise(() => host.sentSignal.promise);
      host.emit({ type: "update-accepted", updateId: "other-operation" });
      expect(host.listenerCount()).toBe(2);
      host.emit({ type: "update-accepted", updateId: input.operationId });
      expect(yield* Fiber.join(request)).toBe(input.operationId);
      expect(host.sent).toEqual([{ type: "request-update", ...input }]);
      expect(host.listenerCount()).toBe(0);
    }),
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

it.effect("ignores launcher context inherited without an IPC channel", () =>
  Effect.gen(function* () {
    const host = new FakeLauncherProcess(
      { protocol: SERVICE_LAUNCHER_PROTOCOL, childVersion: "1.0.0" },
      false,
    );
    const client = yield* makeClient(host, "1.1.0");
    const error = yield* client
      .requestUpdate({ targetVersion: "1.2.0", dbPath: "/tmp/state.sqlite" })
      .pipe(Effect.flip);
    expect(error).toMatchObject({ _tag: "ServiceLauncherClientError", operation: "unmanaged" });
    expect(host.sent).toEqual([]);
  }),
);
