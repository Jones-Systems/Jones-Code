import {
  DEFAULT_SERVER_SETTINGS,
  AuthDiagnosticsReadScope,
  AuthOrchestrationReadScope,
  type AuthSessionState,
  EnvironmentId,
  type ServerConfig,
  type TokenAccountingReadResult,
  WS_METHODS,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";

import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type PreparedConnection,
  type SupervisorConnectionState,
} from "../connection/model.ts";
import * as EnvironmentRegistry from "../connection/registry.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import { EnvironmentCacheStore } from "../platform/persistence.ts";
import type { WsRpcProtocolClient } from "../rpc/protocol.ts";
import type { RpcSession } from "../rpc/session.ts";
import { createServerEnvironmentAtoms } from "./server.ts";

vi.mock("./session.ts", () => ({
  createEnvironmentSessionAtoms: () => ({ sessionStateAtom: grantedSessions }),
}));
const grantedSessions = Atom.family((_id: EnvironmentId) =>
  Atom.make<AsyncResult.AsyncResult<AuthSessionState>>(AsyncResult.initial()),
);

const target = new PrimaryConnectionTarget({
  environmentId: EnvironmentId.make("accounting-environment"),
  label: "Accounting environment",
  httpBaseUrl: "https://accounting.example.test",
  wsBaseUrl: "wss://accounting.example.test",
});
const result: TokenAccountingReadResult = {
  state: "unavailable",
  status: "missing",
  reason: "configured_report_missing",
  configuredReportId: "a".repeat(64),
  readAt: "2026-10-02T12:00:00Z",
};

const makeHarness = Effect.fn("ServerTokenAccountingTest.makeHarness")(function* (
  supported = true,
  connected = true,
  diagnosticsGranted = true,
) {
  const config = {
    settings: DEFAULT_SERVER_SETTINGS,
    environment: {
      serverVersion: "0.0.1",
      capabilities: supported ? { savedTokenAccounting: true } : {},
    },
  } as ServerConfig;
  let reads = 0;
  const client = {
    [WS_METHODS.subscribeServerConfig]: () => Stream.make({ version: 1, type: "snapshot", config }),
    [WS_METHODS.serverReadTokenAccounting]: () =>
      Effect.sync(() => {
        reads += 1;
        return result;
      }),
  } as unknown as WsRpcProtocolClient;
  const session: RpcSession = {
    client,
    initialConfig: Effect.succeed(config),
    subscribeServerConfig: (input) => client.subscribeServerConfig(input),
    ready: Effect.void,
    probe: Effect.void,
    closed: Effect.never,
  };
  const sessionRef = yield* SubscriptionRef.make(
    connected ? Option.some(session) : Option.none<RpcSession>(),
  );
  const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
    target,
    state: yield* SubscriptionRef.make<SupervisorConnectionState>({
      ...AVAILABLE_CONNECTION_STATE,
      phase: connected ? "connected" : "available",
    }),
    session: sessionRef,
    prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
    connect: Effect.void,
    disconnect: Effect.void,
    retryNow: Effect.void,
  });
  const environments = EnvironmentRegistry.EnvironmentRegistry.of({
    run: (_environmentId, effect) =>
      Effect.provideService(effect, EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
    followStream: (_environmentId, stream) =>
      Stream.provideService(stream, EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
  } as EnvironmentRegistry.EnvironmentRegistry["Service"]);
  const cache = EnvironmentCacheStore.of({
    loadShell: () => Effect.succeedNone,
    saveShell: () => Effect.void,
    loadThread: () => Effect.succeedNone,
    saveThread: () => Effect.void,
    removeThread: () => Effect.void,
    loadServerConfig: () => Effect.succeedNone,
    saveServerConfig: () => Effect.void,
    loadVcsRefs: () => Effect.succeedNone,
    saveVcsRefs: () => Effect.void,
    removeVcsRefs: () => Effect.void,
    clearVcsRefs: () => Effect.void,
    clear: () => Effect.void,
  });
  const runtime = Atom.runtime(
    Layer.merge(
      Layer.succeed(EnvironmentRegistry.EnvironmentRegistry, environments),
      Layer.succeed(EnvironmentCacheStore, cache),
    ),
  );
  const atoms = createServerEnvironmentAtoms(runtime, {
    initialConfigValueAtom: () => Atom.make(config),
  });
  const registry = yield* Effect.acquireRelease(Effect.sync(AtomRegistry.make), (registry) =>
    Effect.sync(() => registry.dispose()),
  );
  const sessionState = grantedSessions(target.environmentId);
  yield* Effect.acquireRelease(
    Effect.sync(() => registry.mount(sessionState)),
    (unmount) => Effect.sync(unmount),
  );
  registry.set(
    sessionState,
    AsyncResult.success({
      authenticated: true,
      auth: {
        policy: "remote-reachable",
        bootstrapMethods: [],
        sessionMethods: [],
        sessionCookieName: "test",
      },
      scopes: diagnosticsGranted ? [AuthDiagnosticsReadScope] : [AuthOrchestrationReadScope],
      permissions: diagnosticsGranted ? [AuthDiagnosticsReadScope] : [AuthOrchestrationReadScope],
    }),
  );
  return { atoms, registry, sessionRef, session, config, reads: () => reads };
});

it.effect("reads saved accounting only after an explicit command and rereads explicitly", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      expect(harness.reads()).toBe(0);
      const first = yield* Effect.promise(() =>
        harness.atoms.readTokenAccounting.run(harness.registry, {
          environmentId: target.environmentId,
          input: {},
        }),
      );
      expect(first._tag).toBe("Success");
      if (first._tag === "Success") expect(first.value).toEqual(result);
      expect(harness.reads()).toBe(1);
      yield* SubscriptionRef.set(harness.sessionRef, Option.none());
      yield* SubscriptionRef.set(harness.sessionRef, Option.some(harness.session));
      expect(harness.reads()).toBe(1);
      yield* Effect.promise(() =>
        harness.atoms.readTokenAccounting.run(harness.registry, {
          environmentId: target.environmentId,
          input: {},
        }),
      );
      expect(harness.reads()).toBe(2);
    }),
  ),
);

it.effect("does not dispatch to a server that omits the optional reader capability", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const harness = yield* makeHarness(false);
      const read = yield* Effect.promise(() =>
        harness.atoms.readTokenAccounting.run(harness.registry, {
          environmentId: target.environmentId,
          input: {},
        }),
      );
      expect(read._tag).toBe("Failure");
      expect(harness.reads()).toBe(0);
    }),
  ),
);

it.effect("keeps saved accounting validation and dispatch on the same session", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      const validationStarted = yield* Deferred.make<void>();
      const finishValidation = yield* Deferred.make<void>();
      const validatedSession: RpcSession = {
        ...harness.session,
        initialConfig: Effect.gen(function* () {
          yield* Deferred.succeed(validationStarted, undefined);
          yield* Deferred.await(finishValidation);
          return harness.config;
        }),
      };
      let replacementReads = 0;
      const replacementConfig: ServerConfig = {
        ...harness.config,
        environment: {
          ...harness.config.environment,
          capabilities: {
            ...harness.config.environment.capabilities,
            savedTokenAccounting: false,
          },
        },
      };
      const replacementSession: RpcSession = {
        ...harness.session,
        initialConfig: Effect.succeed(replacementConfig),
        client: {
          [WS_METHODS.serverReadTokenAccounting]: () =>
            Effect.sync(() => {
              replacementReads += 1;
              return result;
            }),
        } as unknown as WsRpcProtocolClient,
      };
      yield* SubscriptionRef.set(harness.sessionRef, Option.some(validatedSession));
      const readFiber = yield* Effect.promise(() =>
        harness.atoms.readTokenAccounting.run(harness.registry, {
          environmentId: target.environmentId,
          input: {},
        }),
      ).pipe(Effect.forkChild);
      yield* Effect.raceFirst(
        Deferred.await(validationStarted),
        Fiber.join(readFiber).pipe(
          Effect.flatMap((read) =>
            Effect.die(
              new Error(
                read._tag === "Failure"
                  ? Cause.pretty(read.cause)
                  : "Saved accounting completed before capability validation.",
              ),
            ),
          ),
        ),
      );
      yield* SubscriptionRef.set(harness.sessionRef, Option.some(replacementSession));
      yield* Deferred.succeed(finishValidation, undefined);
      const read = yield* Fiber.join(readFiber);

      expect(replacementReads).toBe(0);
      expect(harness.reads()).toBe(1);
      expect(read._tag).toBe("Success");
      if (read._tag === "Success") expect(read.value).toEqual(result);
    }),
  ),
);

it.effect("keeps a disconnected reader failure local without dispatch", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const harness = yield* makeHarness(true, false);
      const read = yield* Effect.promise(() =>
        harness.atoms.readTokenAccounting.run(harness.registry, {
          environmentId: target.environmentId,
          input: {},
        }),
      );
      expect(read._tag).toBe("Failure");
      expect(harness.reads()).toBe(0);
    }),
  ),
);

it.effect("refuses saved accounting without diagnostics access before dispatch", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const harness = yield* makeHarness(true, true, false);
      const read = yield* Effect.promise(() =>
        harness.atoms.readTokenAccounting.run(harness.registry, {
          environmentId: target.environmentId,
          input: {},
        }),
      );
      expect(read._tag).toBe("Failure");
      if (read._tag === "Failure") {
        expect(Cause.pretty(read.cause)).toContain(`requires ${AuthDiagnosticsReadScope}`);
      }
      expect(harness.reads()).toBe(0);
    }),
  ),
);
