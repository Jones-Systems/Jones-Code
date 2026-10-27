import { expect, it } from "@effect/vitest";
import {
  AuthSessionId,
  AuthOrchestrationReadScope,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentId,
  ProviderInstanceId,
  ThreadId,
  RunId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as DateTime from "effect/DateTime";
import * as TestClock from "effect/testing/TestClock";
import { HttpServer } from "effect/http";
import * as NetAddress from "effect/net/NetAddress";
import * as Sessions from "../../../../persistence/AuthSessions.ts";
import { runMigrations } from "../../../../persistence/Migrations.ts";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Credentials from "../../../../mcp/McpSessionRegistry.ts";
import { liveThreadShell } from "../../../../mcp/McpToolAccess.testkit.ts";
import * as Threads from "../../../../orchestration-v2/ThreadManagementService.ts";
import type {
  ProjectionRecords,
  ProjectionRecordField,
} from "../../../../orchestration-v2/ProjectionStore.ts";
import * as Providers from "../../../../orchestration-v2/ProviderSessionManager.ts";
import * as Test from "../store/sql.testkit.ts";
import * as Local from "./memberRecordedCaller.ts";

const environmentId = EnvironmentId.make("member:test");
const persistence = Sessions.layer.pipe(
  Layer.provideMerge(Test.testLayer()),
  Layer.provideMerge(NodeServices.layer),
);
const setup = Effect.gen(function* () {
  yield* TestClock.setTime(1000);
  yield* runMigrations({ toMigrationInclusive: 54 });
  const sessions = yield* Sessions.AuthSessionRepository;
  const principal = {
    sessionId: AuthSessionId.make("local-caller"),
    subject: "owner",
    method: "bearer-access-token" as const,
    scopes: new Set([AuthOrchestrationReadScope]),
  };
  yield* sessions.create({
    ...principal,
    scopes: [...principal.scopes],
    issuedAt: DateTime.makeUnsafe(0),
    expiresAt: DateTime.makeUnsafe(100000),
    client: {
      label: null,
      ipAddress: null,
      userAgent: null,
      deviceType: "bot",
      os: null,
      browser: null,
    },
  });
  return { sessions, principal };
});
it.effect.each(["revoked", "expired", "subject", "scopes"] as const)(
  "rechecks real local auth session %s",
  (change) =>
    Effect.gen(function* () {
      const f = yield* setup;
      yield* Effect.gen(function* () {
        const callers = yield* Local.MemberLocalCallers;
        const record = yield* callers.capture(environmentId);
        expect(record.auth.kind).toBe("bearer");
        if (change === "revoked")
          yield* f.sessions.revoke({
            sessionId: f.principal.sessionId,
            revokedAt: DateTime.makeUnsafe(1000),
          });
        else if (change === "expired") yield* TestClock.setTime(100000);
        else if (change === "subject") {
          const altered = { ...f.principal, subject: "other" };
          expect(
            yield* callers
              .readmit(record)
              .pipe(
                Effect.provideService(EnvironmentAuthenticatedPrincipal, altered),
                Effect.result,
              ),
          ).toMatchObject({ _tag: "Failure" });
          return;
        } else {
          expect(
            yield* callers.readmit(record).pipe(
              Effect.provideService(EnvironmentAuthenticatedPrincipal, {
                ...f.principal,
                scopes: new Set<typeof AuthOrchestrationReadScope>(),
              }),
              Effect.result,
            ),
          ).toMatchObject({ _tag: "Failure" });
          return;
        }
        expect(yield* callers.readmit(record).pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
        });
      }).pipe(
        Effect.provide(Local.layer),
        Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal),
      );
    }).pipe(Effect.provide(persistence)),
);

const agent = Effect.gen(function* () {
  yield* setup;
  const registry = yield* Credentials.__testing
    .make({ now: () => 1000, livenessWindowMs: 100 })
    .pipe(
      Effect.provideService(HttpServer.HttpServer, {
        address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 43123),
        serve: (() => Effect.void) as HttpServer.HttpServer["Service"]["serve"],
      }),
      Effect.provideService(Environment.ServerEnvironment, {
        getEnvironmentId: Effect.succeed(environmentId),
        getDescriptor: Effect.die("unused"),
      }),
    );
  const threadId = ThreadId.make("original-thread");
  const issued = yield* registry.issue({
    threadId,
    providerInstanceId: ProviderInstanceId.make("codex"),
  });
  const scope = yield* registry.lookupByCredentialId(issued.config.providerSessionId);
  if (!scope) return yield* Effect.die("missing issued credential");
  const state = { shell: liveThreadShell(threadId), attached: true };
  // Projection and provider attachment are synthetic external ports; credential issue/revoke is real.
  const threads = Layer.mock(Threads.ThreadManagementService)({
    getThreadShell: (id) => Effect.succeed(id === threadId ? state.shell : liveThreadShell(id)),
    getThreadRecords: <K extends ProjectionRecordField>() =>
      Effect.succeed({
        runs: ["run:mcp-test", "run:new"].map((id) => ({
          id,
          providerInstanceId: "codex",
          providerThreadId: "provider-thread",
        })),
        providerThreads: [{ id: "provider-thread", providerSessionId: "original-native-session" }],
      } as unknown as ProjectionRecords<K>),
  });
  return { registry, scope, state, threads };
});
it.effect.each(["new-run", "revoked", "detached", "limits", "different-caller"] as const)(
  "original exact run/credential survives no %s substitution",
  (change) =>
    Effect.gen(function* () {
      const f = yield* agent;
      const providers = Layer.mock(Providers.ProviderSessionManagerV2)({
        isMcpCallerAttached: () => Effect.sync(() => f.state.attached),
      });
      yield* Effect.gen(function* () {
        const callers = yield* Local.MemberLocalCallers;
        const record = yield* callers.capture(environmentId);
        expect(record.auth).toMatchObject({
          run_id: "run:mcp-test",
          provider_session_id: "original-native-session",
        });
        if (change === "new-run")
          f.state.shell = { ...f.state.shell, activeRunId: RunId.make("run:new") };
        if (change === "revoked") yield* f.registry.revokeThread(f.state.shell.id);
        if (change === "detached") f.state.attached = false;
        if (change === "different-caller") {
          expect(
            yield* callers.readmit(record).pipe(
              Effect.provideService(Mcp.McpInvocationContext, {
                ...f.scope,
                requestNamespace: "replacement",
              }),
              Effect.result,
            ),
          ).toMatchObject({ _tag: "Failure" });
          return;
        }
        if (change === "limits") {
          f.state.shell = {
            ...f.state.shell,
            runtimeMode: "approval-required",
            interactionMode: "plan",
          };
          expect((yield* callers.readmit(record)).ceiling.limits).toEqual({
            runtimeMode: "approval-required",
            interactionMode: "plan",
          });
          return;
        }
        expect(yield* callers.readmit(record).pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
        });
        expect(yield* callers.readmitRetained(record).pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
        });
      }).pipe(
        Effect.provide(Layer.mergeAll(Local.layer, f.threads, providers)),
        Effect.provideService(Credentials.McpSessionRegistry, f.registry),
        Effect.provideService(Mcp.McpInvocationContext, f.scope),
      );
    }).pipe(Effect.provide(persistence)),
);
it.effect("historical missing evidence and linked generic callers remain ineligible", () =>
  Effect.gen(function* () {
    const f = yield* setup;
    yield* Effect.gen(function* () {
      const callers = yield* Local.MemberLocalCallers;
      for (const input of [null, {}, { version: 1 }])
        expect(yield* callers.readmit(input).pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
        });
    }).pipe(
      Effect.provide(Local.layer),
      Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal),
    );
    const linked: Mcp.McpInvocationScope = {
      environmentId,
      capabilities: new Set(["orchestration"]),
      issuedAt: 1000,
      requestNamespace: "peer",
      thread: undefined,
      client: {
        sessionId: f.principal.sessionId,
        access: "full-access",
        label: "peer",
        linked: true,
      },
    };
    expect(
      yield* Effect.gen(function* () {
        return yield* (yield* Local.MemberLocalCallers).capture(environmentId);
      }).pipe(
        Effect.provide(Local.layer),
        Effect.provideService(Mcp.McpInvocationContext, linked),
        Effect.result,
      ),
    ).toMatchObject({ _tag: "Failure" });
  }).pipe(Effect.provide(persistence)),
);
it.effect(
  "linked own-thread local evidence fences generic metadata, other threads and revoked original links",
  () =>
    Effect.gen(function* () {
      const f = yield* agent;
      const sessions = yield* Sessions.AuthSessionRepository;
      const linkId = AuthSessionId.make("original-link");
      yield* sessions.create({
        sessionId: linkId,
        subject: "mcp-client",
        method: "bearer-access-token",
        scopes: [AuthOrchestrationReadScope],
        issuedAt: DateTime.makeUnsafe(0),
        expiresAt: DateTime.makeUnsafe(100000),
        client: {
          label: null,
          ipAddress: null,
          userAgent: null,
          deviceType: "bot",
          os: null,
          browser: null,
        },
      });
      f.state.shell = {
        ...f.state.shell,
        linkOrigin: { sessionId: linkId, label: "original", access: "read-only" },
      };
      yield* Effect.gen(function* () {
        const callers = yield* Local.MemberLocalCallers;
        expect(yield* callers.capture(environmentId).pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
        });
        const record = yield* callers.captureOwnPlacement(environmentId);
        expect(yield* callers.readmit(record).pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
        });
        const own = yield* callers.readmit(record, true);
        expect(own.ceiling.limits?.runtimeMode).toBe("approval-required");
        yield* own.checkTarget(f.state.shell.id);
        expect(yield* own.checkTarget("another-thread").pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
        });
        expect(yield* own.checkTarget(null).pipe(Effect.result)).toMatchObject({ _tag: "Failure" });
        yield* sessions.revoke({ sessionId: linkId, revokedAt: DateTime.makeUnsafe(1000) });
        expect(yield* own.checkTarget(f.state.shell.id).pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
        });
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            Local.layer,
            f.threads,
            Layer.mock(Providers.ProviderSessionManagerV2)({
              isMcpCallerAttached: () => Effect.succeed(true),
            }),
          ),
        ),
        Effect.provideService(Credentials.McpSessionRegistry, f.registry),
        Effect.provideService(Mcp.McpInvocationContext, f.scope),
      );
    }).pipe(Effect.provide(persistence)),
);
