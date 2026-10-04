import { describe, expect, vi } from "vite-plus/test";
import { it } from "@effect/vitest";
import {
  AuthSessionId,
  EnvironmentId,
  VoiceReviewForbiddenError,
  type EnvironmentSessionPrincipalShape,
  EventId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2AppThread,
  type ThreadRegistryThread,
  type T3PlacementResult,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Data from "effect/Data";
import * as DateTime from "effect/DateTime";
import { ServerEnvironmentIdentity } from "../environment/ServerEnvironment.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import { WorkstreamGateway } from "../workstreams/WorkstreamGateway.ts";
import { makeVoiceReviewComposition, makeVoiceReviewCompositionFactory } from "./composition.ts";

class NativeReadFailure extends Data.TaggedError("NativeReadFailure")<{
  readonly cause: unknown;
}> {}

const principal: EnvironmentSessionPrincipalShape = {
  sessionId: AuthSessionId.make("reader"),
  subject: "reader",
  method: "bearer-access-token",
  scopes: new Set(["orchestration:read"]),
};
const binding = {
  registry_host: "native-host",
  registry_environment: "registered-environment",
  native_environment_id: "native-environment",
};
const reviewConfig = {
  broker_url: "http://127.0.0.1:7000",
  reviewer_token_file: "/not-read",
  source_id: "source",
  allowed_session_ids: new Set(["reader"]),
};
const registryThread: ThreadRegistryThread = {
  thread_key: '["native-host","registered-environment","thread"]',
  registration: null,
  summary: null,
  activity: null,
  freshness: {},
  associations: [],
};
const result: T3PlacementResult = {
  page: {
    inventory_sha256: "a".repeat(64),
    context: {
      owner_id: "owner",
      principal_id: "principal",
      authorization_revision: 1,
      server_generation: 1,
      registry_version: 1,
    },
    items: [],
    next_cursor: null,
  },
  trustedEnvironments: [],
  readiness: "ready",
};
const unavailableGatewayMethod = () => Effect.die("Unexpected gateway method");
const gatewayWithRead = (
  read: WorkstreamGateway["Service"]["readThreadPlacements"],
): WorkstreamGateway["Service"] => ({
  readThreadPlacements: read,
  readRegistryCounts: unavailableGatewayMethod,
  readSession: unavailableGatewayMethod,
  readMetadata: unavailableGatewayMethod,
  readDetail: unavailableGatewayMethod,
  readReferences: unavailableGatewayMethod,
  readReference: unavailableGatewayMethod,
  readMemberships: unavailableGatewayMethod,
  readDeclarations: unavailableGatewayMethod,
  readEdges: unavailableGatewayMethod,
  readHistory: unavailableGatewayMethod,
  pollCommand: unavailableGatewayMethod,
  submit: unavailableGatewayMethod,
  purgeAuthorization: () => {},
});

const seam = Effect.fn("voiceReview.test.seam")(function* () {
  const projection = yield* ProjectionStore.ProjectionStoreV2;
  const now = DateTime.makeUnsafe("2026-10-02T12:00:00Z");
  const threadId = ThreadId.make("thread");
  const thread: OrchestrationV2AppThread = {
    id: threadId,
    projectId: ProjectId.make("project"),
    title: "Native voice thread",
    providerInstanceId: ProviderInstanceId.make("codex"),
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
    createdBy: "user",
    creationSource: "web",
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    unsettledAt: now,
    lastVisitedAt: null,
    deletedAt: null,
  };
  yield* projection.apply({
    id: EventId.make("voice-thread-created"),
    type: "thread.created",
    threadId,
    occurredAt: now,
    payload: thread,
  });
  let environmentId = "native-environment";
  const read = vi.fn(() => Effect.succeed(result));
  const projectionRead = vi.fn(projection.getShellSnapshot);
  const run = (
    input: {
      binding: typeof binding | null;
      reviewConfig: typeof reviewConfig | null;
      principal: EnvironmentSessionPrincipalShape;
    } = { binding, reviewConfig, principal },
  ) =>
    makeVoiceReviewComposition(input).pipe(
      Effect.provideService(ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.suspend(() => Effect.succeed(EnvironmentId.make(environmentId))),
      }),
      Effect.provideService(ProjectionStore.ProjectionStoreV2, {
        ...projection,
        getShellSnapshot: projectionRead,
      }),
      Effect.provideService(WorkstreamGateway, gatewayWithRead(read)),
    );
  return {
    run,
    read,
    projectionRead,
    removeThread: () =>
      projection.apply({
        id: EventId.make("voice-thread-deleted"),
        type: "thread.deleted",
        threadId,
        occurredAt: now,
        payload: { ...thread, deletedAt: now },
      }),
    archiveThread: () =>
      projection.apply({
        id: EventId.make("voice-thread-archived"),
        type: "thread.archived",
        threadId,
        occurredAt: now,
        payload: { ...thread, archivedAt: now },
      }),
    changeEnvironment: () => {
      environmentId = "replaced";
    },
  };
}, Effect.provide(ProjectionStore.layerMemory));
describe("voice native service composition", () => {
  it.effect(
    "captures production services in a factory without leaking their Effect requirements",
    () =>
      Effect.gen(function* () {
        const read = vi.fn(() => Effect.succeed(result));
        const factory = yield* makeVoiceReviewCompositionFactory().pipe(
          Effect.provideService(ServerEnvironmentIdentity, {
            getEnvironmentId: Effect.succeed(EnvironmentId.make("native-environment")),
          }),
          Effect.provide(ProjectionStore.layerMemory),
          Effect.provideService(WorkstreamGateway, gatewayWithRead(read)),
        );
        expect(yield* factory({ binding: null, reviewConfig, principal })).toBeUndefined();
        const port = yield* factory({ binding, reviewConfig, principal });
        expect(port).toBeDefined();
        expect(yield* Effect.promise(() => port!.read(principal, []))).toEqual(result);
        expect(read).toHaveBeenCalledExactlyOnceWith({ identities: [] });
      }),
  );
  it.effect("keeps absent binding unavailable without native reads", () =>
    Effect.gen(function* () {
      const fixture = yield* seam();
      expect(yield* fixture.run({ binding: null, reviewConfig, principal })).toBeUndefined();
      expect(fixture.projectionRead).not.toHaveBeenCalled();
      expect(fixture.read).not.toHaveBeenCalled();
    }),
  );
  it.effect(
    "joins explicit deployment binding to fresh native ids and rechecks deletion before membership lookup",
    () =>
      Effect.gen(function* () {
        const fixture = yield* seam();
        const port = yield* fixture.run();
        expect(port).toBeDefined();
        const identities = Array.from(port!.identities([registryThread]).values());
        expect(identities).toEqual([
          { source_instance_id: "native-environment", native_thread_id: "thread" },
        ]);
        yield* fixture.removeThread();
        const error = yield* Effect.tryPromise({
          try: () => port!.read(principal, identities),
          catch: (cause) => new NativeReadFailure({ cause }),
        }).pipe(Effect.flip);
        expect(error._tag).toBe("NativeReadFailure");
        expect(error.cause).toBeInstanceOf(Error);
        expect(fixture.projectionRead).toHaveBeenCalledTimes(2);
        expect(fixture.read).not.toHaveBeenCalled();
      }),
  );
  it.effect("holds an environment identity mismatch rather than choosing a similar thread", () =>
    Effect.gen(function* () {
      const fixture = yield* seam();
      fixture.changeEnvironment();
      expect(yield* fixture.run()).toBeUndefined();
      expect(fixture.projectionRead).not.toHaveBeenCalled();
      expect(fixture.read).not.toHaveBeenCalled();
    }),
  );
  it.effect("denies non-enrolled sessions before native projection access", () =>
    Effect.gen(function* () {
      const fixture = yield* seam();
      const error = yield* fixture
        .run({
          binding,
          reviewConfig,
          principal: { ...principal, sessionId: AuthSessionId.make("other") },
        })
        .pipe(Effect.flip);
      expect(error).toBeInstanceOf(VoiceReviewForbiddenError);
      expect(fixture.projectionRead).not.toHaveBeenCalled();
      expect(fixture.read).not.toHaveBeenCalled();
    }),
  );
  it.effect("uses the qualified read gateway after a current projection check", () =>
    Effect.gen(function* () {
      const fixture = yield* seam();
      const port = yield* fixture.run();
      const identities = Array.from(port!.identities([registryThread]).values());
      expect(yield* Effect.promise(() => port!.read(principal, identities))).toEqual(result);
      expect(fixture.read).toHaveBeenCalledExactlyOnceWith({ identities });
    }),
  );
});

it.effect("excludes archived V2 threads from the native identity inventory", () =>
  Effect.gen(function* () {
    const fixture = yield* seam();
    yield* fixture.archiveThread();
    const port = yield* fixture.run();
    expect(port).toBeDefined();
    expect(Array.from(port!.identities([registryThread]).values())).toEqual([]);
    expect(fixture.projectionRead).toHaveBeenCalledExactlyOnceWith({ location: "active" });
    expect(fixture.read).not.toHaveBeenCalled();
  }),
);

it.effect("rechecks the environment binding before the placement read", () =>
  Effect.gen(function* () {
    const fixture = yield* seam();
    const port = yield* fixture.run();
    const identities = Array.from(port!.identities([registryThread]).values());
    fixture.changeEnvironment();
    const error = yield* Effect.tryPromise({
      try: () => port!.read(principal, identities),
      catch: (cause) => new NativeReadFailure({ cause }),
    }).pipe(Effect.flip);
    expect(error.cause).toMatchObject({ reason: "offline" });
    expect(fixture.projectionRead).toHaveBeenCalledTimes(1);
    expect(fixture.read).not.toHaveBeenCalled();
  }),
);

it.effect("denies a session without orchestration read scope before native access", () =>
  Effect.gen(function* () {
    const fixture = yield* seam();
    const error = yield* fixture
      .run({
        binding,
        reviewConfig,
        principal: { ...principal, scopes: new Set() },
      })
      .pipe(Effect.flip);
    expect(error).toBeInstanceOf(VoiceReviewForbiddenError);
    expect(fixture.projectionRead).not.toHaveBeenCalled();
    expect(fixture.read).not.toHaveBeenCalled();
  }),
);
