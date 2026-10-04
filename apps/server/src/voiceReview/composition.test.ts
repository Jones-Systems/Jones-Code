import { describe, expect, vi } from "vite-plus/test";
import { it } from "@effect/vitest";
import {
  AuthSessionId,
  EnvironmentId,
  VoiceReviewForbiddenError,
  type EnvironmentSessionPrincipalShape,
  type OrchestrationV2ThreadShellSnapshot,
  type ThreadRegistryThread,
  type T3PlacementResult,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Data from "effect/Data";
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
const seam = () => {
  let threadIds = ["thread"];
  let environmentId = "native-environment";
  const read = vi.fn(() => Effect.succeed(result));
  const projectionRead = vi.fn(() =>
    Effect.succeed({
      schemaVersion: 2,
      snapshotSequence: 1,
      archivedThreads: [],
      threads: threadIds.map((id) => ({ id })),
    } as unknown as OrchestrationV2ThreadShellSnapshot),
  );
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
        getShellSnapshot: projectionRead,
      } as unknown as ProjectionStore.ProjectionStoreV2["Service"]),
      Effect.provideService(WorkstreamGateway, {
        readThreadPlacements: read,
      } as unknown as WorkstreamGateway["Service"]),
    );
  return {
    run,
    read,
    projectionRead,
    removeThread: () => {
      threadIds = [];
    },
    changeEnvironment: () => {
      environmentId = "replaced";
    },
  };
};
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
          Effect.provideService(ProjectionStore.ProjectionStoreV2, {
            getShellSnapshot: () =>
              Effect.succeed({
                schemaVersion: 2,
                snapshotSequence: 1,
                archivedThreads: [],
                threads: [],
                        }),
          } as unknown as ProjectionStore.ProjectionStoreV2["Service"]),
          Effect.provideService(WorkstreamGateway, {
            readThreadPlacements: read,
          } as unknown as WorkstreamGateway["Service"]),
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
      const fixture = seam();
      expect(yield* fixture.run({ binding: null, reviewConfig, principal })).toBeUndefined();
      expect(fixture.projectionRead).not.toHaveBeenCalled();
      expect(fixture.read).not.toHaveBeenCalled();
    }),
  );
  it.effect(
    "joins explicit deployment binding to fresh native ids and rechecks deletion before membership lookup",
    () =>
      Effect.gen(function* () {
        const fixture = seam();
        const port = yield* fixture.run();
        expect(port).toBeDefined();
        const identities = Array.from(port!.identities([registryThread]).values());
        expect(identities).toEqual([
          { source_instance_id: "native-environment", native_thread_id: "thread" },
        ]);
        fixture.removeThread();
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
      const fixture = seam();
      fixture.changeEnvironment();
      expect(yield* fixture.run()).toBeUndefined();
      expect(fixture.projectionRead).not.toHaveBeenCalled();
      expect(fixture.read).not.toHaveBeenCalled();
    }),
  );
  it.effect("denies non-enrolled sessions before native projection access", () =>
    Effect.gen(function* () {
      const fixture = seam();
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
      const fixture = seam();
      const port = yield* fixture.run();
      const identities = Array.from(port!.identities([registryThread]).values());
      expect(yield* Effect.promise(() => port!.read(principal, identities))).toEqual(result);
      expect(fixture.read).toHaveBeenCalledExactlyOnceWith({ identities });
    }),
  );
});
