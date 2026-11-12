import { describe, expect, vi } from "vite-plus/test";
import { it } from "@effect/vitest";
import {
  AuthSessionId,
  EnvironmentId,
  ThreadId,
  type EnvironmentSessionPrincipalShape,
  type ThreadRegistryThread,
} from "@t3tools/contracts";
import { WorkstreamsPersonalError } from "@t3tools/contracts/jones/workstreamsPersonalRpc";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ServerEnvironmentIdentity } from "../environment/ServerEnvironment.ts";
import * as ThreadMetadataReads from "../jones/orchestration/threadMetadataReads.ts";
import * as PersonalWorkstreams from "../jones/workstreams/personal/service.ts";
import { voiceReviewConfigFromEnv } from "./config.ts";
import { makeVoiceReviewComposition, makeVoiceReviewCompositionFactory } from "./composition.ts";

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
const reviewConfig = voiceReviewConfigFromEnv({
  T3CODE_VOICE_REVIEW_BROKER_URL: "http://127.0.0.1:7000",
  T3CODE_VOICE_REVIEW_REVIEWER_TOKEN_FILE: "/not-read",
  T3CODE_VOICE_REVIEW_SOURCE_ID: "source",
  T3CODE_VOICE_REVIEW_ALLOWED_SESSION_IDS: '["reader"]',
})!;
const registryThread: ThreadRegistryThread = {
  thread_key: '["native-host","registered-environment","thread"]',
  registration: null,
  summary: null,
  activity: null,
  freshness: {},
  associations: [],
};
const workstream = {
  id: "legacy-group:kept",
  name: "Synthetic group",
  lifecycle: "active" as const,
  color: null,
  orderKey: "n",
  createdAt: "2026-01-01T00:00:00.000Z",
  updatedAt: "2026-01-01T00:00:00.000Z",
  deletedAt: null,
  rev: 1,
  origin: "env:native-environment",
};
const input = { binding, reviewConfig, principal };
const seam = (options?: { metadataFails?: boolean }) => {
  let environmentId = "native-environment";
  let exists = true;
  let assigned = true;
  const read = vi.fn((id: ThreadId) =>
    !exists
      ? Effect.fail(new WorkstreamsPersonalError({ code: "thread_not_found" }))
      : Effect.succeed({
          assignment: {
            threadId: id,
            workstreamId: assigned ? workstream.id : null,
            revision: 7,
            updatedAt: "2026-01-01T00:00:00.000Z",
          },
          workstream: assigned ? workstream : null,
        }),
  );
  const metadataRead = vi.fn(() =>
    options?.metadataFails
      ? Effect.fail(new ThreadMetadataReads.ThreadMetadataReadError({ cause: "synthetic" }))
      : Effect.succeed([ThreadId.make("thread")]),
  );
  const dependencies = Layer.mergeAll(
    Layer.succeed(ServerEnvironmentIdentity, {
      getEnvironmentId: Effect.suspend(() => Effect.succeed(EnvironmentId.make(environmentId))),
    }),
    Layer.mock(ThreadMetadataReads.ThreadMetadataReads)({ listIds: metadataRead }),
    Layer.mock(PersonalWorkstreams.PersonalWorkstreams)({ readThread: read }),
  );
  return {
    dependencies,
    run: (value = input) => makeVoiceReviewComposition(value).pipe(Effect.provide(dependencies)),
    read,
    metadataRead,
    removeThread: () => { exists = false; },
    unassign: () => { assigned = false; },
    changeEnvironment: () => { environmentId = "replacement"; },
  };
};

describe("voice personal service composition", () => {
  it.effect("reads local membership without a registry, placement gateway or projection service", () =>
    Effect.gen(function* () {
      const h = seam();
      const port = yield* h.run();
      expect(port).toBeDefined();
      const identities = [...port!.identities([registryThread]).values()];
      const result = yield* Effect.promise(() => port!.read(principal, identities));
      expect(result).toEqual([{
        workstream_ref: "native:legacy-group:kept",
        workstream_id: workstream.id,
        name: workstream.name,
        environment_id: binding.native_environment_id,
        thread_id: "thread",
        revision: 7,
      }]);
      expect(h.read).toHaveBeenCalledExactlyOnceWith(ThreadId.make("thread"));
      h.unassign();
      expect(yield* Effect.promise(() => port!.read(principal, identities))).toEqual([]);
    }),
  );
  it.effect("rechecks thread existence and environment ownership on each read", () =>
    Effect.gen(function* () {
      const h = seam();
      const port = yield* h.run();
      const identities = [...port!.identities([registryThread]).values()];
      h.removeThread();
      yield* Effect.promise(() => expect(port!.read(principal, identities)).rejects.toThrow());
      h.changeEnvironment();
      yield* Effect.promise(() => expect(port!.read(principal, identities)).rejects.toThrow());
      expect(h.read).toHaveBeenCalledTimes(1);
    }),
  );
  it.effect("requires read scope before accessing local thread metadata", () =>
    Effect.gen(function* () {
      const h = seam();
      const denied = yield* h.run({ ...input, principal: { ...principal, scopes: new Set() } }).pipe(Effect.flip);
      expect(denied).toMatchObject({ _tag: "VoiceReviewForbiddenError" });
      expect(h.metadataRead).not.toHaveBeenCalled();
      expect(h.read).not.toHaveBeenCalled();
    }),
  );
  it.effect("keeps missing or mismatched deployment binding unavailable", () =>
    Effect.gen(function* () {
      const h = seam();
      expect(yield* makeVoiceReviewComposition({ ...input, binding: null }).pipe(Effect.provide(h.dependencies))).toBeUndefined();
      h.changeEnvironment();
      expect(yield* h.run()).toBeUndefined();
      expect(h.metadataRead).not.toHaveBeenCalled();
      expect(h.read).not.toHaveBeenCalled();
    }),
  );
  it.effect("keeps failed metadata unavailable without an alternate thread source", () =>
    Effect.gen(function* () {
      const h = seam({ metadataFails: true });
      const result = yield* h.run();
      expect(result).toBeUndefined();
      expect(h.read).not.toHaveBeenCalled();
    }),
  );
  it.effect("captures the personal service in the production factory", () =>
    Effect.gen(function* () {
      const h = seam();
      const factory = yield* makeVoiceReviewCompositionFactory().pipe(Effect.provide(h.dependencies));
      const port = yield* factory(input);
      const identities = [...port!.identities([registryThread]).values()];
      expect(yield* Effect.promise(() => port!.read(principal, identities))).toHaveLength(1);
    }),
  );
});
