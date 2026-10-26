// @effect-diagnostics nodeBuiltinImport:off -- Synthetic in-memory encrypted stores and exact emitted byte assertions.
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Wire from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as Fixture from "../jones/workstreams/native/store/sql.testkit.ts";
import migration from "../jones/workstreams/native/../../../jones/persistence/Migrations/125_JonesWorkstreamsNative.ts";
import * as Admin from "../jones/workstreams/native/cli.ts";
import * as Registry from "../jones/workstreams/native/registry/service.ts";
import * as Session from "../jones/workstreams/native/registry/session.ts";
import * as Store from "../jones/workstreams/native/store/registryStore.ts";
import * as Projection from "../jones/workstreams/native/read/projection.ts";
import * as Appearance from "../jones/workstreams/native/registry/appearance.ts";
import * as Admission from "../jones/workstreams/native/runtime/admission.ts";
import { NativeHome, type AdmittedCaller } from "../jones/workstreams/native/mode/authorityMode.ts";
import * as ControlPlane from "../jones/workstreams/native/../../../workstreams/ControlPlaneWorkstreamTransport.ts";
const home = {
  ...Fixture.scope,
  canonical_origin: "https://11111111-1111-4111-8111-111111111111.workstreams.invalid",
  home_environment_id: "home:test",
  served_by_environment_id: "home:test",
  server_generation: 1,
};
const caller: AdmittedCaller = {
  principal_id: "principal:test",
  source_environment_id: "home:test",
  binding_version: 1,
  origin: { kind: "app" },
};
const base = (keys = Fixture.secrets()) => {
  const registry = Registry.layer.pipe(
    Layer.provideMerge(Session.layer),
    Layer.provideMerge(Fixture.testLayer({ keys })),
  );
  return Layer.mergeAll(Projection.layer, Appearance.layer, Admission.layer).pipe(
    Layer.provideMerge(registry),
    Layer.provideMerge(Layer.succeed(NativeHome, home)),
  );
};
const initialize = Effect.gen(function* () {
  yield* TestClock.setTime(Date.parse(Fixture.now));
  yield* migration;
  const admin = yield* Admin.NativeAdmin.pipe(Effect.provide(Admin.layer));
  const { served_by_environment_id: _served, ...identity } = home;
  yield* admin.initialize({
    registry: identity,
    principal_id: caller.principal_id,
    content_key_id: "test-home",
    local_key_id: "test-local",
  });
});
const commands = Effect.gen(function* () {
  const registry = yield* Registry.Registry;
  let sequence = 0;
  const submit = Effect.fn("ReadTest.submit")(function* (action: Wire.WorkstreamCommand["action"]) {
    const s = yield* registry.snapshot(caller);
    return yield* registry.submit(
      caller,
      JSON.stringify({
        command_id: "read-command-" + String(++sequence).padStart(20, "0"),
        expected_server_generation: s.context.server_generation,
        expected_registry_version: s.context.registry_version,
        action,
      }),
    );
  });
  const create = (name = "Synthetic private name") =>
    submit({
      operation: "create_workstream",
      name,
      lifecycle: "active",
      progress: { state: "unknown" },
      sort_order: 0,
    }).pipe(
      Effect.map((accepted) => {
        if (accepted.receipt.state !== "committed") throw new Error("create rejected");
        return accepted.receipt.effects.workstream_versions[0]!.workstream_id;
      }),
    );
  return { registry, submit, create };
});
const nativeReference = Effect.fn("ReadTest.nativeReference")(function* (
  h: Effect.Success<typeof commands>,
  workstream: string,
  nativeId = "thread:test",
  source = "home:test",
) {
  const store = yield* Store.RegistryStore;
  if (
    !(yield* store.listPlain(Fixture.scope, "source_bindings")).some(
      (b) => b.source_instance_id === source,
    )
  )
    yield* store.appendPlain(Fixture.scope, "source_bindings", {
      provider: "t3",
      source_instance_id: source,
      binding_version: 1,
      authority_namespace: "authority:test",
      store_generation: 1,
      state: "active",
    });
  const registered = yield* h.submit({
    operation: "register_reference",
    identity: {
      provider: "t3",
      source_instance_id: source,
      resource_kind: "thread",
      id_kind: "internal",
      native_id: nativeId,
      account_provenance: { kind: "not_account_scoped" },
    },
    pr_locator: null,
  });
  if (registered.receipt.state !== "committed") throw new Error("register rejected");
  const id = registered.receipt.effects.native_reference_id!;
  const verification = yield* h.submit({
    operation: "verify_reference",
    native_reference_id: id,
    expected_attestation_version: 0,
  });
  if (!verification.intent) throw new Error("missing external verification intent");
  const receipt = yield* h.registry.complete(verification.intent, {
    registration: {
      state: "attested",
      attestation_version: 1,
      attested_at: Fixture.now,
      expires_at: "2026-01-01T00:05:00.000Z",
      evidence: {
        provider: "t3",
        source_instance_id: source,
        authority_namespace: "authority:test",
        native_id: nativeId,
        store_generation: 1,
        evidence_sha256: Fixture.hash,
      },
    },
  });
  expect(receipt.state).toBe("committed");
  const snapshot = yield* h.registry.snapshot(caller);
  const attached = yield* h.submit({
    operation: "attach_primary",
    workstream_id: workstream,
    expected_version: snapshot.state.workstreams.get(workstream)!.version,
    native_reference_id: id,
  });
  expect(attached.receipt.state).toBe("committed");
  return id;
});

import * as Clock from "effect/Clock";
import * as Local from "../jones/workstreams/native/registry/localTransport.ts";
import * as Counts from "../jones/workstreams/native/registry/counts.ts";
import * as Placements from "../jones/workstreams/native/registry/placements.ts";
import * as Runner from "../jones/workstreams/native/registry/effectRunner.ts";
import * as Gateway from "../workstreams/WorkstreamGateway.ts";
import * as ThreadMetadataReads from "../jones/orchestration/threadMetadataReads.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import { ServerEnvironmentIdentity } from "../environment/ServerEnvironment.ts";
import { makeVoiceReviewComposition } from "./composition.ts";
import { voiceReviewConfigFromEnv } from "./config.ts";
const localLayer = Local.layer.pipe(
  Layer.provideMerge(Layer.mergeAll(Counts.layer, Placements.layer)),
  Layer.provideMerge(base()),
  Layer.provide(
    Layer.succeed(Runner.EffectRunner, {
      run: () => Effect.die("Unexpected external call"),
      recover: () => Effect.die("Unexpected recovery"),
      recoverOne: () => Effect.die("Unexpected lookup"),
    }),
  ),
  Layer.provide(
    Layer.succeed(Placements.PlacementTrust, {
      readTrustSnapshot: () => ({
        readiness: "ready",
        trustedEnvironments: [
          {
            environmentId: home.home_environment_id,
            authorityNamespace: "authority:test",
            storeGeneration: 1,
          },
        ],
      }),
    }),
  ),
);
const principal: Wire.EnvironmentSessionPrincipalShape = {
  sessionId: Wire.AuthSessionId.make("synthetic-voice"),
  subject: "synthetic-reader",
  method: "bearer-access-token",
  scopes: new Set([Wire.AuthOrchestrationReadScope]),
};
const binding = {
  registry_host: "synthetic-host",
  registry_environment: "synthetic-voice-environment",
  native_environment_id: home.home_environment_id,
};
const reviewConfig = voiceReviewConfigFromEnv({
  T3CODE_VOICE_REVIEW_BROKER_URL: "http://127.0.0.1:7000",
  T3CODE_VOICE_REVIEW_REVIEWER_TOKEN_FILE: "/synthetic-never-read",
  T3CODE_VOICE_REVIEW_SOURCE_ID: "synthetic-source",
  T3CODE_VOICE_REVIEW_ALLOWED_SESSION_IDS: '["synthetic-voice"]',
})!;
it.effect(
  "composes actual native home placements and fences revoked grants, missing threads and replacement environments",
  () =>
    Effect.gen(function* () {
      yield* initialize;
      const h = yield* commands,
        id = yield* h.create();
      yield* nativeReference(h, id);
      const cp = vi.spyOn(ControlPlane, "makeControlPlaneWorkstreamTransport");
      try {
        const adapter = yield* (yield* Local.LocalTransport).forCaller(caller);
        const clock = yield* Clock.Clock;
        let environment = home.home_environment_id,
          exists = true;
        const dependencies = Layer.mergeAll(
          Layer.succeed(Gateway.WorkstreamGateway, {
            ...adapter.gateway,
            readThreadPlacements: (request) =>
              adapter.gateway
                .readThreadPlacements(request)
                .pipe(Effect.provideService(Clock.Clock, clock)),
          }),
          Layer.succeed(ServerEnvironmentIdentity, {
            getEnvironmentId: Effect.suspend(() =>
              Effect.succeed(Wire.EnvironmentId.make(environment)),
            ),
          }),
          Layer.mock(ThreadMetadataReads.ThreadMetadataReads)({
            listIds: () => Effect.succeed([Wire.ThreadId.make("thread:test")]),
          }),
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getThreadShell: (threadId) =>
              Effect.succeed(
                exists
                  ? ({
                      id: threadId,
                      deletedAt: null,
                    } as Wire.OrchestrationV2ThreadShell)
                  : null,
              ),
          }),
        );
        const port = yield* makeVoiceReviewComposition({ binding, reviewConfig, principal }).pipe(
          Effect.provide(dependencies),
        );
        expect(port).toBeDefined();
        if (!port) throw new Error("native composition absent");
        const thread = {
          thread_key: JSON.stringify([
            binding.registry_host,
            binding.registry_environment,
            "thread:test",
          ]),
          registration: null,
          summary: null,
          activity: null,
          freshness: {},
          associations: [],
        };
        const identities = [...port.identities([thread]).values()];
        const result = yield* Effect.promise(() => port.read(principal, identities));
        expect(result.page.items).toMatchObject([
          {
            workstream_id: id,
            native_thread_id: "thread:test",
            source_instance_id: home.home_environment_id,
            kind: "primary",
          },
        ]);
        expect(result.readiness).toBe("ready");
        exists = false;
        yield* Effect.promise(() => expect(port.read(principal, identities)).rejects.toThrow());
        exists = true;
        environment = "replacement:environment";
        yield* Effect.promise(() => expect(port.read(principal, identities)).rejects.toThrow());
        environment = home.home_environment_id;
        const store = yield* Store.RegistryStore;
        yield* store.appendPlain(Fixture.scope, "grants", {
          principal_id: caller.principal_id,
          grant_id: "grant:principal:test",
          grant_version: 2,
          permissions: '["workstreams:read"]',
          state: "revoked",
        });
        yield* Effect.promise(() => expect(port.read(principal, identities)).rejects.toThrow());
        expect(cp).not.toHaveBeenCalled();
      } finally {
        cp.mockRestore();
      }
    }).pipe(Effect.provide(localLayer)),
);

it.effect("rejects a voice principal lacking read scope before native placement composition", () =>
  Effect.gen(function* () {
    const denied = yield* makeVoiceReviewComposition({
      binding,
      reviewConfig,
      principal: { ...principal, scopes: new Set() },
    }).pipe(
      Effect.flip,
      Effect.provideService(ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(Wire.EnvironmentId.make(home.home_environment_id)),
      }),
      Effect.provide(
        Layer.mergeAll(
          Layer.mock(ThreadMetadataReads.ThreadMetadataReads)({}),
          Layer.mock(ProjectionStore.ProjectionStoreV2)({}),
          Layer.mock(Gateway.WorkstreamGateway)({ purgeAuthorization: () => {} }),
        ),
      ),
    );
    expect(denied).toMatchObject({ _tag: "VoiceReviewForbiddenError" });
  }),
);
