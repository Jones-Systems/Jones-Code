import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it as effectIt } from "@effect/vitest";
import {
  AuthSessionId,
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
  WorkstreamsNativeSettlementRequest,
  type OrchestrationV2AppThread,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentAuthInvalidError,
  EnvironmentHttpApi,
  WorkstreamsNativeAttestationRequest,
  WorkstreamsNativeContextResponse,
  WorkstreamsNativeAttestationResponse,
  WorkstreamsNativeSettlementResponse,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Etag from "effect/unstable/http/Etag";
import * as HttpPlatform from "effect/unstable/http/HttpPlatform";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { expect, it } from "vite-plus/test";

import { workstreamResponseHeadersLayer } from "../http.ts";
import {
  NativeProviderBuild,
  NativeProviderEnrollment,
  NativeProviderEnrollmentError,
  NATIVE_PROVIDER_SCOPES,
} from "../nativeProvider/enrollment.ts";
import {
  NativeProviderAttempts,
  makeNativeProviderAttempts,
} from "../nativeProvider/attemptRepository.ts";
import { sha256Bytes } from "../nativeProvider/service.ts";
import { makeNativeEnrollments } from "../enrollment/service.ts";
import { type NativeEnrollmentRequest } from "../enrollment/request.ts";
import { NativeStoreAuthority } from "../../environment/NativeStoreAuthority.ts";
import { NativeStoreAuthorityPersistenceError } from "../../environment/nativeStoreAuthorityPersistence.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import { layer as nativeRepositoryLayer } from "../../persistence/Layers/NativeCreationRepository.ts";
import * as AuthSessions from "../../persistence/AuthSessions.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import { NativeCreationAuthority } from "../../orchestration-v2/NativeCreationAuthority.ts";
import { ThreadManagementService } from "../../orchestration-v2/ThreadManagementService.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { makeLayer as makeProviderAdapterRegistryLayer } from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import type { ProviderAdapterV2Shape } from "../../orchestration-v2/ProviderAdapter.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import {
  binding,
  makeProviderFixture,
  requestText,
  attestationRequest,
  request as nativeRequest,
  enrolledContext,
  nativeTestPrincipal,
} from "../nativeProvider/testFixtures.ts";
import {
  NativeWorkstreamsRuntime,
  nativeWorkstreamsHttpApiLayer,
  makeNativeWorkstreamsRuntime,
} from "./native.ts";

class NativeTestApi extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.workstreamsNative,
) {}

const authLayer = Layer.succeed(EnvironmentAuthenticatedAuth, (handler) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const credential = request.headers.authorization;
    if (credential === undefined)
      return yield* new EnvironmentAuthInvalidError({
        code: "auth_invalid",
        reason: "missing_credential",
        traceId: "native-integration-test",
      });
    return yield* handler.pipe(
      Effect.provideService(EnvironmentAuthenticatedPrincipal, {
        sessionId: AuthSessionId.make(binding.session_id),
        subject:
          credential === "Bearer wrong-subject"
            ? "other-subject"
            : `workstreams-native:${binding.enrollment_id}`,
        method: credential === "Bearer cookie" ? "browser-session-cookie" : "bearer-access-token",
        scopes: new Set<AuthEnvironmentScope>(
          credential === "Bearer browser"
            ? ["orchestration:read"]
            : Object.values(NATIVE_PROVIDER_SCOPES),
        ),
      }),
    );
  }),
);

const makeApp = (enrolled = true) => {
  const fixture = makeProviderFixture();
  let lookups = 0;
  const app = HttpRouter.toWebHandler(
    HttpApiBuilder.layer(NativeTestApi).pipe(
      Layer.provide(nativeWorkstreamsHttpApiLayer),
      Layer.provide(
        Layer.succeed(NativeWorkstreamsRuntime, {
          provider: fixture.provider(),
          enrollments: {
            getBySessionId: () =>
              Effect.sync(() => {
                lookups += 1;
                return enrolled ? Option.some(binding) : Option.none();
              }),
          },
        }),
      ),
      Layer.provide(authLayer),
      Layer.provide(workstreamResponseHeadersLayer),
      Layer.provide(HttpPlatform.layer.pipe(Layer.provide(NodeServices.layer))),
      Layer.provide(Etag.layerWeak),
      Layer.provide(NodeServices.layer),
    ),
    { disableLogger: true },
  );
  return { ...app, fixture, lookups: () => lookups };
};

const request = (operation: string, credential = "Bearer enrolled", body?: string) =>
  new Request(`http://local/api/workstreams/native/v1/${operation}`, {
    method: operation === "context" || operation.startsWith("context?") ? "GET" : "POST",
    headers: { authorization: credential, "content-type": "application/json" },
    ...(body === undefined ? {} : { body }),
  });

it("authenticates and checks dedicated scope before consulting enrollment", async () => {
  const app = makeApp();
  try {
    expect(
      (await app.handler(new Request("http://local/api/workstreams/native/v1/context"))).status,
    ).toBe(401);
    expect((await app.handler(request("context", "Bearer browser"))).status).toBe(403);
    expect(app.lookups()).toBe(0);
    for (const credential of ["Bearer wrong-subject", "Bearer cookie"]) {
      const response = await app.handler(request("context", credential));
      expect(response.status).toBe(403);
      expect(
        Schema.decodeUnknownSync(WorkstreamsNativeContextResponse)(await response.json()),
      ).toEqual({
        protocol: "workstreams-t3-provider/1.0.0",
        state: "rejected",
        reason: "forbidden",
      });
    }
    expect(app.fixture.calls).toHaveLength(0);
  } finally {
    await app.dispose();
  }
});

it("fails closed when an authenticated dedicated session has no enrollment", async () => {
  const app = makeApp(false);
  try {
    for (const operation of ["context", "attestations", "settlements", "settlements/lookup"]) {
      const response = await app.handler(request(operation));
      expect(response.status).toBe(403);
    }
    expect(app.fixture.calls).toHaveLength(0);
    expect(app.fixture.attempts.size).toBe(0);
  } finally {
    await app.dispose();
  }
});

it("mounts all four closed native handlers and preserves the raw settlement digest", async () => {
  const app = makeApp();
  try {
    const context = await app.handler(request("context"));
    expect(context.status).toBe(200);
    expect(context.headers.get("cache-control")).toBe("no-store");
    expect(
      Schema.decodeUnknownSync(WorkstreamsNativeContextResponse)(await context.json()).state,
    ).toBe("ready");
    const text = Schema.encodeSync(Schema.fromJsonString(WorkstreamsNativeAttestationRequest))(
      attestationRequest,
    );
    const attestation = await app.handler(request("attestations", "Bearer enrolled", text));
    expect(
      Schema.decodeUnknownSync(WorkstreamsNativeAttestationResponse)(await attestation.json())
        .state,
    ).toBe("attested");
    const settled = await app.handler(request("settlements", "Bearer enrolled", requestText));
    expect(settled.status).toBe(200);
    const observation = Schema.decodeUnknownSync(WorkstreamsNativeSettlementResponse)(
      await settled.json(),
    );
    expect(observation.state).toBe("unknown");
    if (observation.state === "unknown") expect(observation.reason).toBe("authority_unavailable");
    const lookup = await app.handler(request("settlements/lookup", "Bearer enrolled", requestText));
    expect(
      Schema.decodeUnknownSync(WorkstreamsNativeSettlementResponse)(await lookup.json()),
    ).toEqual(observation);
    const changed = await app.handler(request("settlements", "Bearer enrolled", ` ${requestText}`));
    expect(changed.status).toBe(409);
    expect(
      Schema.decodeUnknownSync(WorkstreamsNativeSettlementResponse)(await changed.json()),
    ).toEqual({
      protocol: "workstreams-t3-provider/1.0.0",
      state: "rejected",
      reason: "idempotency_conflict",
    });
    expect(app.fixture.calls).toHaveLength(1);
  } finally {
    await app.dispose();
  }
});

it("rejects query selectors and excessive raw bodies without reserving a command", async () => {
  const app = makeApp();
  try {
    expect((await app.handler(request("context?source_instance_id=other"))).status).toBe(400);
    expect(
      (await app.handler(request("settlements?owner_id=other", "Bearer enrolled", requestText)))
        .status,
    ).toBe(400);
    expect(
      (await app.handler(request("settlements", "Bearer enrolled", "x".repeat(32_769)))).status,
    ).toBe(400);
    expect(app.fixture.calls).toHaveLength(0);
    expect(app.fixture.attempts.size).toBe(0);
  } finally {
    await app.dispose();
  }
});

const qualifiedDatabase = SqlitePersistenceMemory;
const qualifiedInstanceId = ProviderInstanceId.make("synthetic-workstreams-codex");
const qualifiedDriver = ProviderDriverKind.make("codex");
const qualifiedAdapter = {
  instanceId: qualifiedInstanceId,
  driver: qualifiedDriver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("Native settlement must not open a provider process"),
} as ProviderAdapterV2Shape;
const qualifiedNativeRepository = nativeRepositoryLayer.pipe(Layer.provide(qualifiedDatabase));
const noCreationAuthority = Layer.mock(NativeCreationAuthority)({
  authorize: () => Effect.die("Workstream settlement must not authorize native creation"),
  isAutomationEnrolled: () =>
    Effect.die("Workstream settlement must not inspect creation enrollment"),
  issueExecution: () => Effect.die("The disabled worker must not issue native execution"),
  authorizeExecution: () => Effect.die("Workstream settlement must not execute native creation"),
});
const qualifiedSqlLayer = Layer.mergeAll(
  qualifiedDatabase,
  AuthSessions.layer.pipe(Layer.provide(qualifiedDatabase)),
  ProjectionStore.layer.pipe(Layer.provide(qualifiedDatabase)),
  makeOrchestratorV2ReplayLayerWithRegistry(
    { name: "native-workstream-witness" },
    makeProviderAdapterRegistryLayer([qualifiedAdapter]),
    { databaseLayer: qualifiedDatabase, runEffectWorker: false },
  ).pipe(
    Layer.provide(
      Layer.mergeAll(qualifiedDatabase, qualifiedNativeRepository, noCreationAuthority),
    ),
  ),
);

const qualifiedFixture = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const sink = yield* EventSink.EventSinkV2;
  const projection = yield* ProjectionStore.ProjectionStoreV2;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const attempts = yield* makeNativeProviderAttempts;
  const sessions = yield* AuthSessions.AuthSessionRepository;
  const now = yield* DateTime.now;
  const threadId = ThreadId.make(nativeRequest.identity.native_id);
  const tuple = {
    environmentId: binding.source_instance_id,
    authorityNamespace: binding.authority_namespace,
    storeGeneration: binding.store_generation,
  };
  let available = true;
  const authority = NativeStoreAuthority.of({
    readCurrent: Effect.suspend(() =>
      available
        ? Effect.succeed(tuple)
        : Effect.fail(
            new NativeStoreAuthorityPersistenceError(
              "source_unavailable",
              "Synthetic selected source is unavailable.",
            ),
          ),
    ),
    trustProvider: {
      readTrustSnapshot: () => ({
        trustedEnvironments: available ? [tuple] : [],
        readiness: available ? "ready" : "trust-provider-required",
      }),
    },
  });
  const build = NativeProviderBuild.of({ readCurrent: Effect.succeed(Option.some(binding.build)) });
  const enrollments = yield* makeNativeEnrollments({ authority, build: build.readCurrent });
  const enrollmentRequest: NativeEnrollmentRequest = {
    schema: "jones-code.workstreams-native-enrollment/v1",
    enrollment_id: binding.enrollment_id,
    registry_origin: binding.registry_origin,
    context: enrolledContext,
    session: {
      session_id: nativeTestPrincipal.sessionId,
      issued_at: DateTime.formatIso(now),
      expires_at: DateTime.formatIso(DateTime.add(now, { days: 1 })),
    },
  };
  yield* enrollments.reserve(enrollmentRequest);
  const lookup = NativeProviderEnrollment.of({
    getBySessionId: (id) =>
      enrollments
        .getBySessionId(id)
        .pipe(Effect.mapError(() => new NativeProviderEnrollmentError())),
  });
  const enrolled = Option.getOrThrow(yield* lookup.getBySessionId(binding.session_id));
  assert.deepEqual(enrolled, binding);
  const thread: OrchestrationV2AppThread = {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId: ProjectId.make("project-synthetic-workstream"),
    title: "Qualified synthetic workstream",
    providerInstanceId: qualifiedInstanceId,
    modelSelection: { instanceId: qualifiedInstanceId, model: "synthetic-model" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: "synthetic-branch",
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
    pinnedAt: now,
    pinOrderKey: "synthetic-order",
    snoozedAt: now,
    snoozedUntil: DateTime.add(now, { days: 1 }),
  };
  yield* sink.write({
    events: [
      {
        id: EventId.make("event-synthetic-qualified-birth"),
        threadId,
        providerInstanceId: qualifiedInstanceId,
        type: "thread.created",
        occurredAt: now,
        payload: thread,
      },
      {
        id: EventId.make("event-synthetic-qualified-history"),
        threadId,
        providerInstanceId: qualifiedInstanceId,
        type: "message.updated",
        occurredAt: now,
        payload: {
          id: MessageId.make("message-synthetic-qualified-history"),
          threadId,
          runId: null,
          nodeId: null,
          createdBy: "user",
          creationSource: "web",
          role: "user",
          text: "Synthetic retained history",
          attachments: [],
          streaming: false,
          createdAt: now,
          updatedAt: now,
        },
      },
    ],
  });
  let calls = 0;
  let loseReply = false;
  let failBeforeDispatch = false;
  const management = Layer.mock(ThreadManagementService)({
    dispatchNativeWorkstreamSettlement: (input) =>
      Effect.gen(function* () {
        calls += 1;
        if (failBeforeDispatch)
          return yield* new Orchestrator.OrchestratorDispatchError({
            commandId: CommandId.make(input.attempt.nativeCommandId),
            commandType: "thread.settle",
            cause: "Synthetic response loss before dispatch",
          });
        const result = yield* orchestrator.dispatchNativeWorkstreamSettlement(input);
        if (loseReply)
          return yield* new Orchestrator.OrchestratorDispatchError({
            commandId: CommandId.make(input.attempt.nativeCommandId),
            commandType: "thread.settle",
            cause: "Synthetic lost reply after commit",
          });
        return result;
      }),
    observeNativeWorkstreamSettlementBinding: orchestrator.observeNativeWorkstreamSettlementBinding,
  });
  const runtime = yield* makeNativeWorkstreamsRuntime.pipe(
    Effect.provide(management),
    Effect.provideService(NativeStoreAuthority, authority),
    Effect.provideService(NativeProviderBuild, build),
    Effect.provideService(NativeProviderEnrollment, lookup),
    Effect.provideService(NativeProviderAttempts, attempts),
  );
  const facts = Effect.gen(function* () {
    const attempt = Option.getOrThrow(yield* attempts.get(nativeRequest));
    return yield* sink.readNativeCommandFacts({
      threadId,
      commandId: CommandId.make(attempt.nativeCommandId),
    });
  });
  return {
    sql,
    sink,
    projection,
    sessions,
    runtime,
    attempts,
    facts,
    threadId,
    thread,
    now,
    calls: () => calls,
    unavailable: () => {
      available = false;
    },
    loseReply: () => {
      loseReply = true;
    },
    holdDispatch: () => {
      failBeforeDispatch = true;
    },
  };
});

const qualifiedTest = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    Effect.Services<typeof qualifiedFixture> | EnvironmentAuthenticatedPrincipal
  >,
) =>
  effect.pipe(
    Effect.provideService(EnvironmentAuthenticatedPrincipal, nativeTestPrincipal),
    Effect.provide(Layer.fresh(qualifiedSqlLayer)),
  );

const terminalCommitted = (result: typeof WorkstreamsNativeSettlementResponse.Type) => {
  assert.strictEqual(result.state, "terminal");
  if (result.state !== "terminal")
    throw new Error(`Expected actual qualified settlement, got ${result.state}`);
  assert.strictEqual(result.result.native_outcome, "committed");
  return result;
};

// Only the explicit server services are synthetic; command, witness, receipt and events use real V2 SQL.
effectIt.effect(
  "qualified native runtime commits one atomic witness and observes its original identity after provider advancement",
  () =>
    qualifiedTest(
      Effect.gen(function* () {
        const f = yield* qualifiedFixture;
        const before = yield* f.projection.getThreadProjection(f.threadId);
        const first = terminalCommitted(
          yield* f.runtime.provider.settle(binding, nativeRequest, sha256Bytes(requestText)),
        );
        const facts = yield* f.facts;
        assert.isNotNull(facts.workstreamWitness);
        assert.isNotNull(facts.identity);
        assert.strictEqual(facts.receipt?.status, "accepted");
        assert.strictEqual(facts.eventMetadata.length, 1);
        assert.strictEqual(facts.eventMetadata[0]!.type, "thread.settled");
        assert.strictEqual(facts.eventMetadata[0]!.commandId, facts.receipt!.commandId);
        assert.strictEqual(facts.eventMetadata[0]!.sequence, facts.receipt!.resultSequence);
        assert.strictEqual(facts.workstreamWitness!.requestBytesSha256, sha256Bytes(requestText));
        assert.strictEqual(facts.workstreamWitness!.actorSessionId, nativeTestPrincipal.sessionId);
        assert.strictEqual(facts.workstreamWitness!.provider, null);
        const settled = yield* f.projection.getThreadProjection(f.threadId);
        assert.strictEqual(settled.thread.settledOverride, "settled");
        assert.isNull(settled.thread.pinnedAt);
        assert.isNull(settled.thread.pinOrderKey);
        assert.isNull(settled.thread.snoozedAt);
        assert.isNull(settled.thread.snoozedUntil);
        assert.deepEqual(settled.messages, before.messages);
        const providerThreadId = ProviderThreadId.make("provider-thread-synthetic-advanced");
        const providerSessionId = ProviderSessionId.make("provider-session-synthetic-advanced");
        yield* f.sink.write({
          events: [
            {
              id: EventId.make("event-synthetic-advanced-thread"),
              threadId: f.threadId,
              type: "thread.metadata-updated",
              occurredAt: f.now,
              payload: { ...settled.thread, activeProviderThreadId: providerThreadId },
            },
            {
              id: EventId.make("event-synthetic-advanced-session"),
              threadId: f.threadId,
              type: "provider-session.attached",
              driver: qualifiedDriver,
              providerInstanceId: qualifiedInstanceId,
              occurredAt: f.now,
              payload: {
                id: providerSessionId,
                driver: qualifiedDriver,
                providerInstanceId: qualifiedInstanceId,
                status: "ready",
                cwd: "/synthetic",
                model: "synthetic-model",
                capabilities: CodexProviderCapabilitiesV2,
                createdAt: f.now,
                updatedAt: f.now,
                lastError: null,
              },
            },
            {
              id: EventId.make("event-synthetic-advanced-provider-thread"),
              threadId: f.threadId,
              type: "provider-thread.updated",
              driver: qualifiedDriver,
              providerInstanceId: qualifiedInstanceId,
              occurredAt: f.now,
              payload: {
                id: providerThreadId,
                driver: qualifiedDriver,
                providerInstanceId: qualifiedInstanceId,
                providerSessionId,
                appThreadId: f.threadId,
                ownerNodeId: null,
                nativeThreadRef: {
                  driver: qualifiedDriver,
                  nativeId: "native-advanced",
                  strength: "strong",
                },
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: 1,
                lastRunOrdinal: 1,
                handoffIds: [],
                forkedFrom: null,
                createdAt: f.now,
                updatedAt: f.now,
              },
            },
          ],
        });
        const advancedBinding = {
          threadId: f.threadId,
          providerThreadId,
          providerSessionId,
          instanceId: qualifiedInstanceId,
          driver: qualifiedDriver,
          nativeThreadId: "native-advanced",
          runtimeGeneration: null,
        };
        assert.isTrue(
          (yield* f.sink.registerProviderRuntime({
            expectedBinding: advancedBinding,
            expectedEvidenceRevision: 0,
            actualBinding: { ...advancedBinding, runtimeGeneration: "generation-advanced" },
          })).committed,
        );
        assert.isNotNull(yield* f.sink.readCurrentProviderRuntimeOwner(f.threadId));
        assert.deepEqual(
          yield* f.runtime.provider.lookup(binding, nativeRequest, sha256Bytes(requestText)),
          first,
        );
        assert.deepEqual(
          yield* f.runtime.provider.settle(binding, nativeRequest, sha256Bytes(requestText)),
          first,
        );
        assert.deepEqual((yield* f.facts).workstreamWitness, facts.workstreamWitness);
        assert.deepEqual((yield* f.facts).identity, facts.identity);
        assert.strictEqual(f.calls(), 1);
        assert.strictEqual(
          (yield* f.sql`SELECT * FROM orchestration_v2_workstream_settlement_witnesses`).length,
          1,
        );
      }),
    ),
);

effectIt.effect("actual qualified receipt survives a lost dispatch reply without redispatch", () =>
  qualifiedTest(
    Effect.gen(function* () {
      const f = yield* qualifiedFixture;
      f.loseReply();
      const first = terminalCommitted(
        yield* f.runtime.provider.settle(binding, nativeRequest, sha256Bytes(requestText)),
      );
      assert.deepEqual(
        yield* f.runtime.provider.lookup(binding, nativeRequest, sha256Bytes(requestText)),
        first,
      );
      assert.deepEqual(
        yield* f.runtime.provider.settle(binding, nativeRequest, sha256Bytes(requestText)),
        first,
      );
      assert.strictEqual(f.calls(), 1);
      assert.isNotNull((yield* f.facts).workstreamWitness);
    }),
  ),
);

effectIt.effect(
  "witness insert failure rolls back the actual receipt, identity, events and projection",
  () =>
    qualifiedTest(
      Effect.gen(function* () {
        const f = yield* qualifiedFixture;
        const before = yield* f.projection.getThreadProjection(f.threadId);
        yield* f.sql`CREATE TRIGGER synthetic_reject_workstream_witness BEFORE INSERT ON orchestration_v2_workstream_settlement_witnesses BEGIN SELECT RAISE(ABORT, 'synthetic witness failure'); END`;
        const first = yield* f.runtime.provider.settle(
          binding,
          nativeRequest,
          sha256Bytes(requestText),
        );
        assert.strictEqual(first.state, "unknown");
        const facts = yield* f.facts;
        assert.isNull(facts.workstreamWitness);
        assert.isNull(facts.identity);
        assert.isNull(facts.receipt);
        assert.deepEqual(facts.eventMetadata, []);
        assert.deepEqual(yield* f.projection.getThreadProjection(f.threadId), before);
        assert.strictEqual(
          (yield* f.sql`SELECT * FROM orchestration_v2_workstream_settlement_witnesses`).length,
          0,
        );
        yield* f.runtime.provider.lookup(binding, nativeRequest, sha256Bytes(requestText));
        yield* f.runtime.provider.settle(binding, nativeRequest, sha256Bytes(requestText));
        assert.strictEqual(f.calls(), 1);
      }),
    ),
);

effectIt.effect(
  "qualified replay rejects changed raw bytes and current revoked enrollment without changing the original witness",
  () =>
    qualifiedTest(
      Effect.gen(function* () {
        const f = yield* qualifiedFixture;
        terminalCommitted(
          yield* f.runtime.provider.settle(binding, nativeRequest, sha256Bytes(requestText)),
        );
        const witness = (yield* f.facts).workstreamWitness;
        const conflict = yield* f.runtime.provider.lookup(
          binding,
          nativeRequest,
          sha256Bytes(` ${requestText}`),
        );
        assert.deepEqual(conflict, {
          protocol: binding.protocol,
          state: "rejected",
          reason: "idempotency_conflict",
        });
        yield* f.sessions.revoke({ sessionId: nativeTestPrincipal.sessionId, revokedAt: f.now });
        assert.strictEqual(
          (yield* f.runtime.provider.lookup(binding, nativeRequest, sha256Bytes(requestText)))
            .state,
          "unknown",
        );
        assert.deepEqual((yield* f.facts).workstreamWitness, witness);
        assert.strictEqual(f.calls(), 1);
      }),
    ),
);

effectIt.effect("a supplied build never qualifies an unavailable selected store", () =>
  qualifiedTest(
    Effect.gen(function* () {
      const f = yield* qualifiedFixture;
      const before = yield* f.projection.getThreadProjection(f.threadId);
      f.unavailable();
      const result = yield* f.runtime.provider.settle(
        binding,
        nativeRequest,
        sha256Bytes(requestText),
      );
      assert.deepEqual(result, {
        protocol: binding.protocol,
        state: "unknown",
        request: nativeRequest,
        reason: "authority_unavailable",
      });
      assert.strictEqual(f.calls(), 0);
      assert.isTrue(Option.isNone(yield* f.attempts.get(nativeRequest)));
      assert.strictEqual(
        (yield* f.sql`SELECT * FROM orchestration_v2_workstream_settlement_witnesses`).length,
        0,
      );
      assert.deepEqual(yield* f.projection.getThreadProjection(f.threadId), before);
    }),
  ),
);

effectIt.effect(
  "qualified unsettlement uses the actual native command and retains pins, snooze and history",
  () =>
    qualifiedTest(
      Effect.gen(function* () {
        const f = yield* qualifiedFixture;
        const before = yield* f.projection.getThreadProjection(f.threadId);
        const input = { ...nativeRequest, native_action: "unsettle" as const };
        const bytes = yield* Schema.encodeEffect(
          Schema.fromJsonString(WorkstreamsNativeSettlementRequest),
        )(input).pipe(Effect.orDie);
        const result = terminalCommitted(
          yield* f.runtime.provider.settle(binding, input, sha256Bytes(bytes)),
        );
        assert.strictEqual(result.settlement_event?.type, "thread.unsettled");
        const facts = yield* f.facts;
        assert.strictEqual(facts.identity?.commandType, "thread.unsettle");
        assert.strictEqual(facts.workstreamWitness?.command.type, "thread.unsettle");
        const after = yield* f.projection.getThreadProjection(f.threadId);
        assert.strictEqual(after.thread.settledOverride, "active");
        assert.deepEqual(after.thread.pinnedAt, before.thread.pinnedAt);
        assert.strictEqual(after.thread.pinOrderKey, before.thread.pinOrderKey);
        assert.deepEqual(after.thread.snoozedUntil, before.thread.snoozedUntil);
        assert.deepEqual(after.messages, before.messages);
        assert.deepEqual(
          yield* f.runtime.provider.lookup(binding, input, sha256Bytes(bytes)),
          result,
        );
        assert.strictEqual(f.calls(), 1);
      }),
    ),
);

effectIt.effect(
  "orphaned SQL receipt, identity and events without an original witness cannot produce terminal observation",
  () =>
    qualifiedTest(
      Effect.gen(function* () {
        const f = yield* qualifiedFixture;
        f.holdDispatch();
        assert.strictEqual(
          (yield* f.runtime.provider.settle(binding, nativeRequest, sha256Bytes(requestText)))
            .state,
          "unknown",
        );
        const attempt = Option.getOrThrow(yield* f.attempts.get(nativeRequest));
        assert.isNotNull(attempt.dispatchStartedAt);
        const commandId = CommandId.make(attempt.nativeCommandId);
        // Seed partial SQL evidence initially; opaque digests and receipt rows never establish qualified acceptance.
        yield* f.sink.withTransaction(
          Effect.gen(function* () {
            const stored = yield* f.sink.write({
              commandId,
              events: [
                {
                  id: EventId.make("event-synthetic-orphaned-settlement"),
                  threadId: f.threadId,
                  type: "thread.settled",
                  providerInstanceId: qualifiedInstanceId,
                  occurredAt: f.now,
                  payload: {
                    ...f.thread,
                    settledOverride: "settled",
                    settledAt: f.now,
                    pinnedAt: null,
                    pinOrderKey: null,
                    activeOrderKey: null,
                    snoozedAt: null,
                    snoozedUntil: null,
                  },
                },
              ],
            });
            yield* f.sql`INSERT INTO orchestration_command_receipts
        (command_id, aggregate_kind, aggregate_id, command_type, accepted_at, result_sequence, status, error)
        VALUES (${commandId}, 'thread', ${f.threadId}, 'thread.settle', ${DateTime.formatIso(f.now)}, ${stored.at(-1)!.sequence}, 'accepted', NULL)`;
            yield* f.sql`INSERT INTO orchestration_v2_native_command_identities
        (command_id, kind, version, command_type, aggregate_kind, aggregate_id, normalized_command_digest, binding_digest)
        VALUES (${commandId}, 'workstream_settlement', 2, 'thread.settle', 'thread', ${f.threadId}, ${"a".repeat(64)}, ${"b".repeat(64)})`;
          }),
        );
        const original = yield* f.facts;
        assert.isNotNull(original.receipt);
        assert.isNotNull(original.identity);
        assert.strictEqual(original.eventMetadata.length, 1);
        assert.isNull(original.workstreamWitness);
        assert.strictEqual(
          (yield* f.sql`SELECT * FROM orchestration_v2_workstream_settlement_witnesses WHERE command_id = ${commandId}`)
            .length,
          0,
        );
        const projectionBeforeObservation = yield* f.projection.getThreadProjection(f.threadId);
        const observed = yield* f.runtime.provider.lookup(
          binding,
          nativeRequest,
          sha256Bytes(requestText),
        );
        assert.deepEqual(observed, {
          protocol: binding.protocol,
          state: "unknown",
          request: nativeRequest,
          reason: "authority_unavailable",
        });
        assert.deepEqual(
          yield* f.runtime.provider.settle(binding, nativeRequest, sha256Bytes(requestText)),
          observed,
        );
        const current = yield* f.facts;
        assert.isNull(current.workstreamWitness);
        assert.deepEqual(current.receipt, original.receipt);
        assert.deepEqual(current.identity, original.identity);
        assert.deepEqual(current.eventMetadata, original.eventMetadata);
        assert.deepEqual(
          yield* f.projection.getThreadProjection(f.threadId),
          projectionBeforeObservation,
        );
        assert.strictEqual(f.calls(), 1);
      }),
    ),
);
