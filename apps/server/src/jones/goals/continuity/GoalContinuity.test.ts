import { expect, it } from "@effect/vitest";
import * as C from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { makeProviderEventProducer } from "@t3tools/provider-core/server/jones/ProviderEventOrigin";
import type { RecoveryBoundary } from "@t3tools/provider-core/server/jones/GoalContinuityContext";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import { idleThreadProjection, liveThreadShell } from "../../../mcp/McpToolAccess.testkit.ts";
import * as Threads from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Sessions from "../../../orchestration-v2/ProviderSessionManager.ts";
import { RecordedAppThreadJson, RecordedRunJson } from "../../../orchestration-v2/RecordedTypes.ts";
import migration from "../../persistence/Migrations/134_JonesGoalContinuity.ts";
import * as Store from "./GoalContinuityStore.ts";
import * as Continuity from "./GoalContinuity.ts";
import { canonicalJson, continuityDigest, snapshotRef } from "./model.ts";

const owner: C.ContinuityOwner = {
  environmentId: C.EnvironmentId.make("synthetic-environment"),
  threadId: C.ThreadId.make("synthetic-thread"),
  providerInstanceId: C.ProviderInstanceId.make("codex"),
  providerThreadId: C.ProviderThreadId.make("synthetic-provider-thread"),
  driver: C.ProviderDriverKind.make("codex"),
  nativeThreadId: "synthetic-native-thread",
};
const witness: C.ContinuityWitness = {
  ...owner,
  providerSessionId: C.ProviderSessionId.make("synthetic-runtime-session"),
  runtimeGeneration: "synthetic-generation",
  runId: C.RunId.make("synthetic-run"),
  attemptId: C.RunAttemptId.make("synthetic-attempt"),
};
const now = DateTime.makeUnsafe("2026-01-01T00:00:00Z");
const shell = {
  ...liveThreadShell(owner.threadId, { activeRunId: witness.runId }),
  activeProviderThreadId: owner.providerThreadId,
};
const thread = idleThreadProjection(shell).thread;
const provider: C.OrchestrationV2ProviderThread = {
  id: owner.providerThreadId,
  driver: owner.driver,
  providerInstanceId: owner.providerInstanceId,
  providerSessionId: witness.providerSessionId,
  appThreadId: owner.threadId,
  ownerNodeId: null,
  nativeThreadRef: { driver: owner.driver, nativeId: owner.nativeThreadId, strength: "strong" },
  nativeConversationHeadRef: null,
  status: "active",
  firstRunOrdinal: 1,
  lastRunOrdinal: 1,
  handoffIds: [],
  forkedFrom: null,
  pendingBackgroundTasks: [],
  contextUsage: null,
  nativeMetadata: null,
  goal: null,
  runtimeIdentity: {
    runtimeGeneration: witness.runtimeGeneration,
    requested: {
      providerInstanceId: owner.providerInstanceId,
      providerDriver: owner.driver,
      model: "synthetic-model",
      serviceTier: null,
    },
    observed: {
      backend: { status: "unknown" },
      model: { status: "unknown" },
      account: { status: "unknown" },
      serviceTier: { status: "unknown" },
    },
  },
  createdAt: now,
  updatedAt: now,
};
const run: C.OrchestrationV2Run = {
  id: witness.runId,
  threadId: owner.threadId,
  ordinal: 1,
  providerInstanceId: owner.providerInstanceId,
  providerThreadId: owner.providerThreadId,
  modelSelection: { instanceId: owner.providerInstanceId, model: "synthetic-model" },
  userMessageId: C.MessageId.make("synthetic-message"),
  rootNodeId: C.NodeId.make("synthetic-node"),
  activeAttemptId: witness.attemptId,
  status: "running",
  requestedAt: now,
  startedAt: now,
  completedAt: null,
  checkpointId: null,
  contextHandoffId: null,
};
const attempt: C.OrchestrationV2RunAttempt = {
  id: witness.attemptId,
  runId: witness.runId,
  attemptOrdinal: 1,
  rootNodeId: C.NodeId.make("synthetic-node"),
  providerInstanceId: owner.providerInstanceId,
  providerThreadId: owner.providerThreadId,
  nativeThreadId: owner.nativeThreadId,
  providerTurnId: null,
  reason: "initial",
  status: "running",
  startedAt: now,
  completedAt: null,
};
const projection = {
  ...idleThreadProjection(shell),
  providerThreads: [provider],
  runs: [run],
  attempts: [attempt],
};
const intent: C.ContinuityIntent = {
  goalId: "synthetic-goal",
  revision: "1",
  northStar: "Keep the accepted intent",
  preservedQualities: [],
  finishItems: [{ id: "finish", text: "Prove completion" }],
  source: { type: "owner_request", identity: "synthetic-owner", reference: "synthetic-request" },
  derivation: null,
  initializationMode: "direct",
  predecessor: null,
  startEvidence: [
    { id: "start", reference: "synthetic-request", assertion: "Explicit initialization" },
  ],
};
const initialize: C.ContinuityRequest = {
  action: "initialize",
  clientRequestId: "initialize-1",
  expectedOwner: owner,
  expectedCurrent: null,
  intent,
  initialization: { type: "direct" },
};
const scope = (
  access: "full-access" | "read-only" = "full-access",
): Invocation.McpInvocationScope => ({
  environmentId: owner.environmentId,
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
  requestNamespace: "client:synthetic-owner",
  thread: undefined,
  client: { sessionId: "synthetic-owner-client", label: "Owner", access },
});
const agentScope = (credential: string): Invocation.McpInvocationScope => ({
  ...scope(),
  requestNamespace: `credential:${credential}`,
  client: undefined,
  thread: {
    threadId: owner.threadId,
    providerInstanceId: owner.providerInstanceId,
    providerSessionId: credential,
  },
});
const database = NodeSqliteClient.layer({ filename: ":memory:" });
// A real in-memory database with the exact indexed projection columns used here.
// No runtime, provider process, filesystem state or application startup is involved.
const setup = Layer.effectDiscard(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* migration;
    yield* sql`CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT PRIMARY KEY,payload_json TEXT)`;
    yield* sql`CREATE TABLE orchestration_v2_projection_provider_threads(provider_thread_id TEXT PRIMARY KEY,thread_id TEXT,payload_json TEXT)`;
    yield* sql`CREATE TABLE orchestration_v2_projection_runs(run_id TEXT PRIMARY KEY,thread_id TEXT,status TEXT,ordinal INTEGER,payload_json TEXT)`;
    yield* sql`CREATE INDEX synthetic_runs_thread_status ON orchestration_v2_projection_runs(thread_id,status)`;
    yield* sql`CREATE TABLE orchestration_v2_projection_run_attempts(attempt_id TEXT PRIMARY KEY,thread_id TEXT,payload_json TEXT)`;
    yield* sql`CREATE TABLE orchestration_v2_projection_provider_sessions(provider_session_id TEXT PRIMARY KEY,provider_instance_id TEXT,driver TEXT,status TEXT)`;
    yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES (${owner.threadId},${yield* Schema.encodeEffect(Schema.fromJsonString(RecordedAppThreadJson))(thread)})`;
    yield* sql`INSERT INTO orchestration_v2_projection_provider_threads VALUES (${owner.providerThreadId},${owner.threadId},${yield* Schema.encodeEffect(Schema.fromJsonString(C.OrchestrationV2ProviderThreadJson))(provider)})`;
    yield* sql`INSERT INTO orchestration_v2_projection_runs VALUES (${run.id},${owner.threadId},'running',1,${yield* Schema.encodeEffect(Schema.fromJsonString(RecordedRunJson))(run)})`;
    yield* sql`INSERT INTO orchestration_v2_projection_run_attempts VALUES (${attempt.id},${owner.threadId},${yield* Schema.encodeEffect(Schema.fromJsonString(C.OrchestrationV2RunAttemptJson))(attempt)})`;
    yield* sql`INSERT INTO orchestration_v2_projection_provider_sessions VALUES (${witness.providerSessionId},${owner.providerInstanceId},${owner.driver},'running')`;
  }),
).pipe(Layer.provideMerge(database));
const storeLayer = Store.layer.pipe(Layer.provideMerge(setup));
const threadsLayer = Layer.mock(Threads.ThreadManagementService)({
  getThreadShell: (id) => Effect.succeed(id === owner.threadId ? shell : null),
  getThreadRecords: () => Effect.succeed(projection),
});
const layer = Continuity.layer.pipe(
  Layer.provideMerge(storeLayer),
  Layer.provideMerge(threadsLayer),
);
const expected = (snapshot: C.ContinuitySnapshot): C.ExpectedContinuity => ({
  ...snapshotRef(snapshot),
  owner: snapshot.owner,
});
const target = (snapshot: C.ContinuitySnapshot): C.ContinuityTarget => ({
  goalId: snapshot.intent.goalId,
  revision: snapshot.intent.revision,
  generation: snapshot.generation,
  owner: snapshot.owner,
});
function applied(result: C.ContinuityResult): C.ContinuitySnapshot {
  if (result.outcome !== "receipt" || result.receipt.result.outcome !== "applied")
    throw new Error("Expected applied receipt");
  return result.receipt.result.snapshot;
}
const start = Effect.gen(function* () {
  const service = yield* Continuity.GoalContinuity;
  const caller = yield* service.authorize(owner.threadId);
  const snapshot = applied(yield* service.execute(caller, initialize));
  return { service, caller, snapshot };
});
const boundary = (): RecoveryBoundary => ({
  witness,
  producer: {
    producerId: "synthetic-producer",
    driver: owner.driver,
    providerInstanceId: owner.providerInstanceId,
    providerSessionId: witness.providerSessionId,
    runtimeGeneration: witness.runtimeGeneration,
  },
  producerOrigin: makeProviderEventProducer({
    driver: owner.driver,
    instanceId: owner.providerInstanceId,
    providerSessionId: witness.providerSessionId,
    runtimeGeneration: witness.runtimeGeneration,
  }).origin,
  point: { type: "compaction", nativeTurnId: "synthetic-turn", itemId: "synthetic-compaction" },
});
const dispatch = (service: Continuity.GoalContinuity["Service"], captured: RecoveryBoundary) =>
  Effect.gen(function* () {
    yield* service.recordRecoveryTransition(captured, { type: "started", eventId: "start-event" });
    const completed = yield* service.recordRecoveryTransition(captured, {
      type: "completed",
      eventId: "complete-event",
    });
    if (completed.outcome !== "recorded" || completed.pendingTicket === null)
      throw new Error("Expected pending ticket");
    const projection = yield* service.prepareRecovery(captured);
    if (projection.outcome !== "context") throw new Error("Expected fresh context");
    const prepared = yield* service.beginDelivery({
      pendingTicket: completed.pendingTicket,
      projection,
    });
    if (prepared.outcome !== "prepared") throw new Error("Expected preparation");
    const sent = yield* service.markDispatching(prepared.ticket);
    if (sent.outcome !== "dispatching") throw new Error("Expected dispatch permit");
    return { permit: sent.permit, ticket: prepared.ticket, projection };
  });

it.effect(
  "owner administration is provider-offline and durable replay returns its historical result after reconstruction",
  () =>
    Effect.gen(function* () {
      const { service, caller, snapshot } = yield* start;
      const checkpoint: C.ContinuityRequest = {
        action: "checkpoint",
        clientRequestId: "checkpoint-1",
        expected: target(snapshot),
        patch: { focus: "new focus" },
      };
      const first = yield* service.execute(caller, checkpoint);
      expect(applied(first).generation).toBe("1");
      const rebuilt = yield* Continuity.GoalContinuity.pipe(
        Effect.provide(Layer.fresh(Continuity.layer)),
      );
      const nextCaller = yield* rebuilt.authorize(owner.threadId);
      const replay = yield* rebuilt.execute(nextCaller, initialize);
      expect(replay.outcome).toBe("receipt");
      if (replay.outcome !== "receipt") return;
      expect(replay.replayed).toBe(true);
      expect(applied(replay)).toEqual(snapshot);
      const conflict = yield* rebuilt.execute(nextCaller, {
        ...initialize,
        intent: { ...intent, northStar: "changed body" },
      });
      expect(conflict).toEqual({ outcome: "refused", reason: "request_key_conflict" });
      const readback = yield* rebuilt.execute(nextCaller, {
        action: "receipt",
        clientRequestId: checkpoint.clientRequestId,
        requestDigest: continuityDigest(checkpoint),
      });
      expect(readback.outcome === "receipt" && readback.receipt).toEqual(
        first.outcome === "receipt" && first.receipt,
      );
      const read = yield* rebuilt.execute(nextCaller, { action: "read" });
      expect(read.outcome === "snapshot" && read.snapshot.generation).toBe("1");
    }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope()), Effect.provide(layer)),
);

it.effect(
  "a lost committed reply survives reconstruction and replacement of the agent credential namespace",
  () =>
    Effect.gen(function* () {
      const service = yield* Continuity.GoalContinuity;
      let attachedCredential = "original-credential";
      const manager = yield* Sessions.ProviderSessionManagerV2.pipe(
        Effect.provide(
          Layer.mock(Sessions.ProviderSessionManagerV2)({
            isMcpCallerAttached: ({ mcpCredentialId }: { mcpCredentialId: string }) =>
              Effect.sync(() => mcpCredentialId === attachedCredential),
          }),
        ),
      );
      const originalScope = agentScope(attachedCredential);
      const originalCaller = yield* service
        .authorize()
        .pipe(
          Effect.provideService(Invocation.McpInvocationContext, originalScope),
          Effect.provideService(Sessions.ProviderSessionManagerV2, manager),
        );
      // Commit succeeds, but the caller receives none of the reply or snapshot.
      yield* service.execute(originalCaller, initialize).pipe(Effect.asVoid);
      attachedCredential = "replacement-credential";
      const rebuilt = yield* Continuity.GoalContinuity.pipe(
        Effect.provide(Layer.fresh(Continuity.layer)),
      );
      const replacementCaller = yield* rebuilt
        .authorize()
        .pipe(
          Effect.provideService(Invocation.McpInvocationContext, agentScope(attachedCredential)),
          Effect.provideService(Sessions.ProviderSessionManagerV2, manager),
        );
      expect(replacementCaller.actorNamespace).not.toBe(originalCaller.actorNamespace);
      expect(replacementCaller.actorId).not.toBe(originalCaller.actorId);
      const receiptRequest: C.ContinuityRequest = {
        action: "receipt",
        clientRequestId: initialize.clientRequestId,
        requestDigest: continuityDigest(initialize),
      };
      expect(
        (yield* service.execute(originalCaller, receiptRequest).pipe(Effect.flip)).reason,
      ).toBe("forbidden");
      const found = yield* rebuilt.execute(replacementCaller, receiptRequest);
      if (found.outcome !== "receipt") throw new Error("Expected original committed receipt");
      expect(found.replayed).toBe(true);
      expect(found.receipt.actorNamespace).toBe(originalCaller.actorNamespace);
      expect(found.receipt.actorId).toBe(originalCaller.actorId);
      expect(found.receipt.witness).toEqual(witness);
      const originalSnapshot = applied(found);
      yield* rebuilt.execute(replacementCaller, {
        action: "checkpoint",
        clientRequestId: "after-replacement",
        expected: target(originalSnapshot),
        patch: { focus: "After replacement" },
      });
      expect(yield* rebuilt.execute(replacementCaller, initialize)).toEqual(found);
      expect(
        yield* rebuilt.execute(replacementCaller, {
          ...initialize,
          intent: { ...intent, northStar: "changed body" },
        }),
      ).toEqual({ outcome: "refused", reason: "request_key_conflict" });
      expect(
        yield* rebuilt.execute(replacementCaller, {
          ...receiptRequest,
          requestDigest: continuityDigest("wrong digest"),
        }),
      ).toEqual({ outcome: "refused", reason: "request_key_conflict" });
      const read = yield* rebuilt.execute(replacementCaller, { action: "read" });
      expect(read.outcome === "snapshot" && read.snapshot.generation).toBe("1");
      const sql = yield* SqlClient.SqlClient;
      expect(
        yield* sql`SELECT receipt_key FROM jones_goal_continuity_receipts WHERE kind='mutation' AND receipt_key=${initialize.clientRequestId}`,
      ).toHaveLength(1);
    }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope()), Effect.provide(layer)),
);

it.effect(
  "an offline owner reconciles an agent mutation without taking its provenance or bypassing current permissions",
  () =>
    Effect.gen(function* () {
      const service = yield* Continuity.GoalContinuity;
      let attached = true;
      const manager = yield* Sessions.ProviderSessionManagerV2.pipe(
        Effect.provide(
          Layer.mock(Sessions.ProviderSessionManagerV2)({
            isMcpCallerAttached: () => Effect.sync(() => attached),
          }),
        ),
      );
      const agent = yield* service
        .authorize()
        .pipe(
          Effect.provideService(Invocation.McpInvocationContext, agentScope("agent-credential")),
          Effect.provideService(Sessions.ProviderSessionManagerV2, manager),
        );
      const initial = applied(yield* service.execute(agent, initialize));
      const checkpoint: C.ContinuityRequest = {
        action: "checkpoint",
        clientRequestId: "lost-agent-checkpoint",
        expected: target(initial),
        patch: { focus: "Committed before lost reply" },
      };
      yield* service.execute(agent, checkpoint).pipe(Effect.asVoid);
      attached = false;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE orchestration_v2_projection_provider_threads SET payload_json=json_set(payload_json,'$.providerSessionId',NULL) WHERE provider_thread_id=${owner.providerThreadId}`;
      yield* sql`UPDATE orchestration_v2_projection_runs SET status='completed',payload_json=json_set(payload_json,'$.status','completed') WHERE run_id=${witness.runId}`;
      const rebuilt = yield* Continuity.GoalContinuity.pipe(
        Effect.provide(Layer.fresh(Continuity.layer)),
      );
      const ownerScope: Invocation.McpInvocationScope = {
        ...scope(),
        requestNamespace: "client:replacement-owner",
        client: { sessionId: "replacement-owner-session", label: "Owner", access: "full-access" },
      };
      const ownerCaller = yield* rebuilt
        .authorize(owner.threadId)
        .pipe(Effect.provideService(Invocation.McpInvocationContext, ownerScope));
      const receiptRequest: C.ContinuityRequest = {
        action: "receipt",
        clientRequestId: checkpoint.clientRequestId,
        requestDigest: continuityDigest(checkpoint),
      };
      const recovered = yield* rebuilt.execute(ownerCaller, receiptRequest);
      if (recovered.outcome !== "receipt") throw new Error("Expected committed agent receipt");
      expect(recovered.receipt.actorNamespace).toBe(agent.actorNamespace);
      expect(recovered.receipt.actorId).toBe(agent.actorId);
      expect(recovered.receipt.witness).toEqual(witness);
      expect(recovered.receipt.targetOwner).toEqual(owner);
      expect(applied(recovered).focus).toBe("Committed before lost reply");
      expect(yield* rebuilt.execute(ownerCaller, checkpoint)).toEqual(recovered);
      expect(
        yield* rebuilt.execute(ownerCaller, {
          ...checkpoint,
          patch: { focus: "Different request body" },
        }),
      ).toEqual({ outcome: "refused", reason: "request_key_conflict" });
      const reader = yield* rebuilt.authorize(owner.threadId).pipe(
        Effect.provideService(Invocation.McpInvocationContext, {
          ...ownerScope,
          client: { ...ownerScope.client!, access: "read-only" },
        }),
      );
      expect(yield* rebuilt.execute(reader, receiptRequest)).toEqual(recovered);
      expect((yield* rebuilt.execute(reader, checkpoint).pipe(Effect.flip)).reason).toBe(
        "forbidden",
      );
      expect((yield* service.execute(agent, receiptRequest).pipe(Effect.flip)).reason).toBe(
        "forbidden",
      );
      for (const deniedScope of [
        { ...ownerScope, capabilities: new Set<Invocation.McpCapability>() },
        { ...ownerScope, client: undefined },
        { ...agentScope("revoked-credential"), client: ownerScope.client },
      ]) {
        expect(
          (yield* rebuilt
            .authorize(owner.threadId)
            .pipe(
              Effect.provideService(Invocation.McpInvocationContext, deniedScope),
              Effect.provideService(Sessions.ProviderSessionManagerV2, manager),
              Effect.flip,
            )).reason,
        ).toBe("forbidden");
      }
      const repaired = applied(
        yield* rebuilt.execute(ownerCaller, {
          action: "checkpoint",
          clientRequestId: "owner-next-checkpoint",
          expected: target(applied(recovered)),
          patch: { focus: "Owner verified receipt" },
        }),
      );
      expect(repaired.generation).toBe("2");
      expect(
        yield* sql`SELECT receipt_key FROM jones_goal_continuity_receipts WHERE kind='mutation' AND receipt_key=${checkpoint.clientRequestId}`,
      ).toHaveLength(1);
    }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope()), Effect.provide(layer)),
);

it.effect.each([false, true])(
  "concurrent agent and owner share one request key; changed body: %s",
  (changedBody) =>
    Effect.gen(function* () {
      const { service, caller: ownerCaller, snapshot } = yield* start;
      const manager = yield* Sessions.ProviderSessionManagerV2.pipe(
        Effect.provide(
          Layer.mock(Sessions.ProviderSessionManagerV2)({
            isMcpCallerAttached: () => Effect.succeed(true),
          }),
        ),
      );
      const agentCaller = yield* service
        .authorize()
        .pipe(
          Effect.provideService(Invocation.McpInvocationContext, agentScope("concurrent-agent")),
          Effect.provideService(Sessions.ProviderSessionManagerV2, manager),
        );
      const request: C.ContinuityRequest = {
        action: "checkpoint",
        clientRequestId: "shared-request-key",
        expected: target(snapshot),
        patch: { focus: "First body" },
      };
      const competing = changedBody ? { ...request, patch: { focus: "Competing body" } } : request;
      const results = yield* Effect.all(
        [service.execute(agentCaller, request), service.execute(ownerCaller, competing)],
        { concurrency: "unbounded" },
      );
      const committed = results.filter(
        (result) => result.outcome === "receipt" && !result.replayed,
      );
      expect(committed).toHaveLength(1);
      const winner = committed[0];
      if (winner?.outcome !== "receipt") throw new Error("Expected one committed receipt");
      expect(winner.receipt.result.outcome).toBe("applied");
      if (changedBody)
        expect(results.filter((result) => result.outcome === "refused")).toEqual([
          { outcome: "refused", reason: "request_key_conflict" },
        ]);
      else
        expect(results.filter((result) => result.outcome === "receipt" && result.replayed)).toEqual(
          [{ ...winner, replayed: true }],
        );
      const read = yield* service.execute(ownerCaller, { action: "read" });
      expect(read.outcome === "snapshot" && read.snapshot).toEqual(applied(winner));
      expect(applied(winner).generation).toBe("1");
      expect(
        yield* service.execute(ownerCaller, {
          action: "receipt",
          clientRequestId: request.clientRequestId,
          requestDigest: winner.receipt.requestDigest,
        }),
      ).toEqual({ ...winner, replayed: true });
      const sql = yield* SqlClient.SqlClient;
      expect(
        yield* sql`SELECT receipt_key FROM jones_goal_continuity_receipts WHERE kind='mutation' AND receipt_key=${request.clientRequestId}`,
      ).toHaveLength(1);
    }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope()), Effect.provide(layer)),
);

it.effect(
  "refused close keeps exact snapshot bytes/generation while persisting its refusal receipt",
  () =>
    Effect.gen(function* () {
      const { service, caller, snapshot } = yield* start;
      const result = yield* service.execute(caller, {
        action: "close",
        clientRequestId: "close-1",
        expected: target(snapshot),
        completionEvidenceIds: ["start"],
      });
      expect(result.outcome === "receipt" && result.receipt.result).toEqual({
        outcome: "refused",
        reason: "uncovered_finish",
      });
      const read = yield* service.execute(caller, { action: "read" });
      expect(read.outcome === "snapshot" && canonicalJson(read.snapshot)).toBe(
        canonicalJson(snapshot),
      );
    }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope()), Effect.provide(layer)),
);

it.effect("authenticated owner repairs an idle source with no provider session or live run", () =>
  Effect.gen(function* () {
    const { service, snapshot } = yield* start;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE orchestration_v2_projection_provider_threads SET payload_json=json_set(payload_json,'$.providerSessionId',NULL) WHERE provider_thread_id=${owner.providerThreadId}`;
    yield* sql`UPDATE orchestration_v2_projection_runs SET status='completed',payload_json=json_set(payload_json,'$.status','completed') WHERE run_id=${witness.runId}`;
    const caller = yield* service.authorize(owner.threadId);
    const repaired = yield* service.execute(caller, {
      action: "checkpoint",
      clientRequestId: "offline-repair",
      expected: target(snapshot),
      patch: { focus: "Owner repaired offline" },
    });
    expect(applied(repaired).generation).toBe("1");
    expect(repaired.outcome === "receipt" && repaired.receipt.witness).toBe(null);
  }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope()), Effect.provide(layer)),
);

it.effect("an ended or stale agent never falls back to authenticated owner-client authority", () =>
  Effect.gen(function* () {
    const service = yield* Continuity.GoalContinuity;
    const agent = {
      ...scope(),
      thread: {
        threadId: owner.threadId,
        providerInstanceId: owner.providerInstanceId,
        providerSessionId: "synthetic-old-attachment",
      },
    };
    const error = yield* service
      .authorize(owner.threadId)
      .pipe(Effect.provideService(Invocation.McpInvocationContext, agent), Effect.flip);
    expect(error.reason).toBe("forbidden");
    const forged = yield* service
      .execute({ owner, kind: "owner", writable: true } as Continuity.ContinuityCaller, initialize)
      .pipe(Effect.flip);
    expect(forged.reason).toBe("forbidden");
  }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope()), Effect.provide(layer)),
);

it.effect(
  "a once-live agent loses mutation authority on attachment revocation or attempt takeover at commit",
  () =>
    Effect.gen(function* () {
      const service = yield* Continuity.GoalContinuity;
      let attached = true;
      const manager = yield* Sessions.ProviderSessionManagerV2.pipe(
        Effect.provide(
          Layer.mock(Sessions.ProviderSessionManagerV2)({
            isMcpCallerAttached: () => Effect.sync(() => attached),
          }),
        ),
      );
      const agent: Invocation.McpInvocationScope = {
        ...scope(),
        client: undefined,
        thread: {
          threadId: owner.threadId,
          providerInstanceId: owner.providerInstanceId,
          providerSessionId: "synthetic-attachment",
        },
      };
      const caller = yield* service
        .authorize()
        .pipe(
          Effect.provideService(Invocation.McpInvocationContext, agent),
          Effect.provideService(Sessions.ProviderSessionManagerV2, manager),
        );
      attached = false;
      expect((yield* service.execute(caller, initialize).pipe(Effect.flip)).reason).toBe(
        "forbidden",
      );
      attached = true;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE orchestration_v2_projection_runs SET payload_json=json_set(payload_json,'$.activeAttemptId','synthetic-takeover') WHERE run_id=${witness.runId}`;
      expect((yield* service.execute(caller, initialize).pipe(Effect.flip)).reason).toBe(
        "binding_mismatch",
      );
      expect(yield* sql`SELECT 1 FROM jones_goal_continuity`).toEqual([]);
    }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope()), Effect.provide(layer)),
);

it.effect.each([
  scope("read-only"),
  {
    ...scope(),
    client: {
      ...scope().client!,
      narrowedTo: {
        runtimeMode: "approval-required" as const,
        interactionMode: "default" as const,
      },
    },
  },
  {
    ...scope(),
    client: { ...scope().client!, access: "auto-accept-edits" as const, linked: true },
  },
])("read authority cannot become mutation authority: %o", (invocation) =>
  Effect.gen(function* () {
    const service = yield* Continuity.GoalContinuity;
    const caller = yield* service.authorize(owner.threadId);
    expect(yield* service.execute(caller, { action: "read" })).toEqual({ outcome: "absent" });
    expect((yield* service.execute(caller, initialize).pipe(Effect.flip)).reason).toBe("forbidden");
  }).pipe(
    Effect.provideService(Invocation.McpInvocationContext, invocation),
    Effect.provide(layer),
  ),
);

it.effect(
  "fresh preparation binds new checkpoint generation; permits are once-only and serialized copies carry no authority",
  () =>
    Effect.gen(function* () {
      const { service, caller, snapshot } = yield* start;
      const captured = boundary();
      expect(
        yield* service.recordRecoveryTransition(captured, {
          type: "completed",
          eventId: "foreign-completion",
        }),
      ).toEqual({ outcome: "refused", reason: "invalid_transition" });
      yield* service.recordRecoveryTransition(captured, {
        type: "started",
        eventId: "start-event",
      });
      const changed = applied(
        yield* service.execute(caller, {
          action: "checkpoint",
          clientRequestId: "fresh-1",
          expected: target(snapshot),
          patch: { focus: "fresh checkpoint" },
        }),
      );
      const completed = yield* service.recordRecoveryTransition(captured, {
        type: "completed",
        eventId: "complete-event",
      });
      if (completed.outcome !== "recorded" || !completed.pendingTicket)
        throw new Error("Expected matched boundary");
      const duplicate = yield* service.recordRecoveryTransition(captured, {
        type: "completed",
        eventId: "complete-event",
      });
      expect(duplicate).toEqual(completed);
      const fresh = yield* service.prepareRecovery(captured);
      if (fresh.outcome !== "context") throw new Error("Expected projection");
      expect(fresh.generation).toBe(changed.generation);
      expect(fresh.text).toContain("fresh checkpoint");
      const prepared = yield* service.beginDelivery({
        pendingTicket: completed.pendingTicket,
        projection: fresh,
      });
      if (prepared.outcome !== "prepared") throw new Error("Expected preparation");
      const sent = yield* service.markDispatching(prepared.ticket);
      if (sent.outcome !== "dispatching") throw new Error("Expected permit");
      expect(yield* service.markDispatching(prepared.ticket)).toEqual({
        outcome: "refused",
        reason: "permit_consumed",
      });
      expect(
        yield* service.settleDelivery({
          permit: JSON.parse(JSON.stringify(sent.permit)),
          evidence: { outcome: "unknown", cause: "lost_response" },
        }),
      ).toEqual({ outcome: "refused", reason: "permit_revoked" });
      expect((yield* service.requirementFor(owner.environmentId, owner.threadId)).outcome).toBe(
        "hold",
      );
    }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope()), Effect.provide(layer)),
);

it.effect(
  "unknown remains held through detach and owner decisions; authentic late ack preserves both unknown history and decision",
  () =>
    Effect.gen(function* () {
      const { service, caller, snapshot } = yield* start;
      const captured = boundary();
      const { permit } = yield* dispatch(service, captured);
      const unknown = yield* service.settleDelivery({
        permit,
        evidence: { outcome: "unknown", cause: "timeout" },
      });
      if (unknown.outcome !== "settled") throw new Error("Expected unknown settlement");
      expect(unknown.gate.outcome === "hold" && unknown.gate.reason).toBe("delivery_unknown");
      const detached = applied(
        yield* service.execute(caller, {
          action: "detach",
          clientRequestId: "detach-1",
          expected: target(snapshot),
          reason: "Owner inspection",
        }),
      );
      const decision = yield* service.execute(caller, {
        action: "reconcile_delivery",
        clientRequestId: "decision-1",
        deliveryId: permit.identity.deliveryId,
        expectedOriginalOwner: owner,
        expectedDeliveryRevision: unknown.receipt.deliveryRevision,
        expectedFenceRevision: unknown.gate.fenceRevision,
        expectedCurrentRecord: expected(detached),
        reason: "Owner accepts uncertainty",
        evidenceRefs: ["assertion:time-elapsed", "assertion:missing-process"],
        disposition: {
          type: "accept_unknown_for_one_recovery",
          recoveryTarget: expected(detached),
        },
      });
      if (
        decision.outcome !== "receipt" ||
        decision.receipt.result.outcome !== "delivery_reconciled"
      )
        throw new Error("Expected decision");
      expect(decision.receipt.result.disposition.quiescence).toEqual({ outcome: "unproven" });
      expect(decision.receipt.result.disposition.effect).toEqual({
        type: "held",
        reason: "quiescence_required",
      });
      expect(decision.receipt.result.gate.outcome).toBe("hold");
      const ack = yield* service.settleDelivery({
        permit,
        evidence: {
          outcome: "acknowledged",
          rpcRequestId: permit.rpcRequestId,
          acknowledgementRef: "synthetic-native-ack",
          witness,
          producer: captured.producer,
        },
      });
      if (ack.outcome !== "settled") throw new Error("Expected original acknowledgement");
      expect(ack.receipt.events.some((event) => event.phase === "unknown")).toBe(true);
      expect(ack.receipt.dispositions).toHaveLength(1);
      const requirement = yield* service.requirementFor(owner.environmentId, owner.threadId);
      expect(requirement.outcome === "hold" && requirement.reason).toBe("detached");
    }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope()), Effect.provide(layer)),
);

it.effect(
  "reconstructed unfinished dispatching is unknown and its sticky history survives losing the current-record pointer",
  () =>
    Effect.gen(function* () {
      const { service } = yield* start;
      const captured = boundary();
      const { permit, ticket } = yield* dispatch(service, captured);
      const rebuilt = yield* Continuity.GoalContinuity.pipe(
        Effect.provide(Layer.fresh(Continuity.layer)),
      );
      expect(rebuilt).not.toBe(service);
      const requirement = yield* rebuilt.requirementFor(owner.environmentId, owner.threadId);
      expect(requirement.outcome === "hold" && requirement.reason).toBe("delivery_unknown");
      expect(yield* rebuilt.markDispatching(ticket)).toEqual({
        outcome: "refused",
        reason: "binding_mismatch",
      });
      expect(
        yield* rebuilt.settleDelivery({
          permit,
          evidence: { outcome: "unknown", cause: "reconstruction" },
        }),
      ).toEqual({ outcome: "refused", reason: "permit_revoked" });
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE jones_goal_continuity SET current_record=0 WHERE thread_id=${owner.threadId}`;
      const missing = yield* rebuilt.requirementFor(owner.environmentId, owner.threadId);
      expect(missing.outcome === "hold" && missing.reason).toBe("delivery_unknown");
      expect(missing.outcome === "hold" && missing.current).toBe(null);
      expect(yield* rebuilt.prepareRecovery(captured)).toEqual({
        outcome: "hold",
        reason: "delivery_unknown",
      });
    }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope()), Effect.provide(layer)),
);

it.effect(
  "only the original internal once-only executor can prove no send; recovery still remains required",
  () =>
    Effect.gen(function* () {
      const { service } = yield* start;
      const { permit, ticket } = yield* dispatch(service, boundary());
      const evidence: C.ContinuityDeliveryEvidence = {
        outcome: "not_sent",
        proof: { type: "dispatch_precluded", revocationRef: permit.dispatchAttemptId },
      };
      expect(
        yield* service.settleDelivery({ permit: JSON.parse(JSON.stringify(permit)), evidence }),
      ).toEqual({ outcome: "refused", reason: "permit_revoked" });
      expect(
        yield* service.settleDelivery({
          permit,
          evidence: {
            outcome: "not_sent",
            proof: { type: "dispatch_precluded", revocationRef: "another-attempt" },
          },
        }),
      ).toEqual({ outcome: "refused", reason: "no_effect_proof_required" });
      const noSend = yield* service.settleDelivery({ permit, evidence });
      if (noSend.outcome !== "settled") throw new Error("Expected original no-send callback");
      expect(noSend.receipt.phase).toBe("not_sent");
      expect(noSend.gate.outcome === "hold" && noSend.gate.reason).toBe("recovery_required");
      expect(yield* service.markDispatching(ticket)).toEqual({
        outcome: "refused",
        reason: "permit_consumed",
      });
      expect(yield* service.settleDelivery({ permit, evidence })).toEqual({
        outcome: "refused",
        reason: "no_effect_proof_required",
      });
      expect((yield* service.requirementFor(owner.environmentId, owner.threadId)).outcome).toBe(
        "hold",
      );
    }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope()), Effect.provide(layer)),
);

it.effect("source ownership deletion and producer replacement refuse before dispatch", () =>
  Effect.gen(function* () {
    const { service } = yield* start;
    const captured = boundary();
    yield* service.recordRecoveryTransition(captured, { type: "started", eventId: "start-event" });
    const replacement = { ...boundary(), producer: captured.producer };
    expect(
      (yield* service
        .recordRecoveryTransition(replacement, { type: "completed", eventId: "complete-event" })
        .pipe(Effect.flip)).reason,
    ).toBe("binding_mismatch");
    const sql = yield* SqlClient.SqlClient;
    yield* sql`DELETE FROM orchestration_v2_projection_threads WHERE thread_id=${owner.threadId}`;
    expect((yield* service.prepareRecovery(captured).pipe(Effect.flip)).reason).toBe("unavailable");
  }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope()), Effect.provide(layer)),
);

it.effect(
  "confirm-first stores no active snapshot until the owner explicitly confirms the persisted exact proposal",
  () =>
    Effect.gen(function* () {
      const service = yield* Continuity.GoalContinuity;
      const caller = yield* service.authorize(owner.threadId);
      const proposed: C.ContinuityRequest = {
        ...initialize,
        intent: { ...intent, initializationMode: "confirm_first" },
        initialization: { type: "propose", proposalId: "synthetic-proposal" },
      };
      const proposal = yield* service.execute(caller, proposed);
      expect(proposal.outcome === "receipt" && proposal.receipt.result.outcome).toBe("proposed");
      expect(yield* service.execute(caller, { action: "read" })).toEqual({ outcome: "absent" });
      const confirmed = yield* service.execute(caller, {
        ...proposed,
        clientRequestId: "confirm-1",
        initialization: {
          type: "confirm",
          proposalId: "synthetic-proposal",
          confirmationId: "synthetic-confirmation",
        },
      });
      expect(applied(confirmed).intent.initializationMode).toBe("confirm_first");
    }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope()), Effect.provide(layer)),
);
