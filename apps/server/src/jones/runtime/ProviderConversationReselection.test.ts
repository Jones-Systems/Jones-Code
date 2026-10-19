import { assert, it } from "@effect/vitest";
import {
  AuthSessionId,
  CommandId,
  ContextHandoffId,
  ContextTransferId,
  EventId,
  MessageId,
  NodeId,
  ProviderTurnId,
  RunAttemptId,
  RuntimeRequestId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunId,
  ThreadId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type EnvironmentAuthenticatedPrincipal,
  type ReselectProviderConversationInput,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as Persistence from "../../persistence/Sqlite.ts";
import * as AuthSessions from "../../persistence/AuthSessions.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as ProviderSessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as ThreadCommands from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as Reselection from "./ProviderConversationReselection.ts";

const stores = Layer.mergeAll(
  EventStore.layer,
  Projections.layer,
  Receipts.layer,
  AuthSessions.layer,
).pipe(Layer.provideMerge(Persistence.layerMemory));
const testLayer = EventSink.layer.pipe(
  Layer.provideMerge(stores),
  Layer.provideMerge(ThreadCommands.layer),
);
const fixture = Effect.fnUntraced(function* (attachmentAvailable = true) {
  const sink = yield* EventSink.EventSinkV2;
  const projections = yield* Projections.ProjectionStoreV2;
  const sql = yield* SqlClient.SqlClient;
  const auth = yield* AuthSessions.AuthSessionRepository;
  const now = yield* DateTime.now;
  const threadId = ThreadId.make("thread:synthetic-reselection");
  const instance = ProviderInstanceId.make("steven");
  const oldId = ProviderThreadId.make("provider-thread:45");
  const targetId = ProviderThreadId.make("provider-thread:69");
  const input = {
    threadId,
    targetProviderThreadId: targetId,
    expectedTargetNativeThreadId: "native:69",
    expectedActiveProviderThreadId: oldId,
  };
  yield* sink.write({
    events: [
      {
        id: EventId.make("event:birth"),
        type: "thread.created",
        threadId,
        occurredAt: now,
        payload: {
          id: threadId,
          projectId: ProjectId.make("project:synthetic-reselection"),
          title: "Synthetic reselection",
          providerInstanceId: instance,
          modelSelection: { instanceId: instance, model: "synthetic" },
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
          lastVisitedAt: null,
          deletedAt: null,
          createdBy: "user",
          creationSource: "web",
        },
      },
    ],
  });
  const requested = (account: string) => ({
    providerInstanceId: ProviderInstanceId.make(account),
    providerDriver: ProviderDriverKind.make("codex"),
    model: "synthetic",
    serviceTier: null,
  });
  const observed = {
    backend: { status: "unknown" as const },
    account: { status: "unknown" as const },
    model: { status: "unknown" as const },
    serviceTier: { status: "unknown" as const },
  };
  const row = (id: ProviderThreadId, ordinal: number): OrchestrationV2ProviderThread => ({
    id,
    driver: ProviderDriverKind.make("codex"),
    providerInstanceId: instance,
    providerSessionId: ProviderSessionId.make(`session:${ordinal}`),
    appThreadId: threadId,
    ownerNodeId: null,
    nativeThreadRef: {
      driver: ProviderDriverKind.make("codex"),
      nativeId: `native:${ordinal}`,
      strength: "strong",
    },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: ordinal,
    lastRunOrdinal: ordinal === 69 ? 72 : 78,
    handoffIds: [],
    forkedFrom: null,
    pendingBackgroundTasks: [],
    contextUsage: null,
    nativeMetadata: null,
    goal: null,
    runtimeIdentity: {
      requested: requested(ordinal === 45 ? "wallace" : "steven"),
      observed,
      ...(ordinal === 69 ? { runtimeGeneration: "generation:69", evidenceRevision: 7 } : {}),
    },
    createdAt: now,
    updatedAt: now,
  });
  const target = row(targetId, 69);
  const old = row(oldId, 45);
  const writeRow = (value: OrchestrationV2ProviderThread, suffix: string) =>
    sink.write({
      events: [
        {
          id: EventId.make(`event:row:${suffix}`),
          type: "provider-thread.updated",
          threadId,
          occurredAt: now,
          payload: value,
        },
      ],
    });
  yield* writeRow(target, "target");
  yield* writeRow(old, "old");
  const run = (ordinal: number, started: boolean): OrchestrationV2Run => ({
    id: RunId.make(`run:${ordinal}`),
    threadId,
    ordinal,
    providerInstanceId: instance,
    modelSelection: { instanceId: instance, model: "synthetic" },
    providerThreadId: started ? targetId : oldId,
    userMessageId: MessageId.make(`message:${ordinal}`),
    rootNodeId: null,
    activeAttemptId: null,
    status: started ? "completed" : "failed",
    requestedAt: now,
    startedAt: started ? now : null,
    completedAt: now,
    checkpointId: null,
    contextHandoffId: null,
  });
  const writeRun = (value: OrchestrationV2Run, suffix: string) =>
    sink.write({
      events: [
        {
          id: EventId.make(`event:run:${suffix}`),
          type: "run.created",
          threadId,
          occurredAt: now,
          payload: value,
        },
      ],
    });
  for (let ordinal = 69; ordinal <= 78; ordinal++)
    yield* writeRun(run(ordinal, ordinal <= 72), String(ordinal));
  const principal: EnvironmentAuthenticatedPrincipal["Service"] = {
    sessionId: AuthSessionId.make("actor:reselection"),
    subject: "Synthetic maintainer",
    method: "browser-session-cookie",
    scopes: new Set(["orchestration:read", "orchestration:operate", "environment:maintain"]),
  };
  yield* auth.create({
    sessionId: principal.sessionId,
    subject: principal.subject,
    method: principal.method,
    scopes: [...principal.scopes],
    client: {
      label: null,
      ipAddress: null,
      userAgent: null,
      deviceType: "bot",
      os: null,
      browser: null,
    },
    issuedAt: now,
    expiresAt: DateTime.add(now, { hours: 1 }),
  });
  const state = {
    attachment: "stopped" as "stopped" | "attached" | "unknown" | "unavailable",
    current: true,
  };
  const manager = Layer.succeed(ProviderSessions.ProviderSessionManagerV2, {
    isMcpCallerAttached: () => Effect.succeed(false),
    shutdown: Effect.void,
    isLive: () => Effect.die("unexpected_isLive"),
    open: () => Effect.die("unexpected_open"),
    get: () => Effect.succeed(Option.none()),
    close: () => Effect.die("unexpected_close"),
    closeInstance: () => Effect.die("unexpected_close_instance"),
    release: () => Effect.die("unexpected_release"),
    detach: () => Effect.die("unexpected_detach"),
    ...(attachmentAvailable
      ? {
          readCurrentThreadRuntimeAttachment: () =>
            Effect.sync(() =>
              state.attachment === "stopped"
                ? {
                    status: "stopped" as const,
                    observedAt: DateTime.formatIso(now),
                    isCurrent: Effect.sync(() => state.current),
                  }
                : state.attachment === "attached"
                  ? {
                      status: "attached" as const,
                      binding: {
                        threadId,
                        providerThreadId: oldId,
                        providerInstanceId: instance,
                        providerSessionId: old.providerSessionId!,
                        driver: ProviderDriverKind.make("codex"),
                        nativeThreadId: "native:45",
                        runtimeGeneration: "generation:old",
                      },
                      evidenceRevision: null,
                      observedAt: DateTime.formatIso(now),
                      isCurrent: Effect.succeed(true),
                      physicalIncarnation: {
                        status: "unknown" as const,
                        reason: "physical_incarnation_capture_unavailable" as const,
                      },
                    }
                  : {
                      status: "unknown" as const,
                      observedAt: DateTime.formatIso(now),
                      reason: "synthetic_unavailable",
                    },
            ),
        }
      : {}),
  });
  const service = Reselection.layer.pipe(Layer.provide(manager));
  return {
    sink,
    projections,
    sql,
    now,
    input,
    old,
    target,
    principal,
    service,
    manager,
    state,
    writeRow,
    writeRun,
    run,
  };
});

it.effect(
  "reselects the incident's healthy native conversation without changing old evidence or creating execution",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* Effect.gen(function* () {
        const service = yield* Reselection.ProviderConversationReselection;
        const preview = yield* service.preview(f.input, f.principal);
        assert.strictEqual(preview.status, "eligible");
        assert.deepEqual(
          preview.runsSinceTarget.map((run) => run.ordinal),
          [73, 74, 75, 76, 77, 78],
        );
        const before = yield* f.projections.getThreadRecords(f.input.threadId, [
          "providerThreads",
          "runs",
        ]);
        const result = yield* service.reselect(
          {
            ...f.input,
            commandId: CommandId.make("command:reselect"),
            expectedPreimageDigest: preview.preimageDigest,
          },
          f.principal,
        );
        assert.strictEqual(result.status, "committed");
        assert.strictEqual(result.activeProviderThreadId, f.target.id);
        const after = yield* f.projections.getThreadRecords(f.input.threadId, [
          "providerThreads",
          "runs",
        ]);
        assert.deepEqual(
          after.providerThreads.find((row) => row.id === f.old.id),
          before.providerThreads.find((row) => row.id === f.old.id),
        );
        const updated = after.providerThreads.find((row) => row.id === f.target.id)!;
        assert.deepEqual({ ...updated, updatedAt: f.target.updatedAt }, f.target);
        assert.deepEqual(after.runs, before.runs);
        const events = yield* (yield* EventStore.EventStoreV2)
          .readByCommandId({ commandId: result.commandId })
          .pipe(Stream.runCollect);
        assert.strictEqual(events.length, 1);
        assert.strictEqual(events[0]!.event.type, "provider-thread.updated");
        assert.strictEqual((yield* f.sql`SELECT * FROM orchestration_command_receipts`).length, 1);
        assert.deepEqual(yield* f.sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
        const skipped = yield* f.sink.write({
          runtimeIdentityBoundary: {
            expectedGeneration: null,
            expectedActiveProviderThreadId: f.old.id,
          },
          events: [
            {
              id: EventId.make("event:stale-retirement"),
              type: "provider-thread.updated",
              threadId: f.input.threadId,
              occurredAt: f.now,
              payload: f.old,
            },
          ],
        });
        assert.deepEqual(skipped, []);
        assert.strictEqual(
          (yield* f.projections.getThread(f.input.threadId)).activeProviderThreadId,
          f.target.id,
        );
      }).pipe(Effect.provide(f.service));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect.each(["generation", "old-row", "new-failed-run"] as const)(
  "preview CAS rejects %s changes without a receipt or event",
  (change) =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* Effect.gen(function* () {
        const service = yield* Reselection.ProviderConversationReselection;
        const preview = yield* service.preview(f.input, f.principal);
        if (change === "generation")
          yield* f.writeRow(
            {
              ...f.target,
              runtimeIdentity: {
                ...f.target.runtimeIdentity!,
                runtimeGeneration: "generation:replaced",
              },
            },
            "changed",
          );
        if (change === "old-row") yield* f.writeRow({ ...f.old, lastRunOrdinal: 99 }, "changed");
        if (change === "new-failed-run") yield* f.writeRun(f.run(79, false), "added");
        const result = yield* service
          .reselect(
            {
              ...f.input,
              commandId: CommandId.make("command:cas"),
              expectedPreimageDigest: preview.preimageDigest,
            },
            f.principal,
          )
          .pipe(Effect.result);
        assert.strictEqual(result._tag, "Failure");
        if (result._tag === "Failure")
          assert.strictEqual(result.failure.reason, "preimage_changed");
        assert.deepEqual(yield* f.sql`SELECT * FROM orchestration_command_receipts`, []);
        assert.deepEqual(
          yield* (yield* EventStore.EventStoreV2)
            .readByCommandId({ commandId: CommandId.make("command:cas") })
            .pipe(Stream.runCollect),
          [],
        );
      }).pipe(Effect.provide(f.service));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect(
  "replays only the matching durable event and reports current pointer rather than claiming a repair happened again",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* Effect.gen(function* () {
        const service = yield* Reselection.ProviderConversationReselection;
        const preview = yield* service.preview(f.input, f.principal);
        const input: ReselectProviderConversationInput = {
          ...f.input,
          commandId: CommandId.make("command:replay"),
          expectedPreimageDigest: preview.preimageDigest,
        };
        const first = yield* service.reselect(input, f.principal);
        const newCommand = yield* service
          .reselect(
            { ...input, commandId: CommandId.make("command:replay-new-command") },
            f.principal,
          )
          .pipe(Effect.result);
        assert.strictEqual(newCommand._tag, "Failure");
        if (newCommand._tag === "Failure")
          assert.strictEqual(newCommand.failure.reason, "preimage_changed");
        assert.deepEqual(
          yield* (yield* EventStore.EventStoreV2)
            .readByCommandId({ commandId: CommandId.make("command:replay-new-command") })
            .pipe(Stream.runCollect),
          [],
        );
        yield* f.writeRow(f.old, "later-pointer");
        const replay = yield* service.reselect(input, f.principal);
        assert.strictEqual(replay.status, "already_committed");
        assert.strictEqual(replay.previousActiveProviderThreadId, f.old.id);
        assert.strictEqual(replay.resultSequence, first.resultSequence);
        assert.strictEqual(replay.activeProviderThreadId, f.old.id);
        const conflict = yield* service
          .reselect({ ...input, targetProviderThreadId: f.old.id }, f.principal)
          .pipe(Effect.result);
        assert.strictEqual(conflict._tag, "Failure");
        if (conflict._tag === "Failure")
          assert.strictEqual(conflict.failure.reason, "command_identity_conflict");
        assert.strictEqual((yield* f.sql`SELECT * FROM orchestration_command_receipts`).length, 1);
      }).pipe(Effect.provide(f.service));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect.each([
  "queued",
  "started-later",
  "model-mismatch",
  "target-request-mismatch",
  "runtime-unknown",
  "runtime-attached",
  "runtime-unavailable",
  "missing-maintain",
  "revoked",
  "stale-stopped",
] as const)("refuses %s before changing the pointer", (scenario) =>
  Effect.gen(function* () {
    const f = yield* fixture(scenario !== "runtime-unavailable");
    yield* Effect.gen(function* () {
      const service = yield* Reselection.ProviderConversationReselection;
      let principal = f.principal;
      if (scenario === "queued")
        yield* f.writeRun({ ...f.run(79, false), status: "queued", completedAt: null }, "queued");
      if (scenario === "started-later")
        yield* f.writeRun({ ...f.run(79, true), providerThreadId: f.old.id }, "started-later");
      if (scenario === "model-mismatch") {
        const thread = yield* f.projections.getThread(f.input.threadId);
        yield* f.sink.write({
          events: [
            {
              id: EventId.make("event:model-change"),
              type: "thread.metadata-updated",
              threadId: f.input.threadId,
              occurredAt: f.now,
              payload: {
                ...thread,
                modelSelection: {
                  ...thread.modelSelection,
                  instanceId: ProviderInstanceId.make("other"),
                },
              },
            },
          ],
        });
      }
      if (scenario === "target-request-mismatch") {
        yield* f.writeRow(
          {
            ...f.target,
            runtimeIdentity: {
              ...f.target.runtimeIdentity!,
              requested: {
                ...f.target.runtimeIdentity!.requested,
                providerInstanceId: ProviderInstanceId.make("wallace"),
              },
            },
          },
          "identity-mismatch",
        );
        yield* f.writeRow(f.old, "restore-old-after-invalid-target");
      }
      if (scenario === "runtime-attached") f.state.attachment = "attached";
      if (scenario === "runtime-unknown") f.state.attachment = "unknown";
      if (scenario === "stale-stopped") f.state.current = false;
      if (scenario === "missing-maintain")
        principal = { ...principal, scopes: new Set(["orchestration:operate"]) };
      if (scenario === "revoked")
        yield* f.sql`UPDATE auth_sessions SET revoked_at = ${DateTime.formatIso(f.now)} WHERE session_id = ${principal.sessionId}`;
      const preview = yield* service.preview(f.input, principal).pipe(Effect.result);
      if (scenario === "missing-maintain" || scenario === "revoked") {
        assert.strictEqual(preview._tag, "Failure");
      } else {
        assert.strictEqual(preview._tag, "Success");
        if (preview._tag === "Success") {
          assert.strictEqual(preview.success.status, "ineligible");
          if (scenario === "target-request-mismatch")
            assert.include(preview.success.refusalCodes, "target_identity_conflict");
          const committed = yield* service
            .reselect(
              {
                ...f.input,
                commandId: CommandId.make("command:refused"),
                expectedPreimageDigest: preview.success.preimageDigest,
              },
              principal,
            )
            .pipe(Effect.result);
          assert.strictEqual(committed._tag, "Failure");
        }
      }
      assert.strictEqual(
        (yield* f.projections.getThread(f.input.threadId)).activeProviderThreadId,
        f.old.id,
      );
      assert.deepEqual(yield* f.sql`SELECT * FROM orchestration_command_receipts`, []);
    }).pipe(Effect.provide(f.service));
  }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect.each([
  "thread-missing",
  "archived",
  "deleted",
  "pointer-changed",
  "target-selected",
  "current-missing",
  "target-missing",
  "row-owned",
  "row-active",
  "background-pending",
  "runtime-input",
  "fork-pending",
  "merge-pending",
  "handoff-pending",
  "provider-turn-later",
] as const)("reports eligibility refusal for %s", (scenario) =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* Effect.gen(function* () {
      const service = yield* Reselection.ProviderConversationReselection;
      let input = f.input;
      let reason = "thread_unavailable";
      if (scenario === "thread-missing")
        input = { ...input, threadId: ThreadId.make("thread:missing") };
      if (scenario === "archived" || scenario === "deleted" || scenario === "current-missing") {
        const thread = yield* f.projections.getThread(input.threadId);
        yield* f.sink.write({
          events: [
            {
              id: EventId.make("event:thread-state"),
              type: "thread.metadata-updated",
              threadId: input.threadId,
              occurredAt: f.now,
              payload: {
                ...thread,
                ...(scenario === "archived"
                  ? { archivedAt: f.now }
                  : scenario === "deleted"
                    ? { deletedAt: f.now }
                    : { activeProviderThreadId: null }),
              },
            },
          ],
        });
        if (scenario === "current-missing") reason = "current_unavailable";
      }
      if (scenario === "pointer-changed") {
        input = {
          ...input,
          expectedActiveProviderThreadId: ProviderThreadId.make("provider-thread:unexpected"),
        };
        reason = "active_pointer_changed";
      }
      if (scenario === "target-selected") {
        input = { ...input, targetProviderThreadId: f.old.id };
        reason = "target_already_selected";
      }
      if (scenario === "target-missing") {
        input = {
          ...input,
          targetProviderThreadId: ProviderThreadId.make("provider-thread:missing"),
        };
        reason = "target_unavailable";
      }
      if (scenario === "row-owned") {
        yield* f.writeRow({ ...f.old, ownerNodeId: NodeId.make("node:other") }, "owned");
        reason = "row_ownership_conflict";
      }
      if (scenario === "row-active") {
        yield* f.writeRow({ ...f.old, status: "active" }, "active");
        reason = "row_active";
      }
      if (scenario === "background-pending") {
        yield* f.writeRow(
          { ...f.old, pendingBackgroundTasks: [{ kind: "command", taskId: "task:background" }] },
          "background",
        );
        reason = "background_work_pending";
      }
      if (scenario === "runtime-input") {
        yield* f.sink.write({
          events: [
            {
              id: EventId.make("event:request"),
              type: "runtime-request.updated",
              threadId: input.threadId,
              occurredAt: f.now,
              payload: {
                id: RuntimeRequestId.make("request:live"),
                nodeId: NodeId.make("node:request"),
                providerTurnId: null,
                nativeRequestRef: null,
                kind: "user_input",
                status: "pending",
                responseCapability: { type: "live", providerSessionId: f.old.providerSessionId! },
                createdAt: f.now,
                resolvedAt: null,
              },
            },
          ],
        });
        reason = "runtime_input_pending";
      }
      if (scenario === "fork-pending" || scenario === "merge-pending") {
        yield* f.sink.write({
          events: [
            {
              id: EventId.make("event:transfer"),
              type: "context-transfer.created",
              threadId: input.threadId,
              occurredAt: f.now,
              payload: {
                id: ContextTransferId.make("transfer:pending"),
                type: scenario === "fork-pending" ? "fork" : "merge_back",
                sourceThreadId: input.threadId,
                targetThreadId: input.threadId,
                sourcePoint: { threadId: input.threadId },
                basePoint: null,
                sourceProviderInstanceId: f.old.providerInstanceId,
                targetProviderInstanceId: f.target.providerInstanceId,
                targetRunId: null,
                status: "pending",
                resolution: null,
                createdBy: "user",
                error: null,
                createdAt: f.now,
                updatedAt: f.now,
                consumedAt: null,
              },
            },
          ],
        });
        reason = "context_transfer_pending";
      }
      if (scenario === "handoff-pending") {
        yield* f.sink.write({
          events: [
            {
              id: EventId.make("event:handoff"),
              type: "context-handoff.updated",
              threadId: input.threadId,
              occurredAt: f.now,
              payload: {
                id: ContextHandoffId.make("handoff:pending"),
                threadId: input.threadId,
                targetRunId: RunId.make("run:73"),
                fromProviderThreadIds: [f.old.id],
                toProviderThreadId: f.target.id,
                coveredRunOrdinals: { from: 69, to: 72 },
                strategy: "manual_context",
                status: "pending",
                summaryMessageId: null,
                summaryText: "Synthetic",
                createdByProviderInstanceId: null,
                createdAt: f.now,
                updatedAt: f.now,
              },
            },
          ],
        });
        reason = "handoff_pending";
      }
      if (scenario === "provider-turn-later") {
        const attemptId = RunAttemptId.make("attempt:73");
        const turnId = ProviderTurnId.make("turn:73");
        yield* f.sink.write({
          events: [
            {
              id: EventId.make("event:attempt"),
              type: "run-attempt.created",
              threadId: input.threadId,
              occurredAt: f.now,
              payload: {
                id: attemptId,
                runId: RunId.make("run:73"),
                attemptOrdinal: 1,
                rootNodeId: NodeId.make("node:73"),
                providerInstanceId: f.old.providerInstanceId,
                providerThreadId: f.old.id,
                providerTurnId: turnId,
                reason: "initial",
                status: "failed",
                startedAt: null,
                completedAt: f.now,
              },
            },
            {
              id: EventId.make("event:turn"),
              type: "provider-turn.updated",
              threadId: input.threadId,
              occurredAt: f.now,
              payload: {
                id: turnId,
                providerThreadId: f.old.id,
                nodeId: NodeId.make("node:73"),
                runAttemptId: attemptId,
                nativeTurnRef: null,
                ordinal: 1,
                status: "failed",
                startedAt: null,
                completedAt: f.now,
              },
            },
          ],
        });
        reason = "later_run_started";
      }
      const preview = yield* service.preview(input, f.principal);
      assert.strictEqual(preview.status, "ineligible");
      assert.include(preview.refusalCodes, reason);
      assert.deepEqual(yield* f.sql`SELECT * FROM orchestration_command_receipts`, []);
    }).pipe(Effect.provide(f.service));
  }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect("rechecks session revocation inside receipt transaction and rolls the receipt back", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* Effect.gen(function* () {
      const service = yield* Reselection.ProviderConversationReselection;
      const preview = yield* service.preview(f.input, f.principal);
      let observed = false;
      const racedSink = EventSink.EventSinkV2.of({
        ...f.sink,
        commitCommand: (input) => {
          const context = input.providerConversationReselection!;
          return f.sink.commitCommand({
            ...input,
            providerConversationReselection: {
              revalidate: (envelope, replay) =>
                Effect.gen(function* () {
                  observed = true;
                  yield* f.sql`UPDATE auth_sessions SET revoked_at = ${DateTime.formatIso(f.now)} WHERE session_id = ${f.principal.sessionId}`.pipe(
                    Effect.orDie,
                  );
                  yield* context.revalidate(envelope, replay);
                }),
            },
          });
        },
      });
      const result = yield* Effect.gen(function* () {
        const owner = yield* Reselection.ProviderConversationReselection;
        return yield* owner
          .reselect(
            {
              ...f.input,
              commandId: CommandId.make("command:revocation-race"),
              expectedPreimageDigest: preview.preimageDigest,
            },
            f.principal,
          )
          .pipe(Effect.result);
      }).pipe(
        Effect.provide(
          Layer.fresh(Reselection.layer).pipe(
            Layer.provide(Layer.succeed(EventSink.EventSinkV2, racedSink)),
            Layer.provide(f.manager),
          ),
        ),
      );
      assert.isTrue(observed);
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") assert.strictEqual(result.failure.reason, "actor_unavailable");
      assert.deepEqual(yield* f.sql`SELECT * FROM orchestration_command_receipts`, []);
      assert.strictEqual(
        (yield* f.projections.getThread(f.input.threadId)).activeProviderThreadId,
        f.old.id,
      );
    }).pipe(Effect.provide(f.service));
  }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect(
  "uses actual run completion order when an older queued ordinal executed after the healthy target",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* Effect.gen(function* () {
        const service = yield* Reselection.ProviderConversationReselection;
        yield* f.writeRun(
          {
            ...f.run(68, true),
            providerThreadId: f.old.id,
            startedAt: DateTime.add(f.now, { minutes: 1 }),
            completedAt: DateTime.add(f.now, { minutes: 2 }),
          },
          "older-queued-started-later",
        );
        const preview = yield* service.preview(f.input, f.principal);
        assert.strictEqual(preview.status, "ineligible");
        assert.include(preview.refusalCodes, "started_history_conflict");
        assert.include(preview.refusalCodes, "later_run_started");
        assert.include(
          preview.runsSinceTarget.map((run) => run.ordinal),
          68,
        );
        const result = yield* service
          .reselect(
            {
              ...f.input,
              commandId: CommandId.make("command:older-queued-started-later"),
              expectedPreimageDigest: preview.preimageDigest,
            },
            f.principal,
          )
          .pipe(Effect.result);
        assert.strictEqual(result._tag, "Failure");
        assert.deepEqual(yield* f.sql`SELECT * FROM orchestration_command_receipts`, []);
      }).pipe(Effect.provide(f.service));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect.each([
  "expectedActiveProviderThreadId",
  "expectedPreimageDigest",
  "expectedTargetNativeThreadId",
] as const)("rejects altered replay %s without a second durable write", (field) =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* Effect.gen(function* () {
      const service = yield* Reselection.ProviderConversationReselection;
      const preview = yield* service.preview(f.input, f.principal);
      const input: ReselectProviderConversationInput = {
        ...f.input,
        commandId: CommandId.make("command:altered-replay"),
        expectedPreimageDigest: preview.preimageDigest,
      };
      const first = yield* service.reselect(input, f.principal);
      const altered: ReselectProviderConversationInput =
        field === "expectedActiveProviderThreadId"
          ? {
              ...input,
              expectedActiveProviderThreadId: ProviderThreadId.make(
                "provider-thread:different-previous",
              ),
            }
          : field === "expectedPreimageDigest"
            ? { ...input, expectedPreimageDigest: "different-preimage" }
            : { ...input, expectedTargetNativeThreadId: "native:different" };
      const result = yield* service.reselect(altered, f.principal).pipe(Effect.result);
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure")
        assert.strictEqual(result.failure.reason, "command_identity_conflict");
      assert.strictEqual((yield* f.sql`SELECT * FROM orchestration_command_receipts`).length, 1);
      const events = yield* (yield* EventStore.EventStoreV2)
        .readByCommandId({ commandId: input.commandId })
        .pipe(Stream.runCollect);
      assert.strictEqual(events.length, 1);
      assert.strictEqual(events[0]!.sequence, first.resultSequence);
      assert.deepEqual(yield* f.sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
      assert.strictEqual(
        (yield* f.projections.getThread(input.threadId)).activeProviderThreadId,
        f.target.id,
      );
    }).pipe(Effect.provide(f.service));
  }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

const goldenReselectionInput: ReselectProviderConversationInput = {
  commandId: CommandId.make("command:golden-reselection"),
  threadId: ThreadId.make("thread:golden-reselection"),
  targetProviderThreadId: ProviderThreadId.make("provider-thread:golden-target"),
  expectedTargetNativeThreadId: "native:golden-target",
  expectedActiveProviderThreadId: ProviderThreadId.make("provider-thread:golden-old"),
  expectedPreimageDigest: "0".repeat(64),
};

it("keeps the versioned reselection event identity deterministic and independent of request key order", () => {
  const expected =
    "event:command:golden-reselection:provider-conversation-reselect:9f317f2050db25804ddf506e4697601b7eb4b62f02d2bfca224d0c0de8f6d8f2";
  assert.strictEqual(
    Reselection.providerConversationReselectionEventId(goldenReselectionInput),
    expected,
  );
  assert.strictEqual(
    Reselection.providerConversationReselectionEventId(goldenReselectionInput),
    expected,
  );
  const reordered: ReselectProviderConversationInput = {
    expectedPreimageDigest: goldenReselectionInput.expectedPreimageDigest,
    expectedActiveProviderThreadId: goldenReselectionInput.expectedActiveProviderThreadId,
    expectedTargetNativeThreadId: goldenReselectionInput.expectedTargetNativeThreadId,
    targetProviderThreadId: goldenReselectionInput.targetProviderThreadId,
    threadId: goldenReselectionInput.threadId,
    commandId: goldenReselectionInput.commandId,
  };
  assert.strictEqual(Reselection.providerConversationReselectionEventId(reordered), expected);
});

it.each([
  { ...goldenReselectionInput, commandId: CommandId.make("command:changed") },
  { ...goldenReselectionInput, threadId: ThreadId.make("thread:changed") },
  {
    ...goldenReselectionInput,
    targetProviderThreadId: ProviderThreadId.make("provider-thread:changed-target"),
  },
  { ...goldenReselectionInput, expectedTargetNativeThreadId: "native:changed" },
  {
    ...goldenReselectionInput,
    expectedActiveProviderThreadId: ProviderThreadId.make("provider-thread:changed-old"),
  },
  { ...goldenReselectionInput, expectedPreimageDigest: "1".repeat(64) },
])("binds every decoded reselection request field in event identity %#", (input) => {
  assert.notStrictEqual(
    Reselection.providerConversationReselectionEventId(input),
    Reselection.providerConversationReselectionEventId(goldenReselectionInput),
  );
});
