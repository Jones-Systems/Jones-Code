import { assert, it } from "@effect/vitest";
import { CommandId, MessageId, OrchestrationV2ProviderSession } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Fixture from "../jones/threadResultDependencies/testkit.ts";
import * as Changes from "../jones/threadResultDependencies/DependencyChanges.ts";
import { CodexFailoverError } from "../jones/codexFailover/CodexFailover.ts";
import * as Titles from "../jones/threadTitle/store.ts";
import { withRuntimeUseAttachmentPublication } from "../jones/cleanup/RuntimeUseAttachment.ts";
import { RuntimeUseError } from "../jones/cleanup/RuntimeUseProtocol.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as Admission from "../jones/updates/UpdateAdmissionFence.ts";

// Reuse the synthetic core persistence harness. These cases never start a provider or worker.
const decodeProviderSession = Schema.decodeEffect(OrchestrationV2ProviderSession);
it.effect(
  "commits request, title provenance, receipt and effects once across concurrent duplicates",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const changes = yield* Changes.DependencyChanges;
      const titles = yield* Titles.make;
      const request = f.requestInput("core-atomic");
      const commandId = CommandId.make(request.commandId);
      const effectId = "effect:core-atomic";
      let acceptanceCount = 0;
      const commit = f.sink.commitCommand({
        commandId,
        threadId: f.receiver.id,
        commandType: "message.dispatch",
        acceptedAt: Fixture.time,
        resultRequest: request,
        threadTitle: {
          accept: titles.write(f.receiver.id, {
            status: "active",
            accepted: [request.messageId],
            covered: [],
            knownInsufficient: false,
            lastRequestId: null,
            pending: null,
          }),
        },
        codexFailover: {
          accept: () =>
            Effect.gen(function* () {
              assert.equal(
                (yield* f.store.get(request.localDependency!.dependencyId))?.state,
                "awaiting",
              );
              assert.deepEqual((yield* titles.read(f.receiver.id))?.accepted, [request.messageId]);
              assert.equal(
                (yield* f.sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE effect_id = ${effectId}`)
                  .length,
                0,
              );
              assert.equal(changes.generation(f.waiter.id), 0);
              acceptanceCount++;
              // A failed observation read is a test defect, not a failover refusal.
            }).pipe(Effect.orDie),
        },
        events: [
          f.event({
            type: "message.updated",
            threadId: f.receiver.id,
            runId: f.receiverRun.id,
            payload: f.message(request.messageId),
          }),
        ],
        effects: [
          {
            id: effectId,
            commandId,
            threadId: f.receiver.id,
            request: { type: "provider-turn.start", runId: f.receiverRun.id },
          },
        ],
      });
      const results = yield* Effect.all([commit, commit], { concurrency: 2 });
      assert.equal(results.filter((result) => result.committed).length, 1);
      assert.equal(acceptanceCount, 1);
      assert.equal(results[0]!.receipt.resultSequence, results[1]!.receipt.resultSequence);
      assert.equal(
        (yield* f.sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id = ${commandId}`)
          .length,
        1,
      );
      assert.equal(
        (yield* f.sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE effect_id = ${effectId}`)
          .length,
        1,
      );
      assert.equal(changes.generation(f.waiter.id), 1);
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect(
  "rolls back events, sparse metadata, receipt and outbox when the final transaction participant fails",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const changes = yield* Changes.DependencyChanges;
      const request = f.requestInput("core-rollback");
      const commandId = CommandId.make(request.commandId);
      const before = yield* f.sink.latestSequence();
      const result = yield* f.sink
        .commitCommand({
          commandId,
          threadId: f.receiver.id,
          commandType: "message.dispatch",
          acceptedAt: Fixture.time,
          resultRequest: request,
          codexFailover: {
            accept: () => Effect.fail(new CodexFailoverError({ reason: "state_unavailable" })),
          },
          events: [
            f.event({
              type: "message.updated",
              threadId: f.receiver.id,
              runId: f.receiverRun.id,
              payload: f.message(request.messageId),
            }),
          ],
          effects: [
            {
              id: "effect:rollback",
              commandId,
              threadId: f.receiver.id,
              request: { type: "provider-turn.start", runId: f.receiverRun.id },
            },
          ],
        })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(result));
      assert.equal(yield* f.sink.latestSequence(), before);
      assert.isUndefined(yield* f.store.get(request.localDependency!.dependencyId));
      assert.equal(
        (yield* f.sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id = ${commandId}`)
          .length,
        0,
      );
      assert.equal(
        (yield* f.sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE effect_id = 'effect:rollback'`)
          .length,
        0,
      );
      assert.equal(changes.generation(f.waiter.id), 0);
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect(
  "persists a no-event Stop and its generation fence atomically and replays without fencing again",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      yield* f.send("stop-core");
      const stop = {
        commandId: CommandId.make("stop-core"),
        threadId: f.waiter.id,
        commandType: "thread.stop",
        acceptedAt: Fixture.time,
        events: [],
        effects: [],
      };
      const first = yield* f.sink.commitCommand(stop);
      assert.isTrue(first.committed);
      assert.equal((yield* f.store.get("dependency:stop-core"))?.state, "released");
      const generation = yield* f.store.waiterGeneration(f.waiter.id);
      const replay = yield* f.sink.commitCommand(stop);
      assert.isFalse(replay.committed);
      assert.equal(yield* f.store.waiterGeneration(f.waiter.id), generation);
      assert.deepEqual(replay.receipt, first.receipt);
      const stale = f.requestInput("stale-generation");
      const result = yield* f.send("stale-generation", f.receiverRun, stale).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(result));
      assert.isUndefined(yield* f.store.get(stale.localDependency!.dependencyId));
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect("keeps sparse dependency holds visible in full, point and deferred shell snapshots", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    yield* f.send("shell-core");
    const point = yield* f.projection.getThreadShell(f.waiter.id);
    const full = yield* f.projection.getShellSnapshot();
    const decode = yield* f.projection.readShellSnapshot();
    const deferred = yield* decode;
    for (const shell of [
      point,
      full.threads.find((thread) => thread.id === f.waiter.id),
      deferred.threads.find((thread) => thread.id === f.waiter.id),
    ])
      assert.isTrue(
        shell?.pendingBackgroundTasks?.some(
          (task) => task.taskId === "jones-dependency:dependency:shell-core",
        ),
      );
  }).pipe(Effect.provide(Fixture.layer)),
);

it.effect(
  "rolls back an outer command transaction including deferred publications before an idempotent retry",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const request = f.requestInput("outer-core");
      const before = yield* f.sink.latestSequence();
      const failed = yield* f.sink
        .withProviderBatch(
          f.send("outer-core").pipe(Effect.andThen(Effect.fail("outer transaction aborted"))),
        )
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(failed));
      assert.equal(yield* f.sink.latestSequence(), before);
      assert.isUndefined(yield* f.store.get(request.localDependency!.dependencyId));
      yield* f.send("outer-core");
      const stored = yield* f.sink
        .readByCommandId({ commandId: CommandId.make(request.commandId) })
        .pipe(Stream.runCollect);
      assert.equal(stored.length, 1);
      assert.equal(
        (yield* f.store.requests(f.receiver.id)).filter(
          (item) => item.messageId === MessageId.make(request.messageId),
        ).length,
        1,
      );
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect(
  "checks physical attachment outside SQL and its persisted fence before the actual projection write",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const session = yield* decodeProviderSession({
        id: "session:core-attachment",
        driver: "codex",
        providerInstanceId: f.receiver.providerInstanceId,
        status: "ready",
        cwd: "/synthetic-core",
        model: "test",
        capabilities: CodexProviderCapabilitiesV2,
        createdAt: Fixture.time,
        updatedAt: Fixture.time,
        lastError: null,
      });
      const event = f.event({
        type: "provider-session.attached",
        threadId: f.receiver.id,
        payload: session,
      });
      const before = yield* f.sink.latestSequence();
      const calls: string[] = [];
      let valid = false;
      const publish = withRuntimeUseAttachmentPublication(
        {
          threadId: f.receiver.id,
          providerSessionId: session.id,
          revalidate: Effect.gen(function* () {
            assert.isTrue(Option.isNone(yield* Effect.serviceOption(f.sql.transactionService)));
            calls.push("physical");
          }),
          validate: Effect.gen(function* () {
            assert.isTrue(Option.isSome(yield* Effect.serviceOption(f.sql.transactionService)));
            assert.equal(
              (yield* f.sql`SELECT 1 FROM orchestration_v2_projection_provider_sessions
          WHERE thread_id = ${f.receiver.id}`).length,
              0,
            );
            calls.push("transaction");
            if (!valid) return yield* new RuntimeUseError({ reason: "fixture_owner_replaced" });
          }).pipe(
            Effect.mapError(
              (cause) => new RuntimeUseError({ reason: "fixture_validation_failed", cause }),
            ),
          ),
        },
        f.sink.write({ events: [event] }),
      );
      assert.isTrue(Exit.isFailure(yield* publish.pipe(Effect.exit)));
      assert.equal(yield* f.sink.latestSequence(), before);
      assert.equal(
        (yield* f.sql`SELECT 1 FROM orchestration_v2_projection_provider_sessions
      WHERE thread_id = ${f.receiver.id}`).length,
        0,
      );
      valid = true;
      assert.equal((yield* publish).length, 1);
      assert.deepEqual(calls, ["physical", "transaction", "physical", "transaction"]);
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect(
  "rejects a retiring workspace binding before events, projections or the receipt can commit",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const path = "/synthetic-core/retiring";
      yield* f.sql`INSERT INTO jones_workspace_retirement_state
      (worktree,incarnation_id,state,receipt_id) VALUES (${path},'fixture-incarnation','retiring','fixture-receipt')`;
      const commandId = CommandId.make("core-retirement-conflict");
      const before = yield* f.sink.latestSequence();
      const result = yield* f.sink
        .commitCommand({
          commandId,
          threadId: f.receiver.id,
          commandType: "thread.metadata.update",
          acceptedAt: Fixture.time,
          events: [
            f.event({
              type: "thread.metadata-updated",
              threadId: f.receiver.id,
              payload: { ...f.receiver, worktreePath: path },
            }),
          ],
          effects: [],
        })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(result));
      assert.equal(yield* f.sink.latestSequence(), before);
      assert.equal((yield* f.projection.getThread(f.receiver.id)).worktreePath, null);
      assert.equal(
        (yield* f.sql`SELECT 1 FROM orchestration_command_receipts WHERE command_id = ${commandId}`)
          .length,
        0,
      );
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect(
  "holds new starts at the existing continuity fence and accepts the same command only after release",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const admission = yield* Admission.UpdateAdmissionFence;
      assert.isTrue(yield* admission.attached);
      const ticket = yield* admission.raise("manual");
      const commandId = CommandId.make("core-continuity-held");
      const commit = f.sink.commitCommand({
        commandId,
        threadId: f.receiver.id,
        commandType: "message.dispatch",
        acceptedAt: Fixture.time,
        events: [
          f.event({
            type: "message.updated",
            threadId: f.receiver.id,
            runId: f.receiverRun.id,
            payload: f.message("core-continuity-message"),
          }),
        ],
        effects: [
          {
            id: "core-continuity-effect",
            commandId,
            threadId: f.receiver.id,
            request: { type: "provider-turn.start", runId: f.receiverRun.id },
          },
        ],
      });
      const before = yield* f.sink.latestSequence();
      assert.isTrue(Exit.isFailure(yield* commit.pipe(Effect.exit)));
      assert.equal(yield* f.sink.latestSequence(), before);
      assert.equal(
        (yield* f.sql`SELECT 1 FROM orchestration_command_receipts WHERE command_id = ${commandId}`)
          .length,
        0,
      );
      assert.equal(
        (yield* f.sql`SELECT 1 FROM orchestration_v2_effect_outbox WHERE effect_id = 'core-continuity-effect'`)
          .length,
        0,
      );
      yield* ticket.release;
      assert.isTrue((yield* commit).committed);
    }).pipe(Effect.provide(Fixture.layer.pipe(Layer.provideMerge(Admission.layer)))),
);

it.effect(
  "rolls back native acceptance without its actual custody owner and refuses an unbound native effect",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const command = {
        type: "message.dispatch" as const,
        commandId: CommandId.make("core-native-accept"),
        threadId: f.receiver.id,
        messageId: MessageId.make("core-native-message"),
        text: "Native original",
        attachments: [],
        createdBy: "user" as const,
        creationSource: "web" as const,
        dispatchMode: { type: "start_immediately" as const },
      };
      const effect = {
        id: "core-native-effect",
        commandId: command.commandId,
        threadId: command.threadId,
        request: { type: "provider-turn.start" as const, runId: f.receiverRun.id },
        nativeCreationExecutionReference: {
          version: 2 as const,
          claimId: "original-native-claim",
          stageCommandId: command.commandId,
          effectId: "core-native-effect",
          stage: "native_command" as const,
        },
      };
      const accepted = {
        commandId: command.commandId,
        threadId: command.threadId,
        commandType: command.type,
        acceptedAt: Fixture.time,
        events: [
          f.event({
            type: "message.updated",
            threadId: command.threadId,
            runId: f.receiverRun.id,
            payload: f.message(command.messageId),
          }),
        ],
        effects: [effect],
      };
      const before = yield* f.sink.latestSequence();
      assert.isTrue(Exit.isFailure(yield* f.sink.commitCommand(accepted).pipe(Effect.exit)));
      assert.isTrue(
        Exit.isFailure(
          yield* f.sink
            .commitCommand({
              ...accepted,
              nativeCreation: { claimId: "original-native-claim", command },
            })
            .pipe(Effect.exit),
        ),
      );
      assert.equal(yield* f.sink.latestSequence(), before);
      assert.equal(
        (yield* f.sql`SELECT 1 FROM orchestration_command_receipts WHERE command_id = ${command.commandId}`)
          .length,
        0,
      );
      assert.equal(
        (yield* f.sql`SELECT 1 FROM orchestration_v2_effect_outbox WHERE effect_id = ${effect.id}`)
          .length,
        0,
      );
    }).pipe(Effect.provide(Fixture.layer)),
);
