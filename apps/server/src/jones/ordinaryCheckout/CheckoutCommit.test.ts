import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import { isPublicStoredOrchestrationEvent } from "../../orchestration-v2/WireProjection.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import { CheckoutError, canonical } from "./CheckoutProtocol.ts";
import * as Commit from "./CheckoutCommit.ts";
import * as LegacyStorage from "../legacyBootstrap/Storage.ts";
import type { CommandBinding } from "../legacyBootstrap/StorageSchema.ts";
import legacyMigration from "../persistence/Migrations/161_JonesLegacyBootstrap.ts";
import { CheckoutCommitAuthority, type CommittedCheckoutOperation } from "./OrdinaryCheckout.ts";
import * as Intent from "./CheckoutIntent.ts";
import { makeCommitFixture } from "./CheckoutCommit.testkit.ts";
import { decodedCommand } from "./CheckoutPreparationSchema.ts";

const legacyBootstrap = {
  version: 1 as const,
  createCommandId: CommandId.make("legacy-create"),
  birthCommandId: CommandId.make("legacy-birth"),
  releaseCommandId: CommandId.make("legacy-release"),
  projectId: ProjectId.make("legacy-project"),
  threadId: ThreadId.make("legacy-thread"),
  messageId: MessageId.make("legacy-message"),
  payloadHash: "legacy-payload",
  ownsNewThread: true,
};
const serverCommands = [
  {
    type: "thread.project.adopt-folder",
    commandId: "adopt-folder",
    threadId: "server-thread",
    projectId: "server-project",
  },
  {
    type: "thread.pull-request-watch.checks-reopen",
    commandId: "reopen-checks",
    threadId: "server-thread",
  },
  {
    type: "legacy-bootstrap.failure-delete",
    commandId: "delete-legacy-failure",
    threadId: legacyBootstrap.threadId,
    runId: "legacy-run",
    legacyBootstrap,
  },
  {
    type: "thread.stop",
    commandId: "legacy-stop",
    threadId: legacyBootstrap.threadId,
    legacyBootstrap,
    legacyPreparationFailureKnown: false,
  },
  {
    type: "thread.background-work.settle",
    commandId: "settle-subagent",
    threadId: "server-thread",
    providerThreadId: "server-provider-thread",
    providerTurnId: "server-provider-turn",
    runOnly: true,
    subagent: {
      id: "server-subagent",
      stoppedAt: DateTime.makeUnsafe("2026-11-16T00:00:00Z"),
      outcome: {
        status: "completed",
        completedAt: DateTime.makeUnsafe("2026-11-16T00:00:00Z"),
        result: "Completed",
      },
    },
  },
];

it.effect.each(serverCommands)(
  "full server command $type retains private input identity and refuses forged extras",
  (command) =>
    Effect.gen(function* () {
      const decoded = yield* decodedCommand(command);
      assert.deepEqual<unknown>(decoded.command, command);
      assert.isTrue(
        Exit.isFailure(
          yield* decodedCommand({ ...command, forgedAuthority: true }).pipe(Effect.exit),
        ),
      );
    }),
);

it.effect(
  "legacy authority fields independently alter full-command identity; malformed private inputs refuse",
  () =>
    Effect.gen(function* () {
      const command = {
        type: "thread.stop" as const,
        commandId: CommandId.make("legacy-identity"),
        threadId: legacyBootstrap.threadId,
        legacyBootstrap,
        legacyPreparationFailureKnown: false,
      };
      const original = yield* decodedCommand(command);
      for (const change of [
        { ...command, legacyPreparationFailureKnown: true },
        { ...command, legacyBootstrap: { ...legacyBootstrap, ownsNewThread: false } },
        { ...command, legacyBootstrap: { ...legacyBootstrap, payloadHash: "changed" } },
        {
          ...command,
          legacyBootstrap: {
            ...legacyBootstrap,
            releaseCommandId: CommandId.make("another-release"),
          },
        },
      ])
        assert.notEqual((yield* decodedCommand(change)).digest, original.digest);
      for (const forged of [
        { ...command, legacyPreparationFailureKnown: "true" },
        { ...command, legacyBootstrap: { ...legacyBootstrap, forgedAuthority: true } },
        { ...command, legacyBootstrap: { ...legacyBootstrap, ownsNewThread: "true" } },
        { type: "unknown.internal-command", commandId: "unknown", threadId: command.threadId },
      ])
        assert.isTrue(Exit.isFailure(yield* decodedCommand(forged).pipe(Effect.exit)));
    }),
);

it.effect(
  "full self-configuration input binds revision, runtime precondition, selection and environment",
  () =>
    Effect.gen(function* () {
      const command = {
        type: "jones.thread.self-configure" as const,
        commandId: CommandId.make("self-configure-command"),
        threadId: ThreadId.make("self-configure-thread"),
        environmentId: EnvironmentId.make("self-configure-environment"),
        input: {
          clientRequestId: "self-configure-request",
          expectedConfigurationRevision: 12,
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "synthetic" },
          executionPrecondition: {
            runId: RunId.make("self-configure-run"),
            attemptId: RunAttemptId.make("self-configure-attempt"),
            providerThreadId: ProviderThreadId.make("self-configure-provider-thread"),
            providerSessionId: ProviderSessionId.make("self-configure-provider-session"),
            runtimeGeneration: "resident:exact-native-generation",
          },
        },
      };
      const original = yield* decodedCommand(command);
      assert.deepEqual<unknown>(original.command, command);
      for (const changed of [
        { ...command, environmentId: EnvironmentId.make("another-environment") },
        { ...command, input: { ...command.input, expectedConfigurationRevision: 13 } },
        {
          ...command,
          input: {
            ...command.input,
            modelSelection: { ...command.input.modelSelection, model: "another-model" },
          },
        },
        {
          ...command,
          input: {
            ...command.input,
            executionPrecondition: {
              ...command.input.executionPrecondition,
              runtimeGeneration: "another-generation",
            },
          },
        },
      ])
        assert.notEqual((yield* decodedCommand(changed)).digest, original.digest);
      for (const forged of [
        { type: command.type, commandId: command.commandId, threadId: command.threadId },
        { ...command, input: { ...command.input, privileged: true } },
        {
          ...command,
          input: {
            ...command.input,
            executionPrecondition: { ...command.input.executionPrecondition, ready: true },
          },
        },
      ])
        assert.isTrue(Exit.isFailure(yield* decodedCommand(forged).pipe(Effect.exit)));
    }),
);

const prepareCase = Effect.gen(function* () {
  const graph = yield* makeCommitFixture();
  const id = ThreadId.make("witness-thread");
  const now = yield* DateTime.now;
  const iso = DateTime.formatIso(now);
  yield* graph.sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at)
    VALUES('project','Synthetic',${graph.f.bPath},'[]',${iso},${iso})`;
  yield* graph.orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make("witness-create"),
    threadId: id,
    projectId: ProjectId.make("project"),
    title: "Original",
    modelSelection: { instanceId: graph.instanceId, model: "synthetic" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });
  const thread = yield* Context.get(graph.context, Projections.ProjectionStoreV2).getThread(id);
  const command = {
    type: "thread.metadata.update" as const,
    commandId: CommandId.make("witness-update"),
    threadId: id,
    title: "Updated",
  };
  const events = [
    {
      id: EventId.make("witness-update-event"),
      type: "thread.metadata-updated" as const,
      threadId: id,
      occurredAt: now,
      payload: { ...thread, title: "Updated", updatedAt: now },
    },
  ];
  const plan: Intent.IntentPlan = { command, threadId: id, events, effects: [] };
  const prepared = yield* graph.intent.prepare(plan);
  const input = {
    commandId: command.commandId,
    commandType: command.type,
    threadId: id,
    acceptedAt: now,
    ordinaryCheckoutCommand: command,
    ordinaryCheckout: { prepared },
    events,
    effects: [],
  };
  return { ...graph, input, prepared };
});

it.effect(
  "actual orchestrator accepts and replays a server-only command with ordinary services installed",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const g = yield* prepareCase;
        const command = {
          type: "thread.pull-request-watch.checks-reopen" as const,
          commandId: CommandId.make("server-reopen"),
          threadId: g.input.threadId,
        };
        const accepted = yield* g.orchestrator.dispatch(command);
        assert.isUndefined(accepted.replayed);
        const observations = g.observe().observations;
        const replayed = yield* g.orchestrator.dispatch(command);
        assert.isTrue(replayed.replayed);
        assert.equal(replayed.sequence, accepted.sequence);
        assert.equal(g.observe().observations, observations);
        const forgedInternal = { ...command, legacyPreparationFailureKnown: true };
        assert.isTrue(
          Exit.isFailure(
            yield* g.intent.replay(forgedInternal, command.threadId).pipe(Effect.exit),
          ),
        );
        const before =
          yield* g.sql`SELECT * FROM orchestration_command_receipts ORDER BY command_id`;
        const forged = { ...g.input.ordinaryCheckoutCommand, forgedAuthority: true };
        assert.isTrue(
          Exit.isFailure(
            yield* g.sink
              .commitCommand({
                ...g.input,
                ordinaryCheckoutCommand: forged,
              })
              .pipe(Effect.exit),
          ),
        );
        assert.deepEqual(
          yield* g.sql`SELECT * FROM orchestration_command_receipts ORDER BY command_id`,
          before,
        );
        assert.equal(g.observe().observations, observations);
      }),
    ),
);

it.effect.each([
  "forged",
  "changed-command",
  "changed-plan",
  "fiber",
  "provider-batch",
  "foreign-client",
] as const)(
  "actual EventSink refuses %s prepared command without receipt, projection or publication changes",
  (variant) =>
    Effect.scoped(
      Effect.gen(function* () {
        const g = yield* prepareCase;
        const before =
          yield* g.sql`SELECT * FROM orchestration_command_receipts ORDER BY command_id`;
        const events = yield* g.sql`SELECT * FROM orchestration_events ORDER BY sequence`;
        const published = g.observe().publications;
        let input = g.input;
        if (variant === "forged")
          input = { ...input, ordinaryCheckout: { prepared: { ...g.prepared } } };
        if (variant === "changed-command")
          input = {
            ...input,
            ordinaryCheckoutCommand: { ...input.ordinaryCheckoutCommand, title: "Forged" },
          };
        if (variant === "changed-plan")
          input = {
            ...input,
            events: input.events.map((event) => ({
              ...event,
              payload: { ...event.payload, title: "Forged" },
            })),
          };
        const { ordinaryCheckout: _prepared, ...unpreparedInput } = input;
        const write = g.sink.commitCommand(variant === "provider-batch" ? unpreparedInput : input);
        const result =
          variant === "fiber"
            ? yield* write.pipe(Effect.forkChild, Effect.flatMap(Fiber.await))
            : variant === "provider-batch"
              ? yield* g.sink.withProviderBatch(write).pipe(Effect.exit)
              : variant === "foreign-client"
                ? yield* Effect.gen(function* () {
                    const foreign = yield* makeCommitFixture();
                    return yield* foreign.sink.commitCommand(input).pipe(Effect.exit);
                  })
                : yield* write.pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(result));
        assert.deepEqual(
          yield* g.sql`SELECT * FROM orchestration_command_receipts ORDER BY command_id`,
          before,
        );
        assert.deepEqual(
          yield* g.sql`SELECT * FROM orchestration_events ORDER BY sequence`,
          events,
        );
        assert.equal(g.observe().publications, published);
      }),
    ),
);

it.effect.each([
  "escaped",
  "nested-savepoint",
  "fiber",
  "foreign-client",
  "forged-plan",
  "interruption",
] as const)(
  "real new-reservation witness fences %s use and cannot outlive its accepting operation",
  (variant) =>
    Effect.scoped(
      Effect.gen(function* () {
        const g = yield* prepareCase;
        let retained:
          | { authority: CheckoutCommitAuthority["Service"]; operation: CommittedCheckoutOperation }
          | undefined;
        const real = g.intent;
        const entered = yield* Deferred.make<void>();
        const observed = Layer.succeed(Intent.CheckoutIntent, {
          ...real,
          accept: (prepared, committed) =>
            Effect.gen(function* () {
              const authority = yield* CheckoutCommitAuthority;
              const operation = {
                prepared,
                command: prepared.plan.command,
                threadId: prepared.plan.threadId,
                ...committed,
              };
              retained = { authority, operation };
              if (variant === "nested-savepoint") {
                // Even if the nested failure is caught, that witness stays revoked.
                yield* g.sql.withTransaction(authority.assertOrigin(operation)).pipe(Effect.exit);
              } else if (variant === "fiber") {
                yield* authority
                  .assertOrigin(operation)
                  .pipe(Effect.forkChild, Effect.flatMap(Fiber.join));
              } else if (variant === "foreign-client") {
                yield* authority
                  .assertOrigin(operation)
                  .pipe(Effect.provideService(SqlClient.SqlClient, g.f.a.local));
              } else if (variant === "forged-plan") {
                yield* authority.assertOrigin({ ...operation, prepared: { ...prepared } });
              }
              // This is the actual core acceptance, never a permissive substitute.
              yield* real.accept(prepared, committed);
              if (variant === "interruption") {
                yield* Deferred.succeed(entered, undefined);
                return yield* Effect.never;
              }
            }),
        });
        const database = Layer.succeed(SqlClient.SqlClient, g.sql);
        const sinkLayer = EventSink.layer.pipe(
          Layer.provide(
            Layer.mergeAll(observed, EventStore.layer, Projections.layer).pipe(
              Layer.provideMerge(database),
            ),
          ),
        );
        const sink = Context.get(yield* Layer.build(sinkLayer), EventSink.EventSinkV2);
        const before = yield* g.sql`SELECT * FROM orchestration_events ORDER BY sequence`;
        const result =
          variant === "interruption"
            ? yield* Effect.gen(function* () {
                const { ordinaryCheckout: _prepared, ...freshInput } = g.input;
                const fiber = yield* sink.commitCommand(freshInput).pipe(Effect.forkChild);
                yield* Deferred.await(entered);
                yield* Fiber.interrupt(fiber);
                return yield* Fiber.await(fiber);
              })
            : yield* sink.commitCommand(g.input).pipe(Effect.exit);
        if (variant === "escaped") {
          assert.isTrue(Exit.isSuccess(result));
          assert.isDefined(retained);
          if (!retained) return;
          assert.isTrue(
            Exit.isFailure(
              yield* retained.authority.assertOrigin(retained.operation).pipe(Effect.exit),
            ),
          );
          // A later transaction on the same connection also carries no authority.
          assert.isTrue(
            Exit.isFailure(
              yield* g.sql
                .withTransaction(retained.authority.assertOrigin(retained.operation))
                .pipe(Effect.provideService(SqlClient.SqlClient, g.sql), Effect.exit),
            ),
          );
        } else {
          assert.isTrue(Exit.isFailure(result));
          assert.deepEqual(
            yield* g.sql`SELECT * FROM orchestration_events ORDER BY sequence`,
            before,
          );
          assert.isTrue(
            Option.isNone(
              yield* Context.get(g.context, Receipts.CommandReceiptStoreV2).getByCommandId(
                g.input.commandId,
              ),
            ),
          );
          if (retained)
            assert.isTrue(
              Exit.isFailure(
                yield* retained.authority.assertOrigin(retained.operation).pipe(Effect.exit),
              ),
            );
        }
      }),
    ),
);

it.effect(
  "receiving guard's late material plan rewrite refuses before apply and leaves no publication",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const g = yield* prepareCase;
        const before = yield* g.sql`SELECT * FROM orchestration_events ORDER BY sequence`;
        const published = g.observe().publications;
        const result = yield* g.sink
          .commitCommand({
            ...g.input,
            threadSelfConfiguration: {
              revalidate: () =>
                Effect.succeed(
                  g.input.events.map((event) => ({
                    ...event,
                    payload: { ...event.payload, title: "late receiving rewrite" },
                  })),
                ),
            },
          })
          .pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(result));
        assert.deepEqual(
          yield* g.sql`SELECT * FROM orchestration_events ORDER BY sequence`,
          before,
        );
        assert.equal(g.observe().publications, published);
      }),
    ),
);

it.effect(
  "a preprepared SQL-only command in a provider batch publishes only after the outer commit and rolls back with it",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const g = yield* prepareCase;
        const before = yield* g.sql`SELECT * FROM orchestration_events ORDER BY sequence`;
        const published = g.observe().publications;
        const result = yield* g.sink
          .withProviderBatch(
            g.sink.commitCommand(g.input).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  assert.equal(g.observe().publications, published);
                }),
              ),
              Effect.andThen(Effect.fail(new CheckoutError({ reason: "storage" }))),
            ),
          )
          .pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(result));
        assert.deepEqual(
          yield* g.sql`SELECT * FROM orchestration_events ORDER BY sequence`,
          before,
        );
        assert.isTrue(
          Option.isNone(
            yield* Context.get(g.context, Receipts.CommandReceiptStoreV2).getByCommandId(
              g.input.commandId,
            ),
          ),
        );
        assert.equal(g.observe().publications, published);
      }),
    ),
);

const preparePrivateDelete = Effect.gen(function* () {
  const g = yield* prepareCase;
  const storage = LegacyStorage.make(g.sql);
  const eventStore = Context.get(
    yield* Layer.build(
      EventStore.layer.pipe(Layer.provide(Layer.succeed(SqlClient.SqlClient, g.sql))),
    ),
    EventStore.EventStoreV2,
  );
  if (!(yield* storage.available))
    yield* legacyMigration.pipe(Effect.provideService(SqlClient.SqlClient, g.sql));
  const thread = yield* Context.get(g.context, Projections.ProjectionStoreV2).getThread(
    g.input.threadId,
  );
  const runId = RunId.make("private-delete-run");
  const policy = { ...legacyBootstrap, threadId: thread.id, projectId: thread.projectId, runId };
  const control = {
    version: 1 as const,
    type: "no_control" as const,
    policy,
    runId,
    threadId: thread.id,
    claimEventId: EventId.make("private-claim"),
    claimSequence: 1,
    claimReceiptSequence: 1,
    birthEventId: EventId.make("private-birth"),
    birthSequence: 2,
    birthReceiptSequence: 2,
    preparationGeneration: "private-preparation",
    workspacePath: g.f.bPath,
    projectWorkspaceRoot: g.f.bPath,
  };
  const command = {
    type: "legacy-bootstrap.failure-delete" as const,
    commandId: CommandId.make("private-delete"),
    threadId: thread.id,
    runId,
    legacyBootstrap: policy,
    legacyNoControl: control,
  };
  const now = yield* DateTime.now;
  const events = [
    {
      id: EventId.make("private-delete-event"),
      type: "thread.deleted" as const,
      threadId: thread.id,
      occurredAt: now,
      payload: { ...thread, deletedAt: now, updatedAt: now },
    },
  ];
  const plan: Intent.IntentPlan = { command, threadId: thread.id, events, effects: [] };
  const prepared = yield* g.intent.prepare(plan);
  const binding: CommandBinding = {
    kind: "delete",
    commandId: command.commandId,
    threadId: thread.id,
    projectId: thread.projectId,
    threadCreatedAt: DateTime.formatIso(thread.createdAt),
    bindingJson: canonical(command),
  };
  return { ...g, storage, eventStore, command, events, prepared, binding, now };
});

it.effect("private delete binds exact control bytes and refuses combined or extra authority", () =>
  Effect.gen(function* () {
    const command = {
      type: "legacy-bootstrap.guard-rejection-delete" as const,
      commandId: CommandId.make("private-codec-D"),
      threadId: legacyBootstrap.threadId,
      runId: RunId.make("private-codec-run"),
      legacyBootstrap,
      legacyOwnedControl: {
        version: 1,
        policy: legacyBootstrap,
        runId: "private-codec-run",
        threadId: legacyBootstrap.threadId,
        claimEventId: "claim",
        claimSequence: 1,
        claimReceiptSequence: 1,
        birthEventId: "birth",
        birthSequence: 2,
        birthReceiptSequence: 2,
        preparationGeneration: "preparation",
        terminalId: "terminal",
        generation: "generation",
      },
    };
    const original = yield* decodedCommand(command);
    assert.deepEqual<unknown>(original.command, command);
    assert.notEqual(
      (yield* decodedCommand({
        ...command,
        legacyOwnedControl: { ...command.legacyOwnedControl, generation: "replacement" },
      })).digest,
      original.digest,
    );
    for (const forged of [
      { ...command, legacyNoControl: {} },
      { ...command, legacyOwnedControl: { ...command.legacyOwnedControl, privileged: true } },
    ])
      assert.isTrue(Exit.isFailure(yield* decodedCommand(forged).pipe(Effect.exit)));
  }),
);

it.effect.each([
  "accepted",
  "rollback",
  "fiber",
  "escaped",
  "changed-binding",
  "public-cleanup",
] as const)("private receipt checkout witness preserves actual SQLite authority: %s", (variant) =>
  Effect.scoped(
    Effect.gen(function* () {
      const g = yield* preparePrivateDelete;
      const before = yield* g.sql`SELECT * FROM orchestration_events ORDER BY sequence`;
      let retained: Effect.Effect<void, CheckoutError> | undefined;
      const result = yield* g.sql
        .withTransaction(
          Effect.gen(function* () {
            yield* g.intent.validate(g.prepared);
            const storedEvents = yield* g.eventStore
              .append({
                commandId: g.command.commandId,
                events: g.events,
              })
              .pipe(Effect.map((entries) => entries.filter(isPublicStoredOrchestrationEvent)));
            const receipt: Receipts.CommandReceiptV2 = {
              commandId: g.command.commandId,
              commandType: g.command.type,
              threadId: g.command.threadId,
              acceptedAt: g.now,
              status: "accepted",
              error: null,
              resultSequence: storedEvents[storedEvents.length - 1]!.sequence,
            };
            const committed = {
              receipt,
              storedEvents,
              effects:
                variant === "public-cleanup"
                  ? [
                      {
                        id: "private-delete-forbidden-cleanup",
                        commandId: g.command.commandId,
                        threadId: g.command.threadId,
                        request: { type: "terminal.cleanup" as const },
                      },
                    ]
                  : [],
            };
            const binding =
              variant === "changed-binding"
                ? { ...g.binding, bindingJson: canonical({ ...g.command, runId: "forged-run" }) }
                : g.binding;
            const reserve = Commit.reserveLegacyDeleteCommand(
              g.sql,
              binding,
              receipt,
              g.command,
              g.prepared,
              committed,
            );
            const witness =
              variant === "fiber"
                ? yield* reserve.pipe(Effect.forkChild, Effect.flatMap(Fiber.join))
                : yield* reserve;
            assert.isDefined(witness);
            if (witness === undefined) return;
            const accept = witness.withAuthority(committed, g.intent.accept(g.prepared, committed));
            retained = accept;
            if (variant === "escaped") return;
            yield* accept;
            assert.isTrue(Exit.isFailure(yield* accept.pipe(Effect.exit)));
            assert.isUndefined(
              yield* Commit.reserveLegacyDeleteCommand(
                g.sql,
                g.binding,
                receipt,
                g.command,
                g.prepared,
                committed,
              ),
            );
            if (variant === "rollback") return yield* new CheckoutError({ reason: "storage" });
          }),
        )
        .pipe(Effect.provideService(SqlClient.SqlClient, g.sql), Effect.exit);
      const failed =
        variant === "rollback" ||
        variant === "fiber" ||
        variant === "changed-binding" ||
        variant === "public-cleanup";
      assert.equal(Exit.isFailure(result), failed);
      assert.deepEqual(
        yield* g.sql.unsafe("SELECT * FROM orchestration_command_receipts WHERE command_id = ?", [
          g.command.commandId,
        ]),
        [],
      );
      if (failed) {
        assert.isUndefined(yield* g.storage.readLegacyReceipt(g.command.commandId));
        assert.deepEqual(
          yield* g.sql`SELECT * FROM orchestration_events ORDER BY sequence`,
          before,
        );
      } else {
        assert.equal(
          (yield* g.storage.readLegacyReceipt(g.command.commandId))?.domain,
          "legacy-delete",
        );
        if (retained !== undefined)
          assert.isTrue(Exit.isFailure(yield* retained.pipe(Effect.exit)));
      }
    }),
  ),
);
