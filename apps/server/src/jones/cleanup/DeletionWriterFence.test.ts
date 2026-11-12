// @effect-diagnostics nodeBuiltinImport:off -- Task-only real SQLite service connections; no native runtime or Git effect.
import * as NodePath from "node:path";
import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  CommandId,
  EventId,
  ThreadId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderDriverKind,
  RunId,
  type WorktreeCleanupRules,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Option from "effect/Option";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { runMigrations } from "../../persistence/Migrations.ts";
import { makeDeletionAdmission } from "./DeletionAdmission.ts";

const threadId = ThreadId.make("writer-fence-original");
const projectId = ProjectId.make("writer-fence-project");
const commandId = CommandId.make("writer-fence-delete");
const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "synthetic-only" };
const target = {
  projectId,
  projectRoot: "/synthetic/project",
  path: "/synthetic/worktrees/original",
  branch: "original",
  force: false,
};
const workspace = target.path;
const rules: WorktreeCleanupRules = {
  worktreeSettledScope: "branch-retained" as const,
  worktreeCoveredDiscard: [],
  worktreeKeepWhen: "uncommitted-changes",
  worktreeAfterDays: null,
  worktreeSettledAfterDays: null,
  worktreeOnDelete: true,
  worktreeOnMerge: false,
  worktreeUnchanged: false,
};
type Services =
  | SqlClient.SqlClient
  | EventSink.EventSinkV2
  | ProjectionStore.ProjectionStoreV2
  | ProjectStore.ProjectStoreV2
  | Outbox.EffectOutboxV2
  | Receipts.CommandReceiptStoreV2;
const fixture = <A, E>(
  test: (input: {
    readonly original: Effect.Success<
      ReturnType<ProjectionStore.ProjectionStoreV2["Service"]["getThread"]>
    >;
    readonly now: DateTime.Utc;
    readonly second: <B, F>(effect: Effect.Effect<B, F, Services>) => Effect.Effect<B, F>;
  }) => Effect.Effect<A, E, Services>,
  seedOldEffects = false,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "deletion-writer-fence-" });
      const dbPath = NodePath.join(root, "statev2.sqlite");
      const sqlLayer = NodeSqliteClient.layer({ filename: dbPath });
      const initialized = Layer.effectDiscard(runMigrations()).pipe(Layer.provideMerge(sqlLayer));
      const services = <E, R>(persistence: Layer.Layer<SqlClient.SqlClient, E, R>) =>
        EventSink.layer.pipe(
          Layer.provideMerge(
            Layer.mergeAll(
              EventStore.layer,
              ProjectStore.layer,
              ProjectionStore.layer,
              Outbox.layer,
              Receipts.layer,
            ).pipe(Layer.provideMerge(persistence)),
          ),
        );
      return yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const projects = yield* ProjectStore.ProjectStoreV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const eventSink = yield* EventSink.EventSinkV2;
        const receipts = yield* Receipts.CommandReceiptStoreV2;
        const now = yield* DateTime.now;
        const iso = DateTime.formatIso(now);
        yield* sql.withTransaction(
          projects.apply({
            sequence: 1,
            eventId: EventId.make("runtime-use-project-event"),
            aggregateKind: "project",
            aggregateId: projectId,
            occurredAt: iso,
            commandId: null,
            causationEventId: null,
            correlationId: null,
            metadata: {},
            type: "project.created",
            payload: {
              projectId,
              title: "Synthetic runtime use",
              workspaceRoot: target.projectRoot,
              defaultModelSelection: null,
              scripts: [],
              createdAt: iso,
              updatedAt: iso,
            },
          }),
        );
        yield* eventSink.write({
          events: [
            {
              id: EventId.make("runtime-use-thread-event"),
              type: "thread.created",
              threadId,
              occurredAt: now,
              payload: {
                id: threadId,
                projectId,
                title: "Synthetic runtime use",
                createdBy: "user",
                creationSource: "web",
                providerInstanceId: instanceId,
                modelSelection,
                runtimeMode: "full-access",
                interactionMode: "default",
                worktreePath: workspace,
                branch: target.branch,
                activeProviderThreadId: null,
                lineage: {
                  parentThreadId: null,
                  relationshipToParent: null,
                  rootThreadId: threadId,
                },
                forkedFrom: null,
                createdAt: now,
                updatedAt: now,
                archivedAt: null,
                settledOverride: null,
                settledAt: null,
                lastVisitedAt: null,
                deletedAt: null,
              },
            },
          ],
        });

        yield* eventSink.write({
          events: [
            {
              id: EventId.make("writer-stopped-session"),
              type: "provider-session.updated",
              threadId,
              occurredAt: now,
              payload: {
                id: ProviderSessionId.make("writer-shared-session"),
                providerInstanceId: instanceId,
                driver: ProviderDriverKind.make("codex"),
                status: "stopped",
                cwd: workspace,
                model: modelSelection.model,
                capabilities: CodexProviderCapabilitiesV2,
                createdAt: now,
                updatedAt: now,
                lastError: null,
              },
            },
          ],
        });
        const original = yield* projections.getThread(threadId);
        if (seedOldEffects) {
          const foreign = ThreadId.make("retention-foreign-thread");
          yield* eventSink.write({
            events: [
              {
                id: EventId.make("retention-foreign-birth"),
                type: "thread.created",
                threadId: foreign,
                occurredAt: now,
                payload: {
                  ...original,
                  id: foreign,
                  worktreePath: "/synthetic/unrelated",
                  lineage: {
                    parentThreadId: null,
                    relationshipToParent: null,
                    rootThreadId: foreign,
                  },
                },
              },
            ],
          });
          const outbox = yield* Outbox.EffectOutboxV2;
          for (const [id, owner] of [
            ["reserved-old-effect", threadId],
            ["unrelated-old-effect", foreign],
          ] as const) {
            yield* outbox.enqueue([
              {
                id,
                commandId: CommandId.make(`synthetic-${id}`),
                threadId: owner,
                request: { type: "terminal.cleanup" },
              },
            ]);
            const claim = yield* outbox.claimNext({ workerId: "synthetic-no-native-worker" });
            assert.isTrue(Option.isSome(claim));
            if (Option.isNone(claim))
              return yield* Effect.die("Expected actual metadata-only claim");
            assert.strictEqual(claim.value.id, id);
            assert.isTrue(
              yield* outbox.succeed({ effectId: id, workerId: "synthetic-no-native-worker" }),
            );
          }
          // Retention fixture only: a timestamp is never physical retirement evidence.
          const old = DateTime.formatIso(DateTime.subtractDuration(now, Duration.days(8)));
          yield* sql`UPDATE orchestration_v2_effect_outbox SET completed_at=${old} WHERE effect_id IN ('reserved-old-effect','unrelated-old-effect')`;
        }
        const deleted = yield* eventSink.write({
          commandId,
          events: [
            {
              id: EventId.make("writer-original-delete"),
              type: "thread.deleted",
              threadId,
              occurredAt: now,
              payload: { ...original, deletedAt: now, updatedAt: now },
            },
          ],
        });
        const last = deleted.at(-1);
        if (last === undefined) return yield* Effect.die("Expected actual deletion event");
        yield* receipts.upsert({
          commandId,
          threadId,
          commandType: "thread.delete",
          acceptedAt: now,
          resultSequence: last.sequence,
          status: "accepted",
          error: null,
        });
        const admission = yield* makeDeletionAdmission;
        const admitted = yield* admission.start({
          threadId,
          target,
          rules,
          currentRules: Effect.succeed(rules),
          currentLive: Effect.succeed(true),
        });
        assert.strictEqual(admitted.status, "start_now");
        const other = yield* Layer.build(services(NodeSqliteClient.layer({ filename: dbPath })));
        const reserved =
          yield* sql`SELECT * FROM jones_deletion_worktree_admissions ORDER BY effect_id`;
        const result = yield* test({
          original,
          now,
          second: (effect) => effect.pipe(Effect.provide(other)),
        });
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM jones_deletion_worktree_admissions ORDER BY effect_id`,
          reserved,
          "writers never rewrite original admission intent or its phase",
        );
        return result;
      }).pipe(Effect.provide(services(initialized)));
    }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );
const refused = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    assert.isTrue(Exit.isFailure(yield* Effect.exit(effect)));
  });

it.effect(
  "a separate actual EventSink connection cannot change a reserved thread and rolls back its event",
  () =>
    fixture((input) =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const before = yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`;
        yield* input.second(
          Effect.gen(function* () {
            const sink = yield* EventSink.EventSinkV2;
            yield* refused(
              sink.write({
                events: [
                  {
                    id: EventId.make("blocked-title"),
                    type: "thread.metadata-updated",
                    threadId,
                    occurredAt: input.now,
                    payload: { ...input.original, title: "Must not publish" },
                  },
                ],
              }),
            );
          }),
        );
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`,
          before,
        );
        assert.strictEqual(
          (yield* (yield* ProjectionStore.ProjectionStoreV2).getThread(threadId)).title,
          input.original.title,
        );
      }),
    ),
);
it.effect.each(["same", "parent", "descendant"] as const)(
  "a separate projection writer cannot add a %s overlapping member",
  (relation) =>
    fixture((input) =>
      Effect.gen(function* () {
        const path =
          relation === "same"
            ? target.path
            : relation === "parent"
              ? "/synthetic/worktrees"
              : target.path + "/child";
        const foreign = ThreadId.make("new-overlapping-thread");
        yield* input.second(
          Effect.gen(function* () {
            const projection = yield* ProjectionStore.ProjectionStoreV2;
            yield* refused(
              projection.apply({
                id: EventId.make("blocked-projection-create"),
                type: "thread.created",
                threadId: foreign,
                occurredAt: input.now,
                payload: {
                  ...input.original,
                  id: foreign,
                  worktreePath: path,
                  lineage: {
                    parentThreadId: null,
                    relationshipToParent: null,
                    rootThreadId: foreign,
                  },
                },
              }),
            );
          }),
        );
        assert.strictEqual(
          yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadShell(foreign),
          null,
        );
      }),
    ),
);
it.effect("unrelated workspaces remain writable through a separate actual EventSink", () =>
  fixture((input) =>
    Effect.gen(function* () {
      const foreign = ThreadId.make("unrelated-writer-thread");
      yield* input.second(
        Effect.gen(function* () {
          const sink = yield* EventSink.EventSinkV2;
          yield* sink.write({
            events: [
              {
                id: EventId.make("unrelated-create"),
                type: "thread.created",
                threadId: foreign,
                occurredAt: input.now,
                payload: {
                  ...input.original,
                  id: foreign,
                  worktreePath: "/synthetic/unrelated",
                  lineage: {
                    parentThreadId: null,
                    relationshipToParent: null,
                    rootThreadId: foreign,
                  },
                },
              },
            ],
          });
        }),
      );
      assert.strictEqual(
        (yield* (yield* ProjectionStore.ProjectionStoreV2).getThread(foreign)).worktreePath,
        "/synthetic/unrelated",
      );
    }),
  ),
);
it.effect("a separate project writer cannot reassign the reserved checkout's project root", () =>
  fixture((input) =>
    Effect.gen(function* () {
      yield* input.second(
        Effect.gen(function* () {
          const projects = yield* ProjectStore.ProjectStoreV2;
          yield* refused(
            projects.apply({
              sequence: 20,
              eventId: EventId.make("blocked-project-root"),
              aggregateKind: "project",
              aggregateId: projectId,
              occurredAt: DateTime.formatIso(input.now),
              commandId: null,
              causationEventId: null,
              correlationId: null,
              metadata: {},
              type: "project.meta-updated",
              payload: {
                projectId,
                workspaceRoot: "/synthetic/reassigned",
                updatedAt: DateTime.formatIso(input.now),
              },
            }),
          );
        }),
      );
      const sql = yield* SqlClient.SqlClient;
      assert.deepStrictEqual(
        yield* sql`SELECT workspace_root FROM projection_projects WHERE project_id=${projectId}`,
        [{ workspace_root: target.projectRoot }],
      );
    }),
  ),
);
it.effect("a separate low-level outbox writer cannot admit new work for the reserved group", () =>
  fixture((input) =>
    Effect.gen(function* () {
      yield* input.second(
        Effect.gen(function* () {
          const outbox = yield* Outbox.EffectOutboxV2;
          yield* refused(
            outbox.enqueue([
              {
                id: "blocked-start-effect",
                commandId: CommandId.make("blocked-start"),
                threadId,
                request: { type: "provider-turn.start", runId: RunId.make("blocked-run") },
              },
            ]),
          );
        }),
      );
      const sql = yield* SqlClient.SqlClient;
      assert.deepStrictEqual(yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox`, []);
    }),
  ),
);

it.effect("a global session ID cannot be rebound through another thread's provider update", () =>
  fixture((input) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const before =
        yield* sql`SELECT * FROM orchestration_v2_projection_provider_sessions ORDER BY provider_session_id`;
      yield* input.second(
        Effect.gen(function* () {
          const sink = yield* EventSink.EventSinkV2;
          yield* refused(
            sink.write({
              events: [
                {
                  id: EventId.make("foreign-rebind"),
                  type: "provider-session.updated",
                  threadId: ThreadId.make("foreign-session-writer"),
                  occurredAt: input.now,
                  payload: {
                    id: ProviderSessionId.make("writer-shared-session"),
                    providerInstanceId: instanceId,
                    driver: ProviderDriverKind.make("codex"),
                    status: "ready",
                    cwd: "/synthetic/unrelated",
                    model: modelSelection.model,
                    capabilities: CodexProviderCapabilitiesV2,
                    createdAt: input.now,
                    updatedAt: input.now,
                    lastError: null,
                  },
                },
              ],
            }),
          );
        }),
      );
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM orchestration_v2_projection_provider_sessions ORDER BY provider_session_id`,
        before,
      );
    }),
  ),
);
it.effect(
  "project display metadata remains writable when the reserved physical root is unchanged",
  () =>
    fixture((input) =>
      Effect.gen(function* () {
        yield* input.second(
          Effect.gen(function* () {
            const projects = yield* ProjectStore.ProjectStoreV2;
            yield* projects.apply({
              sequence: 21,
              eventId: EventId.make("unrelated-project-title"),
              aggregateKind: "project",
              aggregateId: projectId,
              occurredAt: DateTime.formatIso(input.now),
              commandId: null,
              causationEventId: null,
              correlationId: null,
              metadata: {},
              type: "project.meta-updated",
              payload: {
                projectId,
                title: "Allowed display update",
                updatedAt: DateTime.formatIso(input.now),
              },
            });
          }),
        );
        const sql = yield* SqlClient.SqlClient;
        assert.deepStrictEqual(
          yield* sql`SELECT title,workspace_root FROM projection_projects WHERE project_id=${projectId}`,
          [{ title: "Allowed display update", workspace_root: target.projectRoot }],
        );
      }),
    ),
);
it.effect("unrelated outbox work remains admissible through the other SQL connection", () =>
  fixture((input) =>
    Effect.gen(function* () {
      const foreign = ThreadId.make("foreign-start-thread");
      yield* input.second(
        Effect.gen(function* () {
          const sink = yield* EventSink.EventSinkV2;
          yield* sink.write({
            events: [
              {
                id: EventId.make("foreign-start-birth"),
                type: "thread.created",
                threadId: foreign,
                occurredAt: input.now,
                payload: {
                  ...input.original,
                  id: foreign,
                  worktreePath: "/synthetic/unrelated",
                  lineage: {
                    parentThreadId: null,
                    relationshipToParent: null,
                    rootThreadId: foreign,
                  },
                },
              },
            ],
          });
          const outbox = yield* Outbox.EffectOutboxV2;
          yield* outbox.enqueue([
            {
              id: "allowed-start-effect",
              commandId: CommandId.make("allowed-start"),
              threadId: foreign,
              request: { type: "provider-turn.start", runId: RunId.make("allowed-run") },
            },
          ]);
        }),
      );
      const sql = yield* SqlClient.SqlClient;
      assert.deepStrictEqual(
        yield* sql`SELECT effect_id,thread_id,status FROM orchestration_v2_effect_outbox`,
        [{ effect_id: "allowed-start-effect", thread_id: foreign, status: "pending" }],
      );
    }),
  ),
);

it.effect(
  "outbox retention preserves reserved terminal history while pruning unrelated history",
  () =>
    fixture(
      (input) =>
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const before =
            yield* sql`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_id='reserved-old-effect'`;
          const count = yield* input.second(
            Effect.flatMap(Outbox.EffectOutboxV2, (outbox) => outbox.pruneSettled),
          );
          assert.strictEqual(count, 1);
          assert.deepStrictEqual(
            yield* sql`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_id='reserved-old-effect'`,
            before,
          );
          assert.deepStrictEqual(
            yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE effect_id='unrelated-old-effect'`,
            [],
          );
        }),
      true,
    ),
);
it.effect(
  "claim and availability skip a legacy conflicting row without starving unrelated work",
  () =>
    fixture(
      (input) =>
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const earlier = DateTime.formatIso(
            DateTime.subtractDuration(input.now, Duration.hours(1)),
          );
          yield* sql`UPDATE orchestration_v2_effect_outbox SET status='pending',attempt_count=0,lease_owner=NULL,completed_at=NULL,available_at=${earlier} WHERE effect_id='reserved-old-effect'`;
          yield* sql`UPDATE orchestration_v2_effect_outbox SET status='pending',attempt_count=0,lease_owner=NULL,completed_at=NULL,available_at=${DateTime.formatIso(input.now)} WHERE effect_id='unrelated-old-effect'`;
          const before =
            yield* sql`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_id='reserved-old-effect'`;
          yield* input.second(
            Effect.gen(function* () {
              const outbox = yield* Outbox.EffectOutboxV2;
              const available = yield* outbox.nextClaimableAt();
              assert.isTrue(Option.isSome(available));
              if (Option.isNone(available))
                return yield* Effect.die("Expected unrelated availability");
              assert.strictEqual(
                DateTime.formatIso(available.value),
                DateTime.formatIso(input.now),
              );
              const claim = yield* outbox.claimNext({ workerId: "unrelated-current-worker" });
              assert.isTrue(Option.isSome(claim));
              if (Option.isNone(claim)) return yield* Effect.die("Expected unrelated claim");
              assert.strictEqual(claim.value.id, "unrelated-old-effect");
            }),
          );
          assert.deepStrictEqual(
            yield* sql`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_id='reserved-old-effect'`,
            before,
          );
        }),
      true,
    ),
);
it.effect(
  "settle retry cancellation and loss reconciliation cannot mutate a reserved snapshot",
  () =>
    fixture(
      (input) =>
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`UPDATE orchestration_v2_effect_outbox SET status='running',lease_owner='captured-worker',completed_at=NULL WHERE effect_id IN ('reserved-old-effect','unrelated-old-effect')`;
          const before =
            yield* sql`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_id='reserved-old-effect'`;
          yield* input.second(
            Effect.gen(function* () {
              const outbox = yield* Outbox.EffectOutboxV2;
              assert.isFalse(
                yield* outbox.succeed({
                  effectId: "reserved-old-effect",
                  workerId: "captured-worker",
                }),
              );
              assert.isFalse(
                yield* outbox.retry({
                  effectId: "reserved-old-effect",
                  workerId: "captured-worker",
                  error: "synthetic",
                  delayMs: 5,
                }),
              );
              assert.isFalse(
                yield* outbox.fail({
                  effectId: "reserved-old-effect",
                  workerId: "captured-worker",
                  error: "synthetic",
                }),
              );
              assert.deepStrictEqual(
                yield* outbox.cancelUnsettled({
                  threadId,
                  effectTypes: ["terminal.cleanup"],
                  reason: "synthetic",
                }),
                [],
              );
              assert.deepStrictEqual(yield* outbox.reconcileAfterProcessLoss, {
                requeued: 1,
                cancelled: 0,
              });
            }),
          );
          assert.deepStrictEqual(
            yield* sql`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_id='reserved-old-effect'`,
            before,
          );
          assert.deepStrictEqual(
            yield* sql`SELECT status,lease_owner FROM orchestration_v2_effect_outbox WHERE effect_id='unrelated-old-effect'`,
            [{ status: "pending", lease_owner: null }],
          );
        }),
      true,
    ),
);
