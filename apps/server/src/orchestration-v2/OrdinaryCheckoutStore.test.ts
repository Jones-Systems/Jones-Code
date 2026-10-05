import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  OrchestrationV2Command,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as Ordinary from "./OrdinaryCheckoutOwnership.ts";
import { makeOrdinaryCheckoutStore } from "./OrdinaryCheckoutStore.ts";

const database = SqlitePersistenceMemory;
const infrastructure = Layer.mergeAll(
  database,
  EventStore.layer.pipe(Layer.provide(database)),
  ProjectionStore.layer.pipe(Layer.provide(database)),
  EffectOutbox.layer.pipe(Layer.provide(database)),
);
const layer = EventSink.layer.pipe(Layer.provideMerge(infrastructure));
const now = DateTime.makeUnsafe("2026-10-03T00:00:00.000Z");
const projectId = ProjectId.make("project:checkout-admission");
const providerInstanceId = ProviderInstanceId.make("checkout-admission-provider");
const modelSelection = { instanceId: providerInstanceId, model: "fixture" };
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

function thread(threadId: ThreadId): OrchestrationV2AppThread {
  return {
    id: threadId,
    projectId,
    title: "Ownership fixture",
    providerInstanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: "fixture/owner",
    worktreePath: "/fixture/worktree",
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
}

const createFixture = Effect.fn("createCheckoutFixture")(function* (id: string) {
  const sink = yield* EventSink.EventSinkV2;
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT OR IGNORE INTO projection_projects
    (project_id, title, workspace_root, scripts_json, created_at, updated_at)
    VALUES (${projectId}, 'Fixture', '/fixture/project', '[]', ${DateTime.formatIso(now)}, ${DateTime.formatIso(now)})`;
  const owner = thread(ThreadId.make(id));
  yield* sink.write({
    events: [
      {
        id: EventId.make(`event:${id}:created`),
        type: "thread.created",
        threadId: owner.id,
        providerInstanceId,
        occurredAt: now,
        payload: owner,
      },
    ],
  });
  return owner;
});

const prepare = Effect.fn("prepareCheckoutFixture")(function* (
  owner: OrchestrationV2AppThread,
  id: string,
) {
  const store = yield* makeOrdinaryCheckoutStore();
  const command = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)({
    type: "message.dispatch",
    commandId: id,
    threadId: owner.id,
    messageId: `message:${id}`,
    text: "Mutate the owned checkout",
    attachments: [],
    modelSelection,
    dispatchMode: { type: "start_immediately" },
    createdBy: "user",
    creationSource: "web",
  });
  const capture = yield* store.capture({
    command,
    threadId: owner.id,
    projectId,
    branch: owner.branch,
    canonicalProjectRoot: "/fixture/project",
    canonicalCheckoutPath: "/fixture/worktree",
    source: { projectWorkspaceRoot: "/fixture/project", worktreePath: owner.worktreePath },
    leaseId: `lease:${id}`,
  });
  const event: OrchestrationV2DomainEvent = {
    id: EventId.make(`event:${id}:accepted`),
    type: "thread.metadata-updated",
    threadId: owner.id,
    providerInstanceId,
    occurredAt: now,
    payload: { ...owner, title: id },
  };
  return { command, capture, event };
});

const claimFixture = Effect.fn("claimCheckoutFixture")(function* (id: string) {
  const owner = yield* createFixture(`owner:${id}`);
  const fixture = yield* prepare(owner, `command:${id}`);
  const sink = yield* EventSink.EventSinkV2;
  const acceptedAt = yield* DateTime.now;
  yield* sink.commitCommand({
    commandId: fixture.command.commandId,
    threadId: owner.id,
    commandType: fixture.command.type,
    acceptedAt,
    events: [fixture.event],
    ordinaryCheckout: fixture.capture,
    effects: [
      {
        id: `effect:${id}`,
        commandId: fixture.command.commandId,
        threadId: owner.id,
        request: { type: "provider-turn.start", runId: RunId.make(`run:${id}`) },
      },
    ],
  });
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const claimed = yield* outbox.claimNext({ workerId: `worker:${id}`, leaseDurationMs: 60000 });
  if (Option.isNone(claimed) || claimed.value.leaseExpiresAt === null)
    return yield* Effect.die("The fixture did not acquire a physical effect claim.");
  const effect = claimed.value;
  const store = yield* makeOrdinaryCheckoutStore();
  const linked = yield* store.readEffectLink(effect);
  if (linked === null) return yield* Effect.die("The fixture effect has no accepted admission.");
  const source: Ordinary.OrdinaryCheckoutUseSourceV1 = {
    kind: "outbox",
    link: linked.link,
    workerId: `worker:${id}`,
    expectedAttempt: effect.attemptCount,
    leaseExpiresAt: DateTime.makeUnsafe(effect.leaseExpiresAt!),
  };
  const input = {
    operationId: Ordinary.ordinaryCheckoutOutboxOperationIdV1(effect.id, effect.attemptCount),
    admission: linked.link.admission,
    source,
    targetSource: fixture.capture.source,
  };
  return { owner, fixture, store, input, outbox, effect };
});

describe("Ordinary checkout physical use", () => {
  it.effect("reserves once and makes a repeated begin observation-only", () =>
    Effect.gen(function* () {
      const fixture = yield* claimFixture("physical-once");
      const first = yield* fixture.store.beginUse(fixture.input);
      assert.equal(first.status, "reserved");
      const repeated = yield* fixture.store.beginUse(fixture.input);
      assert.equal(repeated.status, "observe_only");
      const entered = yield* fixture.store.revalidateUse(first.record.subject.use);
      assert.equal(entered.state, "started");
      assert.isNotNull(entered.startedAt);
      const sql = yield* SqlClient.SqlClient;
      assert.equal(
        (yield* sql`SELECT operation_id FROM orchestration_v2_worktree_path_admissions`).length,
        1,
      );
    }).pipe(Effect.provide(layer)),
  );
  it.effect("refuses an altered worker before writing a physical reservation", () =>
    Effect.gen(function* () {
      const fixture = yield* claimFixture("physical-stale");
      if (fixture.input.source.kind !== "outbox")
        return yield* Effect.die("Unexpected fixture source");
      const result = yield* Effect.exit(
        fixture.store.beginUse({
          ...fixture.input,
          source: { ...fixture.input.source, workerId: "another-worker" },
        }),
      );
      assert.isTrue(Exit.isFailure(result));
      const sql = yield* SqlClient.SqlClient;
      assert.equal(
        (yield* sql`SELECT operation_id FROM orchestration_v2_worktree_path_admissions`).length,
        0,
      );
    }).pipe(Effect.provide(layer)),
  );
  it.effect("retains an unknown entered use and refuses lease expiry as takeover evidence", () =>
    Effect.gen(function* () {
      const fixture = yield* claimFixture("physical-unknown");
      const first = yield* fixture.store.beginUse(fixture.input);
      const use = first.record.subject.use;
      yield* fixture.store.revalidateUse(use);
      const unknown = yield* fixture.store.markUnknown(
        use,
        "Captured producer endpoint unavailable",
      );
      assert.equal(unknown.state, "unknown");
      assert.isTrue(Exit.isFailure(yield* Effect.exit(fixture.store.revalidateUse(use))));
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE worktree_ownership_leases SET expires_at_ms = 1`;
      const other = yield* createFixture("owner:physical-foreign");
      assert.isTrue(Exit.isFailure(yield* Effect.exit(prepare(other, "command:physical-foreign"))));
      const row = (yield* sql<{
        readonly state: string;
        readonly started_at: string;
        readonly outcome_json: string;
      }>`
        SELECT state, started_at, outcome_json FROM orchestration_v2_worktree_path_admissions`)[0]!;
      assert.equal(row.state, "unknown");
      assert.equal(row.started_at, unknown.startedAt);
      assert.include(row.outcome_json, "Captured producer endpoint unavailable");
      assert.equal((yield* fixture.store.beginUse(fixture.input)).status, "observe_only");
    }).pipe(Effect.provide(layer)),
  );
  it.effect("binds only the original executor and preserves its append-only fact", () =>
    Effect.gen(function* () {
      const fixture = yield* claimFixture("physical-binding");
      const begun = yield* fixture.store.beginUse(fixture.input);
      const use = begun.record.subject.use;
      const binding = yield* fixture.store.bindOutboxExecution(use);
      assert.equal(binding.executor.kind, "actual_outbox_claim");
      assert.equal(
        (yield* fixture.store.bindOutboxExecution(use)).associationId,
        binding.associationId,
      );
      const sql = yield* SqlClient.SqlClient;
      const rows = yield* sql<{
        readonly ordinal: number;
        readonly event_kind: string;
        readonly evidence_json: string;
      }>`
        SELECT ordinal, event_kind, evidence_json FROM orchestration_v2_ordinary_checkout_execution_associations`;
      assert.equal(rows.length, 1);
      assert.equal(rows[0]!.ordinal, 0);
      assert.equal(rows[0]!.event_kind, "bind");
      assert.include(rows[0]!.evidence_json, "t3.ordinary-checkout-execution-liveness/v1");
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`UPDATE orchestration_v2_ordinary_checkout_execution_associations SET event_kind = 'retire'`,
          ),
        ),
      );
      yield* fixture.store.markUnknown(use, "No issued managed actor closure");
      assert.isTrue(Exit.isFailure(yield* Effect.exit(fixture.store.bindOutboxExecution(use))));
      assert.equal(
        (yield* sql`SELECT ordinal FROM orchestration_v2_ordinary_checkout_execution_associations`)
          .length,
        1,
      );
    }).pipe(Effect.provide(layer)),
  );
});

describe("Ordinary checkout EventSink admission", () => {
  it.effect(
    "commits the lease, admission, receipt, projection and exact effect association together",
    () =>
      Effect.gen(function* () {
        const owner = yield* createFixture("owner:atomic");
        const fixture = yield* prepare(owner, "command:atomic");
        const sink = yield* EventSink.EventSinkV2;
        const result = yield* sink.commitCommand({
          commandId: fixture.command.commandId,
          threadId: owner.id,
          commandType: fixture.command.type,
          acceptedAt: now,
          events: [fixture.event],
          ordinaryCheckout: fixture.capture,
          effects: [
            {
              id: "effect:atomic",
              commandId: fixture.command.commandId,
              threadId: owner.id,
              request: { type: "provider-turn.start", runId: RunId.make("run:atomic") },
            },
          ],
        });
        const store = yield* makeOrdinaryCheckoutStore();
        const admission = yield* store.readAdmission(fixture.command.commandId, owner.id);
        assert.isTrue(result.committed);
        assert.isNotNull(admission);
        assert.equal(admission!.receipt.resultSequence, result.receipt.resultSequence);
        assert.equal(admission!.eventBasis[0]!.eventId, fixture.event.id);
        const sql = yield* SqlClient.SqlClient;
        const links =
          yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_effect_links WHERE effect_id = 'effect:atomic'`;
        assert.equal(links.length, 1);
        const titles = yield* sql<{
          readonly title: string;
        }>`SELECT title FROM orchestration_v2_projection_threads WHERE thread_id = ${owner.id}`;
        assert.equal(titles[0]!.title, "command:atomic");
      }).pipe(Effect.provide(layer)),
  );

  it.effect("rolls back all acceptance facts when admission attribution fails", () =>
    Effect.gen(function* () {
      const owner = yield* createFixture("owner:rollback");
      const fixture = yield* prepare(owner, "command:rollback");
      const sink = yield* EventSink.EventSinkV2;
      const result = yield* Effect.exit(
        sink.commitCommand({
          commandId: CommandId.make("command:other-body"),
          threadId: owner.id,
          commandType: fixture.command.type,
          acceptedAt: now,
          events: [fixture.event],
          ordinaryCheckout: fixture.capture,
          effects: [
            {
              id: "effect:rollback",
              commandId: CommandId.make("command:other-body"),
              threadId: owner.id,
              request: { type: "provider-turn.start", runId: RunId.make("run:rollback") },
            },
          ],
        }),
      );
      assert.isTrue(Exit.isFailure(result));
      const sql = yield* SqlClient.SqlClient;
      for (const table of [
        "worktree_ownership_leases",
        "orchestration_command_receipts",
        "orchestration_v2_ordinary_checkout_admissions",
        "orchestration_v2_ordinary_checkout_effect_links",
        "orchestration_v2_effect_outbox",
      ])
        assert.equal((yield* sql.unsafe(`SELECT * FROM ${table}`)).length, 0);
      const events =
        yield* sql`SELECT * FROM orchestration_events WHERE event_id = ${fixture.event.id}`;
      assert.equal(events.length, 0);
      const titles = yield* sql<{
        readonly title: string;
      }>`SELECT title FROM orchestration_v2_projection_threads WHERE thread_id = ${owner.id}`;
      assert.equal(titles[0]!.title, "Ownership fixture");
    }).pipe(Effect.provide(layer)),
  );

  it.effect(
    "refuses another thread on the same canonical path without changing the owner's lease",
    () =>
      Effect.gen(function* () {
        const owner = yield* createFixture("owner:exclusive");
        const fixture = yield* prepare(owner, "command:exclusive");
        const sink = yield* EventSink.EventSinkV2;
        yield* sink.commitCommand({
          commandId: fixture.command.commandId,
          threadId: owner.id,
          commandType: fixture.command.type,
          acceptedAt: now,
          events: [fixture.event],
          effects: [],
          ordinaryCheckout: fixture.capture,
        });
        const sql = yield* SqlClient.SqlClient;
        const before = yield* sql`SELECT * FROM worktree_ownership_leases`;
        const contender = yield* createFixture("owner:contender");
        const refused = yield* Effect.exit(prepare(contender, "command:contender"));
        assert.isTrue(Exit.isFailure(refused));
        assert.deepEqual(yield* sql`SELECT * FROM worktree_ownership_leases`, before);
        assert.equal((yield* sql`SELECT * FROM orchestration_command_receipts`).length, 1);
      }).pipe(Effect.provide(layer)),
  );

  it.effect("rejects a changed command on replay and retains immutable admission bytes", () =>
    Effect.gen(function* () {
      const owner = yield* createFixture("owner:replay");
      const fixture = yield* prepare(owner, "command:replay");
      const sink = yield* EventSink.EventSinkV2;
      yield* sink.commitCommand({
        commandId: fixture.command.commandId,
        threadId: owner.id,
        commandType: fixture.command.type,
        acceptedAt: now,
        events: [fixture.event],
        effects: [],
        ordinaryCheckout: fixture.capture,
      });
      const sql = yield* SqlClient.SqlClient;
      const before = yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_admissions`;
      if (fixture.command.type !== "message.dispatch")
        return yield* Effect.die("Unexpected fixture command");
      const changed = yield* (yield* makeOrdinaryCheckoutStore()).capture({
        command: {
          ...fixture.command,
          text: "Changed payload",
          messageId: MessageId.make("message:changed"),
        },
        threadId: owner.id,
        projectId,
        branch: owner.branch,
        canonicalProjectRoot: "/fixture/project",
        canonicalCheckoutPath: "/fixture/worktree",
        source: fixture.capture.source,
        leaseId: fixture.capture.capture.lease.leaseId,
      });
      const refusal = yield* Effect.exit(sink.validateOrdinaryCheckoutReplay!(changed));
      assert.isTrue(Exit.isFailure(refusal));
      assert.equal(
        encodeJson(yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_admissions`),
        encodeJson(before),
      );
    }).pipe(Effect.provide(layer)),
  );
});
