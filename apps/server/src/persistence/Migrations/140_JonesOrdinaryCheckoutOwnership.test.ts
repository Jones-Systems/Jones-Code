import { assert, it } from "@effect/vitest";
import { OrchestrationV2Command } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { canonicalJson } from "../../orchestration-v2/CanonicalJson.ts";
import {
  OrdinaryCheckoutAdmissionV1,
  OrdinaryCheckoutCaptureV1,
  OrdinaryCheckoutEffectLinkV1,
  OrdinaryCheckoutTargetTransitionV1,
  OrdinaryCheckoutUseV1,
  ordinaryApplicationIncarnationV1,
  ordinaryCheckoutAdmissionIdV1,
  ordinaryCheckoutAdmissionMatchesV1,
  ordinaryCheckoutAdmissionRefV1,
  ordinaryCheckoutCaptureMatchesV1,
  ordinaryCheckoutCommandDigestV1,
} from "../../orchestration-v2/OrdinaryCheckoutOwnership.ts";
import { migrationManifest, runMigrations } from "../Migrations.ts";

class SyntheticAcceptanceFailure extends Schema.TaggedError<SyntheticAcceptanceFailure>()(
  "SyntheticAcceptanceFailure",
  { cause: Schema.Defect() },
) {}

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const timestamp = "2026-10-03T00:00:00.000Z";
const birth = {
  kind: "application_v2_thread_birth",
  threadId: "thread:ordinary-fixture",
  eventId: "event:ordinary-fixture:birth",
  sequence: 1,
};
const command = Schema.encodeSync(OrchestrationV2Command)(
  Schema.decodeUnknownSync(OrchestrationV2Command, { onExcessProperty: "error" })({
    type: "message.dispatch",
    commandId: "command:ordinary-fixture",
    threadId: birth.threadId,
    messageId: "message:ordinary-fixture",
    text: "Continue the fixture.",
    attachments: [],
    modelSelection: { instanceId: "fixture-codex", model: "fixture-model" },
    dispatchMode: { type: "defer_start" },
    createdBy: "agent",
    creationSource: "server",
  }),
);
if (command.type !== "message.dispatch")
  throw new Error("Fixture command must be message.dispatch");
const capture = Schema.decodeUnknownSync(OrdinaryCheckoutCaptureV1, { onExcessProperty: "error" })({
  version: 1,
  commandId: command.commandId,
  commandType: command.type,
  canonicalCommand: command,
  commandDigest: ordinaryCheckoutCommandDigestV1(command),
  origin: { kind: "command" },
  threadId: birth.threadId,
  applicationBirth: birth,
  projectId: "project:ordinary-fixture",
  canonicalProjectRoot: "/fixture/repo",
  canonicalCheckoutPath: "/fixture/worktrees/original",
  branch: "fixture-temporary",
  lease: {
    resourcePath: "/fixture/worktrees/original",
    leaseId: "lease:ordinary-fixture",
    ownerThreadId: birth.threadId,
    ownerIncarnation: JSON.stringify([
      "t3.orchestration-v2.thread-birth/v1",
      birth.eventId,
      birth.sequence,
    ]),
    branch: "fixture-temporary",
    acquiredAtMs: 1,
    renewedAtMs: 1,
    expiresAtMs: 300001,
  },
});
const admission = Schema.decodeUnknownSync(OrdinaryCheckoutAdmissionV1, {
  onExcessProperty: "error",
})({
  version: 1,
  admissionId: ordinaryCheckoutAdmissionIdV1(capture),
  capture,
  receipt: {
    commandId: capture.commandId,
    threadId: capture.threadId,
    commandType: capture.commandType,
    acceptedAt: timestamp,
    resultSequence: 6,
    status: "accepted",
    error: null,
  },
  eventBasis: [
    {
      eventId: "event:ordinary-fixture:message",
      sequence: 2,
      threadId: birth.threadId,
      commandId: capture.commandId,
      eventType: "message.updated",
    },
    {
      eventId: "event:ordinary-fixture:prepared",
      sequence: 4,
      threadId: birth.threadId,
      commandId: capture.commandId,
      eventType: "turn-item.updated",
    },
  ],
  run: {
    runId: "run:ordinary-fixture",
    runAttemptId: "attempt:ordinary-fixture",
    nodeId: "node:ordinary-fixture",
    messageId: command.messageId,
  },
  recordedAt: timestamp,
});
const reference = ordinaryCheckoutAdmissionRefV1(admission);
const admissionRow = {
  admission_id: admission.admissionId,
  command_id: capture.commandId,
  thread_id: capture.threadId,
  admission_sha256: reference.admissionSha256,
  admission_json: canonicalJson(Schema.encodeSync(OrdinaryCheckoutAdmissionV1)(admission)),
  recorded_at: timestamp,
};
const link = Schema.decodeUnknownSync(OrdinaryCheckoutEffectLinkV1, { onExcessProperty: "error" })({
  version: 1,
  effectId: "effect:ordinary-fixture:later-start",
  commandId: "command:ordinary-fixture:system-start",
  threadId: capture.threadId,
  requestSha256: ordinaryCheckoutCommandDigestV1({ fixture: "actual request digest seam" }),
  admission: reference,
  recordedAt: timestamp,
});
const linkRow = {
  effect_id: link.effectId,
  admission_id: admission.admissionId,
  link_json: canonicalJson(Schema.encodeSync(OrdinaryCheckoutEffectLinkV1)(link)),
  recorded_at: timestamp,
};
const preparedUse = Schema.decodeUnknownSync(OrdinaryCheckoutUseV1, { onExcessProperty: "error" })({
  version: 1,
  kind: "ordinary_checkout_use",
  operationId: "operation:ordinary-fixture:rename",
  admission: reference,
  source: { kind: "prepared_run", admission: reference, preparation: admission.run },
  lease: capture.lease,
});
const transition = Schema.decodeUnknownSync(OrdinaryCheckoutTargetTransitionV1, {
  onExcessProperty: "error",
})({
  version: 1,
  operationId: preparedUse.operationId,
  admission: reference,
  source: preparedUse.source,
  canonicalCheckoutPath: capture.canonicalCheckoutPath,
  leaseId: capture.lease.leaseId,
  applicationBirth: birth,
  beforeLease: capture.lease,
  afterLease: { ...capture.lease, branch: "fixture-final" },
  fromBranch: capture.branch,
  toBranch: "fixture-final",
  evidence: { fixture: "synthetic verified-transition storage carrier" },
  recordedAt: timestamp,
});
const transitionRow = {
  operation_id: transition.operationId,
  admission_id: admission.admissionId,
  transition_json: canonicalJson(Schema.encodeSync(OrdinaryCheckoutTargetTransitionV1)(transition)),
  recorded_at: timestamp,
};
const operationRow = {
  operation_id: preparedUse.operationId,
  canonical_path: capture.canonicalCheckoutPath,
  kind: "native_operation",
  subject_json: canonicalJson(Schema.encodeSync(OrdinaryCheckoutUseV1)(preparedUse)),
  state: "reserved",
  started_at: null,
  outcome_json: null,
  recorded_at: timestamp,
  updated_at: timestamp,
};

const insertReceipt = () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_command_receipts
    (command_id, command_type, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
    VALUES (${capture.commandId}, ${capture.commandType}, 'thread', ${capture.threadId}, ${timestamp}, 6, 'accepted')`;
  });
const insertLaterEffect = () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_v2_effect_outbox
    (effect_id, command_id, thread_id, effect_type, payload_json, status, attempt_count, available_at, created_at, updated_at)
    VALUES (${link.effectId}, ${link.commandId}, ${link.threadId}, 'provider-turn.start', '{}', 'pending', 0, ${timestamp}, ${timestamp}, ${timestamp})`;
  });

it.effect(
  "delegated child admission binds its own birth and checkout to the actual outer parent command and receipt",
  () =>
    Effect.gen(function* () {
      const outer = yield* Schema.decodeUnknownEffect(OrchestrationV2Command, {
        onExcessProperty: "error",
      })({
        type: "delegated_task.request",
        commandId: "command:delegated-fixture",
        parentThreadId: "thread:parent-fixture",
        parentRunId: "run:parent-fixture",
        parentNodeId: "node:parent-fixture",
        task: "Inspect the fixture.",
        title: "Fixture child",
        modelSelection: { instanceId: "fixture-codex", model: "fixture-model" },
        runtimeMode: "approval-required",
        interactionMode: "plan",
        createdBy: "user",
        creationSource: "web",
      }).pipe(Effect.orDie);
      const encodedOuter = yield* Schema.encodeEffect(OrchestrationV2Command)(outer).pipe(
        Effect.orDie,
      );
      if (outer.type !== "delegated_task.request")
        throw new Error("Expected delegated command fixture");
      const childCapture = {
        ...capture,
        commandId: outer.commandId,
        commandType: outer.type,
        canonicalCommand: encodedOuter,
        commandDigest: ordinaryCheckoutCommandDigestV1(encodedOuter),
        origin: { kind: "delegated_child" as const, parentThreadId: outer.parentThreadId },
      };
      const childAdmission = {
        ...admission,
        admissionId: ordinaryCheckoutAdmissionIdV1(childCapture),
        capture: childCapture,
        receipt: {
          ...admission.receipt,
          commandId: outer.commandId,
          commandType: outer.type,
          threadId: outer.parentThreadId,
        },
        eventBasis: [
          {
            eventId: capture.applicationBirth.eventId,
            sequence: capture.applicationBirth.sequence,
            threadId: capture.threadId,
            commandId: outer.commandId,
            eventType: "thread.created",
          },
        ],
      };
      assert.isTrue(ordinaryCheckoutAdmissionMatchesV1(childAdmission));
      assert.notEqual(childAdmission.receipt.threadId, childCapture.threadId);
      assert.isFalse(
        ordinaryCheckoutAdmissionMatchesV1({
          ...childAdmission,
          receipt: { ...childAdmission.receipt, threadId: childCapture.threadId },
        }),
      );
      assert.isFalse(
        ordinaryCheckoutCaptureMatchesV1({
          ...childCapture,
          lease: { ...childCapture.lease, ownerThreadId: outer.parentThreadId },
        }),
      );
    }),
);

it.effect(
  "runless prepared launch carries actual accepted creation and target evidence without inventing run or outbox claim",
  () =>
    Effect.gen(function* () {
      const creation = yield* Schema.decodeUnknownEffect(OrchestrationV2Command, {
        onExcessProperty: "error",
      })({
        type: "thread.create",
        commandId: "command:runless-fixture",
        threadId: capture.threadId,
        projectId: capture.projectId,
        title: "Runless fixture",
        modelSelection: { instanceId: "fixture-codex", model: "fixture-model" },
        runtimeMode: "approval-required",
        interactionMode: "plan",
        branch: capture.branch,
        worktreePath: capture.canonicalCheckoutPath,
        createdBy: "user",
        creationSource: "web",
      }).pipe(Effect.orDie);
      const encodedCreation = yield* Schema.encodeEffect(OrchestrationV2Command)(creation).pipe(
        Effect.orDie,
      );
      if (creation.type !== "thread.create")
        throw new Error("Expected actual thread creation fixture");
      const runlessBirth = {
        ...capture.applicationBirth,
        eventId: admission.eventBasis[0]!.eventId,
      };
      const runlessCapture = {
        ...capture,
        commandId: creation.commandId,
        commandType: creation.type,
        canonicalCommand: encodedCreation,
        commandDigest: ordinaryCheckoutCommandDigestV1(encodedCreation),
        applicationBirth: runlessBirth,
        lease: {
          ...capture.lease,
          ownerIncarnation: ordinaryApplicationIncarnationV1(runlessBirth),
        },
      };
      const createEvent = {
        eventId: runlessBirth.eventId,
        sequence: runlessBirth.sequence,
        threadId: runlessCapture.threadId,
        commandId: creation.commandId,
        eventType: "thread.created",
      };
      const runlessAdmission = {
        ...admission,
        admissionId: ordinaryCheckoutAdmissionIdV1(runlessCapture),
        capture: runlessCapture,
        receipt: {
          ...admission.receipt,
          commandId: creation.commandId,
          commandType: creation.type,
          resultSequence: 1,
        },
        eventBasis: [createEvent],
        run: null,
      };
      assert.isTrue(ordinaryCheckoutAdmissionMatchesV1(runlessAdmission));
      const runlessRef = ordinaryCheckoutAdmissionRefV1(runlessAdmission);
      const rawUse = {
        version: 1,
        kind: "ordinary_checkout_use",
        operationId: "operation:runless-fixture",
        admission: runlessRef,
        source: {
          kind: "prepared_launch",
          admission: runlessRef,
          preparationCommandId: creation.commandId,
          preparationEvent: createEvent,
          applicationBirth: runlessBirth,
          projectId: capture.projectId,
          canonicalProjectRoot: capture.canonicalProjectRoot,
          canonicalCheckoutPath: capture.canonicalCheckoutPath,
          branch: capture.branch,
        },
        lease: runlessCapture.lease,
      };
      const decoded = yield* Schema.decodeUnknownEffect(OrdinaryCheckoutUseV1, {
        onExcessProperty: "error",
      })(rawUse).pipe(Effect.orDie);
      assert.equal(decoded.source.kind, "prepared_launch");
      assert.equal(runlessAdmission.run, null);
      assert.throws(() =>
        Schema.decodeUnknownSync(OrdinaryCheckoutUseV1, { onExcessProperty: "error" })({
          ...rawUse,
          source: { ...rawUse.source, workerId: "invented", expectedAttempt: 1 },
        }),
      );
    }),
);

it.effect(
  "Jones140 keeps upstream56 explicit replay separate and starts with four empty private companions",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 56 });
      const upstream = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
      assert.deepEqual(
        upstream.map((row): readonly [unknown, unknown] => [row.migration_id, row.name]),
        migrationManifest,
      );
      assert.deepEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE name LIKE 'orchestration_v2_ordinary_checkout_%'`,
        [],
      );
      yield* runMigrations();
      assert.deepEqual(yield* runMigrations(), []);
      assert.deepEqual(
        yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
        upstream,
      );
      assert.deepEqual(
        yield* sql`SELECT migration_id, name FROM jones_sql_migrations WHERE migration_id = 140`,
        [{ migration_id: 140, name: "OrdinaryCheckoutOwnership" }],
      );
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_admissions`, []);
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_effect_links`,
        [],
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_target_transitions`,
        [],
      );
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_commands`, []);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "deferred admission requires the original receipt and retains zero effects until a later fixed effect is linked",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_ordinary_checkout_admissions ${sql.insert(admissionRow)}`,
        ))._tag,
        "Failure",
      );
      yield* insertReceipt();
      for (const invalid of [
        { admission_id: null },
        { admission_id: "A".repeat(64) },
        { admission_id: "g".repeat(64) },
        { admission_sha256: "a".repeat(63) },
        { thread_id: "" },
        { admission_json: "invalid" },
        { admission_json: "[]" },
        { recorded_at: "" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_ordinary_checkout_admissions ${sql.insert({ ...admissionRow, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_admissions ${sql.insert(admissionRow)}`;
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_ordinary_checkout_effect_links ${sql.insert(linkRow)}`,
        ))._tag,
        "Failure",
      );
      yield* insertLaterEffect();
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_effect_links ${sql.insert(linkRow)}`;
      assert.notEqual(link.commandId, capture.commandId);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_admissions`, [
        admissionRow,
      ]);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_effect_links`, [
        linkRow,
      ]);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "original admission association cannot be changed, deleted or replaced by primary ID or command and subject",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* insertReceipt();
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_admissions ${sql.insert(admissionRow)}`;
      for (const changed of [
        { admission_id: "b".repeat(64) },
        { command_id: "changed" },
        { thread_id: "changed" },
        { admission_sha256: "b".repeat(64) },
        { admission_json: "{}" },
        { recorded_at: "changed" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_ordinary_checkout_admissions ${sql.update(changed)}`,
          ))._tag,
          "Failure",
        );
      for (const mutation of [
        sql`DELETE FROM orchestration_v2_ordinary_checkout_admissions`,
        sql`DELETE FROM orchestration_command_receipts WHERE command_id = ${capture.commandId}`,
        sql`INSERT OR REPLACE INTO orchestration_v2_ordinary_checkout_admissions ${sql.insert({ ...admissionRow, admission_json: "{}" })}`,
        sql`INSERT OR REPLACE INTO orchestration_v2_ordinary_checkout_admissions ${sql.insert({ ...admissionRow, admission_id: "b".repeat(64) })}`,
      ])
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_admissions`, [
        admissionRow,
      ]);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "effect links constrain parents and objects and retain original associations through replay or replacement attempts",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* insertReceipt();
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_admissions ${sql.insert(admissionRow)}`;
      yield* insertLaterEffect();
      for (const invalid of [
        { effect_id: "missing" },
        { admission_id: "b".repeat(64) },
        { link_json: "invalid" },
        { link_json: "[]" },
        { recorded_at: "" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_ordinary_checkout_effect_links ${sql.insert({ ...linkRow, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_effect_links ${sql.insert(linkRow)}`;
      for (const changed of [
        { effect_id: "changed" },
        { admission_id: "b".repeat(64) },
        { link_json: "{}" },
        { recorded_at: "changed" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_ordinary_checkout_effect_links ${sql.update(changed)}`,
          ))._tag,
          "Failure",
        );
      for (const mutation of [
        sql`DELETE FROM orchestration_v2_ordinary_checkout_effect_links`,
        sql`DELETE FROM orchestration_v2_effect_outbox WHERE effect_id = ${link.effectId}`,
        sql`INSERT OR REPLACE INTO orchestration_v2_ordinary_checkout_effect_links ${sql.insert({ ...linkRow, link_json: "{}" })}`,
      ])
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_effect_links`, [
        linkRow,
      ]);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "qualified transition storage references original admission and existing operation without changing historical target",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* insertReceipt();
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_admissions ${sql.insert(admissionRow)}`;
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_ordinary_checkout_target_transitions ${sql.insert(transitionRow)}`,
        ))._tag,
        "Failure",
      );
      yield* sql`INSERT INTO orchestration_v2_worktree_path_admissions ${sql.insert(operationRow)}`;
      for (const invalid of [
        { operation_id: "missing" },
        { admission_id: "b".repeat(64) },
        { transition_json: "invalid" },
        { transition_json: "[]" },
        { recorded_at: "" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_ordinary_checkout_target_transitions ${sql.insert({ ...transitionRow, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_target_transitions ${sql.insert(transitionRow)}`;
      for (const changed of [
        { operation_id: "changed" },
        { admission_id: "b".repeat(64) },
        { transition_json: "{}" },
        { recorded_at: "changed" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_ordinary_checkout_target_transitions ${sql.update(changed)}`,
          ))._tag,
          "Failure",
        );
      for (const mutation of [
        sql`DELETE FROM orchestration_v2_ordinary_checkout_target_transitions`,
        sql`INSERT OR REPLACE INTO orchestration_v2_ordinary_checkout_target_transitions ${sql.insert({ ...transitionRow, transition_json: "{}" })}`,
      ])
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_admissions`, [
        admissionRow,
      ]);
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_target_transitions`,
        [transitionRow],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "failed outer acceptance rolls back receipt, admission, effect association and transition reservation together",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      const injectedCause = new Error("synthetic outer transaction failure");
      const result = yield* Effect.result(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* insertReceipt();
            yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_admissions ${sql.insert(admissionRow)}`;
            yield* insertLaterEffect();
            yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_effect_links ${sql.insert(linkRow)}`;
            yield* sql`INSERT INTO orchestration_v2_worktree_path_admissions ${sql.insert(operationRow)}`;
            yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_target_transitions ${sql.insert(transitionRow)}`;
            return yield* Effect.fail(new SyntheticAcceptanceFailure({ cause: injectedCause }));
          }),
        ),
      );
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.equal(result.failure._tag, "SyntheticAcceptanceFailure");
        if (result.failure._tag === "SyntheticAcceptanceFailure") {
          assert.strictEqual(result.failure.cause, injectedCause);
        }
      }
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_worktree_path_admissions`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_admissions`, []);
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_effect_links`,
        [],
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_target_transitions`,
        [],
      );
    }).pipe(Effect.provide(memory)),
);
const laterCommand = Schema.encodeSync(OrchestrationV2Command)(
  Schema.decodeUnknownSync(OrchestrationV2Command, { onExcessProperty: "error" })({
    type: "queued-run.reorder",
    commandId: "command:ordinary-fixture:later-reorder",
    threadId: capture.threadId,
    runId: "run:ordinary-fixture",
    beforeRunId: null,
  }),
);
if (laterCommand.type !== "queued-run.reorder")
  throw new Error("Expected actual later reorder command fixture");
const laterCommandRow = {
  command_id: laterCommand.commandId,
  thread_id: laterCommand.threadId,
  admission_id: admission.admissionId,
  canonical_command_json: canonicalJson(laterCommand),
  command_digest: ordinaryCheckoutCommandDigestV1(laterCommand),
  recorded_at: timestamp,
};
const insertLaterReceipt = () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_command_receipts
    (command_id, command_type, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
    VALUES (${laterCommand.commandId}, ${laterCommand.type}, 'thread', ${capture.threadId}, ${timestamp}, 7, 'accepted')`;
  });

it.effect(
  "effectless later canonical command requires actual receipt and original admission without manufacturing an effect",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* insertReceipt();
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_admissions ${sql.insert(admissionRow)}`;
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_ordinary_checkout_commands ${sql.insert(laterCommandRow)}`,
        ))._tag,
        "Failure",
      );
      yield* insertLaterReceipt();
      for (const invalid of [
        { command_id: null },
        { command_id: "missing-receipt" },
        { thread_id: null },
        { thread_id: "" },
        { admission_id: null },
        { admission_id: "b".repeat(64) },
        { canonical_command_json: null },
        { canonical_command_json: "invalid" },
        { canonical_command_json: "[]" },
        { canonical_command_json: "null" },
        { canonical_command_json: '"text"' },
        { command_digest: null },
        { command_digest: "A".repeat(64) },
        { command_digest: "g".repeat(64) },
        { command_digest: "a".repeat(63) },
        { command_digest: "a".repeat(65) },
        { recorded_at: null },
        { recorded_at: "" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_ordinary_checkout_commands ${sql.insert({ ...laterCommandRow, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_commands ${sql.insert(laterCommandRow)}`;
      assert.notEqual(laterCommand.commandId, capture.commandId);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_commands`, [
        laterCommandRow,
      ]);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_admissions`, [
        admissionRow,
      ]);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_effect_links`,
        [],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "later canonical command association is permanent across changed-body composite replacement with recursive triggers off",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* insertReceipt();
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_admissions ${sql.insert(admissionRow)}`;
      yield* insertLaterReceipt();
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_commands ${sql.insert(laterCommandRow)}`;
      const changedCommand = { ...laterCommand, beforeRunId: "run:different-fixture" };
      const changedRow = {
        ...laterCommandRow,
        canonical_command_json: canonicalJson(changedCommand),
        command_digest: ordinaryCheckoutCommandDigestV1(changedCommand),
      };
      for (const changed of [
        { command_id: "changed" },
        { thread_id: "changed" },
        { admission_id: "b".repeat(64) },
        { canonical_command_json: changedRow.canonical_command_json },
        { command_digest: changedRow.command_digest },
        { recorded_at: "changed" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_ordinary_checkout_commands ${sql.update(changed)}`,
          ))._tag,
          "Failure",
        );
      for (const mutation of [
        sql`DELETE FROM orchestration_v2_ordinary_checkout_commands`,
        sql`DELETE FROM orchestration_command_receipts WHERE command_id = ${laterCommand.commandId}`,
        sql`INSERT INTO orchestration_v2_ordinary_checkout_commands ${sql.insert(laterCommandRow)}`,
        sql`INSERT OR REPLACE INTO orchestration_v2_ordinary_checkout_commands ${sql.insert(changedRow)}`,
      ])
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_commands`, [
        laterCommandRow,
      ]);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_admissions`, [
        admissionRow,
      ]);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "failed later acceptance rolls back its canonical companion and receipt while preserving original admission",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* insertReceipt();
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_admissions ${sql.insert(admissionRow)}`;
      const injectedCause = new Error("synthetic later acceptance failure");
      const result = yield* Effect.result(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* insertLaterReceipt();
            yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_commands ${sql.insert(laterCommandRow)}`;
            return yield* Effect.fail(new SyntheticAcceptanceFailure({ cause: injectedCause }));
          }),
        ),
      );
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.equal(result.failure._tag, "SyntheticAcceptanceFailure");
        if (result.failure._tag === "SyntheticAcceptanceFailure") {
          assert.strictEqual(result.failure.cause, injectedCause);
        }
      }
      assert.deepEqual(yield* sql`SELECT command_id FROM orchestration_command_receipts`, [
        { command_id: capture.commandId },
      ]);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_commands`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_admissions`, [
        admissionRow,
      ]);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_effect_links`,
        [],
      );
    }).pipe(Effect.provide(memory)),
);
