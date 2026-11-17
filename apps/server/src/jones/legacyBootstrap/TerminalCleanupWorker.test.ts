import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  OrchestrationV2AppThreadJson,
  OrchestrationV2DomainEventJson,
  ProjectId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Cause from "effect/Cause";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { legacyBootstrapCreateCommandId } from "../../orchestration-v2/LegacyBootstrap.ts";
import {
  RecordedAppThreadJson,
  RecordedStoredLifecycleEvent,
} from "../../orchestration-v2/RecordedTypes.ts";
import * as TerminalManager from "../../terminal/Manager.ts";
import migration from "../persistence/Migrations/161_JonesLegacyBootstrap.ts";
import * as M from "./RecordedMetadata.ts";
import * as Storage from "./Storage.ts";
import * as S from "./StorageSchema.ts";
import * as Worker from "./TerminalCleanupWorker.ts";
import * as Daemon from "../../orchestration-v2/EffectWorker.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const time = DateTime.makeUnsafe("2026-11-17T11:00:00.000Z");
const stamp = DateTime.formatIso(time);
const threadId = ThreadId.make("terminal-worker:thread");
const projectId = ProjectId.make("terminal-worker:project");
const runId = RunId.make("terminal-worker:run");
const releaseCommandId = CommandId.make("terminal-worker:C");
const createCommandId = legacyBootstrapCreateCommandId(threadId, releaseCommandId);
const commandId = CommandId.make("terminal-worker:D");
const effectId = "terminal-worker:cleanup";
const control: S.TerminalCleanup["control"] = {
  version: 1,
  policy: {
    version: 1,
    threadId,
    projectId,
    runId,
    messageId: MessageId.make("terminal-worker:message"),
    createCommandId,
    birthCommandId: CommandId.make(`${createCommandId}:initial-message`),
    releaseCommandId,
    ownsNewThread: true,
    payloadHash: "terminal-worker:payload",
  },
  threadId,
  runId,
  claimEventId: EventId.make("terminal-worker:claim"),
  claimSequence: 1,
  claimReceiptSequence: 1,
  birthEventId: EventId.make("terminal-worker:birth"),
  birthSequence: 2,
  birthReceiptSequence: 2,
  preparationGeneration: "preparation-original",
  terminalId: "terminal-original",
  generation: "generation-original",
};
const thread = Schema.decodeSync(RecordedAppThreadJson)({
  id: threadId,
  projectId,
  createdBy: "user",
  creationSource: "web",
  title: "Synthetic cleanup",
  providerInstanceId: "codex",
  modelSelection: { instanceId: "codex", model: "test" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: null,
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
  forkedFrom: null,
  createdAt: stamp,
  updatedAt: stamp,
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  lastVisitedAt: null,
  deletedAt: stamp,
});
const deleted = Schema.decodeSync(RecordedStoredLifecycleEvent)({
  sequence: 3,
  commandId,
  event: {
    id: "terminal-worker:deleted",
    threadId,
    type: "thread.deleted",
    occurredAt: time,
    payload: thread,
  },
});
const encodeThread = Schema.encodeEffect(Schema.fromJsonString(OrchestrationV2AppThreadJson));
const encodeEvent = Schema.encodeEffect(OrchestrationV2DomainEventJson);
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
type Close = TerminalManager.TerminalManager["Service"]["close"];
type BoundClose = (
  input: Parameters<Close>[0],
  options?: Parameters<Close>[1],
) => ReturnType<Close>;

const fixture = Effect.fn("TerminalCleanupWorkerFixture.make")(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT PRIMARY KEY,payload_json TEXT NOT NULL)`;
  yield* sql`CREATE TABLE orchestration_command_receipts(command_id TEXT PRIMARY KEY,aggregate_kind TEXT,aggregate_id TEXT,command_type TEXT,accepted_at TEXT,result_sequence INTEGER,status TEXT,error TEXT)`;
  yield* sql`CREATE TABLE orchestration_events(sequence INTEGER PRIMARY KEY,event_id TEXT UNIQUE,command_id TEXT,stream_id TEXT,aggregate_kind TEXT,event_type TEXT,occurred_at TEXT,payload_json TEXT,metadata_json TEXT,application_event_version INTEGER)`;
  yield* sql`CREATE INDEX idx_terminal_worker_command ON orchestration_events(command_id)`;
  yield* migration;
  yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES (${threadId},${yield* encodeThread(thread)})`;
  const event = yield* encodeEvent(deleted.event);
  yield* sql`INSERT INTO orchestration_events VALUES (3,${event.id},${commandId},${threadId},'thread',${event.type},${stamp},${yield* encodeJson(event.payload)},'{}',2)`;
  const storage = Storage.make(sql);
  const command = {
    type: "legacy-bootstrap.guard-rejection-delete",
    commandId,
    threadId,
    runId,
    legacyBootstrap: control.policy,
    legacyOwnedControl: control,
  };
  yield* sql.withTransaction(
    Effect.gen(function* () {
      const writer = yield* storage.forTransaction;
      yield* writer.recordDelete(
        {
          commandId,
          threadId,
          projectId,
          threadCreatedAt: stamp,
          kind: "delete",
          bindingJson: M.canonical(command),
        },
        {
          commandId,
          threadId,
          commandType: command.type,
          acceptedAt: time,
          status: "accepted",
          error: null,
          resultSequence: 3,
        },
        [deleted],
      );
      yield* writer.createTerminalCleanup({ effectId, commandId, threadId, control });
    }),
  );
  let sequence = 0;
  const random = SecureRandom.make((size) => {
    const bytes = new Uint8Array(size);
    bytes[0] = ++sequence;
    return bytes;
  });
  const makeWorker = (close: BoundClose) => {
    function closePort(input: Parameters<Close>[0]): Effect.Effect<void>;
    function closePort(
      input: Parameters<Close>[0],
      options: Parameters<Close>[1],
    ): ReturnType<Close>;
    function closePort(
      input: Parameters<Close>[0],
      options?: Parameters<Close>[1],
    ): ReturnType<Close> {
      if (options?.legacyOwnedControl === undefined)
        return Effect.die("Unbound close is forbidden in this fixture");
      return close(input, options);
    }
    return Worker.TerminalCleanupWorker.pipe(
      Effect.provide(
        Worker.layer.pipe(
          Layer.provide(Layer.mock(TerminalManager.TerminalManager)({ close: closePort })),
          Layer.provide(Layer.succeed(SecureRandom.SecureRandom, random)),
        ),
      ),
    );
  };
  const calls: Array<{ input: Parameters<Close>[0]; options: Parameters<Close>[1] }> = [];
  const confirmedClose: BoundClose = (input, options?) =>
    Effect.gen(function* () {
      calls.push({ input, options });
      assert.isTrue(Option.isNone(yield* Effect.serviceOption(sql.transactionService)));
      const retained = yield* storage.readTerminalCleanup(effectId).pipe(Effect.orDie);
      assert.equal(retained?.state, "started");
      assert.equal(retained?.revision, 1);
      assert.isNotNull(retained?.attemptId);
      assert.deepEqual(input, { threadId, terminalId: control.terminalId, deleteHistory: true });
      assert.deepEqual(options, { legacyOwnedControl: control });
    });
  return { sql, storage, makeWorker, calls, confirmedClose };
});

describe("private legacy terminal cleanup worker", () => {
  it.effect(
    "commits one exact claim before close, finishes in a new transaction and never replays",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const worker = yield* f.makeWorker(f.confirmedClose);
        assert.deepEqual(yield* worker.drain(1), {
          selected: 1,
          claimed: 1,
          closed: 1,
          contended: 0,
          unresolved: 0,
        });
        const retained = yield* f.storage.readTerminalCleanup(effectId);
        assert.equal(retained?.state, "succeeded");
        assert.equal(retained?.outcome, "closed");
        assert.equal(retained?.revision, 2);
        yield* worker.drain();
        assert.equal(f.calls.length, 1);
      }).pipe(Effect.provide(memory)),
  );

  it.effect("a real SQLite claim rollback prevents physical entry and leaves pending", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.sql`CREATE TRIGGER fail_claim BEFORE UPDATE OF state ON jones_legacy_bootstrap_terminal_cleanup WHEN NEW.state = 'started' BEGIN SELECT RAISE(ABORT,'synthetic claim rollback'); END`;
      const worker = yield* f.makeWorker(f.confirmedClose);
      const result = yield* worker.runOnce().pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(result));
      assert.equal(f.calls.length, 0);
      assert.equal((yield* f.storage.readTerminalCleanup(effectId))?.state, "pending");
      yield* f.sql`DROP TRIGGER fail_claim`;
      yield* worker.runOnce();
      assert.equal(f.calls.length, 1);
    }).pipe(Effect.provide(memory)),
  );

  it.effect("concurrent workers use actual storage CAS and have only one physical winner", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const first = yield* f.makeWorker(f.confirmedClose);
      const second = yield* f.makeWorker(f.confirmedClose);
      const results = yield* Effect.all([first.runOnce(), second.runOnce()], { concurrency: 2 });
      assert.equal(
        results.reduce((sum, result) => sum + result.closed, 0),
        1,
      );
      assert.equal(f.calls.length, 1);
      assert.equal((yield* f.storage.readTerminalCleanup(effectId))?.state, "succeeded");
    }).pipe(Effect.provide(memory)),
  );

  it.effect.each(["failure", "defect", "interrupt"] as const)(
    "%s after started becomes unknown and never retries",
    (mode) =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const worker = yield* f.makeWorker((input, options?) =>
          f.confirmedClose(input, options).pipe(
            Effect.andThen(
              mode === "interrupt"
                ? Effect.interrupt
                : mode === "defect"
                  ? Effect.die("synthetic physical defect")
                  : Effect.fail(
                      new TerminalManager.LegacyTerminalControlError({
                        operation: "close",
                        detail: "synthetic missing owner",
                      }),
                    ),
            ),
          ),
        );
        const result = yield* worker.runOnce().pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(result));
        if (mode === "interrupt" && Exit.isFailure(result))
          assert.isTrue(Cause.hasInterrupts(result.cause));
        const retained = yield* f.storage.readTerminalCleanup(effectId);
        assert.equal(retained?.state, "unknown");
        assert.equal(
          retained?.outcome,
          mode === "interrupt"
            ? "interrupted"
            : mode === "failure"
              ? "owner-unavailable"
              : "unconfirmed",
        );
        assert.equal((yield* worker.runOnce()).unresolved, 1);
        assert.equal(f.calls.length, 1);
      }).pipe(Effect.provide(memory)),
  );

  it.effect(
    "an external interruption while closing persists unknown before shutdown completes",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const entered = yield* Deferred.make<void>();
        const worker = yield* f.makeWorker((input, options?) =>
          f
            .confirmedClose(input, options)
            .pipe(
              Effect.andThen(Deferred.succeed(entered, undefined)),
              Effect.andThen(Effect.never),
            ),
        );
        const fiber = yield* Effect.forkChild(worker.runOnce());
        yield* Deferred.await(entered);
        yield* Fiber.interrupt(fiber);
        assert.equal((yield* f.storage.readTerminalCleanup(effectId))?.outcome, "interrupted");
        yield* worker.runOnce();
        assert.equal(f.calls.length, 1);
      }).pipe(Effect.provide(memory)),
  );

  it.effect(
    "lost success persistence fences the started attempt as unknown without another close",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.sql`CREATE TRIGGER fail_success BEFORE UPDATE OF state ON jones_legacy_bootstrap_terminal_cleanup WHEN NEW.state = 'succeeded' BEGIN SELECT RAISE(ABORT,'synthetic lost outcome'); END`;
        const worker = yield* f.makeWorker(f.confirmedClose);
        assert.isTrue(Exit.isFailure(yield* worker.runOnce().pipe(Effect.exit)));
        const retained = yield* f.storage.readTerminalCleanup(effectId);
        assert.equal(retained?.state, "unknown");
        assert.equal(retained?.outcome, "unconfirmed");
        assert.equal(retained?.revision, 2);
        yield* worker.runOnce();
        assert.equal(f.calls.length, 1);
      }).pipe(Effect.provide(memory)),
  );

  it.effect("unavailable persistence leaves started fenced even on a later worker instance", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.sql`CREATE TRIGGER fail_outcomes BEFORE UPDATE OF state ON jones_legacy_bootstrap_terminal_cleanup WHEN OLD.state = 'started' BEGIN SELECT RAISE(ABORT,'synthetic persistence unavailable'); END`;
      const worker = yield* f.makeWorker(f.confirmedClose);
      assert.isTrue(Exit.isFailure(yield* worker.runOnce().pipe(Effect.exit)));
      assert.equal((yield* f.storage.readTerminalCleanup(effectId))?.state, "started");
      const reopened = yield* f.makeWorker(f.confirmedClose);
      assert.equal((yield* reopened.runOnce()).unresolved, 1);
      assert.equal(f.calls.length, 1);
    }).pipe(Effect.provide(memory)),
  );

  it.effect("a replacement terminal owner is never substituted for the durable exact control", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const replacement = { ...control, generation: "replacement-generation" };
      let replacementCloses = 0;
      const worker = yield* f.makeWorker((input, options?) =>
        Effect.gen(function* () {
          yield* f.confirmedClose(input, options);
          if (M.canonical(options?.legacyOwnedControl) !== M.canonical(replacement))
            return yield* new TerminalManager.LegacyTerminalControlError({
              operation: "close",
              detail: "exact owner replaced",
            });
          replacementCloses++;
        }),
      );
      assert.isTrue(Exit.isFailure(yield* worker.runOnce().pipe(Effect.exit)));
      assert.equal(replacementCloses, 0);
      assert.equal((yield* f.storage.readTerminalCleanup(effectId))?.outcome, "owner-unavailable");
      yield* worker.runOnce();
      assert.equal(f.calls.length, 1);
    }).pipe(Effect.provide(memory)),
  );

  it.effect("corrupt exact bindings and ambient transactions refuse before physical entry", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const worker = yield* f.makeWorker(f.confirmedClose);
      assert.isTrue(
        Exit.isFailure(yield* f.sql.withTransaction(worker.runOnce()).pipe(Effect.exit)),
      );
      assert.equal((yield* f.storage.readTerminalCleanup(effectId))?.state, "pending");
      yield* f.sql`UPDATE jones_legacy_bootstrap_terminal_cleanup SET control_json = ${M.canonical({ ...control, generation: "forged-generation" })} WHERE effect_id = ${effectId}`;
      assert.isTrue(Exit.isFailure(yield* worker.runOnce().pipe(Effect.exit)));
      assert.equal(f.calls.length, 0);
    }).pipe(Effect.provide(memory)),
  );

  it.effect("unrelated schema absence is idle and does not install migrations", () =>
    Effect.gen(function* () {
      const random = SecureRandom.make((size) => new Uint8Array(size));
      let closes = 0;
      const worker = yield* Worker.TerminalCleanupWorker.pipe(
        Effect.provide(
          Worker.layer.pipe(
            Layer.provide(
              Layer.mock(TerminalManager.TerminalManager)({
                close: () =>
                  Effect.sync(() => {
                    closes++;
                  }),
              }),
            ),
            Layer.provide(Layer.succeed(SecureRandom.SecureRandom, random)),
          ),
        ),
      );
      assert.equal((yield* worker.drain()).selected, 0);
      assert.equal(closes, 0);
      const sql = yield* SqlClient.SqlClient;
      assert.deepEqual(yield* sql`SELECT name FROM sqlite_master WHERE name LIKE 'jones_%'`, []);
    }).pipe(Effect.provide(memory)),
  );

  it.effect(
    "wake before waiting and during drain remains available, without a background daemon",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const worker = yield* f.makeWorker((input, options?) =>
          f
            .confirmedClose(input, options)
            .pipe(
              Effect.andThen(Deferred.succeed(entered, undefined)),
              Effect.andThen(Deferred.await(release)),
            ),
        );
        yield* worker.wake;
        yield* worker.awaitWork;
        assert.equal(f.calls.length, 0);
        const drain = yield* Effect.forkChild(worker.drain());
        yield* Deferred.await(entered);
        yield* worker.wake;
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(drain);
        yield* worker.awaitWork;
        assert.equal(f.calls.length, 1);
      }).pipe(Effect.provide(memory)),
  );
});

const idlePublicWorker = Layer.mock(Daemon.OrchestrationEffectWorkerV2)({
  runOnce: () => Effect.succeed(false),
  nextClaimableAt: () => Effect.succeedNone,
  awaitWork: Effect.never,
});

it.effect(
  "production daemon startup drains private pending work through the actual SQLite owner",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const worker = yield* f.makeWorker(f.confirmedClose);
      const drained = yield* Deferred.make<void>();
      const observedWorker = Worker.TerminalCleanupWorker.of({
        ...worker,
        drain: (limit) =>
          worker.drain(limit).pipe(Effect.tap(() => Deferred.succeed(drained, undefined))),
      });
      const daemon = yield* Daemon.runDaemonWithOptions({
        concurrency: 1,
        livenessPollIntervalMs: 60_000,
      }).pipe(
        Effect.provideService(Worker.TerminalCleanupWorker, observedWorker),
        Effect.provide(idlePublicWorker),
        Effect.forkChild,
      );
      yield* Deferred.await(drained);
      assert.equal((yield* f.storage.readTerminalCleanup(effectId))?.state, "succeeded");
      assert.equal(f.calls.length, 1);
      yield* Fiber.interrupt(daemon);
      yield* worker.drain();
      assert.equal(f.calls.length, 1);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "production daemon shutdown fences an entered private close as unknown before returning",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const entered = yield* Deferred.make<void>();
      const worker = yield* f.makeWorker((input, options) =>
        f
          .confirmedClose(input, options)
          .pipe(Effect.andThen(Deferred.succeed(entered, undefined)), Effect.andThen(Effect.never)),
      );
      const daemon = yield* Daemon.runDaemonWithOptions({
        concurrency: 1,
        livenessPollIntervalMs: 60_000,
      }).pipe(
        Effect.provideService(Worker.TerminalCleanupWorker, worker),
        Effect.provide(idlePublicWorker),
        Effect.forkChild,
      );
      yield* Deferred.await(entered);
      assert.equal((yield* f.storage.readTerminalCleanup(effectId))?.state, "started");
      yield* Fiber.interrupt(daemon);
      const retained = yield* f.storage.readTerminalCleanup(effectId);
      assert.equal(retained?.state, "unknown");
      assert.equal(retained?.outcome, "interrupted");
      yield* worker.drain();
      assert.equal(f.calls.length, 1);
    }).pipe(Effect.provide(memory)),
);
