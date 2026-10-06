import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EventId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as ServerConfig from "../config.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as PortScanner from "../preview/PortScanner.ts";
import * as NativeTelemetryClient from "../resourceTelemetry/NativeTelemetryClient.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import * as PtyAdapter from "../terminal/PtyAdapter.ts";
import * as EventSink from "./EventSink.ts";
import { readApplicationThreadBirth } from "./ApplicationThreadBirth.ts";
import { makeCommitTransaction } from "./CommitTransaction.ts";
import { terminalOwnerObservationLive } from "./ResourceCleanupService.ts";
import { OrchestrationV2EventSinkLayerLive } from "./runtimeLayer.ts";

const now = DateTime.makeUnsafe("2026-10-05T00:00:00.000Z");
type OwnerObservation = NonNullable<
  Parameters<typeof TerminalManager.makeWithOptions>[0]["ownerObservation"]
>;
const threadId = ThreadId.make("thread:terminal-birth-observer");
const eventId = EventId.make("event:terminal-birth-observer");
const database = SqlitePersistenceMemory;
const sinkLayer = OrchestrationV2EventSinkLayerLive.pipe(Layer.provideMerge(database));
const bridgeLayer = Layer.merge(
  sinkLayer,
  terminalOwnerObservationLive.pipe(Layer.provide(sinkLayer)),
);

class VirtualPty implements PtyAdapter.PtyProcess {
  readonly pid = 2147483000;
  readonly signals: Array<string | undefined> = [];
  readonly writes: string[] = [];
  private readonly exits = new Set<(event: PtyAdapter.PtyExitEvent) => void>();
  write(data: string) {
    this.writes.push(data);
  }
  resize() {}
  kill(signal?: string) {
    this.signals.push(signal);
  }
  onData() {
    return () => {};
  }
  onExit(callback: (event: PtyAdapter.PtyExitEvent) => void) {
    this.exits.add(callback);
    return () => {
      this.exits.delete(callback);
    };
  }
  exit() {
    for (const callback of this.exits) callback({ exitCode: 0, signal: null });
  }
}

const seed = Effect.gen(function* () {
  const sink = yield* EventSink.EventSinkV2;
  const provider = ProviderInstanceId.make("codex");
  const thread: OrchestrationV2AppThread = {
    id: threadId,
    projectId: ProjectId.make("project:terminal-birth-observer"),
    title: "Terminal birth observer",
    providerInstanceId: provider,
    modelSelection: { instanceId: provider, model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
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
  yield* sink.commitCommand({
    commandId: CommandId.make("command:terminal-birth-observer"),
    commandType: "thread.create",
    threadId,
    acceptedAt: now,
    events: [{ id: eventId, type: "thread.created", threadId, occurredAt: now, payload: thread }],
    effects: [],
  });
  const read = sink.readApplicationBirthRecord;
  assert.isDefined(read);
  if (read === undefined) return yield* Effect.die("Actual birth reader missing");
  const birth = yield* read(threadId);
  assert.isNotNull(birth);
  if (birth === null) return yield* Effect.die("Committed birth missing");
  return { sink, thread, birth };
});

const makeProductionManager = Effect.fnUntraced(function* (
  observation?: OwnerObservation,
  constructionMode: "make" | "layer" | "options" = "make",
  optionObservation?: OwnerObservation,
) {
  const fs = yield* FileSystem.FileSystem;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "terminal-birth-observer-" });
  const pty = new VirtualPty();
  let spawns = 0;
  const dependencies = Layer.mergeAll(
    ServerConfig.layerTest(root, root),
    ServerSettings.layerTest(),
    Layer.succeed(PtyAdapter.PtyAdapter, {
      spawn: () =>
        Effect.sync(() => {
          spawns++;
          return pty;
        }),
    }),
    Layer.mock(NativeTelemetryClient.NativeTelemetryClient)({ processTable: Effect.succeed([]) }),
    Layer.mock(PortScanner.PortDiscovery)({
      registerTerminalProcesses: () => Effect.void,
      unregisterTerminal: () => Effect.void,
    }),
    ProcessRunner.layer,
  );
  const construction = Effect.gen(function* () {
    if (constructionMode === "layer")
      return yield* Effect.service(TerminalManager.TerminalManager).pipe(
        Effect.provide(TerminalManager.layer),
      );
    if (constructionMode === "options")
      return yield* TerminalManager.makeWithOptions({
        logsDir: root,
        ptyAdapter: {
          spawn: () =>
            Effect.sync(() => {
              spawns++;
              return pty;
            }),
        },
        processTable: Effect.succeed([]),
        ...(optionObservation === undefined ? {} : { ownerObservation: optionObservation }),
      });
    return yield* TerminalManager.make();
  }).pipe(Effect.provide(dependencies));
  const manager = yield* observation === undefined
    ? construction
    : construction.pipe(
        Effect.provideService(TerminalManager.TerminalOwnerObservation, observation),
      );
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => pty.exit()).pipe(Effect.andThen(Effect.yieldNow)),
  );
  return { manager, pty, root, spawns: () => spawns };
});

const snapshot = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const names = yield* sql<{ readonly name: string }>`SELECT name FROM sqlite_master
    WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`;
  const tables = yield* Effect.forEach(names, ({ name }) =>
    Effect.map(sql`SELECT * FROM ${sql(name)}`, (rows) => ({ name, rows })),
  );
  return { tables, changes: yield* sql`SELECT total_changes() AS changes` };
});

const open = (m: Effect.Success<ReturnType<typeof makeProductionManager>>) =>
  m.manager.open({ threadId, terminalId: "terminal:observer", cwd: m.root, cols: 80, rows: 24 });

it.layer(NodeServices.layer, { excludeTestServices: true })(
  "Terminal birth production construction",
  (it) => {
    it.effect("an explicitly stored null observer never falls back to a genuine SQL birth", () =>
      Effect.gen(function* () {
        const f = yield* seed;
        let observed = 0;
        const m = yield* makeProductionManager({
          observeCurrentBirth: () =>
            Effect.sync(() => {
              observed++;
              return null;
            }),
        });
        yield* m.manager.open({
          threadId,
          terminalId: "terminal:observer",
          cwd: m.root,
          cols: 80,
          rows: 24,
        });
        const capture = yield* m.manager.captureOwnedTargets({ threadId, ownerBirth: f.birth });
        assert.equal(capture.status, "unknown");
        assert.deepEqual(capture.targets, []);
        assert.equal(observed, 1);
        assert.equal(m.spawns(), 1);
        assert.deepEqual(m.pty.signals, []);
        assert.deepEqual(m.pty.writes, []);
        const sql = yield* SqlClient.SqlClient;
        assert.equal((yield* sql`SELECT COUNT(*) AS n FROM orchestration_events`)[0]?.n, 1);
      }).pipe(Effect.provide(sinkLayer), Effect.scoped),
    );

    it.effect(
      "the actual shared layer supplies exact committed birth to production Manager.layer without observation writes",
      () =>
        Effect.gen(function* () {
          const f = yield* seed;
          const sql = yield* SqlClient.SqlClient;
          const observer = yield* TerminalManager.TerminalOwnerObservation;
          assert.strictEqual(yield* EventSink.EventSinkV2, f.sink);
          const before = yield* snapshot;
          assert.deepEqual(yield* observer.observeCurrentBirth(threadId), f.birth);
          assert.deepEqual(yield* readApplicationThreadBirth(threadId), f.birth);
          assert.equal(f.birth.eventId, eventId);
          assert.isNumber(f.birth.sequence);
          const owner = yield* makeCommitTransaction();
          assert.deepEqual(
            yield* owner.withTransaction(observer.observeCurrentBirth(threadId)),
            f.birth,
          );
          const unowned = yield* Effect.exit(
            sql.withTransaction(observer.observeCurrentBirth(threadId)),
          );
          assert.isTrue(Exit.isFailure(unowned));
          if (Exit.isFailure(unowned))
            assert.include(Cause.pretty(unowned.cause), "UnownedCommitTransactionError");
          const m = yield* makeProductionManager(undefined, "layer");
          yield* open(m);
          const capture = yield* m.manager.captureOwnedTargets({ threadId, ownerBirth: f.birth });
          assert.equal(capture.status, "captured");
          assert.equal(capture.targets.length, 1);
          assert.deepEqual(capture.targets[0]?.ownerBirth, f.birth);
          assert.deepEqual(yield* snapshot, before);
          assert.deepEqual(m.pty.signals, []);
          assert.deepEqual(m.pty.writes, []);
        }).pipe(Effect.provide(bridgeLayer), Effect.scoped),
    );

    it.effect.each([
      "missing",
      "legacy",
      "deleted",
      "projection_absent",
      "foreign_id",
      "project_replaced",
      "incarnation_replaced",
      "copied_stream",
    ] as const)(
      "the same SQL bridge refuses %s without observation mutation or fallback",
      (scenario) =>
        Effect.gen(function* () {
          const f = yield* seed;
          const sql = yield* SqlClient.SqlClient;
          if (scenario === "missing")
            yield* sql`DELETE FROM orchestration_events WHERE event_id = ${eventId}`;
          else if (scenario === "legacy")
            yield* sql`UPDATE orchestration_events SET application_event_version = 1 WHERE event_id = ${eventId}`;
          else if (scenario === "projection_absent")
            yield* sql`DELETE FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId}`;
          else if (scenario === "copied_stream")
            yield* sql`UPDATE orchestration_events SET stream_id = 'thread:foreign' WHERE event_id = ${eventId}`;
          else {
            const field =
              scenario === "deleted"
                ? "$.deletedAt"
                : scenario === "foreign_id"
                  ? "$.id"
                  : scenario === "project_replaced"
                    ? "$.projectId"
                    : "$.createdAt";
            const value =
              scenario === "foreign_id"
                ? "thread:foreign"
                : scenario === "project_replaced"
                  ? "project:foreign"
                  : "2026-10-06T00:00:00.000Z";
            yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, ${field}, ${value}) WHERE thread_id = ${threadId}`;
          }
          const before = yield* snapshot;
          const observer = yield* TerminalManager.TerminalOwnerObservation;
          assert.isNull(yield* observer.observeCurrentBirth(threadId));
          assert.isNull(yield* readApplicationThreadBirth(threadId));
          const m = yield* makeProductionManager();
          yield* open(m);
          const capture = yield* m.manager.captureOwnedTargets({ threadId, ownerBirth: f.birth });
          assert.equal(capture.status, "unknown");
          assert.deepEqual(capture.targets, []);
          assert.deepEqual(yield* snapshot, before);
          assert.deepEqual(m.pty.signals, []);
          assert.deepEqual(m.pty.writes, []);
        }).pipe(Effect.provide(bridgeLayer), Effect.scoped),
    );

    it.effect(
      "malformed genuine stored birth preserves the bridge error and Manager unknown guard",
      () =>
        Effect.gen(function* () {
          const f = yield* seed;
          const sql = yield* SqlClient.SqlClient;
          yield* sql`UPDATE orchestration_events SET payload_json = json_set(payload_json, '$.id', 42) WHERE event_id = ${eventId}`;
          const before = yield* snapshot;
          const observer = yield* TerminalManager.TerminalOwnerObservation;
          const result = yield* Effect.result(observer.observeCurrentBirth(threadId));
          assert.equal(result._tag, "Failure");
          if (result._tag === "Failure") assert.equal(result.failure._tag, "EventSinkWriteError");
          const m = yield* makeProductionManager();
          yield* open(m);
          assert.equal(
            (yield* m.manager.captureOwnedTargets({ threadId, ownerBirth: f.birth })).status,
            "unknown",
          );
          assert.deepEqual(yield* snapshot, before);
          assert.deepEqual(m.pty.signals, []);
        }).pipe(Effect.provide(bridgeLayer), Effect.scoped),
    );

    it.effect(
      "an absent optional birth method stays unavailable despite a genuine birth in the same SQL",
      () =>
        Effect.gen(function* () {
          const f = yield* seed;
          const { readApplicationBirthRecord: _read, ...withoutBirthReader } = f.sink;
          const observer = yield* Effect.service(TerminalManager.TerminalOwnerObservation).pipe(
            Effect.provide(terminalOwnerObservationLive),
            Effect.provideService(EventSink.EventSinkV2, withoutBirthReader),
          );
          const before = yield* snapshot;
          assert.isNull(yield* observer.observeCurrentBirth(threadId));
          const m = yield* makeProductionManager(observer);
          yield* open(m);
          assert.equal(
            (yield* m.manager.captureOwnedTargets({ threadId, ownerBirth: f.birth })).status,
            "unknown",
          );
          assert.deepEqual(yield* snapshot, before);
        }).pipe(Effect.provide(sinkLayer), Effect.scoped),
    );

    it.effect.each(["error", "interrupt", "malformed_reference"] as const)(
      "explicit %s observation never falls back or grants an original target",
      (scenario) =>
        Effect.gen(function* () {
          const f = yield* seed;
          const before = yield* snapshot;
          let observations = 0;
          const observer: OwnerObservation = {
            observeCurrentBirth: () =>
              Effect.suspend(() => {
                observations++;
                if (scenario === "interrupt") return Effect.interrupt;
                if (scenario === "error")
                  return Effect.fail(
                    new EventSink.EventSinkWriteError({
                      eventCount: 0,
                      cause: new Error("Explicit synthetic observation unavailable"),
                    }),
                  );
                return Effect.succeed({ ...f.birth, eventId: "", sequence: -1 });
              }),
          };
          const m = yield* makeProductionManager(observer);
          const result = yield* Effect.exit(open(m));
          assert.equal(observations, 1);
          if (scenario === "interrupt") {
            assert.isTrue(Exit.isFailure(result));
            if (Exit.isFailure(result)) assert.isTrue(Cause.hasInterrupts(result.cause));
            assert.equal(m.spawns(), 0);
          } else {
            assert.isTrue(Exit.isSuccess(result));
            assert.equal(m.spawns(), 1);
          }
          const capture = yield* m.manager.captureOwnedTargets({ threadId, ownerBirth: f.birth });
          assert.deepEqual(capture.targets, []);
          if (scenario !== "interrupt") assert.equal(capture.status, "unknown");
          assert.deepEqual(yield* snapshot, before);
          assert.deepEqual(m.pty.signals, []);
          assert.deepEqual(m.pty.writes, []);
        }).pipe(Effect.provide(sinkLayer), Effect.scoped),
    );

    it.effect(
      "production make with no stored observer retains the same captured SQL direct reader",
      () =>
        Effect.gen(function* () {
          const f = yield* seed;
          const before = yield* snapshot;
          const m = yield* makeProductionManager();
          yield* open(m);
          const capture = yield* m.manager.captureOwnedTargets({ threadId, ownerBirth: f.birth });
          assert.equal(capture.status, "captured");
          assert.deepEqual(capture.targets[0]?.ownerBirth, f.birth);
          assert.deepEqual(yield* snapshot, before);
        }).pipe(Effect.provide(sinkLayer), Effect.scoped),
    );

    it.effect("makeWithOptions explicit observation still overrides an installed reference", () =>
      Effect.gen(function* () {
        const f = yield* seed;
        let contextCalls = 0;
        let optionCalls = 0;
        const m = yield* makeProductionManager(
          {
            observeCurrentBirth: () =>
              Effect.sync(() => {
                contextCalls++;
                return f.birth;
              }),
          },
          "options",
          {
            observeCurrentBirth: () =>
              Effect.sync(() => {
                optionCalls++;
                return null;
              }),
          },
        );
        yield* open(m);
        assert.equal(
          (yield* m.manager.captureOwnedTargets({ threadId, ownerBirth: f.birth })).status,
          "unknown",
        );
        assert.equal(contextCalls, 0);
        assert.equal(optionCalls, 1);
      }).pipe(Effect.provide(sinkLayer), Effect.scoped),
    );

    it.effect(
      "makeWithOptions default observation remains unavailable with no stored reference",
      () =>
        Effect.gen(function* () {
          const f = yield* seed;
          const m = yield* makeProductionManager(undefined, "options");
          yield* open(m);
          assert.equal(
            (yield* m.manager.captureOwnedTargets({ threadId, ownerBirth: f.birth })).status,
            "unknown",
          );
        }).pipe(Effect.provide(sinkLayer), Effect.scoped),
    );
  },
);
