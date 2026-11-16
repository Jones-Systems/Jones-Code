import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Agent from "./agentService.ts";
import * as Access from "../../../mcp/McpToolAccess.ts";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
import * as PeerForwarding from "../../../peer/PeerForwarding.ts";
import { OrchestratorMcpFailure } from "@t3tools/contracts";
import { WorkstreamToolkit, assignmentResult } from "../../mcp/workstreams/tools.ts";
import { handlers } from "../../mcp/workstreams/handlers.ts";
import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import * as P from "@t3tools/contracts/jones/workstreamsPersonal";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Config from "../../../config.ts";
import * as Environment from "../../../environment/ServerEnvironment.ts";
import migration from "../../persistence/Migrations/153_JonesPersonalWorkstreams.ts";
import * as Personal from "./service.ts";

const environmentId = EnvironmentId.make("synthetic-personal-server");
const threadId = ThreadId.make("synthetic-personal-thread");
const w = (id: string) =>
  P.newPersonalWorkstream({
    id,
    name: id,
    orderKey: "n",
    now: "2026-01-01T00:00:00.000Z",
    origin: "fixture",
  });
const database = Layer.effectDiscard(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT PRIMARY KEY, deleted_at TEXT)`;
    yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id) VALUES(${threadId})`;
    yield* migration;
  }),
).pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })));
const dependencies = Layer.mergeAll(
  database,
  Config.ServerConfig.layerTest(process.cwd(), { prefix: "personal-workstreams-test-" }),
  Layer.succeed(Environment.ServerEnvironmentIdentity, {
    getEnvironmentId: Effect.succeed(environmentId),
  }),
).pipe(Layer.provideMerge(Layer.merge(NodeServices.layer, NodeCrypto.layer)));
const fixture = Personal.layer.pipe(Layer.provideMerge(dependencies));
const fresh = Personal.PersonalWorkstreams.pipe(Effect.provide(Personal.layer));

it.effect("desired-first retries, ABA/CAS rollback, LWW and terminal deletion use real SQL", () =>
  Effect.gen(function* () {
    const s = yield* Personal.PersonalWorkstreams;
    const a = w("a"),
      b = w("b");
    const first = yield* s.assign({ threadId, workstream: a, expectedRevision: 0 });
    assert.equal(first.assignment.revision, 1);
    const retry = yield* s.assign({ threadId, workstream: a, expectedRevision: 0 });
    assert.equal(retry.outcome, "already_satisfied");
    assert.equal(retry.sequence, first.sequence);
    yield* s.assign({ threadId, workstream: b, expectedRevision: 1 });
    const rejected = yield* s
      .assign({
        threadId,
        workstream: P.editPersonalWorkstream(
          a,
          { name: "not committed" },
          "2026-01-02T00:00:00.000Z",
          "fixture",
        ),
        expectedRevision: 0,
      })
      .pipe(Effect.flip);
    assert.equal(rejected.code, "precondition_failed");
    assert.equal((yield* s.readDefinitions(["a"]))[0]?.name, "a");
    yield* s.assign({ threadId, workstream: null, expectedRevision: 2 });
    yield* s.assign({ threadId, workstream: a, expectedRevision: 3 });
    assert.equal(
      (yield* s.assign({ threadId, workstream: b, expectedRevision: 1 }).pipe(Effect.flip)).code,
      "precondition_failed",
    );
    const tomb = P.deletePersonalWorkstream(a, "2026-01-03T00:00:00.000Z", "fixture");
    yield* s.put(tomb);
    assert.equal((yield* s.readThread(threadId)).assignment.workstreamId, null);
    assert.equal((yield* s.readThread(threadId)).assignment.revision, 5);
    assert.equal((yield* s.put({ ...a, rev: 999 })).outcome, "superseded");
    const before = yield* s.snapshot();
    assert.equal(
      (yield* s
        .assign({ threadId, workstream: w("rollback"), expectedRevision: 0 })
        .pipe(Effect.flip)).code,
      "precondition_failed",
    );
    assert.deepEqual(yield* s.snapshot(), before);
    yield* s.put({ ...b, name: "z" });
    yield* s.put({ ...b, name: "c" });
    assert.equal((yield* s.readDefinitions(["b"]))[0]?.name, "z");
  }).pipe(Effect.provide(fixture)),
);

it.effect("two subscribers receive a consistent initial snapshot and committed invalidation", () =>
  Effect.gen(function* () {
    const s = yield* Personal.PersonalWorkstreams;
    const ready1 = yield* Deferred.make<void>();
    const ready2 = yield* Deferred.make<void>();
    const watch = (ready: Deferred.Deferred<void>) =>
      s.changes.pipe(
        Stream.tap(() => Deferred.succeed(ready, undefined)),
        Stream.take(2),
        Stream.runCollect,
      );
    const f1 = yield* watch(ready1).pipe(Effect.forkChild);
    const f2 = yield* watch(ready2).pipe(Effect.forkChild);
    yield* Deferred.await(ready1);
    yield* Deferred.await(ready2);
    yield* s.put(w("observe"));
    const a = yield* Fiber.join(f1);
    const b = yield* Fiber.join(f2);
    assert.equal(a.length, 2);
    assert.deepEqual(a, b);
    assert.equal(a[0]?.sequence, 0);
    assert.equal(a[1]?.sequence, 1);
    assert.equal(a[1]?.workstreams[0]?.id, "observe");
  }).pipe(Effect.provide(fixture)),
);

const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeAssignmentResult = Schema.decodeUnknownEffect(assignmentResult);
const stage = Effect.fn("test.stagePersonalImport")(function* (
  changes: Record<string, unknown> = {},
) {
  const fs = yield* FileSystem.FileSystem;
  const config = yield* Config.ServerConfig;
  const file = {
    schema: "jones.workstreams-personal-import/v1",
    importId: "synthetic-import",
    targetEnvironmentId: environmentId,
    workstreams: [w("imported")],
    assignments: [{ threadId, workstreamId: "imported" }],
    ...changes,
  };
  yield* fs.makeDirectory(`${config.baseDir}/workstreams`, { recursive: true });
  yield* fs.writeFileString(
    `${config.baseDir}/workstreams/personal-import.v1.json`,
    yield* encodeJson(file),
  );
});
it.effect(
  "startup import commits marker atomically, replays once and rejects changed bytes; legacy data stays intact",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE workstreams_native_preserved(value TEXT)`;
      yield* sql`INSERT INTO workstreams_native_preserved VALUES('synthetic retained ciphertext')`;
      yield* stage();
      const first = yield* fresh;
      const snapshot = yield* first.snapshot();
      assert.equal(snapshot.importIssue, null);
      assert.equal(snapshot.assignments[0]?.revision, 1);
      const restarted = yield* fresh;
      assert.deepEqual(yield* restarted.snapshot(), snapshot);
      yield* stage({ workstreams: [{ ...w("imported"), name: "changed bytes" }] });
      const changed = yield* fresh;
      assert.equal((yield* changed.snapshot()).importIssue?.reason, "content_mismatch");
      assert.equal((yield* changed.readThread(threadId)).workstream?.name, "imported");
      assert.deepEqual(yield* sql`SELECT * FROM workstreams_native_preserved`, [
        { value: "synthetic retained ciphertext" },
      ]);
    }).pipe(Effect.provide(dependencies)),
);

it.effect.each(["wrong_environment", "thread_not_found", "conflict", "invalid_file"] as const)(
  "startup import %s rolls back definition and marker",
  (reason) =>
    Effect.gen(function* () {
      if (reason === "wrong_environment") yield* stage({ targetEnvironmentId: "another-server" });
      if (reason === "thread_not_found")
        yield* stage({ assignments: [{ threadId: "missing-thread", workstreamId: "imported" }] });
      if (reason === "conflict") {
        const s = yield* fresh;
        yield* s.assign({ threadId, workstream: w("other"), expectedRevision: 0 });
        yield* stage();
      }
      if (reason === "invalid_file") {
        const fs = yield* FileSystem.FileSystem;
        const config = yield* Config.ServerConfig;
        yield* fs.makeDirectory(`${config.baseDir}/workstreams`, { recursive: true });
        yield* fs.writeFileString(
          `${config.baseDir}/workstreams/personal-import.v1.json`,
          "invalid",
        );
      }
      const s = yield* fresh;
      assert.equal((yield* s.snapshot()).importIssue?.reason, reason);
      assert.equal((yield* s.readDefinitions(["imported"])).length, 0);
      const sql = yield* SqlClient.SqlClient;
      assert.equal(
        (yield* sql`SELECT * FROM jones_personal_workstream_meta WHERE key LIKE 'import:%'`).length,
        0,
      );
    }).pipe(Effect.provide(dependencies)),
);

it.effect(
  "startup with unreachable legacy activation never opens activation or changes its bytes",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const config = yield* Config.ServerConfig;
      const path = `${config.baseDir}/workstreams/activation.env`;
      yield* fs.makeDirectory(`${config.baseDir}/workstreams`, { recursive: true });
      yield* fs.writeFileString(path, "REGISTRY_URL=https://unreachable.invalid\n");
      const opened: string[] = [];
      const observed = {
        ...fs,
        readFileString: (
          p: string,
          ...rest: Parameters<typeof fs.readFileString> extends [string, ...infer R] ? R : never
        ) => {
          opened.push(p);
          return fs.readFileString(p, ...rest);
        },
        readFile: (p: string) => {
          opened.push(p);
          return fs.readFile(p);
        },
      };
      const s = yield* fresh.pipe(Effect.provideService(FileSystem.FileSystem, observed));
      yield* s.put(w("startup"));
      assert.equal(opened.includes(path), false);
      assert.equal(yield* fs.readFileString(path), "REGISTRY_URL=https://unreachable.invalid\n");
    }).pipe(Effect.provide(dependencies)),
);

it.effect("request cancellation after commit still publishes to a waiting subscriber", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const committed = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    let pause = false;
    // Keep the real transaction and disk writes; pause only the return across the commit boundary.
    const observed = new Proxy(sql, {
      get(target, key) {
        if (key !== "withTransaction") return Reflect.get(target, key);
        return <A, E, R>(body: Effect.Effect<A, E, R>) =>
          sql
            .withTransaction(body)
            .pipe(
              Effect.tap(() =>
                pause
                  ? Deferred.succeed(committed, undefined).pipe(
                      Effect.andThen(Deferred.await(release)),
                    )
                  : Effect.void,
              ),
            );
      },
    });
    const s = yield* fresh.pipe(Effect.provideService(SqlClient.SqlClient, observed));
    const ready = yield* Deferred.make<void>();
    const subscriber = yield* s.changes.pipe(
      Stream.tap(() => Deferred.succeed(ready, undefined)),
      Stream.take(2),
      Stream.runCollect,
      Effect.forkChild,
    );
    yield* Deferred.await(ready);
    pause = true;
    const request = yield* s.put(w("cancelled-request")).pipe(Effect.forkChild);
    yield* Deferred.await(committed);
    // Synchronously request interruption while the committed writer is held at the barrier.
    yield* Effect.sync(() => request.interruptUnsafe());
    pause = false;
    yield* Deferred.succeed(release, undefined);
    assert.isTrue(Exit.isFailure(yield* Fiber.await(request)));
    const snapshots = yield* Fiber.join(subscriber);
    assert.equal(snapshots[1]?.workstreams[0]?.id, "cancelled-request");
    assert.equal(snapshots[1]?.sequence, 1);
  }).pipe(Effect.provide(dependencies)),
);

it.effect(
  "storage failure during import rolls back carried definition, assignments and digest marker",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TRIGGER synthetic_import_failure BEFORE INSERT ON jones_personal_workstream_meta WHEN NEW.key LIKE 'import:%' BEGIN SELECT RAISE(ABORT,'synthetic failure'); END`;
      yield* stage();
      const s = yield* fresh;
      assert.equal((yield* s.readDefinitions()).length, 0);
      assert.equal((yield* s.readThread(threadId)).assignment.revision, 0);
      assert.equal(
        (yield* sql`SELECT * FROM jones_personal_workstream_meta WHERE key LIKE 'import:%'`).length,
        0,
      );
      yield* sql`DROP TRIGGER synthetic_import_failure`;
      const retry = yield* fresh;
      assert.equal((yield* retry.snapshot()).importIssue, null);
      assert.equal((yield* retry.readThread(threadId)).assignment.revision, 1);
    }).pipe(Effect.provide(dependencies)),
);

it.effect(
  "definitions, null assignment revisions, sequence and import marker survive closing and reopening SQLite",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "personal-disk-restart-" });
      const open = () => NodeSqliteClient.layer({ filename: `${dir}/personal.sqlite` });
      const setup = Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT PRIMARY KEY, deleted_at TEXT)`;
        yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id) VALUES(${threadId})`;
        yield* migration;
        yield* stage();
        const s = yield* fresh;
        yield* s.assign({ threadId, workstream: null, expectedRevision: 1 });
        return yield* s.snapshot();
      });
      const snapshot = yield* setup.pipe(Effect.provide(open()), Effect.scoped);
      const after = yield* Effect.gen(function* () {
        const s = yield* fresh;
        return yield* s.snapshot();
      }).pipe(Effect.provide(open()), Effect.scoped);
      assert.deepEqual(after, snapshot);
      assert.equal(after.assignments[0]?.revision, 2);
      assert.equal(after.assignments[0]?.workstreamId, null);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Config.ServerConfig.layerTest(process.cwd(), { prefix: "personal-restart-config-" }),
          Layer.succeed(Environment.ServerEnvironmentIdentity, {
            getEnvironmentId: Effect.succeed(environmentId),
          }),
        ).pipe(Layer.provideMerge(Layer.merge(NodeServices.layer, NodeCrypto.layer))),
      ),
    ),
);

it.effect("concurrent creates use one stable id and never overwrite an existing definition", () =>
  Effect.gen(function* () {
    const s = yield* Personal.PersonalWorkstreams;
    const results = yield* Effect.all(
      [s.create({ id: "stable", name: "stable" }), s.create({ id: "stable", name: "stable" })],
      { concurrency: 2 },
    );
    assert.deepEqual(results.map((r) => r.outcome).sort(), ["already_satisfied", "applied"]);
    assert.equal((yield* s.snapshot()).sequence, 1);
    const before = yield* s.snapshot();
    assert.equal((yield* s.create({ id: "stable", name: "different" })).outcome, "superseded");
    assert.deepEqual(yield* s.snapshot(), before);
    yield* s.put({ ...w("put-race"), rev: 10 });
    yield* Effect.all(
      [
        s.create({ id: "put-race", name: "stale create" }),
        s.put({ ...w("put-race"), rev: 11, name: "newer put" }),
      ],
      { concurrency: 2 },
    );
    assert.equal((yield* s.readDefinitions(["put-race"]))[0]?.name, "newer put");
    yield* s.put(P.deletePersonalWorkstream(w("stable"), "2026-02-01T00:00:00.000Z", "fixture"));
    assert.equal(
      (yield* s.create({ id: "stable", name: "resurrect" }).pipe(Effect.flip)).code,
      "workstream_deleted",
    );
  }).pipe(Effect.provide(fixture)),
);

const scope: Invocation.McpInvocationScope = {
  environmentId,
  thread: undefined,
  client: { sessionId: "synthetic", label: "test", access: "full-access" },
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
  requestNamespace: "synthetic",
};
const toolkit = WorkstreamToolkit.pipe(
  Effect.provide(Access.HandlersLayer.layer(handlers).pipe(Layer.provide(Agent.layer))),
);
const threads = Layer.mock(ThreadManagementService)({ getThreadShell: () => Effect.succeed(null) });

it.effect("MCP catalog creates, assigns and reads without a UI; read-only writes are refused", () =>
  Effect.gen(function* () {
    const tools = yield* toolkit;
    const created = yield* tools
      .handle("t3_workstream_create", { id: "agent-created", name: "Synthetic agent" })
      .pipe(Stream.unwrap, Stream.runCollect);
    assert.equal(created.at(-1)?.isFailure, false);
    const assigned = yield* tools
      .handle("t3_thread_workstream_set", {
        threadId,
        workstreamId: "agent-created",
        expectedRevision: 0,
      })
      .pipe(Stream.unwrap, Stream.runCollect);
    assert.equal(assigned.at(-1)?.isFailure, false);
    const read = yield* tools
      .handle("t3_thread_workstream_read", { threadId })
      .pipe(Stream.unwrap, Stream.runCollect);
    const last = read.at(-1);
    assert.ok(last);
    if (last.isFailure) return yield* Effect.die("MCP read failed");
    const decoded = yield* decodeAssignmentResult(last.result);
    assert.equal(decoded.workstreamName, "Synthetic agent");
    assert.equal(decoded.revision, 1);
    const refused = yield* tools
      .handle("t3_workstream_create", { id: "forbidden", name: "Forbidden" })
      .pipe(
        Stream.unwrap,
        Stream.runCollect,
        Effect.provideService(Invocation.McpInvocationContext, {
          ...scope,
          client: { ...scope.client!, access: "read-only" },
        }),
      );
    assert.equal(refused.at(-1)?.isFailure, true);
    assert.equal(
      (yield* (yield* Personal.PersonalWorkstreams).readDefinitions(["forbidden"])).length,
      0,
    );
  }).pipe(
    Effect.provideService(Invocation.McpInvocationContext, scope),
    Effect.provide(Layer.merge(threads, fixture)),
  ),
);

it.effect(
  "forwarded assignment carries the sender definition and preserves explicit target; unknown target fails",
  () =>
    Effect.gen(function* () {
      const s = yield* Personal.PersonalWorkstreams;
      yield* s.put(w("carried"));
      const received: Array<{ environmentId: string; params: unknown }> = [];
      const forwarding = Layer.mock(PeerForwarding.PeerForwarding)({
        call: ((_scope: unknown, _tool: unknown, target: string, params: unknown) => {
          received.push({ environmentId: target, params });
          return Effect.fail(
            new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "Unknown linked target.",
            }),
          );
        }) as PeerForwarding.PeerForwarding["Service"]["call"],
      });
      const tools = yield* toolkit.pipe(Effect.provide(forwarding));
      const remote = EnvironmentId.make("synthetic-peer");
      const r = yield* tools
        .handle("t3_thread_workstream_set", {
          environmentId: remote,
          threadId,
          workstreamId: "carried",
          expectedRevision: 0,
        })
        .pipe(Stream.unwrap, Stream.runCollect);
      assert.equal(r.at(-1)?.isFailure, true);
      assert.equal(received[0]?.environmentId, remote);
      assert.deepEqual(received[0]?.params, {
        environmentId: remote,
        threadId,
        workstreamId: "carried",
        expectedRevision: 0,
        workstream: w("carried"),
      });
    }).pipe(
      Effect.provideService(Invocation.McpInvocationContext, scope),
      Effect.provide(Layer.merge(threads, fixture)),
    ),
);

it.effect("sparse null rows survive unassignment and snapshots omit deleted upstream threads", () =>
  Effect.gen(function* () {
    const s = yield* Personal.PersonalWorkstreams;
    assert.deepEqual((yield* s.snapshot()).assignments, []);
    assert.deepEqual((yield* s.readThread(threadId)).assignment, {
      threadId,
      workstreamId: null,
      revision: 0,
      updatedAt: null,
    });
    yield* s.assign({ threadId, workstream: w("sparse"), expectedRevision: 0 });
    yield* s.assign({ threadId, workstream: null, expectedRevision: 1 });
    assert.equal((yield* s.snapshot()).assignments[0]?.revision, 2);
    const sql = yield* SqlClient.SqlClient;
    yield* sql`DELETE FROM orchestration_v2_projection_threads WHERE thread_id=${threadId}`;
    assert.deepEqual((yield* s.snapshot()).assignments, []);
    assert.equal((yield* s.readThread(threadId).pipe(Effect.flip)).code, "thread_not_found");
    assert.equal((yield* sql`SELECT * FROM jones_personal_workstream_assignments`).length, 1);
  }).pipe(Effect.provide(fixture)),
);

it.effect("a slow subscriber coalesces commits and reads the latest full snapshot", () =>
  Effect.gen(function* () {
    const s = yield* Personal.PersonalWorkstreams;
    const ready = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const subscriber = yield* s.changes.pipe(
      Stream.tap((frame) =>
        frame.sequence === 0
          ? Deferred.succeed(ready, undefined).pipe(Effect.andThen(Deferred.await(release)))
          : Effect.void,
      ),
      Stream.take(2),
      Stream.runCollect,
      Effect.forkChild,
    );
    yield* Deferred.await(ready);
    yield* s.put(w("one"));
    yield* s.put(w("two"));
    yield* s.put(w("three"));
    yield* Deferred.succeed(release, undefined);
    const frames = yield* Fiber.join(subscriber);
    assert.equal(frames[1]?.sequence, 3);
    assert.equal(frames[1]?.workstreams.length, 3);
  }).pipe(Effect.provide(fixture)),
);

it.effect("a carried tombstone rejected by assign cannot clear any existing assignment", () =>
  Effect.gen(function* () {
    const s = yield* Personal.PersonalWorkstreams;
    const definition = w("atomic-delete");
    yield* s.assign({ threadId, workstream: definition, expectedRevision: 0 });
    const before = yield* s.snapshot();
    const tombstone = P.deletePersonalWorkstream(definition, "2026-02-01T00:00:00.000Z", "fixture");
    assert.equal(
      (yield* s.assign({ threadId, workstream: tombstone, expectedRevision: 1 }).pipe(Effect.flip))
        .code,
      "workstream_deleted",
    );
    assert.deepEqual(yield* s.snapshot(), before);
  }).pipe(Effect.provide(fixture)),
);
