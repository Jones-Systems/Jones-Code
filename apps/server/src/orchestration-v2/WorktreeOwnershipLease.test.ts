import * as NodeServices from "@effect/platform-node/NodeServices";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as NodeSqlite from "node:sqlite";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Cause from "effect/Cause";
import * as Exit from "effect/Exit";
import { runMigrations } from "../persistence/Migrations.ts";
import { ThreadId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { makeWorktreeOwnershipLeaseStore } from "./WorktreeOwnershipLease.ts";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const recordThreadCreation = Effect.fnUntraced(function* (
  threadId: ThreadId,
  eventId: string,
  streamVersion = 1,
) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly sequence: number }>`INSERT INTO orchestration_events
    (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json, application_event_version)
    VALUES (${eventId}, 'thread', ${threadId}, ${streamVersion}, 'thread.created', '2026-01-01T00:00:00.000Z', 'user', '{}', '{}', 2) RETURNING sequence`;
  return rows[0]!.sequence;
});

it.layer(SqlitePersistenceMemory)("WorktreeOwnershipLeaseStore", (it) => {
  it.effect("rotates same-owner generations and never grants expiry-only takeover", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const resourcePath = "/workspace/project";
      const ownerThreadId = ThreadId.make("thread-owner");
      const contenderThreadId = ThreadId.make("thread-contender");
      const ownerIncarnation = encodeJson([
        "t3.orchestration-v2.thread-birth/v1",
        "event-owner-created",
        1,
      ]);
      yield* recordThreadCreation(ownerThreadId, "event-owner-created");
      yield* recordThreadCreation(contenderThreadId, "event-contender-created");

      const first = Option.getOrThrow(
        yield* store.acquire({
          resourcePath,
          leaseId: "lease-1",
          ownerThreadId,
          branch: "feature/owner",
          nowMs: 1_000,
          expiresAtMs: 2_000,
        }),
      );
      const recovered = Option.getOrThrow(
        yield* store.acquire({
          resourcePath,
          leaseId: "lease-2",
          ownerThreadId,
          branch: "feature/owner",
          nowMs: 1_500,
          expiresAtMs: 2_500,
        }),
      );

      assert.equal(recovered.acquiredAtMs, first.acquiredAtMs);
      assert.equal(recovered.leaseId, "lease-2");
      assert.equal(recovered.ownerIncarnation, ownerIncarnation);
      assert.equal(recovered.renewedAtMs, 1_500);
      assert.equal(recovered.expiresAtMs, 2_500);
      assert.isFalse(
        yield* store.renew({
          resourcePath,
          leaseId: "lease-1",
          ownerThreadId,
          ownerIncarnation,
          nowMs: 1_600,
          expiresAtMs: 2_600,
        }),
      );

      yield* store.release(first);
      assert.equal((yield* store.listAll())[0]?.leaseId, "lease-2");

      const expiredTakeover = yield* store.acquire({
        resourcePath,
        leaseId: "lease-3",
        ownerThreadId: contenderThreadId,
        branch: "feature/contender",
        nowMs: 10_000,
        expiresAtMs: 11_000,
      });
      assert.isTrue(Option.isNone(expiredTakeover));

      yield* store.release(recovered);
      const acquiredAfterRelease = yield* store.acquire({
        resourcePath,
        leaseId: "lease-4",
        ownerThreadId: contenderThreadId,
        branch: "feature/contender",
        nowMs: 10_001,
        expiresAtMs: 11_001,
      });
      assert.isTrue(Option.isSome(acquiredAfterRelease));
    }),
  );

  it.effect("does not recover a retained lease for a recreated thread id", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const resourcePath = "/workspace/recreated";
      const ownerThreadId = ThreadId.make("thread-reused-id");
      yield* recordThreadCreation(ownerThreadId, "event-old-incarnation");

      const retained = yield* store.acquire({
        resourcePath,
        leaseId: "lease-old-incarnation",
        ownerThreadId,
        branch: "feature/old",
        nowMs: 1_000,
        expiresAtMs: 2_000,
      });
      assert.isTrue(Option.isSome(retained));
      const newSequence = yield* recordThreadCreation(ownerThreadId, "event-new-incarnation", 2);

      const recreated = yield* store.acquire({
        resourcePath,
        leaseId: "lease-new-incarnation",
        ownerThreadId,
        branch: "feature/new",
        nowMs: 10_000,
        expiresAtMs: 11_000,
      });
      assert.isTrue(Option.isNone(recreated));
      assert.deepEqual(
        yield* store.getThreadIncarnation(ownerThreadId),
        Option.some(
          encodeJson(["t3.orchestration-v2.thread-birth/v1", "event-new-incarnation", newSequence]),
        ),
      );
      assert.equal(
        (yield* store.listAll()).find((lease) => lease.resourcePath === resourcePath)?.leaseId,
        "lease-old-incarnation",
      );
    }),
  );

  it.effect("does not use a legacy creation event as V2 checkout authority", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const ownerThreadId = ThreadId.make("thread:legacy-only");
      yield* recordThreadCreation(ownerThreadId, "event:legacy-only");
      yield* sql`UPDATE orchestration_events SET application_event_version = 1
        WHERE event_id = 'event:legacy-only'`;
      assert.isTrue(Option.isNone(yield* store.getThreadIncarnation(ownerThreadId)));
      assert.isTrue(
        Option.isNone(
          yield* store.acquire({
            resourcePath: "/fixture/legacy-only",
            leaseId: "lease:legacy-only",
            ownerThreadId,
            branch: null,
            nowMs: 1,
            expiresAtMs: 2,
          }),
        ),
      );
      assert.isFalse(
        (yield* store.listAll()).some((lease) => lease.ownerThreadId === ownerThreadId),
      );
    }),
  );

  it.effect("does not create a lease without an authoritative creation event", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const input = {
        resourcePath: "/workspace/missing-creation",
        leaseId: "lease-missing-creation",
        ownerThreadId: ThreadId.make("thread-without-creation"),
        branch: null,
        nowMs: 1_000,
        expiresAtMs: 2_000,
      };
      assert.isTrue(Option.isNone(yield* store.acquire(input)));
      assert.isFalse(
        (yield* store.listAll()).some((lease) => lease.resourcePath === input.resourcePath),
      );
    }),
  );
});

it.effect(
  "acquires in one writer statement after a foreign WAL commit, without a stale read upgrade",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        directory: path.join(process.cwd(), "node_modules"),
        prefix: ".a2-lease-",
      });
      const filename = path.join(directory, "synthetic.sqlite");
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`PRAGMA journal_mode = WAL`;
        yield* sql`PRAGMA busy_timeout = 0`;
        yield* runMigrations();
        const store = yield* makeWorktreeOwnershipLeaseStore();
        const ownerThreadId = ThreadId.make("thread:wal-owner");
        yield* recordThreadCreation(ownerThreadId, "event:wal-owner");
        yield* sql`CREATE TABLE foreign_commit_probe (id INTEGER PRIMARY KEY)`;
        const writer = yield* Effect.acquireRelease(
          Effect.sync(() => new NodeSqlite.DatabaseSync(filename)),
          (database) => Effect.sync(() => database.close()),
        );
        const input = {
          resourcePath: directory,
          leaseId: "lease:wal-owner",
          ownerThreadId,
          branch: "work/owner",
          nowMs: 1,
          expiresAtMs: 100,
        };
        // This reproduces the former read-then-write transaction deterministically.
        yield* sql.unsafe("BEGIN");
        const stale = yield* Effect.exit(
          Effect.gen(function* () {
            yield* store.getThreadIncarnation(ownerThreadId);
            yield* Effect.sync(() => writer.exec("INSERT INTO foreign_commit_probe VALUES (1)"));
            yield* store.acquire(input);
          }).pipe(Effect.ensuring(sql.unsafe("ROLLBACK").pipe(Effect.orDie))),
        );
        assert.isTrue(Exit.isFailure(stale));
        if (Exit.isFailure(stale)) assert.include(Cause.pretty(stale.cause), "SQLITE(517)");
        assert.deepEqual(yield* store.listAll(), []);
        assert.equal(
          writer.prepare("SELECT COUNT(*) AS count FROM foreign_commit_probe").get()?.count,
          1,
        );

        // Path preparation and concurrent commits precede the transaction's first database access.
        yield* Effect.sync(() => writer.exec("INSERT INTO foreign_commit_probe VALUES (2)"));
        const acquired = yield* sql.withTransaction(store.acquire(input));
        assert.isTrue(Option.isSome(acquired));
        assert.equal((yield* store.listAll()).length, 1);
        assert.equal(
          writer.prepare("SELECT COUNT(*) AS count FROM foreign_commit_probe").get()?.count,
          2,
        );
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
