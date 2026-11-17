// @effect-diagnostics nodeBuiltinImport:off -- Real owned SQLite files test the cooperative pre-open boundary and permanent revocation.
import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { assert, describe, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import * as Owner from "../CheckedRuntimeOwner.ts";
import * as Incarnation from "./DatabaseIncarnation.ts";
import { makeOwnedFixture } from "./DatabaseIncarnation.fixture.ts";

describe("cooperative database open incarnation", () => {
  it.effect.each([false, true])(
    "certifies actual writerWorker=%s before migrations",
    (writerWorker) =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* makeOwnedFixture;
          const before = NodeFS.statSync(f.dbPath, { bigint: true });
          assert.strictEqual(before.nlink, 1n);
          const sql = Context.get(
            yield* Layer.build(
              NodeSqliteClient.layer({
                filename: f.dbPath,
                writerWorker,
                readerWorker: writerWorker,
                checkpointWorker: writerWorker,
              }),
            ),
            SqlClient.SqlClient,
          );
          yield* Effect.gen(function* () {
            const checked = yield* Incarnation.bindOpen(f.open);
            yield* sql`CREATE TABLE synthetic_migration(value TEXT)`;
            yield* Incarnation.revalidateOpen(f.open);
            assert.strictEqual(yield* Owner.bindHeldDatabaseOwner(f.owner), checked);
            assert.strictEqual(
              Incarnation.bindingForSql(checked, sql).database.inode,
              String(before.ino),
            );
            assert.strictEqual(f.connection.prepare("PRAGMA busy_timeout").get()?.timeout, 250);
            Owner.assertCheckedDatabaseOwner(checked);
            assert.isTrue(Exit.isFailure(yield* Effect.exit(Incarnation.bindOpen(f.open))));
            assert.isTrue(
              Exit.isFailure(
                yield* Effect.exit(Owner.bindHeldDatabaseOwner(f.owner)).pipe(
                  Effect.provide(NodeSqliteClient.layer({ filename: f.dbPath })),
                ),
              ),
            );
          }).pipe(Effect.provideService(SqlClient.SqlClient, sql));
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "replacement before actual SQLite open cannot be certified as the prepared incarnation",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* makeOwnedFixture;
          NodeFS.renameSync(f.dbPath, `${f.dbPath}.retained`);
          NodeFS.writeFileSync(f.dbPath, "", { flag: "wx" });
          yield* Effect.gen(function* () {
            assert.isTrue(Exit.isFailure(yield* Effect.exit(Incarnation.bindOpen(f.open))));
            const sql = yield* SqlClient.SqlClient;
            assert.deepStrictEqual(
              yield* sql`SELECT name FROM sqlite_schema WHERE type='table'`,
              [],
            );
          }).pipe(
            Effect.provide(NodeSqliteClient.layer({ filename: f.dbPath, writerWorker: true })),
          );
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("an observed closed actual writer permanently revokes its database capability", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* makeOwnedFixture;
        const connectionScope = yield* Scope.make();
        yield* Effect.addFinalizer((exit) => Scope.close(connectionScope, exit));
        const sql = Context.get(
          yield* Layer.build(NodeSqliteClient.layer({ filename: f.dbPath })).pipe(
            Scope.provide(connectionScope),
          ),
          SqlClient.SqlClient,
        );
        const token = yield* Incarnation.bindOpen(f.open).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        );
        yield* Scope.close(connectionScope, Exit.void);
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(Incarnation.validateCheckedOwner(token)).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            ),
          ),
        );
        NodeAssert.throws(() => Owner.assertCheckedDatabaseOwner(token));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "an already-open raw SqlClient cannot acquire production cleanup authority at first bind",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* makeOwnedFixture;
          yield* Effect.gen(function* () {
            assert.isTrue(Exit.isFailure(yield* Effect.exit(Owner.bindHeldDatabaseOwner(f.owner))));
            const checked = yield* Incarnation.bindOpen(f.open);
            assert.strictEqual(yield* Owner.bindHeldDatabaseOwner(f.owner), checked);
          }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: f.dbPath })));
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "persistent replacement before bind refuses and restoring the pathname cannot revive the token",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* makeOwnedFixture;
          yield* Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            yield* sql`CREATE TABLE retained(value TEXT)`;
            NodeFS.renameSync(f.dbPath, `${f.dbPath}.retained`);
            NodeFS.writeFileSync(f.dbPath, "", { flag: "wx" });
            assert.isTrue(Exit.isFailure(yield* Effect.exit(Incarnation.bindOpen(f.open))));
            NodeFS.renameSync(f.dbPath, `${f.dbPath}.replacement`);
            NodeFS.renameSync(`${f.dbPath}.retained`, f.dbPath);
            assert.isTrue(Exit.isFailure(yield* Effect.exit(Incarnation.bindOpen(f.open))));
            assert.isTrue(Exit.isFailure(yield* Effect.exit(Owner.bindHeldDatabaseOwner(f.owner))));
          }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: f.dbPath })));
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("memory, wrong-client, forged and in-transaction opens confer no authority", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* makeOwnedFixture;
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(Incarnation.bindOpen({} as Incarnation.DatabaseOpenIncarnation)),
            ),
          );
          assert.isTrue(
            Exit.isFailure(yield* Effect.exit(sql.withTransaction(Incarnation.bindOpen(f.open)))),
          );
          assert.isTrue(Exit.isFailure(yield* Effect.exit(Incarnation.bindOpen(f.open))));
          assert.isTrue(Exit.isFailure(yield* Effect.exit(Owner.bindHeldDatabaseOwner(f.owner))));
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" })));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("closing the preparing scope permanently revokes a still-open client", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* makeOwnedFixture;
        const preparing = yield* Scope.make();
        yield* Effect.addFinalizer((exit) => Scope.close(preparing, exit));
        const open = yield* Incarnation.prepareOpen(f.owner, f.infrastructure.permit).pipe(
          Effect.provideService(Incarnation.OpenInfrastructure, f.infrastructure),
          Scope.provide(preparing),
        );
        yield* Effect.gen(function* () {
          const bound = yield* Incarnation.bindOpen(open);
          yield* Scope.close(preparing, Exit.void);
          NodeAssert.throws(() => Owner.assertCheckedDatabaseOwner(bound));
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: f.dbPath })));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );
});
