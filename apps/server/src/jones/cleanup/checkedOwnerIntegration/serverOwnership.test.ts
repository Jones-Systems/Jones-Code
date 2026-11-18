// @effect-diagnostics nodeBuiltinImport:off -- Synthetic native acquisition and inode replacement fixtures retain the original SQLite handle.
import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import { assert, describe, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import { vi } from "vite-plus/test";
import * as ProcessRunner from "../../../processRunner.ts";
import * as Ownership from "../../../serverOwnership.ts";
import * as RuntimeState from "../../../serverRuntimeState.ts";
import {
  assertCheckedDatabaseOwner,
  beginCheckedServerOwnership,
  bindHeldDatabaseOwner,
  CheckedRuntimeOwnerError,
  HeldRuntimeOwner,
} from "../CheckedRuntimeOwner.ts";

import * as Incarnation from "../sharedAdmission/DatabaseIncarnation.ts";
import { prepareSyntheticOpen } from "../sharedAdmission/DatabaseIncarnation.fixture.ts";

const isCheckedError = Schema.is(CheckedRuntimeOwnerError);
const platform = ProcessRunner.layer.pipe(
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);
interface Acquisition {
  readonly connection: NodeSqlite.DatabaseSync;
  readonly timeout: number;
}
const fixture = <A, E, R>(
  use: (input: {
    readonly root: string;
    readonly stateDir: string;
    readonly statePath: string;
    readonly dbPath: string;
    readonly acquisitions: Acquisition[];
  }) => Effect.Effect<A, E, R>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const root = yield* fs.realPath(
        yield* fs.makeTempDirectoryScoped({ prefix: "jones-owner-composition-" }),
      );
      const stateDir = NodePath.join(root, "userdata");
      yield* fs.makeDirectory(stateDir);
      const acquisitions: Acquisition[] = [];
      const exec = NodeSqlite.DatabaseSync.prototype.exec;
      yield* Effect.acquireRelease(
        Effect.sync(() =>
          vi.spyOn(NodeSqlite.DatabaseSync.prototype, "exec").mockImplementation(function (
            this: NodeSqlite.DatabaseSync,
            sql: string,
          ) {
            exec.call(this, sql);
            if (/\bBEGIN EXCLUSIVE\b/i.test(sql)) {
              acquisitions.push({
                connection: this,
                timeout: Number(this.prepare("PRAGMA busy_timeout").get()?.timeout),
              });
            }
          }),
        ),
        (spy) => Effect.sync(() => spy.mockRestore()),
      );
      return yield* use({
        root,
        stateDir,
        statePath: NodePath.join(stateDir, "server-runtime.json"),
        dbPath: NodePath.join(stateDir, "statev2.sqlite"),
        acquisitions,
      }).pipe(
        Effect.provideService(HostProcess.HomeDirectory, root),
        Effect.provideService(HostProcess.Environment, {}),
      );
    }).pipe(Effect.provide(platform)),
  );

const checkedRefusal = <E, R>(effect: Effect.Effect<unknown, E, R>) =>
  Effect.gen(function* () {
    const result = yield* Effect.result(effect);
    assert.isTrue(Result.isFailure(result));
    if (Result.isFailure(result) && isCheckedError(result.failure)) return result.failure.reason;
    return assert.fail("Expected a checked ownership refusal");
  });

describe("checked owner production acquisition composition", () => {
  it.effect("returns the original token and lifetime after exactly one tolerant native BEGIN", () =>
    fixture((input) =>
      Effect.gen(function* () {
        assert.strictEqual(yield* Ownership.ServerOwnershipLifetime, undefined);
        const owner = yield* Ownership.acquireServerOwnership(input.statePath);
        assert.lengthOf(input.acquisitions, 1);
        const acquired = input.acquisitions[0]!;
        assert.strictEqual(acquired.timeout, 250);
        assert.isTrue(acquired.connection.isTransaction);
        owner.lifetime.assertCurrent();
        NodeAssert.throws(() => ({ ...owner.lifetime }).assertCurrent());
        const scope = yield* Scope.Scope;
        NodeAssert.throws(
          () =>
            beginCheckedServerOwnership(
              acquired.connection,
              input.stateDir,
              NodePath.join(input.stateDir, "server-owner.sqlite"),
              scope,
            ),
          (error) =>
            isCheckedError(error) && error.reason === "unexpected_existing_owner_transaction",
        );
        const prepared = yield* prepareSyntheticOpen(owner.heldRuntimeOwner, input.root);
        return yield* Effect.gen(function* () {
          yield* Incarnation.bindOpen(prepared.open);
          const sql = yield* SqlClient.SqlClient;
          yield* sql`CREATE TABLE synthetic_fixture (value TEXT)`;
          // Match server.ts's real service composition; no snapshot token is minted.
          yield* Effect.gen(function* () {
            const held = yield* HeldRuntimeOwner;
            const lifetime = yield* Ownership.ServerOwnershipLifetime;
            assert.strictEqual(held.token, owner.heldRuntimeOwner);
            assert.strictEqual(lifetime, owner.lifetime);
            const bound = yield* bindHeldDatabaseOwner(held.token);
            assertCheckedDatabaseOwner(bound);
          }).pipe(
            Effect.provideService(HeldRuntimeOwner, { token: owner.heldRuntimeOwner }),
            Effect.provideService(Ownership.ServerOwnershipLifetime, owner.lifetime),
          );
          assert.lengthOf(input.acquisitions, 1);
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: input.dbPath })));
      }),
    ),
  );

  it.effect("replacement during the original BEGIN never yields checked cleanup authority", () =>
    fixture((input) =>
      Effect.gen(function* () {
        const lockPath = NodePath.join(input.stateDir, "server-owner.sqlite");
        const exec = vi.mocked(NodeSqlite.DatabaseSync.prototype.exec).getMockImplementation()!;
        vi.mocked(NodeSqlite.DatabaseSync.prototype.exec).mockImplementationOnce(function (
          this: NodeSqlite.DatabaseSync,
          sql: string,
        ) {
          exec.call(this, sql);
          NodeFS.renameSync(lockPath, `${lockPath}.retained`);
          NodeFS.writeFileSync(lockPath, "");
        });
        const owner = yield* Ownership.acquireServerOwnership(input.statePath);
        // Do not advance the monitor clock: acquisition itself must revoke.
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`CREATE TABLE synthetic_fixture (value TEXT)`;
          assert.strictEqual(
            yield* checkedRefusal(bindHeldDatabaseOwner(owner.heldRuntimeOwner)),
            "owner_scope_closed",
          );
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: input.dbPath })));
        NodeAssert.throws(() => owner.lifetime.assertCurrent());
        assert.lengthOf(input.acquisitions, 1);
      }),
    ),
  );

  it.effect("revokes before any owning-scope finalizer while the native lock is still held", () =>
    fixture((input) =>
      Effect.gen(function* () {
        const scope = yield* Scope.make();
        yield* Effect.addFinalizer((exit) => Scope.close(scope, exit));
        const owner = yield* Ownership.acquireServerOwnership(input.statePath).pipe(
          Scope.provide(scope),
        );
        const prepared = yield* prepareSyntheticOpen(owner.heldRuntimeOwner, input.root);
        yield* Effect.gen(function* () {
          yield* Incarnation.bindOpen(prepared.open);
          const sql = yield* SqlClient.SqlClient;
          yield* sql`CREATE TABLE synthetic_fixture (value TEXT)`;
          const bound = yield* bindHeldDatabaseOwner(owner.heldRuntimeOwner);
          let finalized = false;
          yield* Scope.addFinalizer(
            scope,
            Effect.sync(() => {
              finalized = true;
              assert.isTrue(input.acquisitions[0]!.connection.isOpen);
              assert.isTrue(input.acquisitions[0]!.connection.isTransaction);
              NodeAssert.throws(
                () => assertCheckedDatabaseOwner(bound),
                (error) => isCheckedError(error) && error.reason === "owner_scope_closed",
              );
              NodeAssert.throws(() => owner.lifetime.assertCurrent());
            }),
          );
          yield* Scope.close(scope, Exit.void);
          assert.isTrue(finalized);
          assert.isFalse(input.acquisitions[0]!.connection.isOpen);
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: input.dbPath })));
      }),
    ),
  );

  it.effect(
    "monitor-detected lock replacement cannot regain authority after path restoration",
    () =>
      fixture((input) =>
        Effect.gen(function* () {
          const owner = yield* Ownership.acquireServerOwnership(input.statePath);
          const lockPath = NodePath.join(input.stateDir, "server-owner.sqlite");
          NodeFS.renameSync(lockPath, `${lockPath}.retained`);
          NodeFS.writeFileSync(lockPath, "");
          yield* TestClock.adjust("5 seconds");
          NodeFS.renameSync(lockPath, `${lockPath}.replacement`);
          NodeFS.renameSync(`${lockPath}.retained`, lockPath);
          NodeAssert.throws(() => owner.lifetime.assertCurrent());
          yield* Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            yield* sql`CREATE TABLE synthetic_fixture (value TEXT)`;
            assert.strictEqual(
              yield* checkedRefusal(bindHeldDatabaseOwner(owner.heldRuntimeOwner)),
              "owner_scope_closed",
            );
          }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: input.dbPath })));
        }),
      ),
  );

  it.effect("unsupported aliased binding preserves ordinary startup and publication", () =>
    fixture((input) =>
      Effect.gen(function* () {
        const alias = NodePath.join(input.root, "aliased-userdata");
        NodeFS.symlinkSync(input.stateDir, alias, "dir");
        const statePath = NodePath.join(alias, "server-runtime.json");
        yield* Effect.scoped(
          Effect.gen(function* () {
            const owner = yield* Ownership.acquireServerOwnership(statePath);
            owner.lifetime.assertCurrent();
            yield* owner.publish({
              version: 1,
              pid: process.pid,
              port: 3773,
              origin: "http://127.0.0.1:3773",
              startedAt: "2026-11-16T00:00:00.000Z",
            });
            assert.isString(
              Option.getOrThrow(yield* RuntimeState.readPersistedServerRuntimeState(statePath))
                .ownerId,
            );
            yield* Effect.gen(function* () {
              assert.strictEqual(
                yield* checkedRefusal(bindHeldDatabaseOwner(owner.heldRuntimeOwner)),
                "owner_binding_unavailable",
              );
            }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: input.dbPath })));
          }),
        );
        assert.isFalse(yield* (yield* FileSystem.FileSystem).exists(statePath));
        assert.lengthOf(input.acquisitions, 1);
      }),
    ),
  );

  it.effect("lock-only startup uses the same single acquisition without publishing state", () =>
    fixture((input) =>
      Effect.gen(function* () {
        yield* Effect.scoped(Ownership.holdServerOwnershipLock(input.statePath));
        assert.lengthOf(input.acquisitions, 1);
        assert.strictEqual(input.acquisitions[0]!.timeout, 250);
        assert.isFalse(input.acquisitions[0]!.connection.isOpen);
        assert.isFalse(yield* (yield* FileSystem.FileSystem).exists(input.statePath));
      }),
    ),
  );

  it.effect("legacy-owner refusal preserves its record and releases the one acquisition", () =>
    fixture((input) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const legacy = {
          version: 1,
          pid: process.pid,
          port: 3773,
          origin: "http://127.0.0.1:3773",
          startedAt: "2100-01-01T00:00:00.000Z",
        };
        const before = JSON.stringify(legacy);
        yield* fs.writeFileString(input.statePath, before);
        const refused = yield* Effect.scoped(
          Ownership.acquireServerOwnership(input.statePath),
        ).pipe(
          // An unavailable start-time probe must conservatively retain a live
          // pre-lock owner. The synthetic fixture starts no process.
          Effect.provideService(ProcessRunner.ProcessRunner, {
            run: (input) =>
              Effect.fail(
                new ProcessRunner.ProcessSpawnError({
                  command: input.command,
                  argumentCount: input.args.length,
                  cause: new Error("Synthetic unavailable start-time probe"),
                }),
              ),
          }),
          Effect.result,
        );
        assert.isTrue(Result.isFailure(refused));
        if (Result.isFailure(refused))
          assert.strictEqual(refused.failure._tag, "ServerAlreadyRunningError");
        assert.strictEqual(yield* fs.readFileString(input.statePath), before);
        assert.lengthOf(input.acquisitions, 1);
        assert.isFalse(input.acquisitions[0]!.connection.isOpen);
        yield* fs.writeFileString(input.statePath, JSON.stringify({ ...legacy, pid: 0 }));
        yield* Effect.scoped(Ownership.acquireServerOwnership(input.statePath));
        assert.lengthOf(input.acquisitions, 2);
      }),
    ),
  );
});
