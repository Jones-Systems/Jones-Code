// @effect-diagnostics nodeBuiltinImport:off -- Synthetic native ownership and inode-race fixtures exercise real handles and a separate process.
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { assert, describe, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Ownership from "../../serverOwnership.ts";
import * as ProcessRunner from "../../processRunner.ts";
import {
  assertCheckedDatabaseOwner,
  bindCheckedDatabaseOwner,
  CheckedRuntimeOwnerError,
  invalidateCheckedServerOwnership,
  type HeldRuntimeOwnerToken,
} from "./CheckedRuntimeOwner.ts";

const isCheckedRuntimeOwnerError = Schema.is(CheckedRuntimeOwnerError);

const platform = ProcessRunner.layer.pipe(
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);
interface Fixture {
  readonly token: HeldRuntimeOwnerToken;
  readonly root: string;
  readonly stateDir: string;
  readonly dbPath: string;
  readonly serverRuntimeStatePath: string;
}
const fixture = <A, E, R>(use: (input: Fixture) => Effect.Effect<A, E, R>) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const root = yield* fs.realPath(
        yield* fs.makeTempDirectoryScoped({ prefix: "l8-checked-owner-" }),
      );
      const stateDir = NodePath.join(root, "userdata");
      yield* fs.makeDirectory(stateDir);
      const serverRuntimeStatePath = NodePath.join(stateDir, "server-runtime.json");
      const dbPath = NodePath.join(stateDir, "statev2.sqlite");
      return yield* Effect.gen(function* () {
        const ownership = yield* Ownership.acquireServerOwnership(serverRuntimeStatePath);
        return yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`CREATE TABLE synthetic_fixture (value TEXT)`;
          return yield* use({
            token: ownership.heldRuntimeOwner,
            root,
            stateDir,
            dbPath,
            serverRuntimeStatePath,
          });
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath })));
      }).pipe(
        Effect.provideService(HostProcess.HomeDirectory, root),
        Effect.provideService(HostProcess.Environment, {}),
      );
    }).pipe(Effect.provide(platform)),
  );

const reason = <E, R>(effect: Effect.Effect<unknown, E, R>) =>
  Effect.gen(function* () {
    const result = yield* Effect.result(effect);
    assert.isTrue(Result.isFailure(result));
    if (Result.isFailure(result) && isCheckedRuntimeOwnerError(result.failure))
      return result.failure.reason;
    assert.fail("Expected a checked-owner refusal");
    return "unexpected_success";
  });

// A separate process is essential: same-process SQLite connection behavior is
// not proof that stat-based capability checks retained the POSIX owner lock.
const childProgram = `
import * as Effect from 'effect/Effect';
import * as NodeServices from '@effect/platform-node/NodeServices';
import * as NodeCrypto from '@effect/platform-node/NodeCrypto';
import * as Layer from 'effect/Layer';
import * as HostProcess from '@t3tools/shared/HostProcess';
import { acquireServerOwnership } from ${JSON.stringify(NodeURL.pathToFileURL(NodePath.resolve(import.meta.dirname, "../../serverOwnership.ts")).href)};
import * as ProcessRunner from ${JSON.stringify(NodeURL.pathToFileURL(NodePath.resolve(import.meta.dirname, "../../processRunner.ts")).href)};
const result = await Effect.runPromise(Effect.scoped(acquireServerOwnership(process.argv[1])).pipe(
  Effect.match({onSuccess:()=>({tag:'acquired'}),onFailure:error=>({tag:error._tag})}),
  Effect.provideService(HostProcess.HomeDirectory,process.argv[2]),
  Effect.provideService(HostProcess.Environment,{}),
  Effect.provide(ProcessRunner.layer.pipe(Layer.provideMerge(Layer.mergeAll(NodeServices.layer,NodeCrypto.layer))))
));
process.stdout.write(JSON.stringify(result));
`;
const assertSecondOwnerRefused = (input: Fixture) => {
  const child = NodeChildProcess.spawnSync(
    process.execPath,
    ["--input-type=module", "--eval", childProgram, input.serverRuntimeStatePath, input.root],
    {
      encoding: "utf8",
      timeout: 5_000,
      env: {
        PATH: process.env.PATH,
        TMPDIR: input.root,
        HOME: input.root,
        LANG: "C.UTF-8",
        LC_ALL: "C.UTF-8",
      },
    },
  );
  assert.strictEqual(child.error, undefined);
  assert.strictEqual(child.signal, null);
  assert.strictEqual(child.status, 0, child.stderr);
  assert.deepStrictEqual(JSON.parse(child.stdout), { tag: "ServerAlreadyRunningError" });
};

describe("checked held runtime owner", () => {
  it.effect(
    "binds the actual application SqlClient and retains the native exclusion after repeated checks",
    () =>
      fixture((input) =>
        Effect.gen(function* () {
          const bound = yield* bindCheckedDatabaseOwner(input.token, input);
          for (let index = 0; index < 3; index++) {
            assertCheckedDatabaseOwner(bound);
            assertSecondOwnerRefused(input);
          }
        }),
      ),
  );

  it.effect("serialized tokens and PID/runtime-record objects authorize nothing", () =>
    fixture((input) =>
      Effect.gen(function* () {
        const bound = yield* bindCheckedDatabaseOwner(input.token, input);
        assert.strictEqual(JSON.stringify(bound), "{}");
        for (const unknown of [
          undefined,
          {},
          JSON.parse(JSON.stringify(bound)),
          { pid: process.pid, ownerId: "synthetic-owner", stateDir: input.stateDir },
        ]) {
          NodeAssert.throws(
            () => assertCheckedDatabaseOwner(unknown),
            (error) =>
              isCheckedRuntimeOwnerError(error) &&
              error.reason === "unknown_database_owner_capability",
          );
        }
      }),
    ),
  );

  it.effect("revokes the capability before the native handle is released", () =>
    fixture((input) =>
      Effect.gen(function* () {
        const bound = yield* bindCheckedDatabaseOwner(input.token, input);
        invalidateCheckedServerOwnership(input.token);
        NodeAssert.throws(
          () => assertCheckedDatabaseOwner(bound),
          (error) => isCheckedRuntimeOwnerError(error) && error.reason === "owner_scope_closed",
        );
        assertSecondOwnerRefused(input);
      }),
    ),
  );

  it.effect("an escaped capability is unavailable after its real owning scope closes", () =>
    Effect.gen(function* () {
      const bound = yield* fixture((input) => bindCheckedDatabaseOwner(input.token, input));
      NodeAssert.throws(
        () => assertCheckedDatabaseOwner(bound),
        (error) => isCheckedRuntimeOwnerError(error) && error.reason === "owner_scope_closed",
      );
    }),
  );

  it.effect("a different state-directory owner cannot bind the configured database", () =>
    fixture((input) =>
      Effect.gen(function* () {
        assert.strictEqual(
          yield* reason(bindCheckedDatabaseOwner(input.token, { ...input, stateDir: input.root })),
          "configured_owner_binding_mismatch",
        );
      }),
    ),
  );

  it.effect("a raw in-memory SQL layer cannot borrow an on-disk owner's capability", () =>
    fixture((input) =>
      Effect.gen(function* () {
        assert.strictEqual(
          yield* reason(bindCheckedDatabaseOwner(input.token, input)).pipe(
            Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" })),
          ),
          "application_sql_client_binding_mismatch",
        );
      }),
    ),
  );

  it.effect("binding refuses a SQL transaction before filesystem observation", () =>
    fixture((input) =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        assert.strictEqual(
          yield* reason(sql.withTransaction(bindCheckedDatabaseOwner(input.token, input))),
          "owner_binding_inside_sql_transaction",
        );
      }),
    ),
  );

  it.effect("replaced application database identity invalidates the captured binding", () =>
    fixture((input) =>
      Effect.gen(function* () {
        const bound = yield* bindCheckedDatabaseOwner(input.token, input);
        NodeFS.renameSync(input.dbPath, `${input.dbPath}.retained`);
        NodeFS.writeFileSync(input.dbPath, "");
        NodeAssert.throws(
          () => assertCheckedDatabaseOwner(bound),
          (error) =>
            isCheckedRuntimeOwnerError(error) && error.reason === "database_identity_changed",
        );
      }),
    ),
  );

  it.effect("a database symlink is not independent owner proof", () =>
    fixture((input) =>
      Effect.gen(function* () {
        NodeFS.renameSync(input.dbPath, `${input.dbPath}.retained`);
        NodeFS.symlinkSync(`${input.dbPath}.retained`, input.dbPath);
        assert.strictEqual(
          yield* reason(bindCheckedDatabaseOwner(input.token, input)),
          "unsupported_database_alias",
        );
      }),
    ),
  );

  it.effect("a cross-state hardlink cannot claim this database has only one owner route", () =>
    fixture((input) =>
      Effect.gen(function* () {
        NodeFS.linkSync(input.dbPath, NodePath.join(input.root, "other-state.sqlite"));
        assert.strictEqual(
          yield* reason(bindCheckedDatabaseOwner(input.token, input)),
          "unsupported_database_alias",
        );
      }),
    ),
  );

  it.effect(
    "a replaced owner-lock inode refuses operations without reading a second lock handle",
    () =>
      fixture((input) =>
        Effect.gen(function* () {
          const bound = yield* bindCheckedDatabaseOwner(input.token, input);
          const lockPath = NodePath.join(input.stateDir, "server-owner.sqlite");
          NodeFS.renameSync(lockPath, `${lockPath}.retained`);
          NodeFS.writeFileSync(lockPath, "");
          NodeAssert.throws(
            () => assertCheckedDatabaseOwner(bound),
            (error) =>
              isCheckedRuntimeOwnerError(error) && error.reason === "owner_lock_identity_changed",
          );
        }),
      ),
  );
});
