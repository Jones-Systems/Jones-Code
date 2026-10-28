// @effect-diagnostics nodeBuiltinImport:off -- Synthetic metadata/OS-owner fixtures and subprocess lock probes are retained for root cleanup.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeChildProcess from "node:child_process";
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as SqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as SqlClient from "effect/sql/SqlClient";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import * as Config from "../../../../config.ts";
import { NativeStoreAuthorityPersistenceError } from "../../../../environment/nativeStoreAuthorityPersistence.ts";
import * as DesktopOwner from "../../DesktopNativeOwner.ts";
import { syntheticT3Home, syntheticVersion, syntheticBuild } from "../../enrollment/testHome.ts";
import type { DesktopNativeEnrollmentOwner } from "../../enrollment/desktopOwner.ts";
import * as Owner from "./setupOwner.ts";
vi.mock("../../../../../package.json", () => ({
  default: {
    version: "0.0.0-preview.20261002.100",
    jonesSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "b".repeat(40),
    },
  },
}));
const fixture = Effect.gen(function* () {
  // This deliberately unclosed scope preserves fixture bytes through parent delivery.
  const retention = yield* Scope.make();
  const f = yield* syntheticT3Home("nw-setup-owner-").pipe(
    Effect.provideService(Scope.Scope, retention),
  );
  const db = new NodeSqlite.DatabaseSync(f.dbPath);
  db.exec("CREATE TABLE writer_probe(value TEXT)");
  db.close();
  NodeFS.chmodSync(f.dbPath, 0o600);
  return f;
});
const live: Authority.NativeStoreAuthority["Service"] = {
  readCurrent: Effect.succeed({
    environmentId: "synthetic-environment",
    authorityNamespace: "t3-native:12345678-1234-4234-8234-123456789abc",
    storeGeneration: 7,
  }),
  trustProvider: { readTrustSnapshot: () => ({ readiness: "ready", trustedEnvironments: [] }) },
};
const competing = (db: string, evidence: string) => {
  const probe = NodeChildProcess.spawnSync(
    "python3",
    [
      "-c",
      "import sqlite3,sys\nc=sqlite3.connect(sys.argv[1],timeout=0)\ntry:\n c.execute('BEGIN IMMEDIATE');c.rollback();print('available')\nexcept sqlite3.OperationalError:\n print('locked')\nfinally:\n c.close()\n",
      db,
    ],
    { encoding: "utf8" },
  );
  NodeFS.appendFileSync(
    evidence,
    JSON.stringify({
      argv: ["python3", "-c", "sqlite-lock-probe", db],
      pid: probe.pid,
      exit: probe.status,
      output: probe.stdout.trim(),
      pid_absent: !NodeFS.existsSync("/proc/" + probe.pid),
    }) + "\n",
  );
  expect(probe.status).toBe(0);
  expect(NodeFS.existsSync("/proc/" + probe.pid)).toBe(false);
  return probe.stdout.trim();
};
it.effect.each(["DELETE", "WAL"] as const)(
  "standalone %s owner uses the selected connection without dropping another writer lock",
  (mode) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql.unsafe("PRAGMA journal_mode=" + mode);
        const owner = yield* Owner.makeStandaloneSetupOwner(f, sql, live);
        const op = yield* Owner.refreshSetupOwner(owner);
        const facts = yield* Owner.setupOperationFacts(op);
        expect(facts.descriptor.build).toEqual(syntheticBuild);
        expect(facts.descriptor.kind).toBe("standalone");
        yield* sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`INSERT INTO writer_probe VALUES ('retained-private-sentinel')`;
            for (let i = 0; i < 2; i++) {
              yield* facts.authority.readCurrent;
              yield* facts.check;
              expect(competing(f.dbPath, NodePath.join(f.baseDir, "lock-probes.jsonl"))).toBe(
                "locked",
              );
            }
            expect((yield* Owner.refreshSetupOwner(owner).pipe(Effect.result))._tag).toBe(
              "Failure",
            );
            expect(competing(f.dbPath, NodePath.join(f.baseDir, "lock-probes.jsonl"))).toBe(
              "locked",
            );
          }),
        );
        expect(competing(f.dbPath, NodePath.join(f.baseDir, "lock-probes.jsonl"))).toBe(
          "available",
        );
        expect((yield* Owner.refreshSetupOwner({ ...owner }).pipe(Effect.result))._tag).toBe(
          "Failure",
        );
        expect((yield* Owner.setupOperationFacts({ ...op }).pipe(Effect.result))._tag).toBe(
          "Failure",
        );
        const authority = NodeFS.readFileSync(f.authorityPath, "utf8");
        NodeFS.writeFileSync(
          f.authorityPath,
          authority.replace('"store_generation":7', '"store_generation":8'),
        );
        expect((yield* facts.check.pipe(Effect.result))._tag).toBe("Failure");
      }).pipe(Effect.provide(SqliteClient.layer({ filename: f.dbPath })), Effect.scoped);
    }),
);
it.effect(
  "desktop refresh waits before operations; in-writer checks perform no refresh and preserve inherited writer evidence",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      let refreshes = 0,
        current = true;
      let wait: Effect.Effect<void> = Effect.void;
      const owner: DesktopNativeEnrollmentOwner = {
        binding: {
          home: f.baseDir,
          databasePath: f.dbPath,
          profile: NodePath.join(f.baseDir, "profile"),
          environmentId: "synthetic-environment",
          version: syntheticVersion,
          sourceSha: syntheticBuild.sha,
          sourceTree: syntheticBuild.tree,
          manifestSha256: "c".repeat(64),
        },
        assertCurrent: () => {
          if (!current) throw new Error("fixture writer no longer owns channel");
        },
        refresh: Effect.suspend(() => {
          refreshes++;
          return wait;
        }),
      };
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const handle = yield* Owner.makeDesktopSetupOwner(f, sql, owner);
        const entered = yield* Deferred.make<void>(),
          release = yield* Deferred.make<void>();
        wait = Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)));
        const worker = yield* Effect.forkChild(Owner.refreshSetupOwner(handle));
        yield* Deferred.await(entered);
        yield* sql.withTransaction(
          sql`INSERT INTO writer_probe VALUES ('unrelated writer during refresh')`,
        );
        yield* Deferred.succeed(release, undefined);
        const op = yield* Fiber.join(worker),
          facts = yield* Owner.setupOperationFacts(op);
        const before = refreshes;
        yield* sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`INSERT INTO writer_probe VALUES ('desktop bounded checker')`;
            yield* facts.check;
            yield* facts.authority.readCurrent;
            expect(refreshes).toBe(before);
            expect(competing(f.dbPath, NodePath.join(f.baseDir, "lock-probes.jsonl"))).toBe(
              "locked",
            );
            current = false;
            expect((yield* facts.check.pipe(Effect.result))._tag).toBe("Failure");
            expect(competing(f.dbPath, NodePath.join(f.baseDir, "lock-probes.jsonl"))).toBe(
              "locked",
            );
          }),
        );
      }).pipe(Effect.provide(SqliteClient.layer({ filename: f.dbPath })), Effect.scoped);
    }),
);
it.effect(
  "wrong SQL client, replacement inode and selected-client substitution refuse qualification",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture,
        other = yield* fixture;
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        expect(
          (yield* Owner.makeStandaloneSetupOwner(other, sql, live).pipe(Effect.result))._tag,
        ).toBe("Failure");
        const handle = yield* Owner.makeStandaloneSetupOwner(f, sql, live),
          op = yield* Owner.refreshSetupOwner(handle);
        const check = (yield* Owner.setupOperationFacts(op)).check;
        yield* Effect.gen(function* () {
          expect((yield* check.pipe(Effect.result))._tag).toBe("Failure");
        }).pipe(Effect.provide(SqliteClient.layer({ filename: other.dbPath })), Effect.scoped);
        NodeFS.renameSync(f.dbPath, f.dbPath + ".original");
        NodeFS.copyFileSync(f.dbPath + ".original", f.dbPath);
        expect((yield* check.pipe(Effect.result))._tag).toBe("Failure");
      }).pipe(Effect.provide(SqliteClient.layer({ filename: f.dbPath })), Effect.scoped);
    }),
);

it.effect(
  "production owner layer construction performs no qualification, desktop IPC, or setup writes",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      let refreshes = 0;
      const desktop = DesktopOwner.DesktopNativeOwner.of({
        available: false,
        binding: undefined,
        assertCurrent: () => {
          throw new Error("unqualified");
        },
        refresh: Effect.sync(() => {
          refreshes++;
        }).pipe(
          Effect.andThen(
            Effect.fail(
              new NativeStoreAuthorityPersistenceError(
                "launcher_upgrade_required",
                "synthetic absent owner",
              ),
            ),
          ),
        ),
      });
      yield* Effect.gen(function* () {
        const config = yield* Config.ServerConfig;
        const context = yield* Layer.build(
          Owner.runtimeLayer.pipe(
            Layer.provide(Config.layer({ ...config, mode: "desktop" })),
            Layer.provide(Layer.succeed(DesktopOwner.DesktopNativeOwner, desktop)),
            Layer.provide(Layer.succeed(Authority.NativeStoreAuthority, live)),
          ),
        );
        expect(context).toBeDefined();
        expect(refreshes).toBe(0);
        expect(NodeFS.existsSync(NodePath.join(f.baseDir, "workstreams"))).toBe(false);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            Config.layerTest(process.cwd(), f.baseDir),
            SqliteClient.layer({ filename: f.dbPath }),
          ),
        ),
        Effect.scoped,
      );
    }).pipe(Effect.provide(NodeServices.layer)),
);
