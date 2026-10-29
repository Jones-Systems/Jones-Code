// @effect-diagnostics nodeBuiltinImport:off -- Synthetic build/launcher metadata and real SQLite ownership scopes; fixtures retained for root cleanup.
import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqlClient from "@t3tools/shared/nodeSqliteClient";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as Result from "effect/Result";
import * as SqlClient from "effect/sql/SqlClient";
import * as Config from "../../../../config.ts";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import {
  initializeNativeStoreAuthority,
  acquireExistingNativeAuthorityReadUse,
} from "../../../../environment/nativeStoreAuthorityPersistence.ts";
import * as Ownership from "../../../../serverOwnership.ts";
import * as ProcessRunner from "../../../../processRunner.ts";
import { ServiceLauncherHostProcess } from "../../../../cloud/serviceLauncherClient.ts";
import {
  SERVICE_LAUNCHER_CONTEXT_ENV,
  SERVICE_LAUNCHER_PROTOCOL,
} from "../../../../cloud/serviceProtocol.ts";
import * as Updates from "../../../updates/UpdateAdmissionFence.ts";
import { syntheticT3Home, syntheticVersion, syntheticBuild } from "../../enrollment/testHome.ts";
import * as Current from "./currentAuthority.ts";
import * as Qualified from "../../../cloud/qualifiedRuntime.ts";
import * as Selected from "../../enrollment/trustedCliContext.ts";

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
  const retained = yield* Scope.make();
  const f = yield* syntheticT3Home("nw-current-authority-").pipe(
    Effect.provideService(Scope.Scope, retained),
  );
  const db = new NodeSqlite.DatabaseSync(f.dbPath);
  db.exec("CREATE TABLE writer_probe(value TEXT)");
  db.close();
  NodeFS.chmodSync(f.dbPath, 0o600);
  initializeNativeStoreAuthority(f.authorityDir, "synthetic-environment");
  return f;
});
const withRuntime = <A, E, R>(
  use: (input: {
    current: Current.CurrentAuthority["Service"];
    sql: SqlClient.SqlClient;
    updates: Updates.UpdateAdmissionFence["Service"];
    disconnect: () => void;
    baseDir: string;
    authorityDir: string;
  }) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const f = yield* fixture;
    // Explicit directory form has no Config.layerTest temporary-scope cleanup.
    const configContext = yield* Layer.build(Config.layerTest(f.baseDir, f.baseDir));
    const config = Context.get(configContext, Config.ServerConfig);
    const disconnects = new Set<(...args: ReadonlyArray<unknown>) => void>();
    let connected = true;
    const host = ServiceLauncherHostProcess.of({
      get connected() {
        return connected;
      },
      send: () => true,
      on: (event, listener) => {
        if (event === "disconnect") disconnects.add(listener);
      },
      off: (event, listener) => {
        if (event === "disconnect") disconnects.delete(listener);
      },
    });
    return yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      // This is the real original server-owner SQLite lock and its root-owned scope token.
      const ownership = yield* Ownership.acquireServerOwnership(config.serverRuntimeStatePath);
      const updates = yield* Updates.make;
      return yield* Effect.gen(function* () {
        const current = yield* Current.CurrentAuthority;
        return yield* use({
          current,
          sql,
          updates,
          baseDir: f.baseDir,
          authorityDir: f.authorityDir,
          disconnect: () => {
            connected = false;
            for (const listener of disconnects) listener();
          },
        });
      }).pipe(
        Effect.provide(
          Current.layer.pipe(
            Layer.provide(Authority.layer.pipe(Layer.provide(Environment.layerIdentity))),
          ),
        ),
        Effect.provideService(Ownership.ServerOwnershipLifetime, ownership.lifetime),
        Effect.provideService(Updates.UpdateAdmissionFence, updates),
      );
    }).pipe(
      Effect.provide(NodeSqlClient.layer({ filename: f.dbPath })),
      Effect.provideService(Config.ServerConfig, config),
      Effect.provideService(ServiceLauncherHostProcess, host),
      Effect.provideService(HostProcess.Environment, {
        [SERVICE_LAUNCHER_CONTEXT_ENV]: JSON.stringify({
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          childVersion: syntheticVersion,
        }),
      }),
      Effect.provideService(HostProcess.HomeDirectory, f.baseDir),
      Effect.provide(ProcessRunner.layer),
      Effect.scoped,
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

it.effect(
  "refuses actual ambient preparation before lookups and preserves observable outer rollback",
  () =>
    withRuntime(({ current, sql }) =>
      Effect.gen(function* () {
        const selection = vi.spyOn(Selected, "makeTrustedNativeEnrollmentSelectedContext");
        try {
          const ambient = yield* sql
            .withTransaction(current.withPrepared(() => Effect.void))
            .pipe(Effect.result);
          expect(Result.isFailure(ambient) ? ambient.failure : undefined).toMatchObject({
            code: "ambient_transaction",
          });
          expect(selection).not.toHaveBeenCalled();
        } finally {
          selection.mockRestore();
        }
        const failed = yield* current
          .withPrepared((proof) =>
            sql.withTransaction(
              Effect.gen(function* () {
                expect(proof.build).toEqual(syntheticBuild);
                yield* sql`INSERT INTO writer_probe VALUES ('visible inner mutation')`;
                expect(yield* sql`SELECT value FROM writer_probe`).toHaveLength(1);
                const fs = process.getBuiltinModule("node:fs");
                const spies = ["openSync", "readFileSync", "lstatSync", "realpathSync"].map(
                  (name) =>
                    vi.spyOn(fs, name as "openSync").mockImplementation(() => {
                      throw new Error("external FS inside writer");
                    }),
                );
                try {
                  yield* proof.checkCurrent;
                } finally {
                  for (const spy of spies) spy.mockRestore();
                }
                return yield* Effect.fail("induced rollback" as const);
              }),
            ),
          )
          .pipe(Effect.result);
        expect(failed._tag).toBe("Failure");
        expect(yield* sql`SELECT value FROM writer_probe`).toHaveLength(0);
      }),
    ),
);
it.effect("copied, escaped and different-client proofs refuse permanently", () =>
  withRuntime(({ current, sql }) =>
    Effect.gen(function* () {
      let escaped: Current.PreparedRuntimeAuthority | undefined;
      yield* current
        .withPrepared((proof) =>
          Effect.gen(function* () {
            escaped = proof;
            yield* proof.checkCurrent;
            expect(
              (yield* Current.checkPreparedAuthority({ ...proof }).pipe(Effect.result))._tag,
            ).toBe("Failure");
            const second = yield* fixture;
            const result = yield* proof.checkCurrent.pipe(
              Effect.provide(NodeSqlClient.layer({ filename: second.dbPath })),
              Effect.scoped,
              Effect.result,
            );
            expect(Result.isFailure(result) ? result.failure : undefined).toMatchObject({
              code: "wrong_client",
            });
            expect((yield* proof.checkCurrent.pipe(Effect.result))._tag).toBe("Failure");
          }),
        )
        .pipe(Effect.result);
      expect((yield* escaped!.checkCurrent.pipe(Effect.result))._tag).toBe("Failure");
      expect((yield* Current.checkPreparedAuthority(escaped!).pipe(Effect.result))._tag).toBe(
        "Failure",
      );
      expect(yield* sql`SELECT value FROM writer_probe`).toHaveLength(0);
    }),
  ),
);
it.effect(
  "update cancellation completes SQL rollback before launcher drain and cannot release sibling exclusion",
  () =>
    withRuntime(({ current, sql, updates, authorityDir }) =>
      Effect.gen(function* () {
        const sibling = acquireExistingNativeAuthorityReadUse(authorityDir);
        const entered = yield* Deferred.make<void>();
        const rollback = yield* Deferred.make<void>();
        const worker = yield* Effect.forkChild(
          current.withPrepared(() =>
            sql.withTransaction(
              Effect.gen(function* () {
                yield* sql`INSERT INTO writer_probe VALUES ('cancelled')`;
                expect(yield* sql`SELECT value FROM writer_probe`).toHaveLength(1);
                yield* Deferred.succeed(entered, undefined);
                return yield* Effect.never;
              }).pipe(Effect.ensuring(Deferred.succeed(rollback, undefined))),
            ),
          ),
        );
        yield* Deferred.await(entered);
        const handoff = yield* Effect.forkChild(
          updates.withLauncherHandoff(
            Effect.gen(function* () {
              expect(yield* Deferred.isDone(rollback)).toBe(true);
              expect(yield* sql`SELECT value FROM writer_probe`).toHaveLength(0);
              sibling.assertHeld();
              return "launcher accepted";
            }),
            () => false,
          ),
        );
        yield* Fiber.await(worker);
        expect(yield* Fiber.join(handoff)).toBe("launcher accepted");
        sibling.assertHeld();
        sibling.release();
      }),
    ),
);
it.effect("launcher disconnect permanently invalidates a prepared owner", () =>
  withRuntime(({ current, disconnect }) =>
    current
      .withPrepared((proof) =>
        Effect.gen(function* () {
          yield* proof.checkCurrent;
          disconnect();
          expect((yield* proof.checkCurrent.pipe(Effect.result))._tag).toBe("Failure");
        }),
      )
      .pipe(Effect.result, Effect.asVoid),
  ),
);

it.effect("blocked real payload qualification leaves the same-client writer available", () =>
  withRuntime(({ current, sql }) =>
    Effect.gen(function* () {
      let entered!: () => void;
      let release!: () => void;
      const enteredPromise = new Promise<void>((resolve) => {
        entered = resolve;
      });
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const original = Qualified.currentQualifiedRuntimeBinding;
      const spy = vi
        .spyOn(Qualified, "currentQualifiedRuntimeBinding")
        .mockImplementation(async (...args) => {
          entered();
          await gate;
          return original(...args);
        });
      try {
        const worker = yield* Effect.forkChild(current.withPrepared((proof) => proof.checkCurrent));
        yield* Effect.promise(() => enteredPromise);
        yield* sql.withTransaction(
          sql`INSERT INTO writer_probe VALUES ('writer during payload wait')`,
        );
        release();
        yield* Fiber.join(worker);
        expect(yield* sql`SELECT value FROM writer_probe`).toHaveLength(1);
      } finally {
        release();
        spy.mockRestore();
      }
    }),
  ),
);
