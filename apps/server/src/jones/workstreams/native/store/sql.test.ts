import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import * as NativeSql from "./sql.ts";
import * as RegistryStore from "./registryStore.ts";
import { scope, seed, testLayer } from "./sql.testkit.ts";

it.effect(
  "one owner transaction rolls back every nested write and rejects a cross-owner savepoint",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const db = yield* NativeSql.NativeSql;
      const registry = yield* RegistryStore.RegistryStore;
      const sql = yield* SqlClient.SqlClient;
      const failed = yield* Effect.exit(
        db.transaction(
          scope,
          Effect.gen(function* () {
            yield* registry.advance(scope, { server_generation: 1, registry_version: 0 });
            yield* db.transaction({ ...scope, owner_id: "other" }, Effect.void);
          }),
        ),
      );
      expect(Exit.isFailure(failed)).toBe(true);
      expect((yield* sql`SELECT registry_version FROM jones_workstreams_registries`)[0]).toEqual({
        registry_version: 0,
      });
    }).pipe(Effect.provide(testLayer())),
);
