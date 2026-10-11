import {
  MAX_CONNECTION_COMPATIBILITY_HINTS,
  StoredConnectionCompatibilityHint,
  decodeConnectionCompatibilityHints,
} from "@t3tools/client-runtime/platform";
import type { EnvironmentId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Base64 from "effect/encoding/Base64";
import * as Hex from "effect/encoding/Hex";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import type * as ElectronSafeStorage from "../electron/ElectronSafeStorage.ts";

const MAX_ENCRYPTED_BYTES = 131_072;
const HintJson = Schema.fromJsonString(StoredConnectionCompatibilityHint);
const encodeHint = Schema.encodeEffect(HintJson);
const decodeHint = Schema.decodeUnknownOption(HintJson, { onExcessProperty: "error" });
const encodeKey = Schema.encodeEffect(Schema.fromJsonString(Schema.Array(Schema.String)));

export class DesktopConnectionCompatibilityHintStoreError extends Schema.TaggedError<DesktopConnectionCompatibilityHintStoreError>()(
  "DesktopConnectionCompatibilityHintStoreError",
  { operation: Schema.Literals(["read", "write"]), cause: Schema.Defect() },
) {
  override get message(): string {
    return `Could not ${this.operation} desktop connection compatibility hints.`;
  }
}

/** Independent records have no access to the connection catalog or its migration. */
export const make = Effect.fn("desktop.connectionCompatibilityHintStore.make")(function* (input: {
  readonly directory: string;
  readonly protection: Pick<
    ElectronSafeStorage.ElectronSafeStorage["Service"],
    "isEncryptionAvailable" | "encryptString" | "decryptString"
  >;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const filename = path.join(input.directory, "connection-compatibility-hints-v1.sqlite");
  const keyFor = Effect.fn("desktop.connectionCompatibilityHintStore.key")(function* (
    environmentId: EnvironmentId,
    routeBinding: string,
  ) {
    const encoded = yield* encodeKey([environmentId, routeBinding]);
    return Hex.encode(yield* crypto.digest("SHA-256", new TextEncoder().encode(encoded)));
  });
  const withDatabase = <A, E>(
    operation: "read" | "write",
    use: Effect.Effect<A, E, SqlClient.SqlClient>,
  ) =>
    Effect.scoped(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`PRAGMA busy_timeout = 1000`;
        if (operation === "write") {
          yield* sql`CREATE TABLE IF NOT EXISTS compatibility_hints (
            record_key TEXT PRIMARY KEY NOT NULL CHECK(length(record_key) = 64),
            encrypted_hint TEXT NOT NULL CHECK(length(encrypted_hint) <= 131072)
          ) WITHOUT ROWID`;
        }
        return yield* use;
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename, readonly: operation === "read" }))),
    );

  const read = Effect.gen(function* () {
    if (!(yield* fs.exists(filename)) || !(yield* input.protection.isEncryptionAvailable))
      return [];
    const rows = yield* withDatabase(
      "read",
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        return yield* sql<{ record_key: string; encrypted_hint: string | null }>`
        SELECT record_key,
          CASE WHEN length(encrypted_hint) <= ${MAX_ENCRYPTED_BYTES} THEN encrypted_hint END AS encrypted_hint
        FROM compatibility_hints LIMIT ${MAX_CONNECTION_COMPATIBILITY_HINTS + 1}`;
      }),
    );
    if (rows.length > MAX_CONNECTION_COMPATIBILITY_HINTS) return [];
    const hints: StoredConnectionCompatibilityHint[] = [];
    for (const row of rows) {
      if (typeof row.encrypted_hint !== "string") continue;
      const bytes = Option.getOrNull(
        yield* Effect.fromResult(Base64.decode(row.encrypted_hint)).pipe(Effect.option),
      );
      if (bytes === null) continue;
      const raw = yield* input.protection.decryptString(bytes).pipe(Effect.option);
      if (Option.isNone(raw)) continue;
      const hint = Option.getOrNull(decodeHint(raw.value));
      if (
        hint === null ||
        (yield* keyFor(hint.environmentId, hint.routeBinding)) !== row.record_key
      )
        continue;
      hints.push(hint);
    }
    return decodeConnectionCompatibilityHints(hints);
  }).pipe(
    Effect.mapError(
      (cause) => new DesktopConnectionCompatibilityHintStoreError({ operation: "read", cause }),
    ),
  );

  const set = Effect.fn("desktop.connectionCompatibilityHintStore.set")(
    function* (
      environmentId: EnvironmentId,
      routeBinding: string,
      kind: StoredConnectionCompatibilityHint["kind"] | null,
    ) {
      const hint = yield* Schema.decodeUnknownEffect(StoredConnectionCompatibilityHint)({
        environmentId,
        routeBinding,
        kind: kind ?? "outdated-update",
      });
      const key = yield* keyFor(hint.environmentId, hint.routeBinding);
      if (kind === null) {
        if (!(yield* fs.exists(filename))) return true;
        return yield* withDatabase(
          "write",
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            yield* sql.withTransaction(
              sql`DELETE FROM compatibility_hints WHERE record_key = ${key}`,
            );
            return true;
          }),
        );
      }
      if (!(yield* input.protection.isEncryptionAvailable)) return false;
      const encrypted = Base64.encode(
        yield* input.protection.encryptString(yield* encodeHint(hint)),
      );
      if (encrypted.length > MAX_ENCRYPTED_BYTES) return false;
      yield* fs.makeDirectory(input.directory, { recursive: true });
      return yield* withDatabase(
        "write",
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          // BEGIN IMMEDIATE in NodeSqliteClient serializes this limit and mutation across processes.
          return yield* sql.withTransaction(
            Effect.gen(function* () {
              const existing =
                yield* sql`SELECT record_key FROM compatibility_hints WHERE record_key = ${key}`;
              const occupied =
                yield* sql`SELECT record_key FROM compatibility_hints LIMIT ${MAX_CONNECTION_COMPATIBILITY_HINTS}`;
              if (existing.length === 0 && occupied.length >= MAX_CONNECTION_COMPATIBILITY_HINTS)
                return false;
              yield* sql`INSERT INTO compatibility_hints (record_key, encrypted_hint) VALUES (${key}, ${encrypted})
          ON CONFLICT(record_key) DO UPDATE SET encrypted_hint = excluded.encrypted_hint`;
              return true;
            }),
          );
        }),
      );
    },
    Effect.mapError(
      (cause) => new DesktopConnectionCompatibilityHintStoreError({ operation: "write", cause }),
    ),
  );
  return { read, set };
});
