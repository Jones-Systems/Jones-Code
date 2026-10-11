import {
  ConnectionCatalogDocument,
  type ConnectionCatalogDocument as ConnectionCatalogDocumentType,
  EMPTY_CONNECTION_CATALOG_DOCUMENT,
  StoredConnectionCompatibilityHint,
  MAX_CONNECTION_COMPATIBILITY_HINTS,
  decodeConnectionCompatibilityHints,
  listConnectionCompatibilityHints,
  setConnectionCompatibilityHintInRecords,
  catalogCompatibilityRouteBinding,
} from "@t3tools/client-runtime/platform";
import { EnvironmentId } from "@t3tools/contracts";
import { ConnectionTransientError } from "@t3tools/client-runtime/connection";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

import * as MobileSecureStorage from "../persistence/mobile-secure-storage";
import { migrateLegacyConnectionCatalog } from "./migration";

export const CONNECTION_CATALOG_KEY = "t3code.connection-catalog.v1";
export const LEGACY_CONNECTIONS_KEY = "t3code.connections";

function catalogError(operation: string, cause: unknown) {
  return new ConnectionTransientError({
    reason: "remote-unavailable",
    detail: `Could not ${operation} the local connection catalog: ${String(cause)}`,
  });
}

const ConnectionCatalogDocumentJson = Schema.fromJsonString(ConnectionCatalogDocument);
const decodeConnectionCatalogDocument = Schema.decodeEffect(ConnectionCatalogDocumentJson);
const encodeConnectionCatalogDocument = Schema.encodeEffect(ConnectionCatalogDocumentJson);

const decodeCatalog = Effect.fn("mobile.connectionStorage.decodeCatalog")(function* (raw: string) {
  return yield* decodeConnectionCatalogDocument(raw).pipe(
    Effect.mapError((cause) => catalogError("decode", cause)),
  );
});

const encodeCatalog = Effect.fn("mobile.connectionStorage.encodeCatalog")(function* (
  catalog: ConnectionCatalogDocumentType,
) {
  return yield* encodeConnectionCatalogDocument(catalog).pipe(
    Effect.mapError((cause) => catalogError("encode", cause)),
  );
});

export const COMPATIBILITY_HINTS_KEY = "t3code.connection-compatibility-hints.v1";
// SecureStore calls cannot be canceled after dispatch. Hold this shared lock until they settle.
const hintLock = Semaphore.makeUnsafe(1);
const decodeHintJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const encodeHintJson = Schema.encodeEffect(
  Schema.fromJsonString(Schema.Array(StoredConnectionCompatibilityHint)),
);

export const makeCompatibilityHintStore = Effect.fn(
  "mobile.connectionStorage.makeCompatibilityHintStore",
)(function* () {
  const storage = yield* MobileSecureStorage.MobileSecureStorage;
  const load = Effect.gen(function* () {
    const raw = yield* storage.getItem(COMPATIBILITY_HINTS_KEY);
    if (raw === null || raw.length > MAX_CONNECTION_COMPATIBILITY_HINTS * 17_408) return [];
    return yield* decodeHintJson(raw).pipe(
      Effect.map(decodeConnectionCompatibilityHints),
      Effect.catch(() => Effect.succeed([])),
    );
  });
  const read = hintLock
    .withPermits(1)(load.pipe(Effect.uninterruptible))
    .pipe(Effect.mapError((cause) => catalogError("read compatibility hints from", cause)));
  const set = Effect.fn("mobile.connectionStorage.writeCompatibilityHint")(
    function* (environmentId: EnvironmentId, routeBinding: string, kind: "outdated-update" | null) {
      yield* Schema.decodeUnknownEffect(StoredConnectionCompatibilityHint)({
        environmentId,
        routeBinding,
        kind: kind ?? "outdated-update",
      });
      yield* hintLock.withPermits(1)(
        Effect.gen(function* () {
          const records = yield* load;
          if (
            kind !== null &&
            records.length >= MAX_CONNECTION_COMPATIBILITY_HINTS &&
            !records.some(
              (record) =>
                record.environmentId === environmentId && record.routeBinding === routeBinding,
            )
          )
            return yield* Effect.fail(
              catalogError("save compatibility hints in", "Compatibility hint capacity exceeded."),
            );
          const next = setConnectionCompatibilityHintInRecords(
            records,
            environmentId,
            routeBinding,
            kind,
          );
          yield* storage.setItem(COMPATIBILITY_HINTS_KEY, yield* encodeHintJson(next));
        }).pipe(Effect.uninterruptible),
      );
    },
    Effect.mapError((cause) => catalogError("save compatibility hints in", cause)),
  );
  return { read, set };
});

interface CatalogStore {
  readonly read: Effect.Effect<ConnectionCatalogDocumentType, ConnectionTransientError>;
  readonly compatibilityHints: {
    readonly read: Effect.Effect<ReadonlyArray<StoredConnectionCompatibilityHint>>;
    readonly set: (
      environmentId: EnvironmentId,
      routeBinding: string,
      kind: "outdated-update" | null,
    ) => Effect.Effect<void, ConnectionTransientError>;
  };
  readonly update: (
    transform: (catalog: ConnectionCatalogDocumentType) => ConnectionCatalogDocumentType,
  ) => Effect.Effect<void, ConnectionTransientError>;
}

export const make = Effect.fn("mobile.connectionStorage.makeCatalogStore")(function* () {
  const storage = yield* MobileSecureStorage.MobileSecureStorage;
  const getItem = (key: string) =>
    storage.getItem(key).pipe(Effect.mapError((cause) => catalogError("load", cause)));
  const setItem = (key: string, value: string) =>
    storage.setItem(key, value).pipe(Effect.mapError((cause) => catalogError("save", cause)));
  const deleteItem = (key: string) =>
    storage.removeItem(key).pipe(Effect.mapError((cause) => catalogError("delete", cause)));
  const hints = yield* makeCompatibilityHintStore();
  const snapshot = Effect.gen(function* () {
    const raw = yield* getItem(CONNECTION_CATALOG_KEY);
    return raw === null || raw.trim() === ""
      ? EMPTY_CONNECTION_CATALOG_DOCUMENT
      : yield* decodeCatalog(raw);
  });
  const compatibilityHints: CatalogStore["compatibilityHints"] = {
    read: Effect.gen(function* () {
      const records = yield* hints.read;
      return listConnectionCompatibilityHints(yield* snapshot, records);
    }).pipe(
      Effect.catch((error) =>
        Effect.logWarning("Could not load historical mobile connection compatibility hints.", {
          error,
        }).pipe(Effect.as([])),
      ),
    ),
    set: (environmentId, routeBinding, kind) =>
      Effect.gen(function* () {
        if (
          kind !== null &&
          catalogCompatibilityRouteBinding(yield* snapshot, environmentId) !== routeBinding
        )
          return;
        yield* hints.set(environmentId, routeBinding, kind);
      }),
  };
  const state = yield* Ref.make<Option.Option<ConnectionCatalogDocumentType>>(Option.none());
  const lock = yield* Semaphore.make(1);

  const loadLegacyCatalog = Effect.fn("mobile.connectionStorage.loadLegacyCatalog")(function* () {
    const legacyRaw = yield* getItem(LEGACY_CONNECTIONS_KEY);
    const catalog =
      legacyRaw === null || legacyRaw.trim() === ""
        ? EMPTY_CONNECTION_CATALOG_DOCUMENT
        : yield* migrateLegacyConnectionCatalog(legacyRaw).pipe(
            Effect.mapError((cause) => catalogError("migrate", cause)),
            Effect.catch((error) =>
              Effect.logWarning("Discarding corrupt legacy mobile connections", error).pipe(
                Effect.as(EMPTY_CONNECTION_CATALOG_DOCUMENT),
              ),
            ),
          );
    if (legacyRaw !== null && legacyRaw.trim() !== "") {
      const encoded = yield* encodeCatalog(catalog);
      yield* setItem(CONNECTION_CATALOG_KEY, encoded);
      yield* deleteItem(LEGACY_CONNECTIONS_KEY);
    }
    return catalog;
  });

  const loadUnlocked = Effect.fn("mobile.connectionStorage.loadCatalog")(function* () {
    const cached = yield* Ref.get(state);
    if (Option.isSome(cached)) {
      return cached.value;
    }
    const raw = yield* getItem(CONNECTION_CATALOG_KEY);
    let catalog: ConnectionCatalogDocumentType;
    if (raw !== null && raw.trim() !== "") {
      catalog = yield* decodeCatalog(raw).pipe(
        Effect.catch((error) =>
          Effect.logWarning("Discarding corrupt mobile connection catalog", error).pipe(
            Effect.andThen(deleteItem(CONNECTION_CATALOG_KEY)),
            Effect.andThen(loadLegacyCatalog()),
          ),
        ),
      );
    } else {
      catalog = yield* loadLegacyCatalog();
    }
    yield* Ref.set(state, Option.some(catalog));
    return catalog;
  });

  const read = lock.withPermits(1)(loadUnlocked());
  const update: CatalogStore["update"] = Effect.fn("mobile.connectionStorage.updateCatalog")(
    function* (transform) {
      yield* lock.withPermits(1)(
        Effect.gen(function* () {
          const previous = yield* loadUnlocked();
          const next = transform(previous);
          if (previous.targets !== next.targets || previous.profiles !== next.profiles) {
            for (const record of yield* hints.read) {
              if (
                catalogCompatibilityRouteBinding(previous, record.environmentId) ===
                  record.routeBinding &&
                catalogCompatibilityRouteBinding(next, record.environmentId) !== record.routeBinding
              )
                yield* hints.set(record.environmentId, record.routeBinding, null);
            }
          }
          const encoded = yield* encodeCatalog(next);
          yield* setItem(CONNECTION_CATALOG_KEY, encoded);
          yield* Ref.set(state, Option.some(next));
        }),
      );
    },
  );

  return { read, update, compatibilityHints } satisfies CatalogStore;
});
