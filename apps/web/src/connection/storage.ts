import {
  ConnectionCatalogDocument,
  type ConnectionCatalogDocument as ConnectionCatalogDocumentType,
  EMPTY_CONNECTION_CATALOG_DOCUMENT,
  ORCHESTRATION_CACHE_SCHEMA_VERSION,
  ORCHESTRATION_THREAD_CACHE_SCHEMA_VERSION,
  StoredOrchestrationShellSnapshot,
  StoredOrchestrationThreadSnapshot,
  decodeOrDiscardOrchestrationCache,
  putRemoteDpopTokenInCatalog,
  listConnectionCompatibilityHints,
  catalogCompatibilityRouteBinding,
  decodeConnectionCompatibilityHints,
  MAX_CONNECTION_COMPATIBILITY_HINTS,
  StoredConnectionCompatibilityHint,
  putConnectionProfileInCatalog,
  removeConnectionProfileFromCatalog,
  registerConnectionInCatalog,
  removeCatalogValue,
  removeConnectionFromCatalog,
  setConnectionEnabledInCatalog,
  setRoutesInCatalog,
  replaceCatalogValue,
  Persistence,
} from "@t3tools/client-runtime/platform";
import { TokenStore } from "@t3tools/client-runtime/authorization";
import {
  ConnectionTransientError,
  ConnectionBlockedError,
  CredentialStore,
  ProfileStore,
  GitHubRoutingPermissions,
  StoredGitHubRoutingPermission,
  gitHubRoutingConnectionKey,
  gitHubRoutingPermissionFor,
} from "@t3tools/client-runtime/connection";
import { EnvironmentId, ServerConfig, ThreadId, VcsListRefsResult } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { projectFaviconCache } from "../assets/projectFaviconCache";

const DATABASE_NAME = "t3code:connection-runtime";
const DATABASE_VERSION = 4;
const COMPATIBILITY_HINT_DATABASE_NAME = "t3code:connection-compatibility-hints";
const COMPATIBILITY_HINT_STORE_NAME = "compatibility-hints";
const CATALOG_STORE_NAME = "catalog";
const SHELL_STORE_NAME = "shell";
const THREAD_STORE_NAME = "thread";
const SERVER_CONFIG_STORE_NAME = "server-config";
const VCS_REFS_STORE_NAME = "vcs-refs";
const CATALOG_KEY = "document";
const StoredShellSnapshot = StoredOrchestrationShellSnapshot;
const StoredShellSnapshotJson = Schema.fromJsonString(StoredShellSnapshot);
const StoredThreadSnapshot = StoredOrchestrationThreadSnapshot;
const StoredThreadSnapshotJson = Schema.fromJsonString(StoredThreadSnapshot);
const StoredServerConfig = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  environmentId: EnvironmentId,
  config: ServerConfig,
});
const StoredServerConfigJson = Schema.fromJsonString(StoredServerConfig);
const StoredVcsRefs = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  environmentId: EnvironmentId,
  cwd: Schema.String,
  refs: VcsListRefsResult,
});
const StoredVcsRefsJson = Schema.fromJsonString(StoredVcsRefs);
const ConnectionCatalogDocumentJson = Schema.fromJsonString(ConnectionCatalogDocument);
const decodeConnectionCatalogDocument = Schema.decodeUnknownEffect(ConnectionCatalogDocumentJson);
const encodeConnectionCatalogDocument = Schema.encodeEffect(ConnectionCatalogDocumentJson);
const decodeStoredShellSnapshot = Schema.decodeUnknownEffect(StoredShellSnapshotJson);
const encodeStoredShellSnapshot = Schema.encodeEffect(StoredShellSnapshotJson);
const decodeStoredThreadSnapshot = Schema.decodeUnknownEffect(StoredThreadSnapshotJson);
const encodeStoredThreadSnapshot = Schema.encodeEffect(StoredThreadSnapshotJson);
const decodeStoredServerConfig = Schema.decodeUnknownEffect(StoredServerConfigJson);
const encodeStoredServerConfig = Schema.encodeEffect(StoredServerConfigJson);
const decodeStoredVcsRefs = Schema.decodeUnknownEffect(StoredVcsRefsJson);
const encodeStoredVcsRefs = Schema.encodeEffect(StoredVcsRefsJson);

function catalogError(operation: string, cause: unknown) {
  return new ConnectionTransientError({
    reason: "remote-unavailable",
    detail: `Could not ${operation} the local connection catalog: ${String(cause)}`,
  });
}

function persistenceError(
  operation:
    | "list-targets"
    | "list-disabled-targets"
    | "list-compatibility-hints"
    | "set-compatibility-hint"
    | "register-connection"
    | "set-connection-routes"
    | "remove-connection"
    | "set-connection-enabled"
    | "load-shell"
    | "save-shell"
    | "load-thread"
    | "save-thread"
    | "remove-thread"
    | "load-server-config"
    | "save-server-config"
    | "load-vcs-refs"
    | "save-vcs-refs"
    | "remove-vcs-refs"
    | "clear-vcs-refs"
    | "clear-environment",
  cause: unknown,
) {
  return new Persistence.ConnectionPersistenceError({
    operation,
    message: `Could not ${operation.replaceAll("-", " ")}: ${String(cause)}`,
  });
}

const openDatabase = Effect.fn("web.connectionStorage.openDatabase")(function* () {
  return yield* Effect.callback<IDBDatabase, ConnectionTransientError>((resume) => {
    if (typeof indexedDB === "undefined") {
      resume(
        Effect.fail(catalogError("open", "IndexedDB is unavailable in this browser context.")),
      );
      return;
    }
    try {
      const request = indexedDB.open(DATABASE_NAME, DATABASE_VERSION);
      request.addEventListener("upgradeneeded", () => {
        if (!request.result.objectStoreNames.contains(CATALOG_STORE_NAME)) {
          request.result.createObjectStore(CATALOG_STORE_NAME);
        }
        if (!request.result.objectStoreNames.contains(SHELL_STORE_NAME)) {
          request.result.createObjectStore(SHELL_STORE_NAME);
        }
        if (!request.result.objectStoreNames.contains(THREAD_STORE_NAME)) {
          request.result.createObjectStore(THREAD_STORE_NAME);
        }
        if (!request.result.objectStoreNames.contains(SERVER_CONFIG_STORE_NAME)) {
          request.result.createObjectStore(SERVER_CONFIG_STORE_NAME);
        }
        if (!request.result.objectStoreNames.contains(VCS_REFS_STORE_NAME)) {
          request.result.createObjectStore(VCS_REFS_STORE_NAME);
        }
      });
      request.addEventListener("error", () => {
        resume(Effect.fail(catalogError("open", request.error ?? "Unknown IndexedDB error")));
      });
      request.addEventListener("success", () => {
        resume(Effect.succeed(request.result));
      });
    } catch (cause) {
      resume(Effect.fail(catalogError("open", cause)));
    }
  });
});

// An older renderer can keep its version-4 runtime connection while hints open independently.
const openCompatibilityHintDatabase = Effect.fn(
  "web.connectionStorage.openCompatibilityHintDatabase",
)(function* () {
  return yield* Effect.callback<IDBDatabase, ConnectionTransientError>((resume) => {
    let settled = false;
    let request: IDBOpenDBRequest | undefined;
    try {
      request = indexedDB.open(COMPATIBILITY_HINT_DATABASE_NAME, 1);
      const owned = request;
      owned.addEventListener("upgradeneeded", () => {
        if (settled) {
          owned.transaction?.abort();
          return;
        }
        owned.result.createObjectStore(COMPATIBILITY_HINT_STORE_NAME);
      });
      owned.addEventListener("error", () => {
        if (settled) return;
        settled = true;
        resume(Effect.fail(catalogError("open compatibility hints", owned.error)));
      });
      owned.addEventListener("blocked", () => {
        if (settled) return;
        settled = true;
        resume(
          Effect.fail(catalogError("open compatibility hints", "Hint database upgrade blocked.")),
        );
      });
      owned.addEventListener("success", () => {
        if (settled) {
          owned.result.close();
          return;
        }
        settled = true;
        resume(Effect.succeed(owned.result));
      });
    } catch (cause) {
      settled = true;
      resume(Effect.fail(catalogError("open compatibility hints", cause)));
    }
    return Effect.sync(() => {
      if (!settled) {
        settled = true;
        request?.transaction?.abort();
      }
    });
  });
});

interface DatabaseHandle {
  readonly get: Effect.Effect<IDBDatabase, ConnectionTransientError>;
  /** Forget `database` if it is still the shared connection, so the next access reopens. */
  readonly invalidate: (database: IDBDatabase) => Effect.Effect<void>;
}

/** Share a connection until the browser closes it; the next access reopens it. */
const makeDatabaseHandle = Effect.fn("web.connectionStorage.makeDatabaseHandle")(function* (
  open: () => Effect.Effect<IDBDatabase, ConnectionTransientError> = openDatabase,
) {
  const lock = yield* Semaphore.make(1);
  let current: IDBDatabase | null = null;
  const forget = (database: IDBDatabase) => {
    if (current === database) current = null;
  };
  const get = Effect.suspend(() =>
    current !== null
      ? Effect.succeed(current)
      : lock.withPermits(1)(
          Effect.gen(function* () {
            if (current !== null) return current;
            const opened = yield* open();
            current = opened;
            opened.addEventListener("close", () => forget(opened));
            // Another tab upgrading the schema waits on this connection.
            opened.addEventListener("versionchange", () => {
              forget(opened);
              opened.close();
            });
            return opened;
          }),
        ),
  );
  const close = lock.withPermits(1)(
    Effect.sync(() => {
      current?.close();
      current = null;
    }),
  );
  const handle: DatabaseHandle = {
    get,
    invalidate: (database) => Effect.sync(() => forget(database)),
  };
  return { handle, close };
});

/**
 * Runs `use` on the shared connection. A connection the browser already closed
 * throws InvalidStateError even when no close event reached this tab, so drop
 * it and retry once on a fresh one instead of failing every later operation.
 */
function withDatabase<A>(
  database: DatabaseHandle,
  use: (opened: IDBDatabase) => Effect.Effect<A, ConnectionTransientError>,
) {
  // Only a connection that opened and then failed is stale; a failing open is
  // storage being unavailable, which a retry would not fix.
  const closedConnection = Symbol("closedConnection");
  const attempt = Effect.flatMap(database.get, (opened) =>
    use(opened).pipe(
      Effect.catchIf(
        (error) => error.detail.includes("InvalidStateError"),
        (error) =>
          database
            .invalidate(opened)
            .pipe(Effect.andThen(Effect.fail({ [closedConnection]: error } as const))),
      ),
    ),
  );
  return attempt.pipe(
    Effect.catchIf(
      (error): error is { readonly [closedConnection]: ConnectionTransientError } =>
        closedConnection in error,
      () => Effect.flatMap(database.get, use),
    ),
  );
}

function readDatabaseValueOnConnection(database: IDBDatabase, storeName: string, key: IDBValidKey) {
  return Effect.callback<unknown, ConnectionTransientError>((resume) => {
    try {
      const request = database.transaction(storeName, "readonly").objectStore(storeName).get(key);
      request.addEventListener("error", () => {
        resume(Effect.fail(catalogError("read", request.error ?? "Unknown IndexedDB read error")));
      });
      request.addEventListener("success", () => {
        resume(Effect.succeed(request.result));
      });
    } catch (cause) {
      resume(Effect.fail(catalogError("read", cause)));
    }
  }).pipe(Effect.withSpan("web.connectionStorage.readDatabaseValue"));
}

function writeDatabaseValueOnConnection(
  database: IDBDatabase,
  storeName: string,
  key: IDBValidKey,
  value: unknown,
) {
  return Effect.callback<void, ConnectionTransientError>((resume) => {
    try {
      const transaction = database.transaction(storeName, "readwrite");
      // Every failed write fires "abort". A failed commit, such as
      // QuotaExceededError, fires only "abort" and no "error".
      transaction.addEventListener("abort", () => {
        resume(
          Effect.fail(catalogError("write", transaction.error ?? "Unknown IndexedDB write error")),
        );
      });
      transaction.addEventListener("complete", () => {
        resume(Effect.void);
      });
      transaction.objectStore(storeName).put(value, key);
    } catch (cause) {
      resume(Effect.fail(catalogError("write", cause)));
    }
  }).pipe(Effect.withSpan("web.connectionStorage.writeDatabaseValue"));
}

function removeDatabaseValueOnConnection(
  database: IDBDatabase,
  storeName: string,
  key: IDBValidKey,
) {
  return Effect.callback<void, ConnectionTransientError>((resume) => {
    try {
      const transaction = database.transaction(storeName, "readwrite");
      transaction.addEventListener("abort", () => {
        resume(
          Effect.fail(
            catalogError("remove", transaction.error ?? "Unknown IndexedDB remove error"),
          ),
        );
      });
      transaction.addEventListener("complete", () => {
        resume(Effect.void);
      });
      transaction.objectStore(storeName).delete(key);
    } catch (cause) {
      resume(Effect.fail(catalogError("remove", cause)));
    }
  }).pipe(Effect.withSpan("web.connectionStorage.removeDatabaseValue"));
}

function removeDatabaseValuesInRangeOnConnection(
  database: IDBDatabase,
  storeName: string,
  range: IDBKeyRange,
) {
  return Effect.callback<void, ConnectionTransientError>((resume) => {
    try {
      const transaction = database.transaction(storeName, "readwrite");
      transaction.addEventListener("abort", () => {
        resume(
          Effect.fail(
            catalogError("remove", transaction.error ?? "Unknown IndexedDB cursor error"),
          ),
        );
      });
      transaction.addEventListener("complete", () => {
        resume(Effect.void);
      });
      const request = transaction.objectStore(storeName).openCursor(range);
      request.addEventListener("error", () => {
        resume(
          Effect.fail(catalogError("remove", request.error ?? "Unknown IndexedDB cursor error")),
        );
      });
      request.addEventListener("success", () => {
        const cursor = request.result;
        if (cursor === null) {
          return;
        }
        try {
          cursor.delete();
          cursor.continue();
        } catch (cause) {
          resume(Effect.fail(catalogError("remove", cause)));
        }
      });
    } catch (cause) {
      resume(Effect.fail(catalogError("remove", cause)));
    }
  }).pipe(Effect.withSpan("web.connectionStorage.removeDatabaseValuesInRange"));
}

function readDatabaseValue(database: DatabaseHandle, storeName: string, key: IDBValidKey) {
  return withDatabase(database, (opened) => readDatabaseValueOnConnection(opened, storeName, key));
}

function writeDatabaseValue(
  database: DatabaseHandle,
  storeName: string,
  key: IDBValidKey,
  value: unknown,
) {
  return withDatabase(database, (opened) =>
    writeDatabaseValueOnConnection(opened, storeName, key, value),
  );
}

function removeDatabaseValue(database: DatabaseHandle, storeName: string, key: IDBValidKey) {
  return withDatabase(database, (opened) =>
    removeDatabaseValueOnConnection(opened, storeName, key),
  );
}

function removeDatabaseValuesInRange(
  database: DatabaseHandle,
  storeName: string,
  range: IDBKeyRange,
) {
  return withDatabase(database, (opened) =>
    removeDatabaseValuesInRangeOnConnection(opened, storeName, range),
  );
}

function threadCacheKey(environmentId: EnvironmentId, threadId: ThreadId) {
  return `${environmentId}:${threadId}`;
}

function vcsRefsCacheKey(environmentId: EnvironmentId, cwd: string) {
  return `${environmentId}:${cwd}`;
}

const decodeCatalog = Effect.fn("web.connectionStorage.decodeCatalog")(function* (raw: string) {
  return yield* decodeConnectionCatalogDocument(raw).pipe(
    Effect.mapError((cause) => catalogError("decode", cause)),
  );
});

const encodeCatalog = Effect.fn("web.connectionStorage.encodeCatalog")(function* (
  catalog: ConnectionCatalogDocumentType,
) {
  return yield* encodeConnectionCatalogDocument(catalog).pipe(
    Effect.mapError((cause) => catalogError("encode", cause)),
  );
});

export interface CatalogBackend {
  readonly read: Effect.Effect<string | null, ConnectionTransientError>;
  readonly write: (raw: string) => Effect.Effect<void, ConnectionTransientError>;
  readonly snapshot?: Effect.Effect<string | null, ConnectionTransientError>;
  readonly quarantine?: (raw: string) => Effect.Effect<void, ConnectionTransientError>;
}

export function makeCatalogBackend(database: DatabaseHandle): CatalogBackend {
  const bridge = window.desktopBridge;
  if (bridge?.getConnectionCatalog !== undefined && bridge.setConnectionCatalog !== undefined) {
    return {
      snapshot:
        bridge.getConnectionCatalogSnapshot === undefined
          ? Effect.succeed(null)
          : Effect.tryPromise({
              try: () => bridge.getConnectionCatalogSnapshot!(),
              catch: (cause) => catalogError("read snapshot from", cause),
            }),
      read: Effect.tryPromise({
        try: () => bridge.getConnectionCatalog!(),
        catch: (cause) => catalogError("load", cause),
      }),
      write: (raw) =>
        Effect.tryPromise({
          try: () => bridge.setConnectionCatalog!(raw),
          catch: (cause) => catalogError("save", cause),
        }).pipe(
          Effect.flatMap((stored) =>
            stored
              ? Effect.void
              : Effect.fail(
                  catalogError(
                    "save",
                    "Desktop secure storage is unavailable in this system context.",
                  ),
                ),
          ),
        ),
    };
  }

  return {
    read: readDatabaseValue(database, CATALOG_STORE_NAME, CATALOG_KEY).pipe(
      Effect.map((value) => (typeof value === "string" ? value : null)),
    ),
    write: (raw) => writeDatabaseValue(database, CATALOG_STORE_NAME, CATALOG_KEY, raw),
    quarantine: (raw) =>
      writeDatabaseValue(database, CATALOG_STORE_NAME, `${CATALOG_KEY}:corrupt:${Date.now()}`, raw),
  };
}

export interface CompatibilityHintBackend {
  readonly supported: boolean;
  readonly read: Effect.Effect<unknown, ConnectionTransientError>;
  readonly set: (
    environmentId: EnvironmentId,
    routeBinding: string,
    kind: "outdated-update" | null,
  ) => Effect.Effect<void, ConnectionTransientError>;
}

/** No bridge fallback may write a catalog, including on an older desktop build. */
export function makeCompatibilityHintBackend(database: DatabaseHandle): CompatibilityHintBackend {
  const bridge = window.desktopBridge;
  if (bridge !== undefined) {
    const supported =
      bridge.getConnectionCatalogSnapshot !== undefined &&
      bridge.listConnectionCompatibilityHints !== undefined &&
      bridge.setConnectionCompatibilityHint !== undefined;
    return {
      supported,
      read: !supported
        ? Effect.succeed([])
        : Effect.tryPromise({
            try: () => bridge.listConnectionCompatibilityHints!(),
            catch: (cause) => catalogError("read compatibility hints from", cause),
          }),
      set: (environmentId, routeBinding, kind) =>
        !supported
          ? Effect.fail(
              catalogError(
                "save compatibility hints in",
                "This desktop build has no isolated hint store.",
              ),
            )
          : Effect.tryPromise({
              try: () =>
                bridge.setConnectionCompatibilityHint!({ environmentId, routeBinding, kind }),
              catch: (cause) => catalogError("save compatibility hints in", cause),
            }).pipe(
              Effect.flatMap((stored) =>
                stored
                  ? Effect.void
                  : Effect.fail(
                      catalogError(
                        "save compatibility hints in",
                        "Hint storage declined the write.",
                      ),
                    ),
              ),
            ),
    };
  }
  return {
    supported: true,
    read: withDatabase(database, (opened) =>
      Effect.callback<unknown, ConnectionTransientError>((resume) => {
        let transaction: IDBTransaction | undefined;
        let settled = false;
        try {
          transaction = opened.transaction(COMPATIBILITY_HINT_STORE_NAME, "readonly");
          const request = transaction
            .objectStore(COMPATIBILITY_HINT_STORE_NAME)
            .getAll(undefined, MAX_CONNECTION_COMPATIBILITY_HINTS + 1);
          transaction.addEventListener("complete", () => {
            settled = true;
            resume(Effect.succeed(request.result));
          });
          transaction.addEventListener("abort", () => {
            settled = true;
            resume(Effect.fail(catalogError("read compatibility hints", transaction?.error)));
          });
        } catch (cause) {
          settled = true;
          resume(Effect.fail(catalogError("read compatibility hints", cause)));
        }
        return Effect.sync(() => {
          if (!settled) transaction?.abort();
        });
      }),
    ),
    set: (environmentId, routeBinding, kind) =>
      withDatabase(database, (opened) =>
        Effect.callback<void, ConnectionTransientError>((resume) => {
          let transaction: IDBTransaction | undefined;
          let settled = false;
          let failure: unknown;
          try {
            transaction = opened.transaction(COMPATIBILITY_HINT_STORE_NAME, "readwrite");
            const owned = transaction;
            owned.addEventListener("complete", () => {
              settled = true;
              resume(Effect.void);
            });
            owned.addEventListener("abort", () => {
              settled = true;
              resume(
                Effect.fail(catalogError("write compatibility hints", failure ?? owned.error)),
              );
            });
            const store = owned.objectStore(COMPATIBILITY_HINT_STORE_NAME);
            const key: IDBValidKey = [environmentId, routeBinding];
            if (kind === null) store.delete(key);
            else {
              const count = store.count();
              count.addEventListener("success", () => {
                try {
                  // These callbacks keep the native readwrite transaction active through its put.
                  const existing = store.get(key);
                  existing.addEventListener("success", () => {
                    try {
                      if (
                        existing.result === undefined &&
                        count.result >= MAX_CONNECTION_COMPATIBILITY_HINTS
                      ) {
                        failure = "Compatibility hint capacity exceeded.";
                        owned.abort();
                        return;
                      }
                      store.put({ environmentId, routeBinding, kind }, key);
                    } catch (cause) {
                      failure = cause;
                      owned.abort();
                    }
                  });
                } catch (cause) {
                  failure = cause;
                  owned.abort();
                }
              });
            }
          } catch (cause) {
            failure = cause;
            try {
              transaction?.abort();
            } catch {
              /* An already finished transaction has no pending write. */
            }
            settled = true;
            resume(Effect.fail(catalogError("write compatibility hints", cause)));
          }
          return Effect.sync(() => {
            if (!settled) transaction?.abort();
          });
        }),
      ),
  };
}

/** Fresh diagnostic reads never invoke catalog recovery or migration. */
export function makeCompatibilityHintStore(
  backend: CompatibilityHintBackend,
  snapshot: Effect.Effect<string | null, ConnectionTransientError>,
) {
  const fresh = snapshot.pipe(
    Effect.flatMap((raw) =>
      raw === null || raw.trim() === ""
        ? Effect.succeed(EMPTY_CONNECTION_CATALOG_DOCUMENT)
        : decodeCatalog(raw),
    ),
  );
  const read = Effect.gen(function* () {
    const records = yield* backend.read;
    return listConnectionCompatibilityHints(yield* fresh, records);
  }).pipe(
    Effect.catch(() =>
      Effect.logWarning("Could not load historical connection compatibility hints.").pipe(
        Effect.as([]),
      ),
    ),
  );
  const set = Effect.fn("web.connectionStorage.setCompatibilityHint")(function* (
    environmentId: EnvironmentId,
    routeBinding: string,
    kind: "outdated-update" | null,
  ) {
    yield* Schema.decodeUnknownEffect(StoredConnectionCompatibilityHint)({
      environmentId,
      routeBinding,
      kind: kind ?? "outdated-update",
    }).pipe(Effect.mapError((cause) => catalogError("validate compatibility hint", cause)));
    if (!backend.supported) return yield* backend.set(environmentId, routeBinding, kind);
    if (
      kind !== null &&
      catalogCompatibilityRouteBinding(yield* fresh, environmentId) !== routeBinding
    )
      return;
    yield* backend.set(environmentId, routeBinding, kind);
  });
  const invalidate = (
    previous: ConnectionCatalogDocumentType,
    next: ConnectionCatalogDocumentType,
  ) =>
    !backend.supported
      ? Effect.void
      : Effect.gen(function* () {
          if (previous.targets === next.targets && previous.profiles === next.profiles) return;
          const records = decodeConnectionCompatibilityHints(yield* backend.read);
          for (const record of records) {
            if (
              catalogCompatibilityRouteBinding(previous, record.environmentId) ===
                record.routeBinding &&
              catalogCompatibilityRouteBinding(next, record.environmentId) !== record.routeBinding
            )
              yield* backend.set(record.environmentId, record.routeBinding, null);
          }
        }).pipe(
          Effect.catch(() =>
            Effect.logWarning("Could not clear historical connection compatibility hints."),
          ),
        );
  return { read, set, invalidate };
}

interface CatalogStore {
  readonly read: Effect.Effect<ConnectionCatalogDocumentType, ConnectionTransientError>;
  readonly update: (
    transform: (catalog: ConnectionCatalogDocumentType) => ConnectionCatalogDocumentType,
  ) => Effect.Effect<void, ConnectionTransientError>;
}

export const makeCatalogStore = Effect.fn("web.connectionStorage.makeCatalogStore")(function* (
  backend: CatalogBackend,
  beforeWrite?: (
    previous: ConnectionCatalogDocumentType,
    next: ConnectionCatalogDocumentType,
  ) => Effect.Effect<void, ConnectionTransientError>,
) {
  const state = yield* Ref.make<Option.Option<ConnectionCatalogDocumentType>>(Option.none());
  const lock = yield* Semaphore.make(1);

  const loadUnlocked = Effect.fn("web.connectionStorage.loadCatalog")(function* () {
    const cached = yield* Ref.get(state);
    if (Option.isSome(cached)) {
      return cached.value;
    }
    const raw = yield* backend.read;
    let catalog = EMPTY_CONNECTION_CATALOG_DOCUMENT;
    if (raw !== null && raw.trim() !== "") {
      catalog = yield* decodeCatalog(raw).pipe(
        Effect.catch((error) =>
          Effect.gen(function* () {
            yield* Effect.logWarning("Discarding a corrupt web connection catalog.", {
              error: error.message,
            });
            if (backend.quarantine !== undefined) {
              yield* backend.quarantine(raw).pipe(
                Effect.catch((cause) =>
                  Effect.logWarning("Could not quarantine the corrupt web connection catalog.", {
                    error: cause.message,
                  }),
                ),
              );
            }
            const encoded = yield* encodeCatalog(EMPTY_CONNECTION_CATALOG_DOCUMENT);
            yield* backend.write(encoded).pipe(
              Effect.catch((cause) =>
                Effect.logWarning("Could not persist the recovered web connection catalog.", {
                  error: cause.message,
                }),
              ),
            );
            return EMPTY_CONNECTION_CATALOG_DOCUMENT;
          }),
        ),
      );
    }
    yield* Ref.set(state, Option.some(catalog));
    return catalog;
  });

  const read = lock.withPermits(1)(loadUnlocked());
  const update: CatalogStore["update"] = Effect.fn("web.connectionStorage.updateCatalog")(
    function* (transform) {
      yield* lock.withPermits(1)(
        Effect.gen(function* () {
          const previous = yield* loadUnlocked();
          const next = transform(previous);
          if (beforeWrite !== undefined) yield* beforeWrite(previous, next);
          yield* backend.write(yield* encodeCatalog(next));
          yield* Ref.set(state, Option.some(next));
        }),
      );
    },
  );

  return { read, update } satisfies CatalogStore;
});

const GITHUB_ROUTING_KEY_PREFIX = "t3code:github-routing:";
const GITHUB_ROUTING_CHANGED = "t3code:github-routing-changed";
const isStoredGitHubRoutingPermission = Schema.is(StoredGitHubRoutingPermission);
const encodeStoredGitHubRoutingPermission = Schema.encodeSync(
  Schema.fromJsonString(StoredGitHubRoutingPermission),
);

/** Each grant has its own key so stale tabs and unrelated catalog saves cannot restore trust. */
export function makeBrowserGitHubRoutingPermissions(
  browser: Pick<Window, "localStorage"> & EventTarget = window,
) {
  const read = (key: string): StoredGitHubRoutingPermission | null => {
    try {
      const raw = browser.localStorage.getItem(key);
      const value: unknown = raw === null ? null : JSON.parse(raw);
      return isStoredGitHubRoutingPermission(value) &&
        key === `${GITHUB_ROUTING_KEY_PREFIX}${value.environmentId}`
        ? value
        : null;
    } catch {
      return null;
    }
  };
  const readAll = (): ReadonlyArray<StoredGitHubRoutingPermission> => {
    try {
      const values: StoredGitHubRoutingPermission[] = [];
      const storage = browser.localStorage;
      for (let index = 0; index < storage.length; index++) {
        const key = storage.key(index);
        if (key?.startsWith(GITHUB_ROUTING_KEY_PREFIX)) {
          const value = read(key);
          if (value !== null) values.push(value);
        }
      }
      return values;
    } catch {
      return [];
    }
  };
  const write = (environmentId: EnvironmentId, value: StoredGitHubRoutingPermission | null) =>
    Effect.try({
      try: () => {
        const key = `${GITHUB_ROUTING_KEY_PREFIX}${environmentId}`;
        if (value === null) browser.localStorage.removeItem(key);
        else browser.localStorage.setItem(key, encodeStoredGitHubRoutingPermission(value));
        browser.dispatchEvent(new Event(GITHUB_ROUTING_CHANGED));
      },
      catch: (cause) => catalogError("save GitHub routing permissions in", cause),
    });
  return GitHubRoutingPermissions.of({
    get: (entry) =>
      Effect.sync(() => {
        const value = read(`${GITHUB_ROUTING_KEY_PREFIX}${entry.target.environmentId}`);
        return gitHubRoutingPermissionFor(entry, value === null ? [] : [value]);
      }),
    changes: Stream.callback<ReadonlyArray<StoredGitHubRoutingPermission>>((queue) =>
      Effect.acquireRelease(
        Effect.sync(() => {
          const listener = (event: Event) => {
            if (event.type === "storage") {
              const key = (event as StorageEvent).key;
              if (key !== null && !key?.startsWith(GITHUB_ROUTING_KEY_PREFIX)) return;
            }
            Queue.offerUnsafe(queue, readAll());
          };
          browser.addEventListener("storage", listener);
          browser.addEventListener(GITHUB_ROUTING_CHANGED, listener);
          Queue.offerUnsafe(queue, readAll());
          return listener;
        }),
        (listener) =>
          Effect.sync(() => {
            browser.removeEventListener("storage", listener);
            browser.removeEventListener(GITHUB_ROUTING_CHANGED, listener);
          }),
      ).pipe(Effect.asVoid),
    ),
    set: (entry, permission) => {
      const connectionKey = gitHubRoutingConnectionKey(entry);
      if (connectionKey === null)
        return Effect.fail(
          new ConnectionBlockedError({
            reason: "configuration",
            detail: "This environment does not have a saved connection endpoint.",
          }),
        );
      return write(
        entry.target.environmentId,
        permission === "off"
          ? null
          : {
              environmentId: entry.target.environmentId,
              connectionKey,
              permission,
            },
      );
    },
    forget: (environmentId) => write(environmentId, null),
  });
}

export const layer = Layer.effectContext(
  Effect.gen(function* () {
    const { handle: database } = yield* Effect.acquireRelease(
      makeDatabaseHandle(),
      (owned) => owned.close,
    );
    const { handle: hintDatabase } = yield* Effect.acquireRelease(
      makeDatabaseHandle(openCompatibilityHintDatabase),
      (owned) => owned.close,
    );
    const backend = makeCatalogBackend(database);
    const hints = makeCompatibilityHintStore(
      makeCompatibilityHintBackend(hintDatabase),
      backend.snapshot ?? backend.read,
    );
    const catalog = yield* makeCatalogStore(backend, hints.invalidate);
    const githubRoutingPermissions = makeBrowserGitHubRoutingPermissions();

    const targetStore = Persistence.ConnectionTargetStore.of({
      list: catalog.read.pipe(
        Effect.map((document) => document.targets),
        Effect.mapError((cause) => persistenceError("list-targets", cause)),
      ),
      listDisabled: catalog.read.pipe(
        Effect.map((document) => document.disabledEnvironmentIds),
        Effect.mapError((cause) => persistenceError("list-disabled-targets", cause)),
      ),
      listCompatibilityHints: hints.read,
    });
    const registrationStore = Persistence.ConnectionRegistrationStore.of({
      setCompatibilityHint: (environmentId, routeBinding, kind) =>
        hints.set(environmentId, routeBinding, kind).pipe(
          Effect.mapError(
            () =>
              new Persistence.ConnectionPersistenceError({
                operation: "set-compatibility-hint",
                message: "Could not save historical connection compatibility hints.",
              }),
          ),
        ),
      register: (registration, routes) =>
        catalog
          .update((document) => registerConnectionInCatalog(document, registration, routes))
          .pipe(Effect.mapError((cause) => persistenceError("register-connection", cause))),
      setRoutes: (environmentId, routes) =>
        catalog
          .update((document) => setRoutesInCatalog(document, environmentId, routes))
          .pipe(Effect.mapError((cause) => persistenceError("set-connection-routes", cause))),
      remove: (environmentId) =>
        catalog
          .update((document) => removeConnectionFromCatalog(document, environmentId))
          .pipe(Effect.mapError((cause) => persistenceError("remove-connection", cause))),
      setEnabled: (environmentId, enabled) =>
        catalog
          .update((document) => setConnectionEnabledInCatalog(document, environmentId, enabled))
          .pipe(Effect.mapError((cause) => persistenceError("set-connection-enabled", cause))),
    });
    const profileStore = ProfileStore.make({
      get: (connectionId) =>
        catalog.read.pipe(
          Effect.map((document) =>
            Option.fromUndefinedOr(
              document.profiles.find((profile) => profile.connectionId === connectionId),
            ),
          ),
        ),
      put: (profile) =>
        catalog.update((document) => putConnectionProfileInCatalog(document, profile)),
      remove: (connectionId) =>
        catalog.update((document) => removeConnectionProfileFromCatalog(document, connectionId)),
    });
    const credentialStore = CredentialStore.make({
      get: (connectionId) =>
        catalog.read.pipe(
          Effect.map((document) =>
            Option.fromUndefinedOr(
              document.credentials.find((entry) => entry.connectionId === connectionId)?.credential,
            ),
          ),
        ),
      put: (connectionId, credential) =>
        catalog.update((document) => ({
          ...document,
          credentials: replaceCatalogValue(document.credentials, (value) => value.connectionId, {
            connectionId,
            credential,
          }),
        })),
      remove: (connectionId) =>
        catalog.update((document) => ({
          ...document,
          credentials: removeCatalogValue(
            document.credentials,
            (value) => value.connectionId,
            connectionId,
          ),
        })),
    });
    const remoteTokenStore = TokenStore.make({
      get: (environmentId) =>
        catalog.read.pipe(
          Effect.map((document) =>
            Option.fromUndefinedOr(
              document.remoteDpopTokens.find((token) => token.environmentId === environmentId),
            ),
          ),
        ),
      put: (token) => catalog.update((document) => putRemoteDpopTokenInCatalog(document, token)),
      remove: (environmentId) =>
        catalog.update((document) => ({
          ...document,
          remoteDpopTokens: removeCatalogValue(
            document.remoteDpopTokens,
            (value) => value.environmentId,
            environmentId,
          ),
        })),
    });
    const cacheStore = Persistence.EnvironmentCacheStore.of({
      loadShell: (environmentId) =>
        readDatabaseValue(database, SHELL_STORE_NAME, environmentId).pipe(
          Effect.tap(() => Effect.promise(() => projectFaviconCache.hydrate())),
          Effect.flatMap((raw) => {
            if (typeof raw !== "string") {
              return Effect.succeedNone;
            }
            return decodeOrDiscardOrchestrationCache(
              decodeStoredShellSnapshot(raw).pipe(
                Effect.mapError((cause) => persistenceError("load-shell", cause)),
                Effect.map((stored) =>
                  stored.environmentId === environmentId
                    ? Option.some(stored.snapshot)
                    : Option.none(),
                ),
              ),
              removeDatabaseValue(database, SHELL_STORE_NAME, environmentId),
            );
          }),
          Effect.mapError((cause) => persistenceError("load-shell", cause)),
        ),
      saveShell: (environmentId, snapshot) =>
        Effect.gen(function* () {
          const encoded = yield* encodeStoredShellSnapshot({
            schemaVersion: ORCHESTRATION_CACHE_SCHEMA_VERSION,
            environmentId,
            snapshot,
          }).pipe(Effect.mapError((cause) => persistenceError("save-shell", cause)));
          yield* writeDatabaseValue(database, SHELL_STORE_NAME, environmentId, encoded);
        }).pipe(
          Effect.mapError((cause) =>
            cause._tag === "ConnectionPersistenceError"
              ? cause
              : persistenceError("save-shell", cause),
          ),
        ),
      loadServerConfig: (environmentId) =>
        readDatabaseValue(database, SERVER_CONFIG_STORE_NAME, environmentId).pipe(
          Effect.flatMap((raw) => {
            if (typeof raw !== "string") {
              return Effect.succeedNone;
            }
            return decodeStoredServerConfig(raw).pipe(
              Effect.mapError((cause) => persistenceError("load-server-config", cause)),
              Effect.map((stored) =>
                stored.environmentId === environmentId ? Option.some(stored.config) : Option.none(),
              ),
            );
          }),
          Effect.mapError((cause) =>
            cause._tag === "ConnectionPersistenceError"
              ? cause
              : persistenceError("load-server-config", cause),
          ),
        ),
      saveServerConfig: (environmentId, config) =>
        Effect.gen(function* () {
          const encoded = yield* encodeStoredServerConfig({
            schemaVersion: 1,
            environmentId,
            config,
          }).pipe(Effect.mapError((cause) => persistenceError("save-server-config", cause)));
          yield* writeDatabaseValue(database, SERVER_CONFIG_STORE_NAME, environmentId, encoded);
        }).pipe(
          Effect.mapError((cause) =>
            cause._tag === "ConnectionPersistenceError"
              ? cause
              : persistenceError("save-server-config", cause),
          ),
        ),
      loadThread: (environmentId, threadId) =>
        readDatabaseValue(
          database,
          THREAD_STORE_NAME,
          threadCacheKey(environmentId, threadId),
        ).pipe(
          Effect.flatMap((raw) => {
            if (typeof raw !== "string") {
              return Effect.succeedNone;
            }
            return decodeOrDiscardOrchestrationCache(
              decodeStoredThreadSnapshot(raw).pipe(
                Effect.mapError((cause) => persistenceError("load-thread", cause)),
                Effect.map((stored) =>
                  stored.environmentId === environmentId && stored.threadId === threadId
                    ? Option.some(stored.snapshot)
                    : Option.none(),
                ),
              ),
              removeDatabaseValue(
                database,
                THREAD_STORE_NAME,
                threadCacheKey(environmentId, threadId),
              ),
            );
          }),
          Effect.mapError((cause) => persistenceError("load-thread", cause)),
        ),
      saveThread: (environmentId, snapshot) =>
        Effect.gen(function* () {
          const encoded = yield* encodeStoredThreadSnapshot({
            schemaVersion: ORCHESTRATION_THREAD_CACHE_SCHEMA_VERSION,
            environmentId,
            threadId: snapshot.projection.thread.id,
            snapshot,
          }).pipe(Effect.mapError((cause) => persistenceError("save-thread", cause)));
          yield* writeDatabaseValue(
            database,
            THREAD_STORE_NAME,
            threadCacheKey(environmentId, snapshot.projection.thread.id),
            encoded,
          );
        }).pipe(
          Effect.mapError((cause) =>
            cause._tag === "ConnectionPersistenceError"
              ? cause
              : persistenceError("save-thread", cause),
          ),
        ),
      loadVcsRefs: (environmentId, cwd) =>
        readDatabaseValue(database, VCS_REFS_STORE_NAME, vcsRefsCacheKey(environmentId, cwd)).pipe(
          Effect.flatMap((raw) => {
            if (typeof raw !== "string") {
              return Effect.succeedNone;
            }
            return decodeStoredVcsRefs(raw).pipe(
              Effect.mapError((cause) => persistenceError("load-vcs-refs", cause)),
              Effect.map((stored) =>
                stored.environmentId === environmentId && stored.cwd === cwd
                  ? Option.some(stored.refs)
                  : Option.none(),
              ),
            );
          }),
          Effect.mapError((cause) =>
            cause._tag === "ConnectionPersistenceError"
              ? cause
              : persistenceError("load-vcs-refs", cause),
          ),
        ),
      saveVcsRefs: (environmentId, cwd, refs) =>
        Effect.gen(function* () {
          const encoded = yield* encodeStoredVcsRefs({
            schemaVersion: 1,
            environmentId,
            cwd,
            refs,
          }).pipe(Effect.mapError((cause) => persistenceError("save-vcs-refs", cause)));
          yield* writeDatabaseValue(
            database,
            VCS_REFS_STORE_NAME,
            vcsRefsCacheKey(environmentId, cwd),
            encoded,
          );
        }).pipe(
          Effect.mapError((cause) =>
            cause._tag === "ConnectionPersistenceError"
              ? cause
              : persistenceError("save-vcs-refs", cause),
          ),
        ),
      removeVcsRefs: (environmentId, cwd) =>
        removeDatabaseValue(
          database,
          VCS_REFS_STORE_NAME,
          vcsRefsCacheKey(environmentId, cwd),
        ).pipe(Effect.mapError((cause) => persistenceError("remove-vcs-refs", cause))),
      clearVcsRefs: (environmentId) =>
        removeDatabaseValuesInRange(
          database,
          VCS_REFS_STORE_NAME,
          IDBKeyRange.bound(`${environmentId}:`, `${environmentId}:\uffff`),
        ).pipe(Effect.mapError((cause) => persistenceError("clear-vcs-refs", cause))),
      removeThread: (environmentId, threadId) =>
        removeDatabaseValue(
          database,
          THREAD_STORE_NAME,
          threadCacheKey(environmentId, threadId),
        ).pipe(Effect.mapError((cause) => persistenceError("remove-thread", cause))),
      clear: (environmentId) =>
        Effect.all(
          [
            Effect.promise(() => projectFaviconCache.clearEnvironment(environmentId)),
            removeDatabaseValue(database, SHELL_STORE_NAME, environmentId),
            removeDatabaseValuesInRange(
              database,
              THREAD_STORE_NAME,
              IDBKeyRange.bound(`${environmentId}:`, `${environmentId}:\uffff`),
            ),
            removeDatabaseValue(database, SERVER_CONFIG_STORE_NAME, environmentId),
            removeDatabaseValuesInRange(
              database,
              VCS_REFS_STORE_NAME,
              IDBKeyRange.bound(`${environmentId}:`, `${environmentId}:\uffff`),
            ),
          ],
          { concurrency: "unbounded", discard: true },
        ).pipe(Effect.mapError((cause) => persistenceError("clear-environment", cause))),
    });

    return Context.make(Persistence.ConnectionTargetStore, targetStore).pipe(
      Context.add(GitHubRoutingPermissions, githubRoutingPermissions),
      Context.add(Persistence.ConnectionRegistrationStore, registrationStore),
      Context.add(ProfileStore.ConnectionProfileStore, profileStore),
      Context.add(CredentialStore.ConnectionCredentialStore, credentialStore),
      Context.add(TokenStore.RemoteDpopAccessTokenStore, remoteTokenStore),
      Context.add(Persistence.EnvironmentCacheStore, cacheStore),
    );
  }),
);
