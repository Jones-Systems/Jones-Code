import {
  ConnectionTransientError,
  BearerConnectionTarget,
  BearerConnectionProfile,
  BearerConnectionCredential,
  BearerConnectionRegistration,
  ProfileStore,
  PrimaryConnectionTarget,
} from "@t3tools/client-runtime/connection";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import {
  ConnectionCatalogDocument,
  Persistence,
  connectionCompatibilityRouteBinding,
  catalogCompatibilityRouteBinding,
  setConnectionCompatibilityHintInRecords,
  type StoredConnectionCompatibilityHint,
  setConnectionEnabledInCatalog,
  removeConnectionFromCatalog,
} from "@t3tools/client-runtime/platform";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { afterEach, vi } from "vite-plus/test";

import * as ConnectionStorage from "./storage";

const emptyCatalog = {
  schemaVersion: 1,
  targets: [],
  profiles: [],
  credentials: [],
  remoteDpopTokens: [],
  disabledEnvironmentIds: [],
} as const;
const decodeCatalog = Schema.decodeUnknownSync(Schema.fromJsonString(ConnectionCatalogDocument));
const encodeCatalog = Schema.encodeSync(Schema.fromJsonString(ConnectionCatalogDocument));

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("ConnectionStorage.makeCatalogStore", () => {
  it.effect("quarantines malformed catalogs and starts from an empty document", () =>
    Effect.gen(function* () {
      const writes: string[] = [];
      const quarantined: string[] = [];
      const store = yield* ConnectionStorage.makeCatalogStore({
        read: Effect.succeed("{not-json"),
        write: (raw) => Effect.sync(() => writes.push(raw)),
        quarantine: (raw) => Effect.sync(() => quarantined.push(raw)),
      });

      expect(yield* store.read).toEqual(emptyCatalog);
      expect(quarantined).toEqual(["{not-json"]);
      expect(writes).toHaveLength(1);
      expect(decodeCatalog(writes[0]!)).toEqual(emptyCatalog);
    }),
  );

  it.effect("does not hide catalog read failures", () =>
    Effect.gen(function* () {
      const failure = new ConnectionTransientError({
        reason: "remote-unavailable",
        detail: "permission denied",
      });
      const store = yield* ConnectionStorage.makeCatalogStore({
        read: Effect.fail(failure),
        write: () => Effect.void,
      });

      expect(yield* Effect.flip(store.read)).toBe(failure);
    }),
  );
});

const fixedHandle = (database: IDBDatabase) => ({
  get: Effect.succeed(database),
  invalidate: () => Effect.void,
});

describe("ConnectionStorage.makeCatalogBackend", () => {
  it.effect("reports a closed IndexedDB connection as a typed read and write failure", () =>
    Effect.gen(function* () {
      vi.stubGlobal("window", {});
      const database = {
        transaction: () => {
          throw new DOMException("The database connection is closing.", "InvalidStateError");
        },
      } as unknown as IDBDatabase;
      const backend = ConnectionStorage.makeCatalogBackend(fixedHandle(database));

      const readError = yield* Effect.flip(backend.read);
      const writeError = yield* Effect.flip(backend.write("{}"));

      expect(readError).toBeInstanceOf(ConnectionTransientError);
      expect(readError.message).toContain("The database connection is closing.");
      expect(writeError).toBeInstanceOf(ConnectionTransientError);
    }),
  );

  it.effect("fails writes when desktop secure storage declines the catalog", () =>
    Effect.gen(function* () {
      const setConnectionCatalog = vi.fn().mockResolvedValue(false);
      vi.stubGlobal("window", {
        desktopBridge: {
          getConnectionCatalog: vi.fn().mockResolvedValue(null),
          setConnectionCatalog,
        },
      });
      const backend = ConnectionStorage.makeCatalogBackend(fixedHandle({} as IDBDatabase));

      const error = yield* backend.write("{}").pipe(Effect.flip);

      expect(error).toBeInstanceOf(ConnectionTransientError);
      expect(error.message).toContain("Desktop secure storage is unavailable");
      expect(setConnectionCatalog).toHaveBeenCalledWith("{}");
    }),
  );

  it.effect("fails IndexedDB writes whose commit aborts", () =>
    Effect.gen(function* () {
      vi.stubGlobal("window", {});
      const transaction = Object.assign(new EventTarget(), {
        error: null as DOMException | null,
        objectStore: () => ({
          put: () => {
            // A failed commit aborts the transaction without an "error" event.
            queueMicrotask(() => {
              transaction.error = new DOMException("Quota exceeded", "QuotaExceededError");
              transaction.dispatchEvent(new Event("abort"));
            });
          },
        }),
      });
      const backend = ConnectionStorage.makeCatalogBackend(
        fixedHandle({ transaction: () => transaction } as unknown as IDBDatabase),
      );

      const error = yield* backend.write("{}").pipe(Effect.flip);

      expect(error.message).toContain("QuotaExceededError");
    }),
  );
});

describe("environment cache removal", () => {
  it.effect("fails both removal operations when IndexedDB aborts their commits", () =>
    Effect.gen(function* () {
      vi.stubGlobal("window", {});
      vi.stubGlobal("IDBKeyRange", { bound: () => ({}) });
      const database = Object.assign(new EventTarget(), {
        transaction: () => {
          const transaction = Object.assign(new EventTarget(), {
            error: new DOMException("Commit aborted", "AbortError"),
            objectStore: () => ({
              delete: () => queueMicrotask(() => transaction.dispatchEvent(new Event("abort"))),
              openCursor: () => {
                queueMicrotask(() => transaction.dispatchEvent(new Event("abort")));
                return new EventTarget();
              },
            }),
          });
          return transaction;
        },
        close: vi.fn(),
      }) as unknown as IDBDatabase;
      const openRequest = Object.assign(new EventTarget(), { result: database, error: null });
      vi.stubGlobal("indexedDB", {
        open: () => {
          queueMicrotask(() => openRequest.dispatchEvent(new Event("success")));
          return openRequest;
        },
      });

      const [threadError, refsError] = yield* Effect.gen(function* () {
        const cache = yield* Persistence.EnvironmentCacheStore;
        return [
          yield* Effect.flip(
            cache.removeThread(EnvironmentId.make("env"), ThreadId.make("thread")),
          ),
          yield* Effect.flip(cache.clearVcsRefs(EnvironmentId.make("env"))),
        ] as const;
      }).pipe(Effect.provide(ConnectionStorage.layer));

      expect(threadError.message).toContain("Commit aborted");
      expect(refsError.message).toContain("Commit aborted");
      expect(database.close).toHaveBeenCalledOnce();
    }),
  );
});

describe("independent compatibility hint database", () => {
  it.effect(
    "keeps runtime opens at version 4 and opens a separate hint database across reloads",
    () =>
      Effect.gen(function* () {
        vi.stubGlobal("window", {});
        const opened: Array<{
          name: string;
          database: IDBDatabase;
          close: ReturnType<typeof vi.fn>;
        }> = [];
        const open = vi.fn((name: string, version: number) => {
          const isHints = name === "t3code:connection-compatibility-hints";
          expect(version).toBe(isHints ? 1 : 4);
          if (!isHints) expect(name).toBe("t3code:connection-runtime");
          const close = vi.fn();
          const database = Object.assign(new EventTarget(), {
            close,
            transaction: (storeName: string) => {
              expect(storeName).toBe(isHints ? "compatibility-hints" : "catalog");
              const transaction = Object.assign(new EventTarget(), {
                objectStore: () => ({
                  get: () => request(encodeCatalog(emptyCatalog)),
                  getAll: () => request([]),
                }),
              });
              function request(result: unknown) {
                const value = Object.assign(new EventTarget(), { result });
                queueMicrotask(() => {
                  value.dispatchEvent(new Event("success"));
                  transaction.dispatchEvent(new Event("complete"));
                });
                return value;
              }
              return transaction;
            },
          }) as unknown as IDBDatabase;
          opened.push({ name, database, close });
          const request = Object.assign(new EventTarget(), { result: database });
          queueMicrotask(() => request.dispatchEvent(new Event("success")));
          return request;
        });
        vi.stubGlobal("indexedDB", { open });
        for (let reload = 0; reload < 2; reload++) {
          yield* Effect.gen(function* () {
            const targets = yield* Persistence.ConnectionTargetStore;
            expect(yield* targets.list).toEqual([]);
            const runtime = opened.at(-1)!;
            expect(yield* targets.listCompatibilityHints!).toEqual([]);
            expect(runtime.close).not.toHaveBeenCalled();
          }).pipe(Effect.provide(ConnectionStorage.layer));
        }
        expect(open.mock.calls).toEqual([
          ["t3code:connection-runtime", 4],
          ["t3code:connection-compatibility-hints", 1],
          ["t3code:connection-runtime", 4],
          ["t3code:connection-compatibility-hints", 1],
        ]);
        for (const connection of opened) expect(connection.close).toHaveBeenCalledOnce();
      }),
  );
});

describe("IndexedDB connection recovery", () => {
  it.effect("reports an initial open failure from the cache operation", () =>
    Effect.gen(function* () {
      vi.stubGlobal("window", {});
      const open = vi.fn(() => {
        throw new DOMException("Storage is unavailable", "InvalidStateError");
      });
      vi.stubGlobal("indexedDB", { open });

      yield* Effect.gen(function* () {
        const cache = yield* Persistence.EnvironmentCacheStore;
        expect(open).not.toHaveBeenCalled();
        const error = yield* Effect.flip(
          cache.loadThread(EnvironmentId.make("env"), ThreadId.make("thread")),
        );
        expect(error.message).toContain("Storage is unavailable");
      }).pipe(Effect.provide(ConnectionStorage.layer));

      expect(open).toHaveBeenCalledOnce();
    }),
  );

  it.effect("reopens after a forced close and finalizes the current connection", () =>
    Effect.gen(function* () {
      vi.stubGlobal("window", {});
      const makeDatabase = () =>
        Object.assign(new EventTarget(), {
          close: vi.fn(),
          transaction: () => ({
            objectStore: () => ({
              get: () => {
                const request = Object.assign(new EventTarget(), {
                  result: undefined,
                  error: null,
                });
                queueMicrotask(() => request.dispatchEvent(new Event("success")));
                return request;
              },
            }),
          }),
        }) as unknown as IDBDatabase;
      const first = makeDatabase();
      const second = makeDatabase();
      const databases = [first, second];
      let openCount = 0;
      const open = vi.fn(() => {
        const request = Object.assign(new EventTarget(), {
          result: databases[openCount++],
          error: null,
        });
        queueMicrotask(() => request.dispatchEvent(new Event("success")));
        return request;
      });
      vi.stubGlobal("indexedDB", { open });

      yield* Effect.gen(function* () {
        const cache = yield* Persistence.EnvironmentCacheStore;
        const environmentId = EnvironmentId.make("env");
        const threadId = ThreadId.make("thread");
        expect(Option.isNone(yield* cache.loadThread(environmentId, threadId))).toBe(true);
        expect(open).toHaveBeenCalledTimes(1);

        first.dispatchEvent(new Event("close"));
        const recovered = yield* Effect.all(
          [cache.loadThread(environmentId, threadId), cache.loadThread(environmentId, threadId)],
          { concurrency: 2 },
        );
        expect(recovered.every(Option.isNone)).toBe(true);
        expect(open).toHaveBeenCalledTimes(2);
      }).pipe(Effect.provide(ConnectionStorage.layer));

      expect(first.close).not.toHaveBeenCalled();
      expect(second.close).toHaveBeenCalledOnce();
    }),
  );
});

describe("IndexedDB connection closed without a close event", () => {
  it.effect("reopens and retries the failing operation once", () =>
    Effect.gen(function* () {
      vi.stubGlobal("window", {});
      const closing = Object.assign(new EventTarget(), {
        close: vi.fn(),
        transaction: () => {
          // Chromium force-closed this connection; this tab never saw "close".
          throw new DOMException("The database connection is closing.", "InvalidStateError");
        },
      }) as unknown as IDBDatabase;
      const fresh = Object.assign(new EventTarget(), {
        close: vi.fn(),
        transaction: () => ({
          objectStore: () => ({
            get: () => {
              const request = Object.assign(new EventTarget(), { result: undefined, error: null });
              queueMicrotask(() => request.dispatchEvent(new Event("success")));
              return request;
            },
          }),
        }),
      }) as unknown as IDBDatabase;
      const databases = [closing, fresh];
      let openCount = 0;
      const open = vi.fn(() => {
        const request = Object.assign(new EventTarget(), {
          result: databases[openCount++],
          error: null,
        });
        queueMicrotask(() => request.dispatchEvent(new Event("success")));
        return request;
      });
      vi.stubGlobal("indexedDB", { open });

      yield* Effect.gen(function* () {
        const cache = yield* Persistence.EnvironmentCacheStore;
        const loaded = yield* cache.loadThread(EnvironmentId.make("env"), ThreadId.make("thread"));
        expect(Option.isNone(loaded)).toBe(true);
        expect(open).toHaveBeenCalledTimes(2);
      }).pipe(Effect.provide(ConnectionStorage.layer));
    }),
  );
});

describe("browser GitHub routing permissions", () => {
  it.effect("revokes across runtimes before storage events and resists stale catalog writes", () =>
    Effect.gen(function* () {
      const values = new Map<string, string>();
      const localStorage: Storage = {
        get length() {
          return values.size;
        },
        key: (index) => [...values.keys()][index] ?? null,
        getItem: (key) => values.get(key) ?? null,
        setItem: (key, value) => {
          values.set(key, value);
        },
        removeItem: (key) => {
          values.delete(key);
        },
        clear: () => {
          values.clear();
        },
      };
      const firstBrowser = Object.assign(new EventTarget(), { localStorage });
      const secondBrowser = Object.assign(new EventTarget(), { localStorage });
      const first = ConnectionStorage.makeBrowserGitHubRoutingPermissions(firstBrowser);
      const second = ConnectionStorage.makeBrowserGitHubRoutingPermissions(secondBrowser);
      const entry = {
        target: new PrimaryConnectionTarget({
          environmentId: EnvironmentId.make("first"),
          label: "First",
          httpBaseUrl: "http://localhost:3000",
          wsBaseUrl: "ws://localhost:3000",
        }),
        profile: Option.none(),
        enabled: true,
      };
      const other = {
        ...entry,
        target: new PrimaryConnectionTarget({
          ...entry.target,
          environmentId: EnvironmentId.make("second"),
        }),
      };
      expect(yield* first.get(entry)).toBe("off");
      yield* first.set(entry, "read-write");
      expect(yield* second.get(entry)).toBe("read-write");
      const oldPermissions = Option.getOrThrow(yield* Stream.runHead(first.changes));
      const staleCatalog = yield* ConnectionStorage.makeCatalogStore({
        read: Effect.succeed(
          encodeCatalog({ ...emptyCatalog, githubRoutingPermissions: oldPermissions }),
        ),
        write: () => Effect.void,
      });
      yield* staleCatalog.read;
      const listening = yield* Deferred.make<void>();
      const revoked = yield* Deferred.make<void>();
      yield* second.changes.pipe(
        Stream.runForEach((permissions) =>
          Deferred.succeed(permissions.length > 0 ? listening : revoked, undefined),
        ),
        Effect.forkChild,
      );
      yield* Deferred.await(listening);

      yield* first.set(entry, "off");
      expect(yield* second.get(entry)).toBe("off");
      secondBrowser.dispatchEvent(Object.assign(new Event("storage"), { key: null }));
      yield* Deferred.await(revoked);
      yield* second.set(other, "read");
      yield* staleCatalog.update((document) => ({ ...document, accountId: "updated" }));
      expect(yield* second.get(entry)).toBe("off");
      expect(yield* first.get(other)).toBe("read");
      expect(
        yield* ConnectionStorage.makeBrowserGitHubRoutingPermissions(firstBrowser).get(entry),
      ).toBe("off");

      yield* first.set(entry, "read-write");
      yield* second.forget(entry.target.environmentId);
      expect(yield* first.get(entry)).toBe("off");
      expect(yield* first.get(other)).toBe("read");
      vi.spyOn(localStorage, "setItem").mockImplementation(() => {
        throw new Error("Storage unavailable");
      });
      expect(yield* first.set(entry, "read-write").pipe(Effect.flip)).toBeInstanceOf(
        ConnectionTransientError,
      );
      expect(yield* second.get(entry)).toBe("off");
    }).pipe(Effect.scoped),
  );
});

describe("durable compatibility hints", () => {
  const environmentId = EnvironmentId.make("saved-off-environment");
  const target = new BearerConnectionTarget({
    environmentId,
    label: "Saved",
    connectionId: "saved-route",
  });
  const profile = new BearerConnectionProfile({
    environmentId,
    label: "Saved",
    connectionId: target.connectionId,
    httpBaseUrl: "https://saved.example.test",
    wsBaseUrl: "wss://saved.example.test",
  });
  const credential = new BearerConnectionCredential({ token: "saved-test-credential" });
  const registration = new BearerConnectionRegistration({ target, profile, credential });
  const routeBinding = connectionCompatibilityRouteBinding([
    { target, profile: Option.some(profile) },
  ])!;
  const hint = { environmentId, routeBinding, kind: "outdated-update" } as const;

  it.effect("does not quarantine a valid catalog when optional hints are malformed", () =>
    Effect.gen(function* () {
      const writes: string[] = [];
      const quarantined: string[] = [];
      const raw = JSON.stringify({
        ...emptyCatalog,
        targets: [target],
        profiles: [profile],
        credentials: [{ connectionId: target.connectionId, credential }],
        disabledEnvironmentIds: [environmentId],
        compatibilityHints: [{ ...hint, token: "unexpected" }],
      });
      const store = yield* ConnectionStorage.makeCatalogStore({
        read: Effect.succeed(raw),
        write: (value) => Effect.sync(() => writes.push(value)),
        quarantine: (value) => Effect.sync(() => quarantined.push(value)),
      });
      const restored = yield* store.read;
      expect(restored.targets).toEqual([target]);
      expect(restored.credentials[0]?.credential).toEqual(credential);
      expect(restored.disabledEnvironmentIds).toEqual([environmentId]);
      expect("compatibilityHints" in restored).toBe(false);
      expect(writes).toEqual([]);
      expect(quarantined).toEqual([]);
    }),
  );

  it.effect(
    "uses real adapter methods across reload, profile edits and fresh clear while staying Off",
    () =>
      Effect.gen(function* () {
        let raw: string | null = null;
        const getConnectionCatalog = vi.fn(async () => raw);
        const setConnectionCatalog = vi.fn(async (value: string) => {
          raw = value;
          return true;
        });
        let hints: ReadonlyArray<StoredConnectionCompatibilityHint> = [];
        vi.stubGlobal("window", {
          desktopBridge: {
            getConnectionCatalog,
            setConnectionCatalog,
            getConnectionCatalogSnapshot: async () => raw,
            listConnectionCompatibilityHints: async () => hints,
            setConnectionCompatibilityHint: async (
              input:
                | StoredConnectionCompatibilityHint
                | { environmentId: EnvironmentId; routeBinding: string; kind: null },
            ) => {
              hints = setConnectionCompatibilityHintInRecords(
                hints,
                input.environmentId,
                input.routeBinding,
                input.kind,
              );
              return true;
            },
          },
        });
        const open = vi.fn(() => {
          throw new Error("Hint persistence must not open the cache database");
        });
        vi.stubGlobal("indexedDB", { open });
        yield* Effect.gen(function* () {
          const store = yield* Persistence.ConnectionRegistrationStore;
          yield* store.register(registration, [target]);
          yield* store.setEnabled(environmentId, false);
          if (store.setCompatibilityHint === undefined)
            throw new Error("Web adapter must persist hints");
          yield* store.setCompatibilityHint(environmentId, routeBinding, "outdated-update");
        }).pipe(Effect.provide(ConnectionStorage.layer));

        yield* Effect.gen(function* () {
          const targets = yield* Persistence.ConnectionTargetStore;
          const store = yield* Persistence.ConnectionRegistrationStore;
          const profiles = yield* ProfileStore.ConnectionProfileStore;
          if (
            targets.listCompatibilityHints === undefined ||
            store.setCompatibilityHint === undefined
          )
            throw new Error("Web adapter must expose hint storage");
          expect(yield* targets.listCompatibilityHints).toEqual([hint]);
          expect(yield* targets.listDisabled).toEqual([environmentId]);
          const changed = new BearerConnectionProfile({
            ...profile,
            httpBaseUrl: "https://changed.example.test",
          });
          yield* profiles.put(changed);
          expect(yield* targets.listCompatibilityHints).toEqual([]);
          yield* store.setCompatibilityHint(environmentId, routeBinding, "outdated-update");
          expect(yield* targets.listCompatibilityHints).toEqual([]);
          const currentBinding = connectionCompatibilityRouteBinding([
            { target, profile: Option.some(changed) },
          ])!;
          yield* store.setCompatibilityHint(environmentId, currentBinding, "outdated-update");
          yield* store.setCompatibilityHint(environmentId, routeBinding, null);
          expect(yield* targets.listCompatibilityHints).toHaveLength(1);
          yield* store.setCompatibilityHint(environmentId, currentBinding, null);
          expect(yield* targets.listCompatibilityHints).toEqual([]);
          expect(yield* targets.listDisabled).toEqual([environmentId]);
        }).pipe(Effect.provide(ConnectionStorage.layer));

        yield* Effect.gen(function* () {
          const targets = yield* Persistence.ConnectionTargetStore;
          if (targets.listCompatibilityHints === undefined)
            throw new Error("Missing durable hint reader");
          expect(yield* targets.listCompatibilityHints).toEqual([]);
          expect(yield* targets.listDisabled).toEqual([environmentId]);
        }).pipe(Effect.provide(ConnectionStorage.layer));
        expect(getConnectionCatalog).toHaveBeenCalledTimes(3);
        expect(open).not.toHaveBeenCalled();
        expect(decodeCatalog(raw!).credentials[0]?.credential).toEqual(credential);
      }),
  );

  it.effect(
    "preserves another instance's Off, removal and route edits during late hint writes",
    () =>
      Effect.gen(function* () {
        let raw = encodeCatalog({ ...emptyCatalog, targets: [target], profiles: [profile] });
        let writes = 0;
        let records: ReadonlyArray<StoredConnectionCompatibilityHint> = [];
        const backend = {
          read: Effect.sync(() => raw),
          write: (next: string) =>
            Effect.sync(() => {
              writes++;
              raw = next;
            }),
        };
        const hints = ConnectionStorage.makeCompatibilityHintStore(
          {
            supported: true,
            read: Effect.sync(() => records),
            set: (environment, binding, kind) =>
              Effect.sync(() => {
                records = setConnectionCompatibilityHintInRecords(
                  records,
                  environment,
                  binding,
                  kind,
                );
              }),
          },
          backend.read,
        );
        const stale = yield* ConnectionStorage.makeCatalogStore(backend);
        const current = yield* ConnectionStorage.makeCatalogStore(backend, hints.invalidate);
        yield* stale.read;
        yield* current.update((document) =>
          setConnectionEnabledInCatalog(document, environmentId, false),
        );
        const off = raw;
        yield* hints.set(environmentId, routeBinding, "outdated-update");
        yield* hints.set(environmentId, routeBinding, null);
        expect(raw).toBe(off);
        expect(writes).toBe(1);
        expect(decodeCatalog(raw).disabledEnvironmentIds).toEqual([environmentId]);
        yield* hints.set(environmentId, routeBinding, "outdated-update");
        yield* current.update((document) => ({
          ...document,
          profiles: [
            new BearerConnectionProfile({
              ...profile,
              httpBaseUrl: "https://changed.example.test",
            }),
          ],
        }));
        const edited = raw;
        yield* hints.set(environmentId, routeBinding, "outdated-update");
        expect(raw).toBe(edited);
        expect(yield* hints.read).toEqual([]);
        yield* current.update((document) => removeConnectionFromCatalog(document, environmentId));
        const removed = raw;
        yield* hints.set(environmentId, routeBinding, "outdated-update");
        yield* hints.set(environmentId, routeBinding, null);
        expect(raw).toBe(removed);
        expect(decodeCatalog(raw).targets).toEqual([]);
        expect(yield* hints.read).toEqual([]);
      }),
  );

  for (const failure of ["read", "clear"] as const) {
    it.effect(`hint ${failure} failure does not prevent register, edit, Off or remove`, () =>
      Effect.gen(function* () {
        let raw = encodeCatalog({ ...emptyCatalog, targets: [target], profiles: [profile] });
        let cleanups = 0;
        vi.stubGlobal("window", {
          desktopBridge: {
            getConnectionCatalog: async () => raw,
            getConnectionCatalogSnapshot: async () => raw,
            setConnectionCatalog: async (next: string) => {
              raw = next;
              return true;
            },
            listConnectionCompatibilityHints: async () => {
              if (failure === "read") throw new Error("Synthetic private backend detail");
              const binding = catalogCompatibilityRouteBinding(decodeCatalog(raw), environmentId);
              return binding === null ? [] : [{ ...hint, routeBinding: binding }];
            },
            setConnectionCompatibilityHint: async () => {
              cleanups++;
              throw new Error("Synthetic private cleanup detail");
            },
          },
        });
        yield* Effect.gen(function* () {
          const store = yield* Persistence.ConnectionRegistrationStore;
          const targets = yield* Persistence.ConnectionTargetStore;
          expect(yield* targets.list).toEqual([target]);
          if (failure === "read") expect(yield* targets.listCompatibilityHints!).toEqual([]);
          const edited = new BearerConnectionProfile({
            ...profile,
            httpBaseUrl: "https://edited.example.test",
            wsBaseUrl: "wss://edited.example.test",
          });
          yield* store.register(
            new BearerConnectionRegistration({ target, profile: edited, credential }),
            [target],
          );
          expect(decodeCatalog(raw).profiles).toEqual([edited]);
          yield* store.setEnabled(environmentId, false);
          expect(decodeCatalog(raw).disabledEnvironmentIds).toEqual([environmentId]);
          yield* store.remove(environmentId);
          expect(decodeCatalog(raw).targets).toEqual([]);
          yield* store.register(registration, [target]);
          expect(decodeCatalog(raw).targets).toEqual([target]);
          if (failure === "clear") expect(cleanups).toBe(2);
        }).pipe(Effect.provide(ConnectionStorage.layer));
      }),
    );
  }

  it.effect("hint cleanup failure does not hide an authoritative catalog write failure", () =>
    Effect.gen(function* () {
      const failure = new ConnectionTransientError({
        reason: "remote-unavailable",
        detail: "Synthetic catalog failure",
      });
      const backend = {
        read: Effect.succeed(
          encodeCatalog({ ...emptyCatalog, targets: [target], profiles: [profile] }),
        ),
        write: () => Effect.fail(failure),
      };
      const hints = ConnectionStorage.makeCompatibilityHintStore(
        { supported: true, read: Effect.succeed([hint]), set: () => Effect.fail(failure) },
        backend.read,
      );
      const store = yield* ConnectionStorage.makeCatalogStore(backend, hints.invalidate);
      expect(
        yield* store
          .update((catalog) => removeConnectionFromCatalog(catalog, environmentId))
          .pipe(Effect.flip),
      ).toBe(failure);
      expect((yield* store.read).targets).toEqual([target]);
    }),
  );

  it.effect(
    "never falls back to a legacy desktop catalog writer or blocks its ordinary operations",
    () =>
      Effect.gen(function* () {
        let raw = encodeCatalog(emptyCatalog);
        const write = vi.fn(async (next: string) => {
          raw = next;
          return true;
        });
        vi.stubGlobal("window", {
          desktopBridge: {
            getConnectionCatalog: async () => raw,
            setConnectionCatalog: write,
          },
        });
        const backend = ConnectionStorage.makeCompatibilityHintBackend(
          fixedHandle({} as IDBDatabase),
        );
        const store = ConnectionStorage.makeCompatibilityHintStore(backend, Effect.succeed(null));
        expect(yield* store.read).toEqual([]);
        expect(
          yield* store.set(environmentId, routeBinding, "outdated-update").pipe(Effect.flip),
        ).toBeInstanceOf(ConnectionTransientError);
        expect(write).not.toHaveBeenCalled();
        yield* Effect.gen(function* () {
          const registrations = yield* Persistence.ConnectionRegistrationStore;
          yield* registrations.register(registration, [target]);
          expect(decodeCatalog(raw).targets).toEqual([target]);
          yield* registrations.setEnabled(environmentId, false);
          expect(decodeCatalog(raw).disabledEnvironmentIds).toEqual([environmentId]);
          yield* registrations.remove(environmentId);
          expect(decodeCatalog(raw).targets).toEqual([]);
        }).pipe(Effect.provide(ConnectionStorage.layer));
        expect(write).toHaveBeenCalledTimes(3);
      }),
  );

  it.effect("does not publish a hint in memory when its durable write fails", () =>
    Effect.gen(function* () {
      const raw = encodeCatalog({
        ...emptyCatalog,
        targets: [target],
        profiles: [profile],
        credentials: [{ connectionId: target.connectionId, credential }],
        disabledEnvironmentIds: [environmentId],
      });
      vi.stubGlobal("window", {
        desktopBridge: {
          getConnectionCatalog: vi.fn(async () => raw),
          setConnectionCatalog: vi.fn(async () => false),
          getConnectionCatalogSnapshot: vi.fn(async () => raw),
          listConnectionCompatibilityHints: vi.fn(async () => []),
          setConnectionCompatibilityHint: vi.fn(async () => false),
        },
      });
      yield* Effect.gen(function* () {
        const targets = yield* Persistence.ConnectionTargetStore;
        const store = yield* Persistence.ConnectionRegistrationStore;
        if (
          targets.listCompatibilityHints === undefined ||
          store.setCompatibilityHint === undefined
        )
          throw new Error("Web adapter must expose hint storage");
        const error = yield* store
          .setCompatibilityHint(environmentId, routeBinding, "outdated-update")
          .pipe(Effect.flip);
        expect(error.operation).toBe("set-compatibility-hint");
        expect(error.message).toBe("Could not save historical connection compatibility hints.");
        expect(yield* targets.listCompatibilityHints).toEqual([]);
        expect(yield* targets.listDisabled).toEqual([environmentId]);
      }).pipe(Effect.provide(ConnectionStorage.layer));
    }),
  );
});

describe("IndexedDB compatibility hint transactions", () => {
  it.effect("keeps read/count/put inside active callbacks and resolves only after commit", () =>
    Effect.gen(function* () {
      vi.stubGlobal("window", {});
      const events: string[] = [];
      const rows = new Map<string, unknown>();
      let failCommit = false;
      const database = {
        transaction: (name: string) => {
          expect(name).toBe("compatibility-hints");
          let pending = 0;
          let active = true;
          let aborted = false;
          const draft = new Map(rows);
          const transaction = Object.assign(new EventTarget(), {
            error: null as DOMException | null,
            abort: () => {
              aborted = true;
              queueMicrotask(() => transaction.dispatchEvent(new Event("abort")));
            },
            objectStore: () => ({
              count: () => request(draft.size),
              get: (key: IDBValidKey) => request(draft.get(JSON.stringify(key))),
              getAll: (_query: unknown, count: number) =>
                request([...draft.values()].slice(0, count)),
              put: (value: unknown, key: IDBValidKey) => {
                expect(active).toBe(true);
                events.push("put");
                draft.set(JSON.stringify(key), value);
                return request(undefined);
              },
              delete: (key: IDBValidKey) => {
                expect(active).toBe(true);
                draft.delete(JSON.stringify(key));
                return request(undefined);
              },
            }),
          });
          function request(result: unknown) {
            pending++;
            const request = Object.assign(new EventTarget(), { result });
            queueMicrotask(() => {
              if (aborted) return;
              active = true;
              request.dispatchEvent(new Event("success"));
              active = false;
              if (--pending === 0)
                queueMicrotask(() => {
                  if (aborted) return;
                  if (failCommit) {
                    transaction.error = new DOMException("Commit failed", "AbortError");
                    transaction.abort();
                    return;
                  }
                  rows.clear();
                  for (const [key, value] of draft) rows.set(key, value);
                  events.push("commit");
                  transaction.dispatchEvent(new Event("complete"));
                });
            });
            return request;
          }
          return transaction;
        },
      } as unknown as IDBDatabase;
      const backend = ConnectionStorage.makeCompatibilityHintBackend(fixedHandle(database));
      const environmentId = EnvironmentId.make("indexed-environment");
      yield* backend.set(environmentId, "binding", "outdated-update");
      expect(events).toEqual(["put", "commit"]);
      expect(yield* backend.read).toEqual([
        { environmentId, routeBinding: "binding", kind: "outdated-update" },
      ]);
      failCommit = true;
      expect(yield* backend.set(environmentId, "binding", null).pipe(Effect.flip)).toBeInstanceOf(
        ConnectionTransientError,
      );
      failCommit = false;
      expect(yield* backend.read).toHaveLength(1);
      yield* backend.set(environmentId, "binding", null);
      expect(yield* backend.read).toEqual([]);
      for (let index = 0; index < 128; index++) {
        rows.set(JSON.stringify([environmentId, `binding-${index}`]), {
          environmentId,
          routeBinding: `binding-${index}`,
          kind: "outdated-update",
        });
      }
      expect(
        yield* backend.set(environmentId, "overflow", "outdated-update").pipe(Effect.flip),
      ).toBeInstanceOf(ConnectionTransientError);
      expect(rows.size).toBe(128);
      yield* backend.set(environmentId, "binding-0", "outdated-update");
      expect(rows.size).toBe(128);
    }),
  );
});
