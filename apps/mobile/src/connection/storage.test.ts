import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import { EnvironmentId } from "@t3tools/contracts";
import {
  BearerConnectionTarget,
  BearerConnectionProfile,
  BearerConnectionCredential,
  BearerConnectionRegistration,
} from "@t3tools/client-runtime/connection";
import {
  ConnectionCatalogDocument,
  Persistence,
  EMPTY_CONNECTION_CATALOG_DOCUMENT,
  catalogCompatibilityRouteBinding,
  setConnectionEnabledInCatalog,
  removeConnectionFromCatalog,
  registerConnectionInCatalog,
} from "@t3tools/client-runtime/platform";
import { vi } from "vite-plus/test";

vi.mock("react-native", () => ({
  Platform: { OS: "ios" },
}));

vi.mock("expo-secure-store", () => ({
  deleteItemAsync: vi.fn(),
  getItemAsync: vi.fn(),
  setItemAsync: vi.fn(),
}));

import {
  CONNECTION_CATALOG_KEY,
  LEGACY_CONNECTIONS_KEY,
  COMPATIBILITY_HINTS_KEY,
  make,
  makeCompatibilityHintStore,
} from "./catalog-store";
import * as MobileSecureStorage from "../persistence/mobile-secure-storage";
import * as MobileConnectionStorage from "./storage";

function makeStorage(initial: Readonly<Record<string, string>>) {
  const values = new Map(Object.entries(initial));
  const deleted: Array<string> = [];
  const storage = MobileSecureStorage.MobileSecureStorage.of({
    getItem: (key) => Effect.sync(() => values.get(key) ?? null),
    setItem: (key, value) =>
      Effect.sync(() => {
        values.set(key, value);
      }),
    removeItem: (key) =>
      Effect.sync(() => {
        deleted.push(key);
        values.delete(key);
      }),
  });
  return { deleted, storage, values };
}

describe("mobile connection catalog storage", () => {
  it.effect("recovers from a corrupt current catalog", () =>
    Effect.gen(function* () {
      const memory = makeStorage({
        [CONNECTION_CATALOG_KEY]: "{not-json",
      });
      const catalog = yield* make().pipe(
        Effect.provideService(MobileSecureStorage.MobileSecureStorage, memory.storage),
      );

      expect((yield* catalog.read).targets).toEqual([]);
      expect(memory.deleted).toEqual([CONNECTION_CATALOG_KEY]);
    }),
  );

  it.effect("replaces and removes a corrupt legacy catalog", () =>
    Effect.gen(function* () {
      const memory = makeStorage({
        [LEGACY_CONNECTIONS_KEY]: JSON.stringify({ connections: [{ invalid: true }] }),
      });
      const catalog = yield* make().pipe(
        Effect.provideService(MobileSecureStorage.MobileSecureStorage, memory.storage),
      );

      expect((yield* catalog.read).targets).toEqual([]);
      expect(memory.deleted).toEqual([LEGACY_CONNECTIONS_KEY]);
      expect(memory.values.has(CONNECTION_CATALOG_KEY)).toBe(true);
    }),
  );

  it.effect("falls back to valid legacy data when the current catalog is corrupt", () =>
    Effect.gen(function* () {
      const memory = makeStorage({
        [CONNECTION_CATALOG_KEY]: "{not-json",
        [LEGACY_CONNECTIONS_KEY]: JSON.stringify({
          connections: [
            {
              environmentId: "legacy-environment",
              environmentLabel: "Legacy",
              pairingUrl: "https://legacy.example.test/pair",
              displayUrl: "https://legacy.example.test",
              httpBaseUrl: "https://legacy.example.test",
              wsBaseUrl: "wss://legacy.example.test",
              bearerToken: "legacy-token",
              authenticationMethod: "bearer",
            },
          ],
        }),
      });
      const catalog = yield* make().pipe(
        Effect.provideService(MobileSecureStorage.MobileSecureStorage, memory.storage),
      );

      expect((yield* catalog.read).targets).toHaveLength(1);
      expect(memory.deleted).toEqual([CONNECTION_CATALOG_KEY, LEGACY_CONNECTIONS_KEY]);

      yield* catalog.update((document) => document);
      expect(memory.values.has(CONNECTION_CATALOG_KEY)).toBe(true);
      expect(memory.values.has(LEGACY_CONNECTIONS_KEY)).toBe(false);
    }),
  );
});

describe("isolated mobile compatibility hints", () => {
  const environmentId = EnvironmentId.make("mobile-hint-environment");
  const target = new BearerConnectionTarget({
    environmentId,
    label: "Saved",
    connectionId: "mobile-route",
  });
  const profile = new BearerConnectionProfile({
    environmentId,
    connectionId: target.connectionId,
    label: "Saved",
    httpBaseUrl: "https://mobile.example.test",
    wsBaseUrl: "wss://mobile.example.test",
  });
  const document = { ...EMPTY_CONNECTION_CATALOG_DOCUMENT, targets: [target], profiles: [profile] };
  const binding = catalogCompatibilityRouteBinding(document, environmentId)!;
  const hint = { environmentId, routeBinding: binding, kind: "outdated-update" } as const;
  const encode = Schema.encodeSync(Schema.fromJsonString(ConnectionCatalogDocument));
  const decode = Schema.decodeSync(Schema.fromJsonString(ConnectionCatalogDocument));

  it.effect("does not overwrite another instance's Off preference, route edit or removal", () =>
    Effect.gen(function* () {
      const memory = makeStorage({ [CONNECTION_CATALOG_KEY]: encode(document) });
      yield* Effect.gen(function* () {
        const stale = yield* make();
        const current = yield* make();
        yield* stale.read;
        yield* current.update((value) =>
          setConnectionEnabledInCatalog(value, environmentId, false),
        );
        const off = memory.values.get(CONNECTION_CATALOG_KEY);
        yield* stale.compatibilityHints.set(environmentId, binding, "outdated-update");
        expect(memory.values.get(CONNECTION_CATALOG_KEY)).toBe(off);
        expect(yield* stale.compatibilityHints.read).toEqual([hint]);
        yield* stale.compatibilityHints.set(environmentId, binding, null);
        expect(memory.values.get(CONNECTION_CATALOG_KEY)).toBe(off);
        const reloaded = yield* make();
        expect(yield* reloaded.compatibilityHints.read).toEqual([]);
        yield* current.update((value) => ({
          ...value,
          profiles: [
            new BearerConnectionProfile({ ...profile, httpBaseUrl: "https://other.example.test" }),
          ],
        }));
        const edited = memory.values.get(CONNECTION_CATALOG_KEY);
        yield* stale.compatibilityHints.set(environmentId, binding, "outdated-update");
        expect(memory.values.get(CONNECTION_CATALOG_KEY)).toBe(edited);
        expect(yield* stale.compatibilityHints.read).toEqual([]);
        yield* current.update((value) => removeConnectionFromCatalog(value, environmentId));
        const removed = memory.values.get(CONNECTION_CATALOG_KEY);
        yield* stale.compatibilityHints.set(environmentId, binding, "outdated-update");
        yield* stale.compatibilityHints.set(environmentId, binding, null);
        expect(memory.values.get(CONNECTION_CATALOG_KEY)).toBe(removed);
        expect(decode(removed!).targets).toEqual([]);
      }).pipe(Effect.provideService(MobileSecureStorage.MobileSecureStorage, memory.storage));
    }),
  );

  it.effect(
    "ignores malformed independent and embedded hints without migrating or deleting the catalog",
    () =>
      Effect.gen(function* () {
        const raw = JSON.stringify({ ...document, compatibilityHints: [hint] });
        const memory = makeStorage({
          [CONNECTION_CATALOG_KEY]: raw,
          [COMPATIBILITY_HINTS_KEY]: "{bad",
        });
        const store = yield* make().pipe(
          Effect.provideService(MobileSecureStorage.MobileSecureStorage, memory.storage),
        );
        expect(yield* store.compatibilityHints.read).toEqual([]);
        yield* store.compatibilityHints.set(environmentId, binding, null);
        expect(yield* store.compatibilityHints.read).toEqual([]);
        expect(memory.values.get(CONNECTION_CATALOG_KEY)).toBe(raw);
        expect(memory.deleted).toEqual([]);
      }),
  );

  it.effect("surfaces failed native hint writes without changing the catalog", () =>
    Effect.gen(function* () {
      const raw = encode(document);
      const memory = makeStorage({ [CONNECTION_CATALOG_KEY]: raw });
      const store = yield* make().pipe(
        Effect.provideService(MobileSecureStorage.MobileSecureStorage, {
          ...memory.storage,
          setItem: (key, value) =>
            key === COMPATIBILITY_HINTS_KEY
              ? Effect.fail(
                  new MobileSecureStorage.MobileSecureStorageError({
                    operation: "write",
                    key,
                    cause: "unavailable",
                  }),
                )
              : memory.storage.setItem(key, value),
        }),
      );
      expect(
        (yield* Effect.result(
          store.compatibilityHints.set(environmentId, binding, "outdated-update"),
        ))._tag,
      ).toBe("Failure");
      expect(memory.values.get(CONNECTION_CATALOG_KEY)).toBe(raw);
      expect(yield* store.compatibilityHints.read).toEqual([]);
    }),
  );

  it.effect.each(["read", "clear"] as const)(
    "hint %s failure leaves catalog edits, Off and removal available",
    (failure) =>
      Effect.gen(function* () {
        const memory = makeStorage({ [CONNECTION_CATALOG_KEY]: encode(document) });
        let cleanups = 0;
        const store = yield* make().pipe(
          Effect.provideService(MobileSecureStorage.MobileSecureStorage, {
            ...memory.storage,
            getItem: (key) => {
              if (key !== COMPATIBILITY_HINTS_KEY) return memory.storage.getItem(key);
              if (failure === "read")
                return Effect.fail(
                  new MobileSecureStorage.MobileSecureStorageError({
                    operation: "read",
                    key,
                    cause: "Synthetic private failure",
                  }),
                );
              return Effect.sync(() => {
                const current = decode(memory.values.get(CONNECTION_CATALOG_KEY)!);
                const currentBinding = catalogCompatibilityRouteBinding(current, environmentId);
                return JSON.stringify(
                  currentBinding === null ? [] : [{ ...hint, routeBinding: currentBinding }],
                );
              });
            },
            setItem: (key, value) =>
              key !== COMPATIBILITY_HINTS_KEY
                ? memory.storage.setItem(key, value)
                : Effect.sync(() => {
                    cleanups++;
                  }).pipe(
                    Effect.andThen(
                      Effect.fail(
                        new MobileSecureStorage.MobileSecureStorageError({
                          operation: "write",
                          key,
                          cause: "Synthetic private failure",
                        }),
                      ),
                    ),
                  ),
          }),
        );
        expect((yield* store.read).targets).toEqual([target]);
        if (failure === "read") expect(yield* store.compatibilityHints.read).toEqual([]);
        const edited = new BearerConnectionProfile({
          ...profile,
          httpBaseUrl: "https://edited.example.test",
          wsBaseUrl: "wss://edited.example.test",
        });
        const registration = new BearerConnectionRegistration({
          target,
          profile: edited,
          credential: new BearerConnectionCredential({ token: "synthetic" }),
        });
        yield* store.update((current) => registerConnectionInCatalog(current, registration));
        expect(decode(memory.values.get(CONNECTION_CATALOG_KEY)!).profiles).toEqual([edited]);
        yield* store.update((current) =>
          setConnectionEnabledInCatalog(current, environmentId, false),
        );
        expect(decode(memory.values.get(CONNECTION_CATALOG_KEY)!).disabledEnvironmentIds).toEqual([
          environmentId,
        ]);
        yield* store.update((current) => removeConnectionFromCatalog(current, environmentId));
        expect(decode(memory.values.get(CONNECTION_CATALOG_KEY)!).targets).toEqual([]);
        yield* store.update((current) => registerConnectionInCatalog(current, registration));
        expect(decode(memory.values.get(CONNECTION_CATALOG_KEY)!).targets).toEqual([target]);
        if (failure === "clear") expect(cleanups).toBe(2);
      }),
  );

  it.effect("the mobile persistence service reports hint failure without backend details", () =>
    Effect.gen(function* () {
      const memory = makeStorage({ [CONNECTION_CATALOG_KEY]: encode(document) });
      const storage = MobileSecureStorage.MobileSecureStorage.of({
        ...memory.storage,
        setItem: (key, value) =>
          key === COMPATIBILITY_HINTS_KEY
            ? Effect.fail(
                new MobileSecureStorage.MobileSecureStorageError({
                  operation: "write",
                  key,
                  cause: "Synthetic private backend detail",
                }),
              )
            : memory.storage.setItem(key, value),
      });
      yield* Effect.gen(function* () {
        const registrations = yield* Persistence.ConnectionRegistrationStore;
        const error = yield* registrations.setCompatibilityHint!(
          environmentId,
          binding,
          "outdated-update",
        ).pipe(Effect.flip);
        expect(error.operation).toBe("set-compatibility-hint");
        expect(error.message).toBe("Could not save historical connection compatibility hints.");
        yield* registrations.remove(environmentId);
        expect(decode(memory.values.get(CONNECTION_CATALOG_KEY)!).targets).toEqual([]);
      }).pipe(
        Effect.provide(MobileConnectionStorage.layer),
        Effect.provideService(MobileSecureStorage.MobileSecureStorage, storage),
      );
    }),
  );

  it.effect("hint capacity failure does not prevent authoritative removal", () =>
    Effect.gen(function* () {
      const records = Array.from({ length: 128 }, (_, index) => ({
        ...hint,
        routeBinding: `old-binding-${index}`,
      }));
      const memory = makeStorage({
        [CONNECTION_CATALOG_KEY]: encode(document),
        [COMPATIBILITY_HINTS_KEY]: JSON.stringify(records),
      });
      const store = yield* make().pipe(
        Effect.provideService(MobileSecureStorage.MobileSecureStorage, memory.storage),
      );
      expect(
        (yield* Effect.result(
          store.compatibilityHints.set(environmentId, binding, "outdated-update"),
        ))._tag,
      ).toBe("Failure");
      yield* store.update((current) => removeConnectionFromCatalog(current, environmentId));
      expect(decode(memory.values.get(CONNECTION_CATALOG_KEY)!).targets).toEqual([]);
      expect(memory.values.get(COMPATIBILITY_HINTS_KEY)).toBe(JSON.stringify(records));
    }),
  );

  it.effect("continues to fail an authoritative catalog write when hints also fail", () =>
    Effect.gen(function* () {
      const raw = encode(document);
      const memory = makeStorage({ [CONNECTION_CATALOG_KEY]: raw });
      const store = yield* make().pipe(
        Effect.provideService(MobileSecureStorage.MobileSecureStorage, {
          ...memory.storage,
          getItem: (key) =>
            key === COMPATIBILITY_HINTS_KEY
              ? Effect.fail(
                  new MobileSecureStorage.MobileSecureStorageError({
                    operation: "read",
                    key,
                    cause: "Synthetic hint failure",
                  }),
                )
              : memory.storage.getItem(key),
          setItem: (key) =>
            Effect.fail(
              new MobileSecureStorage.MobileSecureStorageError({
                operation: "write",
                key,
                cause: "Synthetic catalog failure",
              }),
            ),
        }),
      );
      const result = yield* Effect.result(
        store.update((current) => removeConnectionFromCatalog(current, environmentId)),
      );
      expect(result._tag).toBe("Failure");
      expect(memory.values.get(CONNECTION_CATALOG_KEY)).toBe(raw);
      expect((yield* store.read).targets).toEqual([target]);
    }),
  );

  it.effect("holds the shared serializer until an interrupted native write settles", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const memory = makeStorage({});
      let reads = 0;
      let writes = 0;
      const storage = MobileSecureStorage.MobileSecureStorage.of({
        ...memory.storage,
        getItem: (key) =>
          Effect.gen(function* () {
            if (key === COMPATIBILITY_HINTS_KEY) reads++;
            return yield* memory.storage.getItem(key);
          }),
        setItem: (key, value) =>
          Effect.gen(function* () {
            if (++writes === 1) {
              yield* Deferred.succeed(started, undefined);
              yield* Deferred.await(release);
            }
            yield* memory.storage.setItem(key, value);
          }),
      });
      yield* Effect.gen(function* () {
        const first = yield* makeCompatibilityHintStore();
        const second = yield* makeCompatibilityHintStore();
        const writing = yield* Effect.forkChild(
          first.set(environmentId, "old-binding", "outdated-update"),
        );
        yield* Deferred.await(started);
        const interrupting = yield* Effect.forkChild(Fiber.interrupt(writing));
        const following = yield* Effect.forkChild(
          second.set(environmentId, "new-binding", "outdated-update"),
        );
        yield* Effect.gen(function* () {
          yield* Effect.yieldNow;
          expect(reads).toBe(1);
          expect(writes).toBe(1);
        }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
        yield* Fiber.join(interrupting);
        yield* Fiber.join(following);
        yield* second.set(environmentId, "old-binding", null);
        expect(yield* first.read).toEqual([
          { environmentId, routeBinding: "new-binding", kind: "outdated-update" },
        ]);
      }).pipe(Effect.provideService(MobileSecureStorage.MobileSecureStorage, storage));
    }),
  );
});
