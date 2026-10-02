import * as NodeAssert from "node:assert/strict";
import { DEFAULT_CLIENT_SETTINGS } from "../../packages/contracts/src/index.ts";
import { EMPTY_CONNECTION_CATALOG_DOCUMENT } from "../../packages/client-runtime/src/platform/index.ts";
import { BearerConnectionTarget } from "../../packages/client-runtime/src/connection/index.ts";
import { describe, it } from "@effect/vitest";
import { afterEach, vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import { makeCatalogBackend, makeCatalogStore } from "../../apps/web/src/connection/storage.ts";
import {
  readBrowserClientSettings,
  writeBrowserClientSettings,
} from "../../apps/web/src/clientPersistenceStorage.ts";
import { withRunScratchEffect } from "./support.mjs";

function syntheticLocalStorage() {
  const values = new Map();
  return {
    values,
    storage: {
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
    },
  };
}

function syntheticIndexedDatabase() {
  const values = new Map();
  const accesses = [];
  const database = {
    transaction: (storeName, mode) => {
      accesses.push({ storeName, mode });
      const transaction = Object.assign(new EventTarget(), { error: null });
      transaction.objectStore = (requestedStore) => ({
        get: (key) => {
          const request = Object.assign(new EventTarget(), { result: undefined, error: null });
          queueMicrotask(() => {
            request.result = values.get(`${requestedStore}:${key}`);
            request.dispatchEvent(new Event("success"));
          });
          return request;
        },
        put: (value, key) => {
          values.set(`${requestedStore}:${key}`, value);
          queueMicrotask(() => transaction.dispatchEvent(new Event("complete")));
        },
      });
      return transaction;
    },
  };
  return { database, values, accesses };
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("T4 browser storage production seams with injected browser APIs", () => {
  it.effect(
    "keeps localStorage settings distinct from IndexedDB catalog records across reopen",
    () =>
      withRunScratchEffect({ label: "browser-storage" }, ({ record }) =>
        Effect.gen(function* () {
          const local = syntheticLocalStorage();
          const indexed = syntheticIndexedDatabase();
          vi.stubGlobal("window", { localStorage: local.storage });
          vi.stubGlobal("localStorage", local.storage);
          const settings = { ...DEFAULT_CLIENT_SETTINGS, timestampFormat: "24-hour" };
          writeBrowserClientSettings(settings);
          NodeAssert.deepEqual(readBrowserClientSettings(), settings);
          const settingsRaw = local.storage.getItem("t3code:client-settings:v1");
          NodeAssert.ok(settingsRaw);
          const expected = {
            ...EMPTY_CONNECTION_CATALOG_DOCUMENT,
            targets: [
              new BearerConnectionTarget({
                environmentId: "synthetic-browser-environment",
                label: "Synthetic browser",
                connectionId: "bearer:synthetic-browser-environment",
              }),
            ],
          };
          const readback = yield* Effect.gen(function* () {
            const first = yield* makeCatalogStore(makeCatalogBackend(indexed.database));
            NodeAssert.deepEqual(yield* first.read, EMPTY_CONNECTION_CATALOG_DOCUMENT);
            yield* first.update(() => expected);
            const second = yield* makeCatalogStore(makeCatalogBackend(indexed.database));
            return yield* second.read;
          });
          NodeAssert.deepEqual(readback, expected);
          NodeAssert.equal(local.values.size, 1);
          NodeAssert.equal(local.storage.getItem("t3code:client-settings:v1"), settingsRaw);
          NodeAssert.deepEqual([...indexed.values.keys()], ["catalog:document"]);
          NodeAssert.ok(
            indexed.accesses.some(
              ({ storeName, mode }) => storeName === "catalog" && mode === "readwrite",
            ),
          );
          NodeAssert.equal(
            indexed.values.get("catalog:document").includes("timestampFormat"),
            false,
          );
          record({
            checkId: "browser-storage",
            proofKind: "production-catalog-backend-injected-indexeddb-api",
            result: "passed",
            readback: {
              localStorageKeys: [...local.values.keys()],
              indexedDbKeys: [...indexed.values.keys()],
              catalogTargets: readback.targets.length,
            },
            limits: [
              "in-memory localStorage and IndexedDB API doubles; browser process, origin partitioning and on-disk browser durability unproved",
            ],
          });
        }),
      ),
  );

  it.effect("quarantines malformed catalog input and reports an injected read failure", () =>
    withRunScratchEffect({ label: "browser-catalog-errors" }, ({ record }) =>
      Effect.gen(function* () {
        const raw = "{synthetic-invalid-json";
        const quarantined = [];
        const writes = [];
        const failure = new Error("synthetic backend unavailable");
        yield* Effect.gen(function* () {
          const recovered = yield* makeCatalogStore({
            read: Effect.succeed(raw),
            write: (value) =>
              Effect.sync(() => {
                writes.push(value);
              }),
            quarantine: (value) =>
              Effect.sync(() => {
                quarantined.push(value);
              }),
          });
          NodeAssert.deepEqual(yield* recovered.read, EMPTY_CONNECTION_CATALOG_DOCUMENT);
          const unavailable = yield* makeCatalogStore({
            read: Effect.fail(failure),
            write: () => Effect.void,
          });
          NodeAssert.equal(yield* unavailable.read.pipe(Effect.flip), failure);
        });
        NodeAssert.deepEqual(quarantined, [raw]);
        NodeAssert.equal(writes.length, 1);
        NodeAssert.deepEqual(JSON.parse(writes[0]), EMPTY_CONNECTION_CATALOG_DOCUMENT);
        record({
          checkId: "browser-catalog-errors",
          proofKind: "production-catalog-store-injected-backend",
          result: "passed",
          readback: {
            quarantineCount: quarantined.length,
            recoveryWriteCount: writes.length,
            readFailurePropagated: true,
          },
          limits: ["injected backend; native browser storage failures unproved"],
        });
      }),
    ),
  );
});
