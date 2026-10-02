import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import {
  ClientSettingsSchema,
  DEFAULT_CLIENT_SETTINGS,
} from "../../packages/contracts/src/index.ts";
import {
  ConnectionCatalogDocument,
  EMPTY_CONNECTION_CATALOG_DOCUMENT,
} from "../../packages/client-runtime/src/platform/index.ts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as DesktopAppIdentity from "../../apps/desktop/src/app/DesktopAppIdentity.ts";
import * as DesktopConfig from "../../apps/desktop/src/app/DesktopConfig.ts";
import * as DesktopEnvironment from "../../apps/desktop/src/app/DesktopEnvironment.ts";
import * as DesktopCatalog from "../../apps/desktop/src/app/DesktopConnectionCatalogStore.ts";
import * as DesktopClientSettings from "../../apps/desktop/src/settings/DesktopClientSettings.ts";
import * as DesktopSavedEnvironments from "../../apps/desktop/src/settings/DesktopSavedEnvironments.ts";
import * as ElectronSafeStorage from "../../apps/desktop/src/electron/ElectronSafeStorage.ts";
import { sha256File, withRunScratchEffect } from "./support.mjs";

function desktopLayer(baseDir, safeStorage) {
  const environment = DesktopEnvironment.layer({
    dirname: NodePath.join(baseDir, "app", "src"),
    homeDirectory: baseDir,
    platform: "linux",
    processArch: "x64",
    appVersion: "1.2.3",
    appPath: baseDir,
    isPackaged: true,
    resourcesPath: NodePath.join(baseDir, "resources"),
    runningUnderArm64Translation: false,
  }).pipe(
    Layer.provide(
      Layer.mergeAll(
        NodeServices.layer,
        DesktopConfig.layerTest({
          T3CODE_HOME: baseDir,
          XDG_CONFIG_HOME: NodePath.join(baseDir, "appdata"),
        }),
      ),
    ),
  );
  const dependencies = Layer.mergeAll(
    environment,
    NodeServices.layer,
    Layer.succeed(ElectronSafeStorage.ElectronSafeStorage, safeStorage),
  );
  return DesktopCatalog.layer.pipe(
    Layer.provideMerge(DesktopSavedEnvironments.layer.pipe(Layer.provideMerge(dependencies))),
    Layer.provideMerge(dependencies),
  );
}

function syntheticSafeStorage(state) {
  return {
    isEncryptionAvailable: Effect.sync(() => state.available),
    encryptString: (value) => Effect.succeed(new TextEncoder().encode(`synthetic:${value}`)),
    decryptString: (value) =>
      Effect.suspend(() =>
        state.decryptFails
          ? Effect.fail(
              new ElectronSafeStorage.ElectronSafeStorageDecryptError({
                cause: new Error("synthetic decrypt failure"),
              }),
            )
          : Effect.succeed(new TextDecoder().decode(value).slice("synthetic:".length)),
      ),
    selectedStorageBackend: Effect.succeedNone,
  };
}

const decodeCatalog = Schema.decodeUnknownSync(Schema.fromJsonString(ConnectionCatalogDocument));
const decodeClientSettings = Schema.decodeUnknownSync(Schema.fromJsonString(ClientSettingsSchema));

describe("T4 desktop storage production seams with synthetic safeStorage", () => {
  it.effect("selects legacy userData when present and current userData otherwise", () =>
    withRunScratchEffect({ label: "desktop-userdata" }, ({ root, record }) =>
      Effect.gen(function* () {
        const state = { available: true, decryptFails: false };
        const layer = desktopLayer(root, syntheticSafeStorage(state));
        const paths = yield* Effect.gen(function* () {
          const environment = yield* DesktopEnvironment.DesktopEnvironment;
          return {
            legacy: NodePath.join(environment.appDataDirectory, environment.legacyUserDataDirName),
            current: NodePath.join(environment.appDataDirectory, environment.userDataDirName),
          };
        }).pipe(Effect.provide(layer), Effect.scoped);
        const resolve = DesktopAppIdentity.resolveUserDataPath.pipe(
          Effect.provide(layer),
          Effect.scoped,
        );
        NodeAssert.equal(yield* resolve, paths.current);
        yield* Effect.promise(() => NodeFSP.mkdir(paths.current, { recursive: true }));
        yield* Effect.promise(() => NodeFSP.mkdir(paths.legacy, { recursive: true }));
        yield* Effect.promise(() =>
          NodeFSP.writeFile(NodePath.join(paths.legacy, "sentinel"), "synthetic legacy data\n"),
        );
        NodeAssert.equal(yield* resolve, paths.legacy);
        NodeAssert.equal(
          yield* Effect.promise(() =>
            NodeFSP.readFile(NodePath.join(paths.legacy, "sentinel"), "utf8"),
          ),
          "synthetic legacy data\n",
        );
        record({
          checkId: "desktop-userdata",
          proofKind: "production-path-resolver",
          result: "passed",
          readback: { current: paths.current, selected: paths.legacy },
          limits: ["synthetic Linux directories; no Electron process"],
        });
      }),
    ),
  );

  it.effect(
    "migrates a legacy registry, reopens its catalog and preserves failed decrypt data",
    () =>
      withRunScratchEffect({ label: "desktop-catalog" }, ({ root, record }) =>
        Effect.gen(function* () {
          const state = { available: true, decryptFails: false };
          const makeLayer = () => desktopLayer(root, syntheticSafeStorage(state));
          const paths = yield* Effect.gen(function* () {
            const environment = yield* DesktopEnvironment.DesktopEnvironment;
            return {
              registry: environment.savedEnvironmentRegistryPath,
              catalog: NodePath.join(environment.stateDir, "connection-catalog.json"),
              state: environment.stateDir,
            };
          }).pipe(Effect.provide(makeLayer()), Effect.scoped);
          yield* Effect.promise(() => NodeFSP.mkdir(paths.state, { recursive: true }));
          const legacy = JSON.stringify({
            version: 1,
            records: [
              {
                environmentId: "synthetic-desktop-environment",
                label: "Synthetic saved environment",
                httpBaseUrl: "https://synthetic.example.invalid/",
                wsBaseUrl: "wss://synthetic.example.invalid/",
                createdAt: "2026-06-01T00:00:00.000Z",
                lastConnectedAt: null,
              },
            ],
          });
          yield* Effect.promise(() => NodeFSP.writeFile(paths.registry, legacy));
          const load = Effect.gen(function* () {
            const store = yield* DesktopCatalog.DesktopConnectionCatalogStore;
            return yield* store.get;
          }).pipe(Effect.provide(makeLayer()), Effect.scoped);
          const migrated = Option.getOrThrow(yield* load);
          const decoded = decodeCatalog(migrated);
          NodeAssert.equal(decoded.targets.length, 1);
          NodeAssert.equal(decoded.targets[0].environmentId, "synthetic-desktop-environment");
          NodeAssert.equal(
            yield* Effect.promise(() => NodeFSP.readFile(paths.registry, "utf8")),
            legacy,
          );
          NodeAssert.equal(Option.getOrThrow(yield* load), migrated);
          const before = yield* Effect.promise(() => sha256File(paths.catalog));
          state.decryptFails = true;
          const decryptError = yield* load.pipe(Effect.flip);
          NodeAssert.equal(decryptError._tag, "DesktopConnectionCatalogStoreProtectionError");
          NodeAssert.equal(decryptError.operation, "decrypt-catalog");
          NodeAssert.equal(yield* Effect.promise(() => sha256File(paths.catalog)), before);
          state.decryptFails = false;
          state.available = false;
          const stored = yield* Effect.gen(function* () {
            const store = yield* DesktopCatalog.DesktopConnectionCatalogStore;
            return yield* store.set(migrated);
          }).pipe(Effect.provide(makeLayer()), Effect.scoped);
          NodeAssert.equal(stored, false);
          NodeAssert.equal(yield* Effect.promise(() => sha256File(paths.catalog)), before);
          state.available = true;
          NodeAssert.equal(Option.getOrThrow(yield* load), migrated);
          record({
            checkId: "desktop-catalog",
            proofKind: "production-store-synthetic-safeStorage",
            result: "passed",
            readback: {
              targets: decoded.targets.length,
              catalogSha256: before,
              failedDecryptPreserved: true,
              unavailableWritePreserved: true,
            },
            limits: [
              "synthetic safeStorage encryption/decryption; real Electron keychain and packaged app persistence unproved",
            ],
          });
        }),
      ),
  );

  it.effect(
    "reloads desktop preferences from their T3 home independently of legacy userData and browser stores",
    () =>
      withRunScratchEffect({ label: "desktop-client-settings" }, ({ root, record }) =>
        Effect.gen(function* () {
          const state = { available: true, decryptFails: false };
          const makeLayer = () =>
            DesktopClientSettings.layer.pipe(
              Layer.provideMerge(desktopLayer(root, syntheticSafeStorage(state))),
            );
          const paths = yield* Effect.gen(function* () {
            const environment = yield* DesktopEnvironment.DesktopEnvironment;
            return {
              preferences: environment.clientSettingsPath,
              state: environment.stateDir,
              legacy: NodePath.join(
                environment.appDataDirectory,
                environment.legacyUserDataDirName,
              ),
              catalog: NodePath.join(environment.stateDir, "connection-catalog.json"),
            };
          }).pipe(Effect.provide(makeLayer()), Effect.scoped);
          NodeAssert.equal(
            paths.preferences,
            NodePath.join(root, "userdata", "client-settings.json"),
          );
          NodeAssert.notEqual(paths.preferences, paths.catalog);
          NodeAssert.equal(paths.preferences.startsWith(`${paths.legacy}${NodePath.sep}`), false);
          yield* Effect.promise(() => NodeFSP.mkdir(paths.state, { recursive: true }));
          yield* Effect.promise(() => NodeFSP.mkdir(paths.legacy, { recursive: true }));
          const legacySentinel = NodePath.join(paths.legacy, "client-settings.json");
          yield* Effect.promise(() =>
            NodeFSP.writeFile(legacySentinel, '{"timestampFormat":"12-hour"}\n'),
          );
          const initial = {
            ...DEFAULT_CLIENT_SETTINGS,
            timestampFormat: "24-hour",
            diffLayout: "split",
          };
          yield* Effect.promise(() =>
            NodeFSP.writeFile(paths.preferences, JSON.stringify({ settings: initial })),
          );
          const browserSettingsRaw = JSON.stringify({
            ...DEFAULT_CLIENT_SETTINGS,
            timestampFormat: "12-hour",
          });
          const browserCatalogRaw = JSON.stringify(EMPTY_CONNECTION_CATALOG_DOCUMENT);
          const browserValues = new Map([["t3code:client-settings:v1", browserSettingsRaw]]);
          const indexedValues = new Map([["catalog:document", browserCatalogRaw]]);
          let browserAccesses = 0;
          yield* Effect.addFinalizer(() => Effect.sync(() => vi.unstubAllGlobals()));
          vi.stubGlobal("window", {
            localStorage: {
              getItem: (key) => {
                browserAccesses++;
                return browserValues.get(key) ?? null;
              },
              setItem: (key, value) => {
                browserAccesses++;
                browserValues.set(key, value);
              },
            },
          });
          vi.stubGlobal("indexedDB", {
            open: () => {
              browserAccesses++;
              throw new Error("browser storage must remain unused");
            },
          });
          const load = () =>
            Effect.gen(function* () {
              const settings = yield* DesktopClientSettings.DesktopClientSettings;
              return Option.getOrThrow(yield* settings.get);
            }).pipe(Effect.provide(makeLayer()), Effect.scoped);
          NodeAssert.deepEqual(yield* load(), initial);
          const updated = { ...initial, timestampFormat: "12-hour", diffLayout: "stacked" };
          yield* Effect.gen(function* () {
            const settings = yield* DesktopClientSettings.DesktopClientSettings;
            yield* settings.set(updated);
          }).pipe(Effect.provide(makeLayer()), Effect.scoped);
          NodeAssert.deepEqual(yield* load(), updated);
          const disk = yield* Effect.promise(() => NodeFSP.readFile(paths.preferences, "utf8"));
          NodeAssert.deepEqual(decodeClientSettings(disk), updated);
          NodeAssert.equal(Object.hasOwn(JSON.parse(disk), "settings"), false);
          NodeAssert.equal(
            yield* Effect.promise(() => NodeFSP.readFile(legacySentinel, "utf8")),
            '{"timestampFormat":"12-hour"}\n',
          );
          NodeAssert.equal(browserAccesses, 0);
          NodeAssert.equal(browserValues.get("t3code:client-settings:v1"), browserSettingsRaw);
          NodeAssert.equal(indexedValues.get("catalog:document"), browserCatalogRaw);
          record({
            checkId: "desktop-client-settings",
            proofKind: "production-desktop-client-settings-file-reopen",
            result: "passed",
            readback: {
              preferencePath: paths.preferences,
              legacyUserDataPath: paths.legacy,
              timestampFormat: updated.timestampFormat,
              diffLayout: updated.diffLayout,
              browserApiAccesses: browserAccesses,
            },
            limits: [
              "synthetic desktop home and injected browser APIs; Electron renderer integration and actual browser persistence unproved",
            ],
          });
        }),
      ),
  );
});
