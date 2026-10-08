import { describe, expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as DesktopConfig from "../../app/DesktopConfig.ts";
import * as DesktopAppIdentity from "../../app/DesktopAppIdentity.ts";
import * as DesktopAppSettings from "../../settings/DesktopAppSettings.ts";
import * as Config from "./CompanionConfig.ts";

const target = "/fixture/companion/jones-preview-companion.json";
const saved = {
  enabled: true,
  environmentId: "env-1",
  hostId: "stable-host",
  label: "Mini",
  browserOnly: true,
};
function fixture(raw?: string, failWrite = false, productLocked = false) {
  const files = new Map<string, string>(raw === undefined ? [] : [[target, raw]]);
  const error = (method: string, path: string) =>
    new PlatformError.PlatformError(
      new PlatformError.SystemError({
        _tag: "NotFound",
        module: "FileSystem",
        method,
        pathOrDescriptor: path,
        description: "fixture missing",
      }),
    );
  const fs = FileSystem.makeNoop({
    makeDirectory: () => Effect.void,
    readFileString: (path) =>
      files.has(path)
        ? Effect.succeed(files.get(path)!)
        : Effect.fail(error("readFileString", path)),
    writeFileString: (path, value) =>
      failWrite
        ? Effect.fail(error("writeFileString", path))
        : Effect.sync(() => {
            files.set(path, value);
          }),
    rename: (from, to) =>
      Effect.sync(() => {
        files.set(to, files.get(from)!);
        files.delete(from);
      }),
    remove: (path) =>
      Effect.sync(() => {
        files.delete(path);
      }),
  });
  const identity = DesktopAppIdentity.DesktopAppIdentity.of({
    resolveUserDataPath: Effect.succeed("/fixture/companion"),
    previewAutomationRuntimeIdentity: Effect.succeed({
      schemaVersion: 1,
      runtimeKind: "electron",
      runtimeInstanceId: "runtime-1",
      appVersion: "1",
      buildCommit: null,
    }),
    configure: Effect.void,
  });
  const layer = Config.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(FileSystem.FileSystem, fs),
        Path.layer,
        NodeCrypto.layer,
        DesktopConfig.layerTest({ JONES_PREVIEW_COMPANION_PRODUCT: String(productLocked) }),
        Layer.succeed(DesktopAppIdentity.DesktopAppIdentity, identity),
      ),
    ),
  );
  return { layer, files };
}

describe("companion configuration", () => {
  it.effect(
    "seeds a missing dedicated product config in an unpaired locked browser-only role",
    () =>
      Effect.gen(function* () {
        const f = fixture(undefined, false, true);
        const state = yield* Effect.gen(function* () {
          const store = yield* Config.CompanionConfig;
          const initial = yield* store.get;
          expect(initial).toMatchObject({ enabled: false, environmentId: null, browserOnly: true });
          yield* Config.prepareStartup;
          expect(
            (yield* (yield* DesktopAppSettings.DesktopAppSettings).get).localEnvironmentEnabled,
          ).toBe(false);
          const rejected = yield* Effect.exit(store.set({ ...initial, browserOnly: false }));
          expect(Exit.isFailure(rejected)).toBe(true);
          expect(yield* store.get).toEqual(initial);
          return initial;
        }).pipe(Effect.provide(Layer.mergeAll(f.layer, DesktopAppSettings.layerTest())));
        expect(JSON.parse(f.files.get(target)!)).toEqual(state);
      }),
  );

  it.effect.each(["{malformed", JSON.stringify({ ...saved, browserOnly: false })])(
    "fails closed for invalid dedicated product config without overwriting it",
    (raw) =>
      Effect.gen(function* () {
        const f = fixture(raw, false, true);
        const result = yield* Effect.exit(
          Effect.flatMap(Config.CompanionConfig, (store) => store.get).pipe(
            Effect.provide(f.layer),
          ),
        );
        expect(Exit.isFailure(result)).toBe(true);
        expect(f.files.get(target)).toBe(raw);
      }),
  );

  it.effect("persists the first host UUID and stores no credentials", () =>
    Effect.gen(function* () {
      const f = fixture();
      const first = yield* Effect.gen(function* () {
        const store = yield* Config.CompanionConfig;
        const initial = yield* store.get;
        expect(initial.enabled).toBe(false);
        expect(initial.hostId).toMatch(/^[a-f0-9-]{36}$/);
        const next = yield* store.set({
          enabled: true,
          environmentId: EnvironmentId.make(saved.environmentId),
          label: " Mini ",
          browserOnly: true,
        });
        expect(next.hostId).toBe(initial.hostId);
        return next;
      }).pipe(Effect.provide(f.layer));
      expect(JSON.parse(f.files.get(target)!)).toEqual(first);
      expect([...f.files.keys()]).toEqual([target]);
      const reread = fixture(f.files.get(target));
      expect(
        yield* Effect.flatMap(Config.CompanionConfig, (store) => store.get).pipe(
          Effect.provide(reread.layer),
        ),
      ).toEqual(first);
    }),
  );

  it.effect("preserves existing configuration after invalid input", () =>
    Effect.gen(function* () {
      const f = fixture(JSON.stringify(saved));
      yield* Effect.gen(function* () {
        const store = yield* Config.CompanionConfig;
        const result = yield* Effect.exit(
          store.set({ enabled: true, environmentId: null, label: "Mini", browserOnly: true }),
        );
        expect(Exit.isFailure(result)).toBe(true);
        expect(yield* store.get).toEqual(saved);
      }).pipe(Effect.provide(f.layer));
      expect(f.files.get(target)).toBe(JSON.stringify(saved));
    }),
  );

  it.effect("does not overwrite corrupt configuration or lose the saved host identity", () =>
    Effect.gen(function* () {
      const f = fixture("{malformed");
      const result = yield* Effect.exit(
        Effect.flatMap(Config.CompanionConfig, (store) => store.get).pipe(Effect.provide(f.layer)),
      );
      expect(Exit.isFailure(result)).toBe(true);
      expect(f.files.get(target)).toBe("{malformed");
    }),
  );

  it.effect("keeps failed persistence out of memory and removes its own temporary path", () =>
    Effect.gen(function* () {
      const f = fixture(JSON.stringify(saved), true);
      yield* Effect.gen(function* () {
        const store = yield* Config.CompanionConfig;
        const result = yield* Effect.exit(
          store.set({
            ...saved,
            environmentId: EnvironmentId.make(saved.environmentId),
            label: "Changed",
          }),
        );
        expect(Exit.isFailure(result)).toBe(true);
        expect(yield* store.get).toEqual(saved);
      }).pipe(Effect.provide(f.layer));
      expect([...f.files.keys()]).toEqual([target]);
    }),
  );

  it.effect("disables local backend settings before the startup branch can request a spawn", () =>
    Effect.gen(function* () {
      const f = fixture(JSON.stringify(saved));
      yield* Effect.gen(function* () {
        const settings = yield* DesktopAppSettings.DesktopAppSettings;
        expect((yield* settings.get).localEnvironmentEnabled).toBe(true);
        yield* Config.prepareStartup;
        let spawned = false;
        if ((yield* settings.get).localEnvironmentEnabled) spawned = true;
        expect(spawned).toBe(false);
      }).pipe(Effect.provide(Layer.mergeAll(f.layer, DesktopAppSettings.layerTest())));
    }),
  );

  it.effect("keeps an unpaired browser-only role independent of transport enablement", () =>
    Effect.gen(function* () {
      const unpaired = { ...saved, enabled: false, environmentId: null };
      const f = fixture(JSON.stringify(unpaired));
      yield* Effect.gen(function* () {
        yield* Config.prepareStartup;
        expect(
          (yield* (yield* DesktopAppSettings.DesktopAppSettings).get).localEnvironmentEnabled,
        ).toBe(false);
        expect(yield* (yield* Config.CompanionConfig).get).toEqual(unpaired);
      }).pipe(Effect.provide(Layer.mergeAll(f.layer, DesktopAppSettings.layerTest())));
    }),
  );

  it.effect.each([
    { ...saved, enabled: false, environmentId: null, browserOnly: false },
    { ...saved, browserOnly: false },
  ])("retains ordinary non-browser-only startup with transport enabled or disabled", (config) =>
    Effect.gen(function* () {
      const f = fixture(JSON.stringify(config));
      yield* Effect.gen(function* () {
        yield* Config.prepareStartup;
        expect(
          (yield* (yield* DesktopAppSettings.DesktopAppSettings).get).localEnvironmentEnabled,
        ).toBe(true);
      }).pipe(Effect.provide(Layer.mergeAll(f.layer, DesktopAppSettings.layerTest())));
    }),
  );
  it.effect(
    "retains legacy absent-config enablement while rejecting active browser-only configuration",
    () =>
      Effect.gen(function* () {
        expect(yield* Config.checkLocalEnvironmentEnable(true)).toBeUndefined();
        const f = fixture(JSON.stringify(saved));
        const rejected = yield* Effect.exit(
          Config.checkLocalEnvironmentEnable(true).pipe(Effect.provide(f.layer)),
        );
        expect(Exit.isFailure(rejected)).toBe(true);
        expect(
          yield* Config.checkLocalEnvironmentEnable(false).pipe(Effect.provide(f.layer)),
        ).toBeUndefined();
      }),
  );
});
