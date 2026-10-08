import { DesktopCompanionConfig, type DesktopCompanionConfigureInput } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SynchronizedRef from "effect/SynchronizedRef";
import * as DesktopConfig from "../../app/DesktopConfig.ts";
import * as DesktopAppIdentity from "../../app/DesktopAppIdentity.ts";
import * as DesktopAppSettings from "../../settings/DesktopAppSettings.ts";

export class CompanionConfigError extends Schema.TaggedError<CompanionConfigError>()(
  "CompanionConfigError",
  { operation: Schema.Literals(["read", "decode", "write", "identity"]) },
) {
  override get message() {
    return `Preview companion configuration ${this.operation} failed.`;
  }
}

export class CompanionConfig extends Context.Service<
  CompanionConfig,
  {
    readonly get: Effect.Effect<DesktopCompanionConfig>;
    readonly set: (
      input: DesktopCompanionConfigureInput,
    ) => Effect.Effect<DesktopCompanionConfig, CompanionConfigError>;
  }
>()("@t3tools/desktop/jones/previewCompanion/CompanionConfig") {}

export const browserOnlyStartup = (config: DesktopCompanionConfig): boolean => config.browserOnly;

export const make = Effect.gen(function* () {
  const productLocked = yield* DesktopConfig.DesktopConfig.pipe(
    Effect.map((config) => config.previewCompanionProduct),
    Effect.mapError(() => new CompanionConfigError({ operation: "read" })),
  );
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const identity = yield* DesktopAppIdentity.DesktopAppIdentity;
  const directory = yield* identity.resolveUserDataPath.pipe(
    Effect.mapError(() => new CompanionConfigError({ operation: "read" })),
  );
  const target = path.join(directory, "jones-preview-companion.json");
  const raw = yield* fs
    .readFileString(target)
    .pipe(
      Effect.catch((error) =>
        error.reason._tag === "NotFound"
          ? Effect.succeed(null)
          : Effect.fail(new CompanionConfigError({ operation: "read" })),
      ),
    );
  const write = (config: DesktopCompanionConfig) =>
    Effect.gen(function* () {
      const id = yield* crypto.randomUUIDv4;
      const temporary = `${target}.${id}.tmp`;
      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(DesktopCompanionConfig))(
        config,
      );
      yield* fs.makeDirectory(directory, { recursive: true });
      yield* fs
        .writeFileString(temporary, encoded, { mode: 0o600 })
        .pipe(
          Effect.andThen(fs.rename(temporary, target)),
          Effect.ensuring(fs.remove(temporary).pipe(Effect.ignore)),
        );
    }).pipe(Effect.mapError(() => new CompanionConfigError({ operation: "write" })));
  const initial =
    raw === null
      ? {
          enabled: false,
          environmentId: null,
          hostId: yield* crypto.randomUUIDv4.pipe(
            Effect.mapError(() => new CompanionConfigError({ operation: "identity" })),
          ),
          label: "Browser host",
          browserOnly: productLocked,
        }
      : yield* Schema.decodeUnknownEffect(Schema.fromJsonString(DesktopCompanionConfig))(raw).pipe(
          Effect.mapError(() => new CompanionConfigError({ operation: "decode" })),
        );
  if (productLocked && !initial.browserOnly) {
    return yield* new CompanionConfigError({ operation: "decode" });
  }
  if (raw === null) yield* write(initial);
  const state = yield* SynchronizedRef.make<DesktopCompanionConfig>(initial);
  return CompanionConfig.of({
    get: SynchronizedRef.get(state),
    set: (input) =>
      SynchronizedRef.modifyEffect(state, (current) =>
        Effect.gen(function* () {
          if (
            (productLocked && !input.browserOnly) ||
            (input.enabled && input.environmentId === null)
          ) {
            return yield* new CompanionConfigError({ operation: "decode" });
          }
          const next = yield* Schema.decodeUnknownEffect(DesktopCompanionConfig)({
            ...input,
            hostId: current.hostId,
          }).pipe(Effect.mapError(() => new CompanionConfigError({ operation: "decode" })));
          yield* write(next);
          return [next, next] as const;
        }),
      ),
  });
});
export const layer = Layer.effect(CompanionConfig, make);

export const prepareStartup = Effect.gen(function* () {
  const config = yield* CompanionConfig;
  if (browserOnlyStartup(yield* config.get)) {
    yield* (yield* DesktopAppSettings.DesktopAppSettings).setLocalEnvironmentEnabled(false);
  }
});

export class CompanionBrowserOnlyError extends Schema.TaggedError<CompanionBrowserOnlyError>()(
  "CompanionBrowserOnlyError",
  {},
) {
  override get message() {
    return "Disable browser-only companion mode before enabling a local environment.";
  }
}

export const checkLocalEnvironmentEnable = (enabled: boolean) =>
  Effect.gen(function* () {
    if (!enabled) return;
    // Main supplies config; legacy isolated IPC contexts retain ordinary behavior.
    const config = yield* Effect.serviceOption(CompanionConfig);
    if (Option.isSome(config) && browserOnlyStartup(yield* config.value.get)) {
      return yield* new CompanionBrowserOnlyError();
    }
  });
