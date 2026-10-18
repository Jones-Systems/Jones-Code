import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

const TcpPort = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65535 }));
export const HostServiceConfigData = Schema.Struct({
  schema: Schema.Literal(1),
  port: TcpPort,
  tailscaleServePort: Schema.optional(TcpPort),
});
export type HostServiceConfigData = typeof HostServiceConfigData.Type;
const ConfigJson = Schema.fromJsonString(HostServiceConfigData);
const decodeConfig = Schema.decodeUnknownEffect(ConfigJson, { onExcessProperty: "error" });
const encodeConfig = Schema.encodeEffect(ConfigJson);

export class HostServiceConfigError extends Schema.TaggedError<HostServiceConfigError>()(
  "HostServiceConfigError",
  {
    baseDir: Schema.String,
    operation: Schema.Literals(["validate-base-dir", "read", "decode", "write"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.operation === "validate-base-dir"
      ? "Jones host service requires an absolute base directory."
      : `Could not ${this.operation} Jones host service config under ${this.baseDir}.`;
  }
}

export function configEnvironment(config: HostServiceConfigData): Readonly<Record<string, string>> {
  return {
    T3CODE_HOST: "127.0.0.1",
    T3CODE_PORT: String(config.port),
    T3CODE_TAILSCALE_SERVE: "false",
  };
}

export class HostServiceConfig extends Context.Service<
  HostServiceConfig,
  {
    readonly read: (
      baseDir: string,
    ) => Effect.Effect<Option.Option<HostServiceConfigData>, HostServiceConfigError>;
    readonly write: (
      baseDir: string,
      config: HostServiceConfigData,
    ) => Effect.Effect<void, HostServiceConfigError>;
  }
>()("t3/jones/hostService/HostServiceConfig") {}

const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const configPath = (baseDir: string) =>
    path.isAbsolute(baseDir)
      ? Effect.succeed(path.join(baseDir, "jones", "host-service.json"))
      : Effect.fail(new HostServiceConfigError({ baseDir, operation: "validate-base-dir" }));

  const read: HostServiceConfig["Service"]["read"] = Effect.fn("HostServiceConfig.read")(
    function* (baseDir) {
      const filePath = yield* configPath(baseDir);
      const text = yield* fs.readFileString(filePath).pipe(
        Effect.map(Option.some),
        Effect.catchIf(
          (cause) => cause.reason._tag === "NotFound",
          () => Effect.succeed(Option.none<string>()),
        ),
        Effect.mapError(
          (cause) => new HostServiceConfigError({ baseDir, operation: "read", cause }),
        ),
      );
      if (Option.isNone(text)) return Option.none<HostServiceConfigData>();
      return Option.some(
        yield* decodeConfig(text.value).pipe(
          Effect.mapError(
            (cause) => new HostServiceConfigError({ baseDir, operation: "decode", cause }),
          ),
        ),
      );
    },
  );

  const write: HostServiceConfig["Service"]["write"] = Effect.fn("HostServiceConfig.write")(
    function* (baseDir, config) {
      const filePath = yield* configPath(baseDir);
      const text = yield* encodeConfig(config).pipe(
        Effect.mapError(
          (cause) => new HostServiceConfigError({ baseDir, operation: "decode", cause }),
        ),
      );
      // Replace on the destination filesystem; scoped scratch is removed on failure or cancellation.
      yield* Effect.scoped(
        Effect.gen(function* () {
          const directory = path.dirname(filePath);
          yield* fs.makeDirectory(directory, { recursive: true });
          const temporaryPath = yield* fs.makeTempFileScoped({
            directory,
            prefix: ".host-service-",
          });
          yield* fs.writeFileString(temporaryPath, `${text}\n`, { mode: 0o600 });
          yield* (yield* fs.open(temporaryPath, { flag: "r+" })).sync;
          yield* fs.rename(temporaryPath, filePath);
        }),
      ).pipe(
        Effect.mapError(
          (cause) => new HostServiceConfigError({ baseDir, operation: "write", cause }),
        ),
      );
    },
  );
  return HostServiceConfig.of({ read, write });
});

export const layer = Layer.effect(HostServiceConfig, make);
