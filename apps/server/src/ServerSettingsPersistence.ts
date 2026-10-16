import {
  DEFAULT_SERVER_SETTINGS,
  ProjectId,
  ProjectSettingsOverrides,
  ResponseStreamingMode,
  ServerSettings,
} from "@t3tools/contracts";
import { fromLenientJson } from "@t3tools/shared/schemaJson";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SchemaTransformation from "effect/SchemaTransformation";

// Migrate saved token delivery without accepting it in settings writes or
// letting one retired value reset the rest of the environment's settings.
const PersistedResponseStreamingMode = Schema.Union([
  ResponseStreamingMode,
  Schema.Literal("token"),
]).pipe(
  Schema.decodeTo(
    ResponseStreamingMode,
    SchemaTransformation.transform({
      decode: (mode) => (mode === "token" ? "paragraph" : mode),
      encode: (mode) => mode,
    }),
  ),
);
const ServerSettingsJson = fromLenientJson(
  Schema.Struct({
    ...ServerSettings.fields,
    responseStreamingMode: PersistedResponseStreamingMode.pipe(
      Schema.withDecodingDefault(Effect.succeed("paragraph" as const)),
    ),
    projectSettingsOverrides: Schema.Record(
      ProjectId,
      Schema.Struct({
        ...ProjectSettingsOverrides.fields,
        responseStreamingMode: Schema.optionalKey(PersistedResponseStreamingMode),
      }),
    ).pipe(Schema.withDecodingDefault(Effect.succeed({}))),
  }),
);
const decodeServerSettingsJsonExit = Schema.decodeUnknownExit(ServerSettingsJson);
/**
 * The retired `providers.<kind>` map, read without its old schemas. Before
 * `providerInstances` existed each built-in driver had one blob there; it
 * now only feeds the live loader's legacy provider migration.
 */
const LegacySettingsSchema = Schema.Struct({
  providers: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
  // Retired for `sourceControlHosts`; read only by the live loader's source-control migration.
  bitbucket: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
  github: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
});
type PersistedLegacySettings = typeof LegacySettingsSchema.Type;
const decodeLegacySettingsJsonExit = Schema.decodeUnknownExit(
  fromLenientJson(LegacySettingsSchema),
);

// The file may hold an opt-out that can no longer be read; never resume threads on a guess.
export const UNTRUSTED_SERVER_SETTINGS: ServerSettings = {
  ...DEFAULT_SERVER_SETTINGS,
  continueThreadsAfterServerUpdate: false,
};

export function decodePersistedServerSettings(raw: string | undefined) {
  if (raw === undefined) {
    return {
      settings: DEFAULT_SERVER_SETTINGS,
      legacy: {} as PersistedLegacySettings,
      settingsFileTrusted: true,
      cause: undefined,
    };
  }
  const decoded = decodeServerSettingsJsonExit(raw);
  const legacy = decodeLegacySettingsJsonExit(raw);
  const settingsFileTrusted = decoded._tag === "Success" && legacy._tag === "Success";
  return {
    settings:
      decoded._tag === "Success" && legacy._tag === "Success"
        ? decoded.value
        : UNTRUSTED_SERVER_SETTINGS,
    legacy: legacy._tag === "Success" ? legacy.value : ({} as PersistedLegacySettings),
    settingsFileTrusted,
    cause:
      decoded._tag === "Failure"
        ? decoded.cause
        : legacy._tag === "Failure"
          ? legacy.cause
          : undefined,
  };
}
