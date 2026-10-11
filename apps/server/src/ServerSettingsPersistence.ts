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
const PersistedOptionalProviderSettingsSchema = Schema.Struct({
  providers: Schema.optionalKey(
    Schema.Struct({
      cursor: Schema.optionalKey(Schema.Struct({ enabled: Schema.optionalKey(Schema.Boolean) })),
      grok: Schema.optionalKey(Schema.Struct({ enabled: Schema.optionalKey(Schema.Boolean) })),
      opencode: Schema.optionalKey(Schema.Struct({ enabled: Schema.optionalKey(Schema.Boolean) })),
    }),
  ),
});
export type PersistedOptionalProviderSettings = typeof PersistedOptionalProviderSettingsSchema.Type;
const decodePersistedOptionalProviderSettingsJsonExit = Schema.decodeUnknownExit(
  fromLenientJson(PersistedOptionalProviderSettingsSchema),
);

export function decodePersistedServerSettings(raw: string | undefined) {
  if (raw === undefined) {
    return {
      settings: DEFAULT_SERVER_SETTINGS,
      persisted: {} as PersistedOptionalProviderSettings,
      settingsFileTrusted: true,
      cause: undefined,
    };
  }
  const decoded = decodeServerSettingsJsonExit(raw);
  const persistedSettings = decodePersistedOptionalProviderSettingsJsonExit(raw);
  const settingsFileTrusted = decoded._tag === "Success" && persistedSettings._tag === "Success";
  return {
    settings:
      decoded._tag === "Success" && persistedSettings._tag === "Success"
        ? decoded.value
        : DEFAULT_SERVER_SETTINGS,
    persisted: persistedSettings._tag === "Success" ? persistedSettings.value : {},
    settingsFileTrusted,
    cause:
      decoded._tag === "Failure"
        ? decoded.cause
        : persistedSettings._tag === "Failure"
          ? persistedSettings.cause
          : undefined,
  };
}
