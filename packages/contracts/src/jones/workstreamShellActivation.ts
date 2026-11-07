import * as Schema from "effect/Schema";

export const WORKSTREAM_SHELL_ENV_NAMES = [
  "T3_WORKSTREAM_CONTROL_PLANE_URL",
  "T3_WORKSTREAM_OWNER_ID",
  "T3_WORKSTREAM_PRINCIPAL_ID",
  "T3_WORKSTREAM_KEY_ID",
  "T3_WORKSTREAM_SIGNING_SECRET",
  "T3_WORKSTREAM_AUTHORIZATION_REVISION",
  "T3_WORKSTREAM_REGISTRY_BINDING",
] as const;
export const JonesPrivateWorkstreamShellActivation = Schema.Struct({
  T3_WORKSTREAM_CONTROL_PLANE_URL: Schema.optionalKey(
    Schema.String.check(Schema.isMaxLength(16384)),
  ),
  T3_WORKSTREAM_OWNER_ID: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(16384))),
  T3_WORKSTREAM_PRINCIPAL_ID: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(16384))),
  T3_WORKSTREAM_KEY_ID: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(16384))),
  T3_WORKSTREAM_SIGNING_SECRET: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(16384))),
  T3_WORKSTREAM_AUTHORIZATION_REVISION: Schema.optionalKey(
    Schema.String.check(Schema.isMaxLength(16384)),
  ),
  T3_WORKSTREAM_REGISTRY_BINDING: Schema.optionalKey(
    Schema.String.check(Schema.isMaxLength(16384)),
  ),
});
export type JonesPrivateWorkstreamShellActivation =
  typeof JonesPrivateWorkstreamShellActivation.Type;
