import * as Schema from "effect/Schema";
import { WorkstreamOwnerGrant, WorkstreamReadContext } from "./workstreams.ts";
import {
  WorkstreamsNativeBuild,
  WORKSTREAMS_T3_PROVIDER_PROTOCOL,
} from "./workstreamsNativeProvider.ts";

export const WORKSTREAMS_REGISTRATION_CONTEXT_FAMILY = "workstreams-registration-context";
export const WORKSTREAMS_REGISTRATION_CONTEXT_VERSION = "1.0.0";
export const WORKSTREAMS_REGISTRATION_CONTEXT_PROTOCOL = "workstreams-registration-context/1.0.0";
export const WORKSTREAMS_REGISTRATION_CONTEXT_MANIFEST_SHA256 =
  "1aa49d8cb4440eb40c877464dd1388a0a5071d65290c6490099db14d60b7789d";
export const WORKSTREAMS_REGISTRATION_CONTEXT_MAX_RESPONSE_BYTES = 8_192;
export const WORKSTREAMS_REGISTRATION_CONTEXT_MAX_JSON_DEPTH = 10;
const WORKSTREAMS_REGISTRATION_CONTEXT_MAX_SOURCES = 2;

// Check Struct input before decoding can discard undeclared wire fields.
const closed = <Fields extends Schema.Struct.Fields>(schema: Schema.Struct<Fields>) =>
  Schema.flip(
    Schema.flip(schema).check(
      Schema.makeFilter(
        (value) =>
          typeof value === "object" &&
          value !== null &&
          !Array.isArray(value) &&
          Reflect.ownKeys(value).every((key) => Object.hasOwn(schema.fields, key)),
      ),
    ),
  );

const Id = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(128),
  Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}(?![\s\S])/),
);
const PositiveVersion = WorkstreamOwnerGrant.fields.grant_version;
const AccountProvenance = closed(Schema.Struct({ kind: Schema.Literal("not_account_scoped") }));

export const WorkstreamsRegistrationContextRequest = closed(Schema.Struct({}));
export type WorkstreamsRegistrationContextRequest =
  typeof WorkstreamsRegistrationContextRequest.Type;
export const WorkstreamsRegistrationContextBuild = WorkstreamsNativeBuild.check(
  Schema.makeFilter((value) => value.sha.length === 40 && value.tree.length === 40),
);
export type WorkstreamsRegistrationContextBuild = typeof WorkstreamsRegistrationContextBuild.Type;

const sourceFields = {
  source_instance_id: Id,
  authority_namespace: Id,
  store_generation: PositiveVersion,
  account_provenance: AccountProvenance,
};
export const WorkstreamsRegistrationContextT3Source = closed(
  Schema.Struct({
    ...sourceFields,
    provider: Schema.Literal("t3"),
    resource_kind: Schema.Literal("thread"),
    id_kind: Schema.Literal("internal"),
    native_protocol: Schema.Literal(WORKSTREAMS_T3_PROVIDER_PROTOCOL),
    build: WorkstreamsRegistrationContextBuild,
  }),
);
export type WorkstreamsRegistrationContextT3Source =
  typeof WorkstreamsRegistrationContextT3Source.Type;
export const WorkstreamsRegistrationContextGitHubSource = closed(
  Schema.Struct({
    ...sourceFields,
    provider: Schema.Literal("github"),
    resource_kind: Schema.Literal("pull_request"),
    id_kind: Schema.Literal("external"),
  }),
);
export type WorkstreamsRegistrationContextGitHubSource =
  typeof WorkstreamsRegistrationContextGitHubSource.Type;
export const WorkstreamsRegistrationContextSource = Schema.Union([
  WorkstreamsRegistrationContextT3Source,
  WorkstreamsRegistrationContextGitHubSource,
]);
export type WorkstreamsRegistrationContextSource = typeof WorkstreamsRegistrationContextSource.Type;

export const WorkstreamsRegistrationContextSources = Schema.Array(
  WorkstreamsRegistrationContextSource,
).check(
  Schema.isMaxLength(WORKSTREAMS_REGISTRATION_CONTEXT_MAX_SOURCES),
  Schema.makeFilter(
    (sources) => new Set(sources.map((source) => source.provider)).size === sources.length,
  ),
);
export type WorkstreamsRegistrationContextSources =
  typeof WorkstreamsRegistrationContextSources.Type;

export const WorkstreamsRegistrationContextResponse = closed(
  Schema.Struct({
    protocol: Schema.Literal(WORKSTREAMS_REGISTRATION_CONTEXT_PROTOCOL),
    state: Schema.Literal("ready"),
    owner_id: Id,
    principal_id: Id,
    grant_id: Id,
    authorization_revision: PositiveVersion,
    server_generation: WorkstreamReadContext.fields.server_generation,
    registry_version: WorkstreamReadContext.fields.registry_version,
    sources: WorkstreamsRegistrationContextSources,
  }),
);
export type WorkstreamsRegistrationContextResponse =
  typeof WorkstreamsRegistrationContextResponse.Type;
