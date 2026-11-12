import * as Schema from "effect/Schema";
import { WorkstreamOwnerGrant, WorkstreamReadContext } from "../workstreams.ts";
import {
  WorkstreamsNativeBuild,
  WORKSTREAMS_T3_PROVIDER_PROTOCOL,
  WORKSTREAMS_T3_PROVIDER_V2_PROTOCOL,
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

// Producer: Jones-Systems/chatgpt-control-plane at this immutable contract checkpoint.
export const WORKSTREAMS_REGISTRATION_CONTEXT_V2_PRODUCER_COMMIT =
  "0817d23ca2547c5ff8b88b0034e6e2b1de0dd431";
export const WORKSTREAMS_REGISTRATION_CONTEXT_V2_VERSION = "2.0.0";
export const WORKSTREAMS_REGISTRATION_CONTEXT_V2_PROTOCOL =
  "workstreams-registration-context/2.0.0";
export const WORKSTREAMS_REGISTRATION_CONTEXT_V2_MANIFEST_SHA256 =
  "9a8c1d320713cd3d9fcc010b635a172b241e04bf34236a6d022322f2c0fcd765";
export type WorkstreamsRegistrationContextVersion =
  | typeof WORKSTREAMS_REGISTRATION_CONTEXT_VERSION
  | typeof WORKSTREAMS_REGISTRATION_CONTEXT_V2_VERSION
  | typeof WORKSTREAMS_REGISTRATION_CONTEXT_V3_VERSION;

export const WorkstreamsRegistrationContextV2Sources = Schema.Array(
  WorkstreamsRegistrationContextSource,
).check(
  Schema.isMaxLength(5),
  Schema.makeFilter(
    (sources) =>
      sources.filter((source) => source.provider === "t3").length <= 4 &&
      sources.filter((source) => source.provider === "github").length <= 1 &&
      new Set(sources.map((source) => `${source.provider}:${source.source_instance_id}`)).size ===
        sources.length,
  ),
);
export const WorkstreamsRegistrationContextV2Response = closed(
  Schema.Struct({
    ...WorkstreamsRegistrationContextResponse.fields,
    protocol: Schema.Literal(WORKSTREAMS_REGISTRATION_CONTEXT_V2_PROTOCOL),
    sources: WorkstreamsRegistrationContextV2Sources,
  }),
);
export type WorkstreamsRegistrationContextV2Response =
  typeof WorkstreamsRegistrationContextV2Response.Type;

export const WORKSTREAMS_REGISTRATION_CONTEXT_V3_PRODUCER_COMMIT =
  "e319288320c25bd6d899b3ab8d60da549354ae6f";
export const WORKSTREAMS_REGISTRATION_CONTEXT_V3_VERSION = "3.0.0";
export const WORKSTREAMS_REGISTRATION_CONTEXT_V3_PROTOCOL =
  "workstreams-registration-context/3.0.0";
export const WORKSTREAMS_REGISTRATION_CONTEXT_V3_MANIFEST_SHA256 =
  "df1d07ecb6fa1785550d62fc040633f30874daa4b5bed634f82a5551f79e2474";

export const WORKSTREAMS_REGISTRATION_CONTEXT_CONTRACTS = {
  "1.0.0": {
    protocol: WORKSTREAMS_REGISTRATION_CONTEXT_PROTOCOL,
    manifest: WORKSTREAMS_REGISTRATION_CONTEXT_MANIFEST_SHA256,
  },
  "2.0.0": {
    protocol: WORKSTREAMS_REGISTRATION_CONTEXT_V2_PROTOCOL,
    manifest: WORKSTREAMS_REGISTRATION_CONTEXT_V2_MANIFEST_SHA256,
  },
  "3.0.0": {
    protocol: WORKSTREAMS_REGISTRATION_CONTEXT_V3_PROTOCOL,
    manifest: WORKSTREAMS_REGISTRATION_CONTEXT_V3_MANIFEST_SHA256,
  },
} as const satisfies Record<
  WorkstreamsRegistrationContextVersion,
  { readonly protocol: string; readonly manifest: string }
>;

export const WorkstreamsRegistrationContextV3T3Source = closed(
  Schema.Struct({
    ...sourceFields,
    provider: Schema.Literal("t3"),
    resource_kind: Schema.Literal("thread"),
    id_kind: Schema.Literal("internal"),
    enrollment_id: Id,
    enrolled_protocol: Schema.Literal(WORKSTREAMS_T3_PROVIDER_PROTOCOL),
    enrolled_build: WorkstreamsRegistrationContextBuild,
    accepted_native_profiles: Schema.Union([
      Schema.Tuple([Schema.Literal(WORKSTREAMS_T3_PROVIDER_PROTOCOL)]),
      Schema.Tuple([
        Schema.Literal(WORKSTREAMS_T3_PROVIDER_V2_PROTOCOL),
        Schema.Literal(WORKSTREAMS_T3_PROVIDER_PROTOCOL),
      ]),
      Schema.Tuple([Schema.Literal(WORKSTREAMS_T3_PROVIDER_V2_PROTOCOL)]),
    ]),
  }),
);
export type WorkstreamsRegistrationContextV3T3Source =
  typeof WorkstreamsRegistrationContextV3T3Source.Type;
export const WorkstreamsRegistrationContextV3Response = closed(
  Schema.Struct({
    ...WorkstreamsRegistrationContextResponse.fields,
    protocol: Schema.Literal(WORKSTREAMS_REGISTRATION_CONTEXT_V3_PROTOCOL),
    sources: Schema.Array(
      Schema.Union([
        WorkstreamsRegistrationContextV3T3Source,
        WorkstreamsRegistrationContextGitHubSource,
      ]),
    ).check(
      Schema.isMaxLength(5),
      Schema.makeFilter(
        (sources) =>
          sources.filter((source) => source.provider === "t3").length <= 4 &&
          sources.filter((source) => source.provider === "github").length <= 1 &&
          new Set(sources.map((source) => `${source.provider}:${source.source_instance_id}`))
            .size === sources.length,
      ),
    ),
  }),
);
export type WorkstreamsRegistrationContextV3Response =
  typeof WorkstreamsRegistrationContextV3Response.Type;

// Local clients keep the v1|v2 wire union; upstream v3 is translated to v2 after local qualification.
export const WorkstreamsRegistrationContextAcceptedResponse = Schema.Union([
  WorkstreamsRegistrationContextResponse,
  WorkstreamsRegistrationContextV2Response,
]);
export type WorkstreamsRegistrationContextAcceptedResponse =
  typeof WorkstreamsRegistrationContextAcceptedResponse.Type;
