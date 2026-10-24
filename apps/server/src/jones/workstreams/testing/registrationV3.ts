import {
  WORKSTREAMS_REGISTRATION_CONTEXT_V3_PROTOCOL,
  WorkstreamsRegistrationContextResponse,
  WorkstreamsRegistrationContextV2Response,
  type WorkstreamsRegistrationContextV3Response,
  type WorkstreamsRegistrationContextV3T3Source,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { RegistrationContextPorts } from "../registrationContext/service.ts";
import {
  build,
  encodeFixtureJson,
  githubSource,
  makeRegistrationFixture,
  response,
  t3Source,
} from "../registrationContext/testFixtures.ts";

// Explicit pre-DW-J2 client union: neither of its existing closed schemas accepts v3 fields.
export const preChangeClientResponse = Schema.Union([
  WorkstreamsRegistrationContextResponse,
  WorkstreamsRegistrationContextV2Response,
]);
export const observedBuild = { ...build, sha: "c".repeat(40), tree: "d".repeat(40) };
export const v3T3Source: WorkstreamsRegistrationContextV3T3Source = {
  provider: t3Source.provider,
  source_instance_id: t3Source.source_instance_id,
  authority_namespace: t3Source.authority_namespace,
  store_generation: t3Source.store_generation,
  resource_kind: t3Source.resource_kind,
  id_kind: t3Source.id_kind,
  account_provenance: t3Source.account_provenance,
  enrollment_id: "enrollment-synthetic",
  enrolled_protocol: "workstreams-t3-provider/1.0.0",
  enrolled_build: build,
  accepted_native_profiles: ["workstreams-t3-provider/2.0.0", "workstreams-t3-provider/1.0.0"],
};
export const v3Response: WorkstreamsRegistrationContextV3Response = {
  ...response,
  protocol: WORKSTREAMS_REGISTRATION_CONTEXT_V3_PROTOCOL,
  sources: [
    { ...v3T3Source, source_instance_id: "remote-source", authority_namespace: "remote-authority" },
    { ...v3T3Source, source_instance_id: "remote-source-2", store_generation: 99 },
    { ...v3T3Source, source_instance_id: "remote-source-3", enrolled_build: observedBuild },
    githubSource,
    v3T3Source,
  ],
};
export const makeV3RegistrationFixture = (overrides: Partial<RegistrationContextPorts> = {}) =>
  makeRegistrationFixture({
    registrationContextVersion: "3.0.0",
    readRegistrationContext: Effect.succeed(encodeFixtureJson(v3Response)),
    build: Effect.succeedSome(observedBuild),
    ...overrides,
  });
