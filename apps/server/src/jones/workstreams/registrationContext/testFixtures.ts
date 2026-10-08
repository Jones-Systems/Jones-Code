import {
  WORKSTREAMS_REGISTRATION_CONTEXT_PROTOCOL,
  type WorkstreamsRegistrationContextBuild,
  type WorkstreamsRegistrationContextGitHubSource,
  WorkstreamsRegistrationContextResponse,
  type WorkstreamsRegistrationContextT3Source,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { WorkstreamTransportError } from "../../../workstreams/WorkstreamGateway.ts";
import { makeWorkstreamsRegistrationContext, type RegistrationContextPorts } from "./service.ts";

// Unknown encoding keeps malformed and excess fields intact for boundary rejection tests.
export const encodeFixtureJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const encodeResponse = Schema.encodeSync(
  Schema.fromJsonString(WorkstreamsRegistrationContextResponse),
);
export const fixtureTransportError = (detail: string) =>
  new WorkstreamTransportError({
    operation: "registration_context",
    effect: "no-effect",
    detail,
  });

export const build: WorkstreamsRegistrationContextBuild = {
  repository: "Jones-Systems/Jones-Code",
  sha: "a".repeat(40),
  tree: "b".repeat(40),
};
export const t3Source: WorkstreamsRegistrationContextT3Source = {
  provider: "t3",
  source_instance_id: "source-t3-synthetic",
  authority_namespace: "authority-t3-synthetic",
  store_generation: 7,
  resource_kind: "thread",
  id_kind: "internal",
  account_provenance: { kind: "not_account_scoped" },
  native_protocol: "workstreams-t3-provider/1.0.0",
  build,
};
export const githubSource: WorkstreamsRegistrationContextGitHubSource = {
  provider: "github",
  source_instance_id: "source-github-synthetic",
  authority_namespace: "authority-github-synthetic",
  store_generation: 11,
  resource_kind: "pull_request",
  id_kind: "external",
  account_provenance: { kind: "not_account_scoped" },
};
export const response: WorkstreamsRegistrationContextResponse = {
  protocol: WORKSTREAMS_REGISTRATION_CONTEXT_PROTOCOL,
  state: "ready",
  owner_id: "owner-synthetic",
  principal_id: "principal-synthetic",
  grant_id: "grant-synthetic",
  authorization_revision: 3,
  server_generation: 5,
  registry_version: 12,
  sources: [t3Source, githubSource],
};

export const makeRegistrationFixture = (overrides: Partial<RegistrationContextPorts> = {}) => {
  const reads = { registry: 0, authority: 0, build: 0 };
  const ports: RegistrationContextPorts = {
    readRegistrationContext: Effect.sync(() => {
      reads.registry += 1;
      return encodeResponse(response);
    }),
    configuredBinding: {
      ownerId: response.owner_id,
      principalId: response.principal_id,
      authorizationRevision: response.authorization_revision,
    },
    authority: {
      readCurrent: Effect.sync(() => {
        reads.authority += 1;
        return {
          environmentId: t3Source.source_instance_id,
          authorityNamespace: t3Source.authority_namespace,
          storeGeneration: t3Source.store_generation,
        };
      }),
    },
    build: Effect.sync(() => {
      reads.build += 1;
      return Option.some(build);
    }),
    ...overrides,
  };
  return { ports, reads, service: makeWorkstreamsRegistrationContext(ports) };
};
