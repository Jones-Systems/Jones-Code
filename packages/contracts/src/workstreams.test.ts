import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import {
  T3WorkstreamListResult,
  WorkstreamReferenceDetail,
  WORKSTREAM_CONTRACT_HEADER_VERSION,
  WORKSTREAM_CONTRACT_MANIFEST_SHA256,
  canonicalGitHubPullRequestUrl,
} from "./workstreams.ts";

describe("Workstream wire contract", () => {
  it("decodes every lifecycle value with a fully fenced client binding", () => {
    const lifecycles = ["planned", "active", "paused", "completed", "deferred", "abandoned"];
    for (const lifecycle of lifecycles) {
      const result = Schema.decodeUnknownSync(T3WorkstreamListResult)({
        binding: {
          registryId: "https://control-plane.example.test:8443",
          ownerId: "owner",
          principalId: "principal",
          authorizationRevision: 3,
          serverGeneration: 7,
          registryVersion: 11,
          permissions: ["workstreams:read"],
          contractVersion: WORKSTREAM_CONTRACT_HEADER_VERSION,
          contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
        },
        items: [
          {
            workstreamId: `ws-${lifecycle}`,
            name: lifecycle,
            lifecycle,
            progress: { state: "progressing" },
            delivery: "none-observed",
            freshness: "current",
            sortOrder: 1,
            version: 1,
            updatedAt: "2026-09-12T12:00:00Z",
          },
        ],
        nextCursor: null,
        source: "live",
        stale: false,
      });
      expect(result.items[0]?.lifecycle).toBe(lifecycle);
    }
  });

  it("preserves the enabled producer's canonical HTTPS registry origin", () => {
    const registryId = "https://control-plane.example.test:8443";
    const decoded = Schema.decodeUnknownSync(T3WorkstreamListResult)({
      binding: {
        registryId,
        ownerId: "owner",
        principalId: "principal",
        authorizationRevision: 3,
        serverGeneration: 7,
        registryVersion: 11,
        permissions: ["workstreams:read"],
        contractVersion: WORKSTREAM_CONTRACT_HEADER_VERSION,
        contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
      },
      items: [],
      nextCursor: null,
      source: "live",
      stale: false,
    });
    expect(decoded.binding.registryId).toBe(registryId);
    for (const invalidOrigin of [
      "http://control-plane.example.test:8443",
      `${registryId}/`,
      `${registryId}/path`,
      "https://user@control-plane.example.test:8443",
    ]) {
      expect(() =>
        Schema.decodeUnknownSync(T3WorkstreamListResult)({
          ...decoded,
          binding: { ...decoded.binding, registryId: invalidOrigin },
        }),
      ).toThrow();
    }
  });

  it("constructs only canonical HTTPS GitHub pull-request URLs", () => {
    expect(
      canonicalGitHubPullRequestUrl({
        host: "github.com",
        repository_owner: "Jones-Systems",
        repository_name: "Codex-V3",
        number: 42,
      }),
    ).toBe("https://github.com/Jones-Systems/Codex-V3/pull/42");
    expect(() =>
      canonicalGitHubPullRequestUrl({
        host: "github.com",
        repository_owner: "Jones-Systems/escape",
        repository_name: "Codex-V3",
        number: 42,
      }),
    ).toThrow("Invalid GitHub pull-request locator");
  });

  it("requires a versioned bounded PR observation DTO", () => {
    const fixture = {
      context: { owner_id: "owner", server_generation: 7, registry_version: 11 },
      reference: {
        native_reference_id: "reference-1",
        owner_id: "owner",
        identity: {
          provider: "github",
          source_instance_id: "github-owner",
          resource_kind: "pull_request",
          id_kind: "external",
          native_id: "Jones-Systems/Codex-V3#42",
          account_provenance: { kind: "not_account_scoped" },
        },
        pr_locator: {
          host: "github.com",
          repository_owner: "jones-systems",
          repository_name: "codex-v3",
          number: 42,
        },
        registration: {
          state: "verification-pending",
          attestation_version: 0,
          attested_at: null,
          expires_at: null,
          evidence: null,
        },
        created_at: "2026-09-12T12:00:00Z",
        created_by: { principal_id: "principal" },
        created_registry_version: 1,
      },
      latest_observation: {
        native_reference_id: "reference-1",
        observation_version: 3,
        attempted_at: "2026-09-12T12:01:00Z",
        outcome: "observed",
        retry_after_seconds: null,
        last_success: {
          state: "open",
          draft: true,
          observed_at: "2026-09-12T12:01:00Z",
          provider_updated_at: null,
        },
        command_id: "command-refresh-0001",
      },
    } as const;
    expect(Schema.decodeUnknownSync(WorkstreamReferenceDetail)(fixture).latest_observation).toEqual(
      fixture.latest_observation,
    );
    expect(() =>
      Schema.decodeUnknownSync(WorkstreamReferenceDetail)({
        ...fixture,
        latest_observation: { ...fixture.latest_observation, observation_version: undefined },
      }),
    ).toThrow();
  });
});
