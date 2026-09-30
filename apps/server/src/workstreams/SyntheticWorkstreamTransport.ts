import * as NodeCrypto from "node:crypto";

import * as Effect from "effect/Effect";
import type { WorkstreamReferenceDetail } from "@t3tools/contracts";

import {
  WORKSTREAM_CONTRACT_FAMILY,
  WORKSTREAM_CONTRACT_MANIFEST_SHA256,
  WORKSTREAM_CONTRACT_VERSION,
  type WorkstreamCapabilities,
  type WorkstreamCommand,
  type WorkstreamMetadata,
  type WorkstreamPage,
  type WorkstreamTransport,
  type WorkstreamReceipt,
  WorkstreamTransportError,
} from "./WorkstreamGateway.ts";

export const SYNTHETIC_WORKSTREAMS: ReadonlyArray<WorkstreamMetadata> = [
  {
    workstream_id: "ws-core-v1",
    owner_id: "owner-fixture",
    name: "Cross-system Workstreams",
    lifecycle: "active",
    progress: { state: "progressing" },
    delivery: "branch-open",
    freshness: "current",
    sort_order: 10,
    version: 3,
    created_at: "2026-09-12T12:00:00Z",
    updated_at: "2026-09-12T12:05:00Z",
    created_by: { principal_id: "principal-fixture" },
    updated_by: { principal_id: "principal-fixture" },
  },
  {
    workstream_id: "ws-follow-on",
    owner_id: "owner-fixture",
    name: "Summary follow-on",
    lifecycle: "planned",
    progress: { state: "waiting", condition: "Core v1 acceptance" },
    delivery: "none-observed",
    freshness: "unknown",
    sort_order: 20,
    version: 1,
    created_at: "2026-09-12T12:01:00Z",
    updated_at: "2026-09-12T12:01:00Z",
    created_by: { principal_id: "principal-fixture" },
    updated_by: { principal_id: "principal-fixture" },
  },
];

export interface SyntheticWorkstreamTransportOptions {
  readonly offline?: boolean;
  readonly manifestSha256?: string;
  readonly permissions?: ReadonlyArray<"workstreams:read" | "workstreams:write">;
}

export const makeSyntheticWorkstreamTransport = (
  options: SyntheticWorkstreamTransportOptions = {},
): WorkstreamTransport => {
  if (process.env.NODE_ENV === "production") {
    throw new Error("Synthetic Workstream transport is test-only");
  }
  const context = {
    owner_id: "owner-fixture",
    server_generation: 7,
    registry_version: 11,
  } as const;
  const unavailable = (operation: string) =>
    Effect.fail(
      new WorkstreamTransportError({
        operation,
        effect: "no-effect",
        detail: "Synthetic transport is offline.",
      }),
    );

  return {
    getCapabilities: () => {
      if (options.offline === true) return unavailable("capabilities");
      return Effect.succeed({
        contract_family: WORKSTREAM_CONTRACT_FAMILY,
        contract_version: WORKSTREAM_CONTRACT_VERSION,
        manifest_sha256: options.manifestSha256 ?? WORKSTREAM_CONTRACT_MANIFEST_SHA256,
        context,
        permissions: options.permissions ?? ["workstreams:read", "workstreams:write"],
        max_request_bytes: 32_768,
        max_response_bytes: 1_048_576,
        max_json_depth: 10,
        max_page_items: 100,
        max_pr_response_bytes: 262_144,
        max_pr_request_seconds: 15,
        cursor_ttl_seconds: 900,
      } satisfies WorkstreamCapabilities);
    },
    listWorkstreams: ({ limit, cursor }) => {
      if (options.offline === true) return unavailable("list");
      const offset = cursor === undefined ? 0 : Number.parseInt(cursor, 10);
      const items = SYNTHETIC_WORKSTREAMS.slice(offset, offset + limit);
      const nextOffset = offset + items.length;
      return Effect.succeed({
        context,
        items,
        next_cursor: nextOffset < SYNTHETIC_WORKSTREAMS.length ? String(nextOffset) : null,
      } satisfies WorkstreamPage);
    },
    getWorkstream: ({ workstreamId }) =>
      Effect.succeed({
        context,
        workstream:
          SYNTHETIC_WORKSTREAMS.find((item) => item.workstream_id === workstreamId) ??
          SYNTHETIC_WORKSTREAMS[0]!,
      }),
    listReferences: () => Effect.succeed({ context, items: [], next_cursor: null }),
    getReference: () =>
      Effect.succeed({
        context,
        reference: {
          native_reference_id: "reference-fixture",
          owner_id: context.owner_id,
          identity: {
            provider: "github",
            source_instance_id: "github-public",
            resource_kind: "pull_request",
            id_kind: "external",
            native_id: "jones-systems/t3code#1",
            account_provenance: { kind: "not_account_scoped" },
          },
          pr_locator: {
            host: "github.com",
            repository_owner: "jones-systems",
            repository_name: "t3code",
            number: 1,
          },
          registration: {
            state: "verification-pending",
            attestation_version: 1,
            attested_at: null,
            expires_at: null,
            evidence: null,
          },
          created_at: "2026-09-12T12:00:00Z",
          created_by: { principal_id: "principal-fixture" },
          created_registry_version: 1,
        },
        latest_observation: null,
      } satisfies WorkstreamReferenceDetail),
    listMemberships: () => Effect.succeed({ context, items: [], next_cursor: null }),
    listDeclarations: () => Effect.succeed({ context, items: [], next_cursor: null }),
    listEdges: () => Effect.succeed({ context, items: [], next_cursor: null }),
    listHistory: () => Effect.succeed({ context, items: [], next_cursor: null }),
    getCommand: ({ commandId }) =>
      Effect.succeed({
        command_id: commandId,
        owner_id: "owner-fixture",
        actor: { principal_id: "principal-fixture" },
        operation: "update_workstream",
        request_sha256: "0".repeat(64),
        server_generation: 7,
        accepted_at: "2026-09-12T12:10:00Z",
        state: "unresolved",
        retry_after_seconds: 1,
      }),
    submitCommand: ({ body, command }) => {
      if (options.offline === true) return unavailable("command");
      return Effect.succeed(makeSyntheticReceipt(command, body));
    },
  };
};

const makeSyntheticReceipt = (
  command: WorkstreamCommand,
  body: string,
): Extract<WorkstreamReceipt, { readonly state: "committed" }> => {
  const coordination =
    command.action.operation === "set_coordination_disposition"
      ? {
          membership_id: String(command.action.membership_id),
          disposition: "completed" as const,
        }
      : null;
  const nativeSettlement =
    command.action.operation === "request_native_t3_settlement"
      ? {
          native_reference_id: String(command.action.native_reference_id),
          native_action: "settle" as const,
          outcome: "committed" as const,
        }
      : null;

  return {
    command_id: command.command_id,
    owner_id: "owner-fixture",
    actor: { principal_id: "principal-fixture" },
    operation: command.action.operation,
    request_sha256: NodeCrypto.createHash("sha256").update(body).digest("hex"),
    server_generation: 7,
    accepted_at: "2026-09-12T12:10:00Z",
    state: "committed",
    completed_at: "2026-09-12T12:10:00Z",
    registry_version: 12,
    changed: true,
    effects: {
      workstream_versions: [],
      native_reference_id: null,
      membership_ids: [],
      declaration_id: null,
      declaration_revision: null,
      edge_id: null,
      observation: null,
      registration: null,
      lifecycle_declaration: null,
      coordination_disposition: coordination,
      native_settlement: nativeSettlement,
    },
  };
};
