import type { T3ThreadPlacement, T3WorkstreamListResult } from "@t3tools/contracts";
import type { DesignRequestBinding } from "@t3tools/contracts/jones/designRequests";

import type { LiveT3Placements } from "../../state/workstreams/placements.ts";
import type { DesignRequestRouteInput } from "./route.ts";

// Synthetic registry state for design-request tests; no live registry or thread data.
export const NOW = Date.parse("2026-10-09T12:00:00.000Z");
export const binding: DesignRequestBinding = {
  bindingId: "0".repeat(32),
  galleryOrigin: "http://100.113.248.16:4339",
  projectKey: "form2",
  workstreamId: "ws-design",
};
const context = {
  owner_id: "owner-1",
  principal_id: "principal-1",
  authorization_revision: 1,
  server_generation: 1,
  registry_version: 7,
};
export const data = {
  binding: {
    registryId: "https://registry.example.ts.net",
    ownerId: context.owner_id,
    principalId: context.principal_id,
    authorizationRevision: 1,
    serverGeneration: 1,
    registryVersion: 7,
    permissions: ["workstreams:read"],
  },
  items: [
    { workstreamId: "ws-design", name: "Form 2 design" },
    { workstreamId: "ws-other", name: "Other" },
  ],
  nextCursor: null,
  source: "live",
  stale: false,
} as unknown as T3WorkstreamListResult;

export const placement = (overrides: Partial<T3ThreadPlacement> = {}): T3ThreadPlacement => ({
  membership_id: "membership-1",
  workstream_id: "ws-design",
  native_reference_id: "ref-1",
  kind: "primary",
  source_instance_id: "env-primary",
  native_thread_id: "thread-1",
  attestation_version: 1,
  attested_at: "2026-10-09T11:00:00.000Z",
  expires_at: "2026-10-09T13:00:00.000Z",
  evidence_sha256: "a".repeat(64),
  source_binding_version: 1,
  authority_namespace: "authority",
  store_generation: 3,
  ...overrides,
});

export const placements = (
  items: readonly T3ThreadPlacement[] = [placement()],
): LiveT3Placements => ({
  context,
  items,
  trustedEnvironments: [
    { environmentId: "env-primary", authorityNamespace: "authority", storeGeneration: 3 },
    { environmentId: "env-remote", authorityNamespace: "authority", storeGeneration: 3 },
  ],
  readiness: "ready",
});

export const thread = {
  environmentId: "env-primary",
  id: "thread-1",
  title: "Form 2 build",
  archivedAt: null,
  runtimeMode: "approval-required",
  interactionMode: "default",
} as const;

export const routeInput = (
  overrides: Partial<DesignRequestRouteInput> = {},
): DesignRequestRouteInput => ({
  binding,
  registry: "ready",
  data,
  placements: placements(),
  primaryEnvironmentId: "env-primary",
  threads: [thread, { ...thread, id: "thread-2", title: "Other thread" }],
  now: NOW,
  ...overrides,
});
