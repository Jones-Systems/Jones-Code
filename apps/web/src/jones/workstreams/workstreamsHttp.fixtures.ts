import {
  t3PlacementIdentityKey,
  t3PlacementInventoryJson,
  type MembershipEpisode,
  type NativeReference,
  type T3PlacementIdentity,
  type T3PlacementResult,
  type T3ThreadPlacement,
  type WorkstreamsRegistrationContextAcceptedResponse,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { data, placements } from "../../components/workstreams/nativeWorkstreamActions.fixtures";
import { PrimaryEnvironmentHttpClient } from "../../environments/primary/httpClient";

/** One primary HTTP client call, captured by route instead of by request order. */
export interface WorkstreamsHttpCall {
  readonly route: string;
  readonly request: {
    readonly params?: Record<string, string>;
    readonly payload?: Record<string, unknown>;
  };
}

const capturingClient = {
  workstreams: new Proxy(
    {},
    {
      get: (_target, route) => (request: WorkstreamsHttpCall["request"]) =>
        Effect.succeed({ route: String(route), request }),
    },
  ),
};

/**
 * Adapts a route handler to the mocked `runPrimaryHttp`: the effect is run against a capturing
 * client so tests can answer concurrent index reads deterministically by route.
 */
export const routeWorkstreamsHttp =
  (handle: (call: WorkstreamsHttpCall, signal?: AbortSignal) => unknown) =>
  async (effect: unknown, options?: { readonly signal?: AbortSignal }): Promise<unknown> => {
    const call = Effect.runSync(
      Effect.provideService(
        effect as Effect.Effect<WorkstreamsHttpCall, never, PrimaryEnvironmentHttpClient>,
        PrimaryEnvironmentHttpClient,
        capturingClient as never,
      ),
    );
    options?.signal?.throwIfAborted();
    return handle(call, options?.signal);
  };

export const sha256Hex = async (text: string): Promise<string> =>
  Array.from(
    new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text))),
    (value) => value.toString(16).padStart(2, "0"),
  ).join("");

export interface SyntheticAssignment {
  readonly environmentId: string;
  readonly threadId: string;
  readonly workstreamId: string;
  readonly kind?: "primary" | "secondary";
  readonly attestedAt?: string;
  readonly expiresAt?: string;
  readonly membershipId?: string;
}

const readContext = (version: number) => ({
  owner_id: data.binding.ownerId,
  server_generation: data.binding.serverGeneration,
  registry_version: version,
});

/**
 * A synthetic control plane for hook tests: list, registration context, references,
 * memberships and placement proofs at one registry version. Proofs follow each attestation's
 * own lifetime, and every placement request carries its own inventory hash.
 */
export function syntheticWorkstreams(input: {
  readonly assignments: readonly SyntheticAssignment[];
  readonly version?: number;
  readonly now: () => number;
  readonly pageLimit?: number;
}) {
  const state = {
    version: input.version ?? data.binding.registryVersion,
    assignments: [...input.assignments],
    /** Return a value to answer or throw to fail a call before the default handler runs. */
    intercept: null as ((call: WorkstreamsHttpCall) => unknown) | null,
    calls: [] as WorkstreamsHttpCall[],
    placementRequests: [] as (readonly T3PlacementIdentity[])[],
  };
  const reference = (assignment: SyntheticAssignment, index: number): NativeReference => ({
    native_reference_id: `reference-${String(index).padStart(5, "0")}`,
    owner_id: data.binding.ownerId,
    identity: {
      provider: "t3",
      source_instance_id: assignment.environmentId,
      native_id: assignment.threadId,
      resource_kind: "thread",
      id_kind: "internal",
      account_provenance: { kind: "not_account_scoped" },
    },
    pr_locator: null,
    registration: {
      state: "attested",
      attestation_version: 2,
      attested_at: assignment.attestedAt ?? "2026-09-30T11:58:00Z",
      expires_at: assignment.expiresAt ?? "2026-09-30T12:03:00Z",
      evidence: {
        provider: "t3",
        source_instance_id: assignment.environmentId,
        native_id: assignment.threadId,
        authority_namespace: "authority",
        store_generation: 1,
        evidence_sha256: "a".repeat(64),
      },
    },
    created_at: "2026-09-30T11:00:00Z",
    created_by: { principal_id: "owner" },
    created_registry_version: 1,
  });
  const membership = (assignment: SyntheticAssignment, index: number): MembershipEpisode => ({
    membership_id: assignment.membershipId ?? `membership-${String(index).padStart(5, "0")}`,
    workstream_id: assignment.workstreamId,
    native_reference_id: reference(assignment, index).native_reference_id,
    kind: assignment.kind ?? "primary",
    opened: {
      at: "2026-09-30T11:00:00Z",
      actor: { principal_id: "owner" },
      command_id: "command-open-0001",
      registry_version: 1,
    },
    closed: null,
  });
  const page = <Item>(items: readonly Item[], cursor: unknown) => {
    const limit = input.pageLimit ?? 100;
    const offset = typeof cursor === "string" ? Number(cursor.slice(1)) : 0;
    const end = offset + limit;
    return {
      context: readContext(state.version),
      items: items.slice(offset, end),
      next_cursor: end < items.length ? `o${end}` : null,
    };
  };
  const registration = (): WorkstreamsRegistrationContextAcceptedResponse => ({
    protocol: "workstreams-registration-context/2.0.0",
    state: "ready",
    owner_id: data.binding.ownerId,
    principal_id: data.binding.principalId,
    grant_id: "grant",
    authorization_revision: data.binding.authorizationRevision,
    server_generation: data.binding.serverGeneration,
    registry_version: state.version,
    sources: [],
  });
  const proofs = async (identities: readonly T3PlacementIdentity[]): Promise<T3PlacementResult> => {
    const requested = new Set(identities.map(t3PlacementIdentityKey));
    const nowMs = input.now();
    const items: T3ThreadPlacement[] = state.assignments.flatMap((assignment, index) => {
      const ref = reference(assignment, index);
      const episode = membership(assignment, index);
      const key = t3PlacementIdentityKey({
        source_instance_id: assignment.environmentId,
        native_thread_id: assignment.threadId,
      });
      if (!requested.has(key) || Date.parse(ref.registration.expires_at!) <= nowMs) return [];
      return [
        {
          native_reference_id: ref.native_reference_id,
          membership_id: episode.membership_id,
          workstream_id: episode.workstream_id,
          kind: episode.kind,
          source_instance_id: assignment.environmentId,
          native_thread_id: assignment.threadId,
          attestation_version: 2,
          attested_at: ref.registration.attested_at!,
          expires_at: ref.registration.expires_at!,
          evidence_sha256: "a".repeat(64),
          authority_namespace: "authority",
          store_generation: 1,
          source_binding_version: 1,
        },
      ];
    });
    items.sort((a, b) =>
      a.native_reference_id < b.native_reference_id
        ? -1
        : a.native_reference_id > b.native_reference_id
          ? 1
          : a.membership_id < b.membership_id
            ? -1
            : 1,
    );
    return {
      page: {
        inventory_sha256: await sha256Hex(t3PlacementInventoryJson(identities)),
        context: { ...placements.context, registry_version: state.version },
        items,
        next_cursor: null,
      },
      trustedEnvironments: placements.trustedEnvironments,
      readiness: "ready",
    };
  };
  const handle = async (call: WorkstreamsHttpCall): Promise<unknown> => {
    state.calls.push(call);
    if (state.intercept) {
      const intercepted = await state.intercept(call);
      if (intercepted !== undefined) return intercepted;
    }
    switch (call.route) {
      case "list":
        return { ...data, binding: { ...data.binding, registryVersion: state.version } };
      case "registrationContext":
        return registration();
      case "references":
        return page(
          state.assignments.map((assignment, index) => reference(assignment, index)),
          call.request.payload?.cursor,
        );
      case "memberships":
        return page(
          state.assignments
            .map((assignment, index) => membership(assignment, index))
            .filter((item) => item.workstream_id === call.request.params?.workstreamId),
          call.request.payload?.cursor,
        );
      case "threadPlacements": {
        const identities = call.request.payload?.identities as readonly T3PlacementIdentity[];
        state.placementRequests.push(identities);
        return proofs(identities);
      }
      default:
        throw new Error(`Unexpected synthetic Workstreams route ${call.route}`);
    }
  };
  return { state, handle, registration };
}
