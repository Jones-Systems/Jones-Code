// @effect-diagnostics-next-line nodeBuiltinImport:off -- Synchronous contract hashing and opaque request identities retain their existing wire encoding.
import * as NodeCrypto from "node:crypto";

import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import {
  WORKSTREAMS_T3_PROVIDER_ATTESTATION_TTL_MS,
  t3PlacementIdentityKey,
  t3PlacementInventoryJson,
  type MembershipEpisode,
  type NativeReference,
  type T3PlacementPage,
  type T3PlacementRequest,
  type T3ThreadPlacement,
  type WorkstreamReferenceDetail,
} from "@t3tools/contracts";

import {
  WORKSTREAM_CONTRACT_FAMILY,
  WORKSTREAM_CONTRACT_MANIFEST_SHA256,
  WORKSTREAM_CONTRACT_VERSION,
  type WorkstreamCapabilities,
  type WorkstreamCommand,
  type WorkstreamMetadata,
  type WorkstreamTransport,
  type WorkstreamReceipt,
  WorkstreamTransportError,
} from "./WorkstreamGateway.ts";

const OWNER_ID = "owner-fixture";
const SERVER_GENERATION = 7;
const INITIAL_REGISTRY_VERSION = 11;
const MAX_PAGE_ITEMS = 100;

const SYNTHETIC_WORKSTREAMS: ReadonlyArray<WorkstreamMetadata> = [
  {
    workstream_id: "ws-core-v1",
    owner_id: OWNER_ID,
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
    owner_id: OWNER_ID,
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

/** A pending GitHub pull-request reference with no membership, kept for detail reads. */
const PENDING_PR_REFERENCE: NativeReference = {
  native_reference_id: "reference-fixture",
  owner_id: OWNER_ID,
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
};

/** One seeded native T3 thread reference and, optionally, its open membership. */
export interface SyntheticT3Assignment {
  readonly sourceInstanceId: string;
  readonly nativeThreadId: string;
  /** Omit or pass null for a registered thread with no membership. */
  readonly workstreamId?: string | null;
  readonly kind?: "primary" | "secondary";
  /**
   * Epoch milliseconds of the seeded attestation; it expires after the fixed provider lifetime.
   * Defaults to the transport's construction time. Null seeds a verification-pending reference.
   */
  readonly attestedAt?: number | null;
}

export interface SyntheticWorkstreamTransportOptions {
  readonly offline?: boolean;
  readonly manifestSha256?: string;
  readonly permissions?: ReadonlyArray<"workstreams:read" | "workstreams:write">;
  /** Clock for receipts, attestations, and proof expiry. Defaults to `Date.now`. */
  readonly now?: () => number;
  /** Principal and grant revision echoed by placement pages and receipts. */
  readonly principalId?: string;
  readonly authorizationRevision?: number;
  /** Native store identity recorded in attestation evidence and placement proofs. */
  readonly authorityNamespace?: string;
  readonly storeGeneration?: number;
  readonly assignments?: ReadonlyArray<SyntheticT3Assignment>;
}

type CommittedReceipt = Extract<WorkstreamReceipt, { readonly state: "committed" }>;
type Effects = CommittedReceipt["effects"];
type RejectedReceipt = Extract<WorkstreamReceipt, { readonly state: "rejected" }>;
type ErrorCode = RejectedReceipt["error"]["code"];
type Registration = NativeReference["registration"];

const ERROR_BINDINGS = {
  not_found: [404, "stop"],
  invalid_request: [400, "correct_request"],
  version_conflict: [409, "reload"],
  registration_unverified: [409, "reverify"],
  membership_conflict: [409, "reload"],
  idempotency_conflict: [409, "reconcile"],
} as const satisfies Partial<
  Record<ErrorCode, readonly [number, RejectedReceipt["error"]["recovery"]]>
>;
type SyntheticErrorCode = keyof typeof ERROR_BINDINGS;

const NO_EFFECTS: Effects = {
  workstream_versions: [],
  native_reference_id: null,
  membership_ids: [],
  declaration_id: null,
  declaration_revision: null,
  edge_id: null,
  observation: null,
  registration: null,
  lifecycle_declaration: null,
  coordination_disposition: null,
  native_settlement: null,
};

const iso = (ms: number) => DateTime.formatIso(DateTime.makeUnsafe(ms));
const sha256 = (text: string) => NodeCrypto.createHash("sha256").update(text).digest("hex");
const pad = (value: number, width: number) => String(value).padStart(width, "0");
const byId = (left: string, right: string) => (left < right ? -1 : left > right ? 1 : 0);
const identityKey = (identity: NativeReference["identity"]) =>
  JSON.stringify([
    identity.provider,
    identity.source_instance_id,
    identity.resource_kind,
    identity.id_kind,
    identity.native_id,
    identity.account_provenance.kind === "account"
      ? [identity.account_provenance.account_id, identity.account_provenance.account_namespace]
      : null,
  ]);
const isT3Thread = (reference: NativeReference) =>
  reference.identity.provider === "t3" && reference.identity.resource_kind === "thread";

/**
 * Deterministic in-memory control plane for server tests. It keeps Workstreams, native
 * references, and membership episodes at one registry version, serves version-bound pages and
 * placement proofs from that state, and applies each command ID at most once.
 */
export const makeSyntheticWorkstreamTransport = (
  options: SyntheticWorkstreamTransportOptions = {},
): WorkstreamTransport => {
  if (process.env.NODE_ENV === "production") {
    throw new Error("Synthetic Workstream transport is test-only");
  }
  const now = options.now ?? Date.now;
  const principalId = options.principalId ?? "principal-fixture";
  const actor = { principal_id: principalId };
  const placementGrant = {
    principal_id: principalId,
    authorization_revision: options.authorizationRevision ?? 1,
  };
  const authorityNamespace = options.authorityNamespace ?? "synthetic-authority";
  const storeGeneration = options.storeGeneration ?? 1;

  let registryVersion = INITIAL_REGISTRY_VERSION;
  let referenceSequence = 0;
  let membershipSequence = 0;
  let workstreamSequence = 0;
  let requestSequence = 0;
  const workstreams: WorkstreamMetadata[] = SYNTHETIC_WORKSTREAMS.map((item) => ({ ...item }));
  const references = new Map<string, NativeReference>([
    [PENDING_PR_REFERENCE.native_reference_id, PENDING_PR_REFERENCE],
  ]);
  const referenceByIdentity = new Map<string, string>([
    [identityKey(PENDING_PR_REFERENCE.identity), PENDING_PR_REFERENCE.native_reference_id],
  ]);
  const memberships = new Map<string, MembershipEpisode>();
  const receipts = new Map<string, WorkstreamReceipt>();
  let sortedReferences: readonly NativeReference[] | null = null;

  const context = () => ({
    owner_id: OWNER_ID,
    server_generation: SERVER_GENERATION,
    registry_version: registryVersion,
  });
  const fail = (operation: string, detail: string) =>
    Effect.fail(new WorkstreamTransportError({ operation, effect: "no-effect", detail }));
  const unavailable = (operation: string) => fail(operation, "Synthetic transport is offline.");

  /** Cursors name the registry version they were issued at, so a commit makes them stale. */
  const page = <Item>(
    operation: string,
    items: readonly Item[],
    input: { readonly limit: number; readonly cursor?: string | undefined },
  ): Effect.Effect<
    { readonly items: readonly Item[]; readonly next_cursor: string | null },
    WorkstreamTransportError
  > => {
    let offset = 0;
    if (input.cursor !== undefined) {
      const match = /^v(\d+)-o(\d+)$/.exec(input.cursor);
      if (match === null) return fail(operation, "cursor_invalid");
      if (Number(match[1]) !== registryVersion) return fail(operation, "cursor_stale");
      offset = Number(match[2]);
    }
    const end = offset + Math.max(1, Math.min(input.limit, MAX_PAGE_ITEMS));
    return Effect.succeed({
      items: items.slice(offset, end),
      next_cursor: end < items.length ? `v${registryVersion}-o${end}` : null,
    });
  };

  const findWorkstream = (workstreamId: string) =>
    workstreams.findIndex((item) => item.workstream_id === workstreamId);
  const bumpWorkstream = (index: number, at: string) => {
    const current = workstreams[index]!;
    const next = { ...current, version: current.version + 1, updated_at: at, updated_by: actor };
    workstreams[index] = next;
    return { workstream_id: next.workstream_id, version: next.version };
  };
  const isCurrent = (registration: Registration, at: number) =>
    registration.state === "attested" &&
    registration.attested_at !== null &&
    registration.expires_at !== null &&
    Date.parse(registration.attested_at) <= at &&
    Date.parse(registration.expires_at) > at;
  const openMemberships = (referenceId: string) =>
    [...memberships.values()].filter(
      (item) => item.native_reference_id === referenceId && item.closed === null,
    );
  const attest = (reference: NativeReference, attestedAt: number): Registration => {
    const version = reference.registration.attestation_version + 1;
    return {
      state: "attested",
      attestation_version: version,
      attested_at: iso(attestedAt),
      expires_at: iso(attestedAt + WORKSTREAMS_T3_PROVIDER_ATTESTATION_TTL_MS),
      evidence: {
        provider: reference.identity.provider,
        source_instance_id: reference.identity.source_instance_id,
        authority_namespace: authorityNamespace,
        native_id: reference.identity.native_id,
        store_generation: storeGeneration,
        evidence_sha256: sha256(`${reference.native_reference_id}:${version}`),
      },
    };
  };
  const putReference = (reference: NativeReference) => {
    references.set(reference.native_reference_id, reference);
    referenceByIdentity.set(identityKey(reference.identity), reference.native_reference_id);
    sortedReferences = null;
  };
  const newReference = (
    identity: NativeReference["identity"],
    prLocator: NativeReference["pr_locator"],
    at: string,
    createdRegistryVersion: number,
  ): NativeReference => ({
    native_reference_id: `reference-t3-${pad(++referenceSequence, 5)}`,
    owner_id: OWNER_ID,
    identity,
    pr_locator: prLocator,
    registration: {
      state: "verification-pending",
      attestation_version: 0,
      attested_at: null,
      expires_at: null,
      evidence: null,
    },
    created_at: at,
    created_by: actor,
    created_registry_version: createdRegistryVersion,
  });
  const openEpisode = (
    workstreamId: string,
    referenceId: string,
    kind: "primary" | "secondary",
    boundary: MembershipEpisode["opened"],
  ): MembershipEpisode => {
    const episode: MembershipEpisode = {
      membership_id: `membership-${pad(++membershipSequence, 5)}`,
      workstream_id: workstreamId,
      native_reference_id: referenceId,
      kind,
      opened: boundary,
      closed: null,
    };
    memberships.set(episode.membership_id, episode);
    return episode;
  };
  const closeEpisode = (
    episode: MembershipEpisode,
    boundary: MembershipEpisode["opened"],
    reason: "removed" | "continued-elsewhere",
  ) =>
    memberships.set(episode.membership_id, {
      ...episode,
      closed: { ...boundary, reason, other_reason: null },
    });

  const seededAt = now();
  for (const [index, seed] of (options.assignments ?? []).entries()) {
    const boundary = {
      at: iso(seededAt),
      actor,
      command_id: `synthetic-seed-${pad(index, 6)}`,
      registry_version: INITIAL_REGISTRY_VERSION,
    };
    const identity: NativeReference["identity"] = {
      provider: "t3",
      source_instance_id: seed.sourceInstanceId,
      resource_kind: "thread",
      id_kind: "internal",
      native_id: seed.nativeThreadId,
      account_provenance: { kind: "not_account_scoped" },
    };
    const existing = referenceByIdentity.get(identityKey(identity));
    let reference =
      existing === undefined
        ? newReference(identity, null, boundary.at, INITIAL_REGISTRY_VERSION)
        : references.get(existing)!;
    if (existing === undefined && seed.attestedAt !== null)
      reference = { ...reference, registration: attest(reference, seed.attestedAt ?? seededAt) };
    putReference(reference);
    const workstreamId = seed.workstreamId ?? null;
    if (workstreamId === null) continue;
    if (findWorkstream(workstreamId) < 0)
      throw new Error(`Synthetic assignment names unknown Workstream ${workstreamId}`);
    const kind = seed.kind ?? "primary";
    const open = openMemberships(reference.native_reference_id);
    if (
      open.some((item) => item.workstream_id === workstreamId) ||
      (kind === "primary" && open.some((item) => item.kind === "primary"))
    )
      throw new Error("Synthetic assignments repeat a membership or primary");
    openEpisode(workstreamId, reference.native_reference_id, kind, boundary);
  }

  const receipt = (command: WorkstreamCommand, requestSha256: string, at: string) => ({
    command_id: command.command_id,
    owner_id: OWNER_ID,
    actor,
    operation: command.action.operation,
    request_sha256: requestSha256,
    server_generation: SERVER_GENERATION,
    accepted_at: at,
    completed_at: at,
  });
  const rejected = (
    command: WorkstreamCommand,
    requestSha256: string,
    at: string,
    code: SyntheticErrorCode,
  ): RejectedReceipt => {
    const [status, recovery] = ERROR_BINDINGS[code];
    return {
      ...receipt(command, requestSha256, at),
      state: "rejected",
      error: {
        code,
        status,
        recovery,
        request_id: `synthetic-request-${pad(++requestSequence, 6)}`,
      },
    };
  };

  /** Validates before mutating, so a rejected command leaves state and registry version intact. */
  const apply = (command: WorkstreamCommand, requestSha256: string): WorkstreamReceipt => {
    const nowMs = now();
    const at = iso(nowMs);
    const reject = (code: SyntheticErrorCode) => rejected(command, requestSha256, at, code);
    if (
      command.expected_server_generation !== SERVER_GENERATION ||
      command.expected_registry_version !== registryVersion
    )
      return reject("version_conflict");
    const next = registryVersion + 1;
    const boundary = { at, actor, command_id: command.command_id, registry_version: next };
    const commit = (effects: Partial<Effects>, changed = true): CommittedReceipt => {
      if (changed) registryVersion = next;
      return {
        ...receipt(command, requestSha256, at),
        state: "committed",
        registry_version: registryVersion,
        changed,
        effects: { ...NO_EFFECTS, ...effects },
      };
    };
    const workstreamAt = (workstreamId: string, expectedVersion: number) => {
      const index = findWorkstream(workstreamId);
      if (index < 0) return "not_found" as const;
      return workstreams[index]!.version === expectedVersion
        ? index
        : ("version_conflict" as const);
    };
    const action = command.action;
    switch (action.operation) {
      case "create_workstream": {
        const workstream: WorkstreamMetadata = {
          workstream_id: `ws-synthetic-${pad(++workstreamSequence, 4)}`,
          owner_id: OWNER_ID,
          name: action.name,
          lifecycle: action.lifecycle,
          progress: action.progress,
          delivery: "none-observed",
          freshness: "unknown",
          sort_order: action.sort_order,
          version: 1,
          created_at: at,
          updated_at: at,
          created_by: actor,
          updated_by: actor,
        };
        workstreams.push(workstream);
        return commit({
          workstream_versions: [{ workstream_id: workstream.workstream_id, version: 1 }],
        });
      }
      case "update_workstream": {
        const index = workstreamAt(action.workstream_id, action.expected_version);
        if (typeof index === "string") return reject(index);
        const current = workstreams[index]!;
        workstreams[index] = {
          ...current,
          name: action.name,
          lifecycle: action.lifecycle,
          progress: action.progress,
          sort_order: action.sort_order,
        };
        return commit({ workstream_versions: [bumpWorkstream(index, at)] });
      }
      case "register_reference": {
        const existing = referenceByIdentity.get(identityKey(action.identity));
        // Registering a known identity is a no-op that names the existing reference.
        if (existing !== undefined) return commit({ native_reference_id: existing }, false);
        const reference = newReference(action.identity, action.pr_locator, at, next);
        putReference(reference);
        return commit({
          native_reference_id: reference.native_reference_id,
          registration: reference.registration,
        });
      }
      case "verify_reference": {
        const reference = references.get(action.native_reference_id);
        if (reference === undefined) return reject("not_found");
        if (reference.registration.attestation_version !== action.expected_attestation_version)
          return reject("version_conflict");
        const registration = attest(reference, nowMs);
        putReference({ ...reference, registration });
        return commit({ native_reference_id: reference.native_reference_id, registration });
      }
      case "attach_primary":
      case "reattach_primary":
      case "link_secondary": {
        const index = workstreamAt(action.workstream_id, action.expected_version);
        if (typeof index === "string") return reject(index);
        const reference = references.get(action.native_reference_id);
        if (reference === undefined) return reject("not_found");
        if (!isCurrent(reference.registration, nowMs)) return reject("registration_unverified");
        const kind = action.operation === "link_secondary" ? "secondary" : "primary";
        const open = openMemberships(reference.native_reference_id);
        if (
          open.some((item) => item.workstream_id === action.workstream_id) ||
          (kind === "primary" && open.some((item) => item.kind === "primary"))
        )
          return reject("membership_conflict");
        const episode = openEpisode(
          action.workstream_id,
          reference.native_reference_id,
          kind,
          boundary,
        );
        return commit({
          workstream_versions: [bumpWorkstream(index, at)],
          native_reference_id: reference.native_reference_id,
          membership_ids: [episode.membership_id],
        });
      }
      case "remove_membership": {
        const index = workstreamAt(action.workstream_id, action.expected_version);
        if (typeof index === "string") return reject(index);
        const episode = memberships.get(action.membership_id);
        if (episode === undefined || episode.workstream_id !== action.workstream_id)
          return reject("not_found");
        if (episode.closed !== null) return reject("membership_conflict");
        closeEpisode(episode, boundary, "removed");
        return commit({
          workstream_versions: [bumpWorkstream(index, at)],
          native_reference_id: episode.native_reference_id,
          membership_ids: [episode.membership_id],
        });
      }
      case "move_primary": {
        if (action.source_workstream_id === action.destination_workstream_id)
          return reject("invalid_request");
        const source = workstreamAt(action.source_workstream_id, action.expected_source_version);
        if (typeof source === "string") return reject(source);
        const destination = workstreamAt(
          action.destination_workstream_id,
          action.expected_destination_version,
        );
        if (typeof destination === "string") return reject(destination);
        const episode = memberships.get(action.source_membership_id);
        if (episode === undefined || episode.workstream_id !== action.source_workstream_id)
          return reject("not_found");
        if (episode.closed !== null || episode.kind !== "primary")
          return reject("membership_conflict");
        if (
          openMemberships(episode.native_reference_id).some(
            (item) => item.workstream_id === action.destination_workstream_id,
          )
        )
          return reject("membership_conflict");
        if (!isCurrent(references.get(episode.native_reference_id)!.registration, nowMs))
          return reject("registration_unverified");
        closeEpisode(episode, boundary, "continued-elsewhere");
        const moved = openEpisode(
          action.destination_workstream_id,
          episode.native_reference_id,
          "primary",
          boundary,
        );
        return commit({
          workstream_versions: [bumpWorkstream(source, at), bumpWorkstream(destination, at)],
          native_reference_id: episode.native_reference_id,
          membership_ids: [episode.membership_id, moved.membership_id],
        });
      }
      case "set_coordination_disposition":
        return commit({
          coordination_disposition: {
            membership_id: action.membership_id,
            disposition: action.disposition,
          },
        });
      case "request_native_t3_settlement":
        return commit({
          native_settlement: {
            native_reference_id: action.native_reference_id,
            native_action: action.native_action,
            outcome: "committed",
          },
        });
      default:
        // Declarations, edges, and PR refreshes are not modeled; they commit with no effect.
        return commit({});
    }
  };

  const placements = (input: T3PlacementRequest) => {
    const requested = new Set(input.identities.map(t3PlacementIdentityKey));
    const nowMs = now();
    const items: T3ThreadPlacement[] = [];
    for (const episode of memberships.values()) {
      if (episode.closed !== null) continue;
      const reference = references.get(episode.native_reference_id)!;
      const { registration, identity } = reference;
      if (
        !isT3Thread(reference) ||
        !isCurrent(registration, nowMs) ||
        registration.evidence === null ||
        !requested.has(
          t3PlacementIdentityKey({
            source_instance_id: identity.source_instance_id,
            native_thread_id: identity.native_id,
          }),
        )
      )
        continue;
      items.push({
        membership_id: episode.membership_id,
        workstream_id: episode.workstream_id,
        native_reference_id: reference.native_reference_id,
        kind: episode.kind,
        source_instance_id: identity.source_instance_id,
        native_thread_id: identity.native_id,
        attestation_version: registration.attestation_version,
        attested_at: registration.attested_at!,
        expires_at: registration.expires_at!,
        evidence_sha256: registration.evidence.evidence_sha256,
        source_binding_version: 1,
        authority_namespace: registration.evidence.authority_namespace,
        store_generation: registration.evidence.store_generation,
      });
    }
    // The gateway requires strictly ascending (reference, membership) order across pages.
    items.sort(
      (left, right) =>
        byId(left.native_reference_id, right.native_reference_id) ||
        byId(left.membership_id, right.membership_id),
    );
    return page("thread_placements", items, input).pipe(
      Effect.map((slice): T3PlacementPage => ({
        inventory_sha256: sha256(t3PlacementInventoryJson(input.identities)),
        context: { ...context(), ...placementGrant },
        ...slice,
      })),
    );
  };

  return {
    getCapabilities: () => {
      if (options.offline === true) return unavailable("capabilities");
      return Effect.succeed({
        contract_family: WORKSTREAM_CONTRACT_FAMILY,
        contract_version: WORKSTREAM_CONTRACT_VERSION,
        manifest_sha256: options.manifestSha256 ?? WORKSTREAM_CONTRACT_MANIFEST_SHA256,
        context: context(),
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
    listWorkstreams: (input) => {
      if (options.offline === true) return unavailable("list");
      return page("list", workstreams, input).pipe(
        Effect.map((slice) => ({ context: context(), ...slice })),
      );
    },
    getWorkstream: ({ workstreamId }) => {
      const workstream = workstreams.find((item) => item.workstream_id === workstreamId);
      return workstream === undefined
        ? fail("detail", "not_found")
        : Effect.succeed({ context: context(), workstream });
    },
    listReferences: (input) => {
      sortedReferences ??= [...references.values()].toSorted((left, right) =>
        byId(left.native_reference_id, right.native_reference_id),
      );
      return page("references", sortedReferences, input).pipe(
        Effect.map((slice) => ({ context: context(), ...slice })),
      );
    },
    getReference: ({ nativeReferenceId }) => {
      const reference = references.get(nativeReferenceId);
      return reference === undefined
        ? fail("reference", "not_found")
        : Effect.succeed({
            context: context(),
            reference,
            latest_observation: null,
          } satisfies WorkstreamReferenceDetail);
    },
    listMemberships: ({ workstreamId, ...input }) =>
      page(
        "memberships",
        [...memberships.values()].filter((item) => item.workstream_id === workstreamId),
        input,
      ).pipe(Effect.map((slice) => ({ context: context(), ...slice }))),
    listDeclarations: () => Effect.succeed({ context: context(), items: [], next_cursor: null }),
    listEdges: () => Effect.succeed({ context: context(), items: [], next_cursor: null }),
    listHistory: () => Effect.succeed({ context: context(), items: [], next_cursor: null }),
    listThreadPlacements: (input) => {
      if (options.offline === true) return unavailable("thread_placements");
      return placements(input);
    },
    getCommand: ({ commandId }) =>
      Effect.succeed(
        receipts.get(commandId) ?? {
          command_id: commandId,
          owner_id: OWNER_ID,
          actor,
          operation: "update_workstream",
          request_sha256: "0".repeat(64),
          server_generation: SERVER_GENERATION,
          accepted_at: "2026-09-12T12:10:00Z",
          state: "unresolved",
          retry_after_seconds: 1,
        },
      ),
    submitCommand: ({ body, command }) => {
      if (options.offline === true) return unavailable("command");
      const requestSha256 = sha256(body);
      const prior = receipts.get(command.command_id);
      // A command ID applies once: the same bytes observe the stored receipt, other bytes conflict.
      if (prior !== undefined)
        return Effect.succeed(
          prior.request_sha256 === requestSha256
            ? prior
            : rejected(command, requestSha256, iso(now()), "idempotency_conflict"),
        );
      const result = apply(command, requestSha256);
      receipts.set(command.command_id, result);
      return Effect.succeed(result);
    },
  };
};
