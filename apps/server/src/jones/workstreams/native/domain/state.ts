import type * as Wire from "@t3tools/contracts";
import { requireDomain } from "./errors.ts";
export type AuditEvent = typeof Wire.WorkstreamAuditEvent.Type;
export type Command = typeof Wire.WorkstreamCommand.Type;
export type CommittedReceipt = typeof Wire.WorkstreamCommittedReceipt.Type;
export type DeclarationRevision = typeof Wire.DeclarationRevision.Type;
export type Edge = typeof Wire.WorkstreamEdge.Type;
export type Effects = typeof Wire.WorkstreamReceiptEffects.Type;
export type LifecycleDeclaration = typeof Wire.WorkstreamLifecycleDeclaration.Type;
export type MembershipEpisode = typeof Wire.MembershipEpisode.Type;
export type NativeReference = typeof Wire.NativeReference.Type;
export type Receipt = typeof Wire.WorkstreamReceipt.Type;
export type RejectedReceipt = typeof Wire.WorkstreamRejectedReceipt.Type;
export type RegistrationVerification = typeof Wire.WorkstreamRegistrationVerification.Type;
export type ResourceVersion = typeof Wire.WorkstreamResourceVersion.Type;
export type Workstream = typeof Wire.Workstream.Type;
export type WorkstreamAction = typeof Wire.WorkstreamAction.Type;
export type NativeIdentity = typeof Wire.WorkstreamNativeIdentity.Type;
export type PrLocator = typeof Wire.WorkstreamPrLocator.Type;
export type PrObservation = typeof Wire.WorkstreamPrObservation.Type;
export type ReadContext = typeof Wire.WorkstreamReadContext.Type;
export type OwnerGrant = typeof Wire.WorkstreamOwnerGrant.Type;
export type Actor = typeof Wire.WorkstreamActor.Type;
export type NativeSettlementEffect = NonNullable<Effects["native_settlement"]>;
export type PrSnapshot = NonNullable<PrObservation["last_success"]>;
export interface PrObservationCache {
  readonly schema_version: 1;
  readonly owner_id: string;
  readonly native_reference_id: string;
  readonly source_instance_id: string;
  readonly provider: string;
  readonly locator: PrLocator;
  readonly server_generation: number;
  readonly authority_namespace: string;
  readonly store_generation: number;
  readonly observation_version: number;
  readonly etag: string;
  readonly representation: PrSnapshot;
}

export interface StoredPrObservation extends PrObservation {
  readonly observation_cache?: PrObservationCache | null;
}

export interface AuthorizedOwner {
  readonly owner_id: string;
  readonly actor: Actor;
  readonly grant: OwnerGrant;
  readonly server_generation: number;
}

export interface RegistrySourceBinding {
  readonly provider: string;
  readonly source_instance_id: string;
  readonly authority_namespace: string;
  readonly store_generation: number;
  readonly state: "active" | "revoked";
}

export interface RegistryCollections {
  readonly workstreams: Workstream;
  readonly references: NativeReference;
  readonly memberships: MembershipEpisode;
  readonly declarations: DeclarationRevision;
  readonly edges: Edge;
  readonly history: AuditEvent;
}

export interface RegistryExternalReservation {
  readonly kind: "verify_reference" | "native_t3_settlement" | "refresh_linked_pr";
  readonly command: Command;
  readonly owner_id: string;
  readonly actor: Actor;
  readonly authorization: AuthorizedOwner;
  readonly request_sha256: string;
  readonly accepted_at: string;
  readonly admitted_registry_version: number;
  readonly reference: NativeReference;
  readonly source_binding: RegistrySourceBinding;
  readonly membership: MembershipEpisode | null;
  readonly previous_observation: StoredPrObservation | null;
}

export interface RegistryExternalResult {
  readonly registration?: RegistrationVerification;
  readonly observation?: PrObservation;
  readonly observation_cache?: PrObservationCache | null;
  readonly native_outcome?: NativeSettlementEffect["outcome"];
  readonly native_evidence?: RegistryNativeResultEvidence;
}

export interface RegistryNativeResultEvidence {
  readonly owner_id: string;
  readonly principal_id: string;
  readonly command_id: string;
  readonly native_reference_id: string;
  readonly native_action: "settle" | "unsettle";
  readonly request_sha256: string;
  readonly reservation_sha256: string;
  readonly result_sha256: string;
}

export interface RegistryCommandInput {
  readonly authorization: AuthorizedOwner;
  readonly current_grant: OwnerGrant;
  readonly command: Command;
  readonly request_sha256: string;
  readonly accepted_at: string;
}

export interface PageQuery {
  readonly limit?: number;
  readonly cursor?: string;
}
export interface WorkstreamDomainState {
  owner_id: string;
  server_generation: number;
  registry_version: number;
  workstreams: Map<string, Workstream>;
  references: Map<string, NativeReference>;
  memberships: Map<string, MembershipEpisode>;
  declarations: Map<string, DeclarationRevision[]>;
  lifecycle_declarations: Map<string, LifecycleDeclaration[]>;
  edges: Map<string, Edge>;
  observations: Map<string, StoredPrObservation[]>;
  receipts: Map<string, Receipt>;
  legacy_receipts: Map<string, Receipt>;
  receipt_archive_state: "native_only" | "complete" | "import_incomplete" | "archive_class_missing";
  external_intents: Map<string, ExternalIntent>;
  external_results: Map<string, RegistryExternalResult>;
  audit: AuditEvent[];
  source_bindings: Map<string, RegistrySourceBinding>;
  active_reservation: ExternalIntent | null;
  edge_path_exists: boolean;
}

export type ExternalIntent = RegistryExternalReservation;

export type DomainCommandResult =
  | { readonly state: "committed_change" | "committed_noop"; readonly receipt: CommittedReceipt }
  | { readonly state: "rejected"; readonly receipt: RejectedReceipt }
  | { readonly state: "pending"; readonly receipt: Receipt }
  | { readonly state: "external"; readonly intent: ExternalIntent };

export function createWorkstreamDomainState(
  ownerId: string,
  serverGeneration = 1,
): WorkstreamDomainState {
  requireDomain(Number.isSafeInteger(serverGeneration) && serverGeneration > 0, "invalid_request");
  requireDomain(ownerId.length > 0, "invalid_request");
  return {
    owner_id: ownerId,
    server_generation: serverGeneration,
    registry_version: 0,
    workstreams: new Map(),
    references: new Map(),
    memberships: new Map(),
    declarations: new Map(),
    lifecycle_declarations: new Map(),
    edges: new Map(),
    observations: new Map(),
    receipts: new Map(),
    legacy_receipts: new Map(),
    receipt_archive_state: "native_only",
    external_intents: new Map(),
    external_results: new Map(),
    audit: [],
    source_bindings: new Map(),
    active_reservation: null,
    edge_path_exists: false,
  };
}

export type ReceiptLookup =
  | {
      readonly kind: "present";
      readonly source: "native" | "legacy_archive" | "legacy_open";
      readonly receipt: Receipt;
    }
  | {
      readonly kind: "proven_absent";
      readonly witness: {
        readonly server_generation: number;
        readonly registry_version: number;
        readonly archive: "complete";
      };
    }
  | {
      readonly kind: "unknown";
      readonly reason:
        | "import_incomplete"
        | "archive_class_missing"
        | "key_unavailable"
        | "generation_mismatch"
        | "home_unreachable";
    };

/** All arguments must come from the same decrypted home-store snapshot. */
export function lookupReceipt(
  commandId: string,
  snapshot: {
    readonly context: ReadContext;
    readonly native: Receipt | null;
    readonly archive: Receipt | null;
    readonly open: Receipt | null;
    readonly completeness:
      | "native_only"
      | "complete"
      | "import_incomplete"
      | "archive_class_missing";
  },
): ReceiptLookup {
  for (const [source, receipt] of [
    ["native", snapshot.native],
    ["legacy_archive", snapshot.archive],
    ["legacy_open", snapshot.open],
  ] as const) {
    if (receipt) {
      requireDomain(
        receipt.command_id === commandId && receipt.owner_id === snapshot.context.owner_id,
        "unknown_effect",
      );
      return { kind: "present", source, receipt };
    }
  }
  if (
    snapshot.completeness === "import_incomplete" ||
    snapshot.completeness === "archive_class_missing"
  )
    return { kind: "unknown", reason: snapshot.completeness };
  return {
    kind: "proven_absent",
    witness: {
      server_generation: snapshot.context.server_generation,
      registry_version: snapshot.context.registry_version,
      archive: "complete",
    },
  };
}
