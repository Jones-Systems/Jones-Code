import {
  WORKSTREAM_CONTRACT_HEADER_VERSION,
  WORKSTREAM_CONTRACT_MANIFEST_SHA256,
  type WorkstreamDelivery,
  type WorkstreamFreshness,
  type WorkstreamLifecycle,
  type WorkstreamProgress,
} from "@t3tools/contracts";

export const WORKSTREAM_CONTRACT_VERSION = WORKSTREAM_CONTRACT_HEADER_VERSION;
export const WORKSTREAM_CONTRACT_MANIFEST = WORKSTREAM_CONTRACT_MANIFEST_SHA256;
export type { WorkstreamDelivery, WorkstreamFreshness, WorkstreamLifecycle, WorkstreamProgress };

export interface WorkstreamRecord {
  readonly workstreamId: string;
  readonly name: string;
  readonly lifecycle: WorkstreamLifecycle;
  readonly progress: WorkstreamProgress;
  readonly delivery: WorkstreamDelivery;
  readonly freshness: WorkstreamFreshness;
  readonly sortOrder: number;
  readonly version: number;
  readonly createdAt: string;
  readonly updatedAt: string;
}

export type AccountProvenance =
  | {
      readonly kind: "account";
      readonly accountId: string;
      readonly accountNamespace: string;
    }
  | { readonly kind: "not_account_scoped" };

export type T3NativeLocator =
  | {
      readonly kind: "thread";
      readonly environmentId: string;
      readonly threadId: string;
      readonly projectId: string;
    }
  | {
      readonly kind: "project";
      readonly environmentId: string;
      readonly projectId: string;
    };

export interface LinkedPullRequestProjection {
  readonly host: "github.com";
  readonly repositoryOwner: string;
  readonly repositoryName: string;
  readonly number: number;
  readonly observationVersion: number;
  readonly attemptedAt: string;
  readonly outcome:
    | "observed"
    | "not_modified"
    | "inaccessible"
    | "rate_limited"
    | "unavailable"
    | "invalid_response";
  readonly lastSuccess: {
    readonly state: "open" | "closed" | "merged";
    readonly draft: boolean;
    readonly observedAt: string;
    readonly providerUpdatedAt: string | null;
  } | null;
}

export interface NativeReferenceRecord {
  readonly nativeReferenceId: string;
  readonly provider: string;
  readonly sourceInstanceId: string;
  readonly resourceKind: string;
  readonly idKind: "internal" | "external";
  readonly nativeId: string;
  readonly accountProvenance: AccountProvenance;
  readonly registrationState:
    | "quarantined"
    | "verification-pending"
    | "attested"
    | "verification-failed"
    | "expired"
    | "re-attestation-required";
  readonly t3Locator: T3NativeLocator | null;
  readonly linkedPullRequest: LinkedPullRequestProjection | null;
}

export interface EpisodeBoundary {
  readonly at: string;
  readonly commandId: string;
  readonly registryVersion: number;
}

export interface MembershipEpisode {
  readonly membershipId: string;
  readonly workstreamId: string;
  readonly nativeReferenceId: string;
  readonly kind: "primary" | "secondary";
  readonly opened: EpisodeBoundary;
  readonly closed:
    | (EpisodeBoundary & {
        readonly reason:
          | "removed"
          | "completed"
          | "completed-and-delivery-verified"
          | "superseded"
          | "continued-elsewhere"
          | "deferred"
          | "abandoned"
          | "other";
        readonly otherReason: string | null;
      })
    | null;
}

export interface LifecycleDeclaration {
  readonly lifecycleDeclarationId: string;
  readonly workstreamId: string;
  readonly revision: number;
  readonly priorLifecycle: WorkstreamLifecycle | null;
  readonly newLifecycle: WorkstreamLifecycle;
  readonly recordedAt: string;
  readonly commandId: string;
  readonly supersedesLifecycleDeclarationId: string | null;
  readonly registryVersion: number;
}

export interface DeclarationRevision {
  readonly declarationId: string;
  readonly workstreamId: string;
  readonly revision: number;
  readonly text: string;
  readonly state: "active" | "withdrawn";
  readonly recordedAt: string;
  readonly commandId: string;
  readonly registryVersion: number;
}

export interface WorkstreamEdge {
  readonly edgeId: string;
  readonly fromWorkstreamId: string;
  readonly toWorkstreamId: string;
  readonly relation: "continues_as" | "superseded_by";
  readonly opened: EpisodeBoundary;
  readonly closed: EpisodeBoundary | null;
}

export type WorkstreamReceiptState = "committed" | "pending" | "unresolved" | "rejected";

export type WorkstreamOperation =
  | "create_workstream"
  | "update_workstream"
  | "register_reference"
  | "verify_reference"
  | "attach_primary"
  | "reattach_primary"
  | "link_secondary"
  | "remove_membership"
  | "move_primary"
  | "set_coordination_disposition"
  | "request_native_t3_settlement"
  | "set_declaration"
  | "withdraw_declaration"
  | "add_edge"
  | "remove_edge"
  | "refresh_linked_pr";

export interface WorkstreamReceipt {
  readonly commandId: string;
  readonly operation: WorkstreamOperation;
  readonly workstreamIds: ReadonlyArray<string>;
  readonly state: WorkstreamReceiptState;
  readonly acceptedAt: string;
  readonly registryVersion: number | null;
  readonly changed: boolean | null;
  readonly coordinationDisposition: {
    readonly membershipId: string;
    readonly disposition: Exclude<NonNullable<MembershipEpisode["closed"]>["reason"], "removed">;
  } | null;
  readonly nativeSettlement: {
    readonly nativeReferenceId: string;
    readonly nativeAction: "settle" | "unsettle";
    readonly outcome: "committed" | "denied" | "unsupported" | "failed" | "unresolved";
  } | null;
}

export interface WorkstreamReadContext {
  readonly registryId: string;
  readonly ownerScopeId: string;
  readonly principalId: string;
  readonly authorizationRevision: number;
  readonly serverGeneration: number;
  readonly registryVersion: number;
  readonly contractVersion: typeof WORKSTREAM_CONTRACT_VERSION;
  readonly contractManifest: typeof WORKSTREAM_CONTRACT_MANIFEST;
}

export interface WorkstreamState {
  readonly context: WorkstreamReadContext;
  readonly workstreams: Readonly<Record<string, WorkstreamRecord>>;
  readonly nativeReferences: Readonly<Record<string, NativeReferenceRecord>>;
  readonly memberships: Readonly<Record<string, MembershipEpisode>>;
  readonly lifecycleDeclarations: Readonly<Record<string, LifecycleDeclaration>>;
  readonly declarations: Readonly<Record<string, DeclarationRevision>>;
  readonly edges: Readonly<Record<string, WorkstreamEdge>>;
  readonly receipts: Readonly<Record<string, WorkstreamReceipt>>;
}

export interface WorkstreamStateUpdate {
  readonly context: WorkstreamReadContext;
  readonly workstreams?: ReadonlyArray<WorkstreamRecord>;
  readonly nativeReferences?: ReadonlyArray<NativeReferenceRecord>;
  readonly memberships?: ReadonlyArray<MembershipEpisode>;
  readonly lifecycleDeclarations?: ReadonlyArray<LifecycleDeclaration>;
  readonly declarations?: ReadonlyArray<DeclarationRevision>;
  readonly edges?: ReadonlyArray<WorkstreamEdge>;
  readonly receipts?: ReadonlyArray<WorkstreamReceipt>;
}
