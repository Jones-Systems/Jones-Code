import type { EventId, ProjectId, ThreadId } from "@t3tools/contracts";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import type {
  LegacyStoppedRuntimeProofV1,
  ProviderSessionRuntime,
} from "../persistence/ProviderSessionRuntime.ts";
import type { ApplicationThreadBirthV2 } from "./Orchestrator.ts";
import type { DeletionWorktreePathAdmissionV1 } from "./EventSink.ts";
import type { WorktreeOwnershipLease } from "./WorktreeOwnershipLease.ts";

export interface LegacyLeaseOwnerV1 {
  readonly originalBirth: {
    readonly kind: "application_v1_thread_birth";
    readonly threadId: ThreadId;
    readonly eventId: EventId;
    readonly sequence: number;
    readonly projectId: ProjectId;
    readonly createdAt: string;
  };
  readonly importedBirth: ApplicationThreadBirthV2 | null;
  readonly replacementBirth: ApplicationThreadBirthV2 | null;
}

export interface LegacyLeaseCleanupBasisV1 {
  readonly lease: WorktreeOwnershipLease;
  readonly owner: LegacyLeaseOwnerV1;
  readonly sourceRow: ProviderSessionRuntime;
  readonly stoppedProof: LegacyStoppedRuntimeProofV1;
  readonly pathAdmission: DeletionWorktreePathAdmissionV1;
  readonly pendingOwnerEffectIds: ReadonlyArray<string>;
  readonly snapshot: string;
}

export type LegacyLeaseCleanupBasisResultV1 =
  | { readonly status: "ready"; readonly basis: LegacyLeaseCleanupBasisV1 }
  | { readonly status: "not_legacy" | "not_current" }
  | { readonly status: "retained"; readonly reason: string };

export type LegacyLeaseCleanupReleaseResultV1 =
  | { readonly status: "released" | "not_current" }
  | { readonly status: "retained"; readonly reason: string };

export class LegacyLeaseInventoryError extends Schema.TaggedError<LegacyLeaseInventoryError>()(
  "LegacyLeaseInventoryError",
  { threadId: Schema.String, reason: Schema.String },
) {}

// The callback remains valid only inside the producer's reserved inventory scope.
export type LegacyOwnerAbsencePort = <A, E, R>(
  owner: LegacyLeaseOwnerV1,
  body: (revalidate: Effect.Effect<void, LegacyLeaseInventoryError>) => Effect.Effect<A, E, R>,
) => Effect.Effect<A, E | LegacyLeaseInventoryError, R>;
