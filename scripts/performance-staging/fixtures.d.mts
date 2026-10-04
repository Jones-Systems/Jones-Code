import type * as Effect from "effect/Effect";
import type { ServerDerivedPaths } from "../../apps/server/src/config.ts";
import type { OrchestrationEngineShape } from "../../apps/server/src/orchestration/Services/OrchestrationEngine.ts";
import type { ProjectionSnapshotQueryShape } from "../../apps/server/src/orchestration/Services/ProjectionSnapshotQuery.ts";
import type {
  OwnedChildReceipt,
  OwnedCleanupReceipt,
  OwnedRoot,
  OwnedRootReceipt,
  PerformanceBinding,
  StagingPolicy,
  SyntheticFixtureReceipt,
  ValidatedSyntheticFixture,
} from "./guard.mjs";

export interface SyntheticDatabaseSource {
  readonly repository: "Jones-Systems/Jones-Code";
  readonly sourceRevision:
    | "e5a31aceec91484b64315c63dcce80f6e7581604"
    | "414bb8da204c3275cd0b76b2ec4d74dfb09a97e4";
  readonly worktreePath: string;
}

export interface SyntheticFixtureRecipe {
  readonly kind: "coherent-v1";
  readonly historyTurns: number;
  readonly payloadBytes: number;
}

export interface SyntheticFixtureOptions {
  readonly parentPath: string;
  readonly childName: string;
  readonly binding: PerformanceBinding;
  readonly policy: StagingPolicy;
  readonly databaseSource: SyntheticDatabaseSource;
  readonly recipe?: {
    readonly kind?: "coherent-v1";
    readonly historyTurns?: number;
    readonly payloadBytes?: number;
  };
  readonly lifecycle?: {
    readonly timeoutMs?: number;
    readonly terminateGraceMs?: number;
    readonly reapTimeoutMs?: number;
  };
  readonly signal?: AbortSignal;
}

declare const originalContext: unique symbol;

export interface SyntheticFixtureContext {
  readonly [originalContext]: never;
  readonly owner: OwnedRoot;
  readonly paths: ServerDerivedPaths;
  readonly databaseSource: SyntheticDatabaseSource;
  readonly recipe: SyntheticFixtureRecipe;
  readonly engine: OrchestrationEngineShape;
  readonly snapshotQuery: ProjectionSnapshotQueryShape;
  readonly run: <Value, Failure, Environment>(
    effect: Effect.Effect<Value, Failure, Environment>,
  ) => Promise<Value>;
}

export type SyntheticTableCapture =
  | { readonly status: "absent" }
  | {
      readonly status: "present";
      readonly count: number;
      readonly sha256: string;
      readonly pagingKey: readonly string[];
    };

export interface SyntheticFileCapture {
  readonly relativePath: string;
  readonly sizeBytes: number;
  readonly sha256: string;
}

export interface SyntheticFixtureCapture {
  readonly schema: "jones-performance-capture/v1";
  readonly databaseSource: SyntheticDatabaseSource;
  readonly recipe: SyntheticFixtureRecipe;
  readonly runtime: {
    readonly nodeVersion: string;
    readonly sqliteVersion: string;
    readonly pragmas: Readonly<Record<string, string | number>>;
    readonly profile: "observed-production-defaults";
  };
  readonly tables: Readonly<Record<string, SyntheticTableCapture>>;
  readonly ledgers: Readonly<
    Record<string, readonly { readonly id: number; readonly name: string }[]>
  >;
  readonly coupling: {
    readonly missing_receipt_events: number;
    readonly missing_thread_projects: number;
    readonly missing_message_threads: number;
    readonly noncontiguous_streams: number;
    readonly maxSequence: number;
    readonly snapshotSequence: number;
    readonly projectionCursors: readonly {
      readonly projector: string;
      readonly sequence: number;
    }[];
  };
  readonly readModel: {
    readonly projectCount: number;
    readonly threadCount: number;
    readonly historyMessages: number;
    readonly snapshotSha256: string;
    readonly replaySha256: string;
    readonly equivalent: boolean;
  };
  readonly pages: {
    readonly cursor: string;
    readonly recentMessageIds: readonly string[];
    readonly olderMessageIds: readonly string[];
    readonly overlap: number;
    readonly recentSha256: string;
    readonly olderSha256: string;
    readonly snapshotSequence: number;
  };
  readonly leaseFencing: {
    readonly incarnation: string;
    readonly currentLeaseId: string;
    readonly staleRenewed: false;
    readonly staleReleasePreservedCurrent: true;
    readonly foreignAcquired: false;
  };
  readonly native:
    | { readonly status: "absent" }
    | {
        readonly status: "present";
        readonly claimId: string;
        readonly normalizedCommandDigest: string;
        readonly effectPhases: readonly ("started" | "completed")[];
        readonly historySha256: string;
      };
  readonly files: readonly SyntheticFileCapture[];
  readonly attachmentFiles: readonly {
    readonly id: string;
    readonly relativePath: string;
    readonly sha256: string;
  }[];
  readonly blobs: readonly {
    readonly threadId: string;
    readonly fromTurnCount: number;
    readonly toTurnCount: number;
    readonly sizeBytes: number;
    readonly sha256: string;
  }[];
  readonly references: {
    readonly workspaces: readonly string[];
    readonly worktrees: readonly string[];
    readonly checkpointFiles: readonly string[];
  };
  readonly integrity: { readonly results: readonly string[]; readonly ok: boolean };
  readonly foreignKeys: { readonly violations: number; readonly sha256: string };
}

export interface ClosedSyntheticFixture {
  readonly fixture: ValidatedSyntheticFixture;
  readonly receipt: SyntheticFixtureReceipt;
  readonly receiptSha256: string;
  readonly capture: SyntheticFixtureCapture;
  readonly captureSha256: string;
  readonly databaseSource: SyntheticDatabaseSource;
  readonly childReceipt: OwnedChildReceipt;
}

export interface ClosedFixtureConsumerOutcome<Value> {
  readonly schema: "jones-performance-fixture-consumer/v1";
  readonly fixtureReceiptSha256: string;
  readonly disposition: "release" | "retain";
  readonly value: Value;
}

export interface SyntheticFixtureResult<Value> {
  readonly value: Value;
  readonly capture: SyntheticFixtureCapture;
  readonly captureSha256: string;
  readonly receipt: SyntheticFixtureReceipt;
  readonly receiptSha256: string;
  readonly cleanup: OwnedCleanupReceipt;
}

export interface ClosedSyntheticFixtureResult<Value> extends SyntheticFixtureResult<Value> {
  readonly childReceipt: OwnedChildReceipt;
}

export interface SyntheticFixtureFailureEvidence {
  readonly creationReceipt: OwnedRootReceipt;
  readonly childReceipt?: OwnedChildReceipt;
  readonly capture?: SyntheticFixtureCapture;
  readonly receipt?: SyntheticFixtureReceipt;
  readonly receiptSha256?: string;
  readonly value?: unknown;
  readonly cleanup: OwnedCleanupReceipt;
  readonly primaryEvidence?: unknown;
}

export interface SyntheticFixtureError extends Error {
  readonly code?: string;
  readonly evidence?: SyntheticFixtureFailureEvidence;
}

export function withOpenSyntheticFixture<Value>(
  options: SyntheticFixtureOptions,
  use: (context: SyntheticFixtureContext) => Value | Promise<Value>,
): Promise<SyntheticFixtureResult<Value>>;

export function captureSyntheticFixture(
  context: SyntheticFixtureContext,
): Promise<SyntheticFixtureCapture>;

export function withClosedSyntheticFixture<Value>(
  options: SyntheticFixtureOptions,
  use: (
    context: ClosedSyntheticFixture,
  ) => ClosedFixtureConsumerOutcome<Value> | Promise<ClosedFixtureConsumerOutcome<Value>>,
): Promise<ClosedSyntheticFixtureResult<Value>>;
