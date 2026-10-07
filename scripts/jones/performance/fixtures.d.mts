import type * as Effect from "effect/Effect";
import type { ServerDerivedPaths } from "../../../apps/server/src/config.ts";
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

import type { SyntheticDatabaseSource, CurrentDatabaseSource } from "./sources.mjs";
export type { SyntheticDatabaseSource, CurrentDatabaseSource } from "./sources.mjs";

export interface SyntheticFixtureRecipe {
  readonly kind: "coherent-v1";
  readonly historyTurns: number;
  readonly payloadBytes: number;
}

export type SyntheticFixtureProfile = "health-offline-delete" | "benchmark-wal";

export interface SyntheticProfilePragmas {
  readonly journal_mode: string;
  readonly synchronous: number;
  readonly foreign_keys: number;
  readonly busy_timeout: number;
  readonly journal_size_limit: number;
  readonly page_size: number;
  readonly user_version: number;
}

export interface SyntheticProfileEvidence {
  readonly kind: SyntheticFixtureProfile;
  readonly stage: string;
  readonly failedStage?: string;
  readonly productionObservations: readonly {
    readonly phase:
      | "before-seed"
      | "after-seed"
      | "before-callback"
      | "after-callback"
      | "before-production-close";
    readonly pragmas: SyntheticProfilePragmas;
  }[];
  readonly productionClosed?: boolean;
  readonly canonicalContent?: {
    readonly originalSha256: string;
    readonly beforeSha256?: string;
    readonly afterSha256?: string;
  };
  readonly maintenance?: {
    readonly beforePragmas?: SyntheticProfilePragmas;
    readonly afterPragmas?: SyntheticProfilePragmas;
    readonly checkpoint?: {
      readonly busy: number;
      readonly logFrames: number;
      readonly checkpointedFrames: number;
    } | null;
    readonly returnedMode?: string | null;
    readonly integrity?: {
      readonly ok: boolean;
      readonly resultCount: number;
      readonly sha256: string;
    };
    readonly foreignKeys?: { readonly violations: number; readonly sha256: string };
    readonly closed?: boolean;
    readonly header?: {
      readonly bytesRead: number;
      readonly writeVersion: number;
      readonly readVersion: number;
    };
    readonly sidecars?: { readonly wal: boolean; readonly shm: boolean; readonly journal: boolean };
  };
  readonly failure?: { readonly code: string };
}

export interface HistoricalSyntheticFixtureOptions {
  readonly producer: "historical-v1";
  readonly parentPath: string;
  readonly childName: string;
  readonly binding: PerformanceBinding;
  readonly policy: StagingPolicy;
  readonly databaseSource: SyntheticDatabaseSource;
  readonly profile?: SyntheticFixtureProfile;
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

// These callbacks run against the pinned historical V1 source, never the host V2 services.
export interface HistoricalFixtureEngine {
  readonly dispatch: (command: unknown, options?: unknown) => Effect.Effect<{ sequence: number }, unknown>;
  readonly latestSequence: Effect.Effect<number>;
}

export interface HistoricalFixtureSnapshotQuery {
  readonly getSnapshot: () => Effect.Effect<unknown, unknown>;
  readonly getThreadDetailById: (threadId: string) => Effect.Effect<unknown, unknown>;
  readonly getThreadDetailSnapshot: (threadId: string, options?: unknown) => Effect.Effect<unknown, unknown>;
}

export interface SyntheticFixtureContext {
  readonly [originalContext]: never;
  readonly owner: OwnedRoot;
  readonly paths: Pick<
    ServerDerivedPaths,
    | "dbPath"
    | "stateDir"
    | "attachmentsDir"
    | "worktreesDir"
    | "settingsPath"
    | "keybindingsConfigPath"
    | "environmentIdPath"
  >;
  readonly databaseSource: SyntheticDatabaseSource;
  readonly recipe: SyntheticFixtureRecipe;
  readonly engine: HistoricalFixtureEngine;
  readonly snapshotQuery: HistoricalFixtureSnapshotQuery;
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
    readonly profile: "observed-production-defaults" | SyntheticFixtureProfile;
  };
  readonly profile?: SyntheticProfileEvidence;
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

import type { CurrentFixtureOptions, CurrentFixtureContext, CurrentFixtureCapture, CurrentFixtureReceipt } from "../../../apps/server/scripts/jones/currentFixtures.ts";
export type SyntheticFixtureOptions = HistoricalSyntheticFixtureOptions | CurrentFixtureOptions;
export type FixtureReceipt = SyntheticFixtureReceipt | CurrentFixtureReceipt;
export type FixtureCapture = SyntheticFixtureCapture | CurrentFixtureCapture;

export interface ClosedSyntheticFixture {
  readonly fixture: ValidatedSyntheticFixture;
  readonly receipt: FixtureReceipt;
  readonly receiptSha256: string;
  readonly capture: FixtureCapture;
  readonly captureSha256: string;
  readonly databaseSource: SyntheticDatabaseSource | CurrentDatabaseSource;
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
  readonly capture: FixtureCapture;
  readonly captureSha256: string;
  readonly receipt: FixtureReceipt;
  readonly receiptSha256: string;
  readonly cleanup: OwnedCleanupReceipt;
}

export interface ClosedSyntheticFixtureResult<Value> extends SyntheticFixtureResult<Value> {
  readonly childReceipt: OwnedChildReceipt;
}

export interface SyntheticFixtureFailureEvidence {
  readonly creationReceipt: OwnedRootReceipt;
  readonly childReceipt?: OwnedChildReceipt;
  readonly capture?: FixtureCapture;
  readonly receipt?: FixtureReceipt;
  readonly receiptSha256?: string;
  readonly profile?: SyntheticProfileEvidence | CurrentFixtureCapture["profile"];
  readonly value?: unknown;
  readonly cleanup: OwnedCleanupReceipt;
  readonly primaryEvidence?: unknown;
}

export interface SyntheticFixtureError extends Error {
  readonly code?: string;
  readonly evidence?: SyntheticFixtureFailureEvidence;
}

export function withOpenSyntheticFixture<Value>(
  options: HistoricalSyntheticFixtureOptions,
  use: (context: SyntheticFixtureContext) => Value | Promise<Value>,
): Promise<SyntheticFixtureResult<Value>>;

export function withOpenSyntheticFixture<Value>(
  options: CurrentFixtureOptions,
  use: (context: CurrentFixtureContext) => Value | Promise<Value>,
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

export function fixtureCustodyReceipt(receipt: FixtureReceipt): SyntheticFixtureReceipt;
