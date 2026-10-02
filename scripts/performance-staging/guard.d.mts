export interface PerformanceBinding {
  readonly repository: string;
  readonly sourceRevision: string;
  readonly taskRef: string;
  readonly runId: string;
}

export interface StagingPolicy {
  readonly homePath: string;
  readonly worktreePaths: readonly string[];
  readonly protectedPaths: readonly string[];
  readonly maxFiles?: number;
  readonly maxFileBytes?: number;
  readonly maxTotalBytes?: number;
  readonly maxReceiptBytes?: number;
}

export interface FileIdentity {
  readonly device: string;
  readonly inode: string;
  readonly uid: number;
  readonly mode: number;
}

export interface OwnedRootReceipt {
  readonly schema: "jones-performance-root/v1";
  readonly provenance: "synthetic-created";
  readonly binding: PerformanceBinding;
  readonly rootId: string;
  readonly canonicalParentPath: string;
  readonly canonicalRootPath: string;
  readonly identity: FileIdentity;
  readonly markerSha256: string;
}

declare const originalOwner: unique symbol;
declare const originalPermit: unique symbol;
declare const originalCloseProof: unique symbol;

export interface OwnedRoot {
  readonly [originalOwner]: never;
  readonly creationReceipt: OwnedRootReceipt;
}

export interface OwnedDatabasePermit {
  readonly [originalPermit]: never;
  readonly canonicalPath: string;
  readonly relativePath: string;
  readonly identity: FileIdentity;
  readonly access: "create" | "readwrite";
}

export interface SyntheticCloseProof {
  readonly [originalCloseProof]: never;
}

export type ClosedLayoutEntry =
  | {
      readonly relativePath: string;
      readonly present: false;
    }
  | {
      readonly relativePath: string;
      readonly present: true;
      readonly identity: FileIdentity;
      readonly nlink: number;
      readonly size: number;
    };

export interface FixtureManifestEntry {
  readonly relativePath: string;
  readonly identity: FileIdentity;
  readonly nlink: number;
  readonly size: number;
  readonly sha256: string;
}

export interface SyntheticFixtureReceipt {
  readonly schema: "jones-performance-fixture/v1";
  readonly provenance: "synthetic-produced";
  readonly creationReceipt: OwnedRootReceipt;
  readonly producerStep: string;
  readonly closure: {
    readonly kind: "observed-resource-close";
    readonly closureId: string;
    readonly completed: true;
  };
  readonly databaseRelativePath: string;
  readonly layout: readonly ClosedLayoutEntry[];
  readonly manifest: readonly FixtureManifestEntry[];
}

export interface ValidatedSyntheticFixture {
  readonly access: "readonly";
  readonly canonicalPath: string;
  readonly receipt: SyntheticFixtureReceipt;
  readonly receiptSha256: string;
  readonly layout: readonly ClosedLayoutEntry[];
  readonly verifiedFiles: number;
  readonly verifiedBytes: number;
}

export interface OwnedChildReceipt {
  readonly schema: "jones-performance-child/v1";
  readonly binding: PerformanceBinding | null;
  readonly outcome:
    | "success"
    | "failed"
    | "timed_out"
    | "cancelled"
    | "output_limited"
    | "spawn_refused"
    | "unknown";
  readonly pid: number | null;
  readonly startIdentity: { readonly platform: "linux"; readonly startTicks: string } | null;
  readonly exitCode: number | null;
  readonly signal: string | null;
  readonly stopReason: string | null;
  readonly observedBytes: number;
  readonly capturedBytes: number;
  readonly stdout: string;
  readonly stderr: string;
  readonly truncated: boolean;
  readonly terminated: boolean;
  readonly escalated: boolean;
  readonly closed: boolean;
  readonly reaped: boolean;
}

export interface OwnedCleanupReceipt {
  readonly schema: "jones-performance-cleanup/v1";
  readonly creationReceipt: OwnedRootReceipt | null;
  readonly outcome: "complete" | "retained" | "unknown";
  readonly absent: boolean;
  readonly childReceipts: readonly OwnedChildReceipt[];
  readonly reason: string | null;
}

export class PerformanceStagingError extends Error {
  readonly code: string;
  constructor(code: string, message: string);
}

export function createOwnedRoot(options: {
  parentPath: string;
  childName: string;
  binding: PerformanceBinding;
  policy: StagingPolicy;
}): OwnedRoot;

export function assertOwnedDatabase(
  owner: OwnedRoot,
  options: {
    databaseRelativePath: string;
    access: "create" | "readwrite";
  },
): OwnedDatabasePermit;

export function observeSyntheticClose<Resource extends object>(
  owner: OwnedRoot,
  options: {
    permit: OwnedDatabasePermit;
    producerStep: string;
    resource: Resource;
    close: (resource: Resource) => void | Promise<void>;
  },
): Promise<SyntheticCloseProof>;

export function sealSyntheticFixture(
  owner: OwnedRoot,
  options: {
    databaseRelativePath: string;
    producerStep: string;
    closedProof: SyntheticCloseProof;
  },
): Promise<SyntheticFixtureReceipt>;

export function syntheticFixtureReceiptSha256(receipt: SyntheticFixtureReceipt): string;

export function validateSyntheticFixture(options: {
  receipt: SyntheticFixtureReceipt;
  expectedReceiptSha256: string;
  expectedBinding: PerformanceBinding;
  policy: StagingPolicy;
  signal?: AbortSignal;
}): Promise<ValidatedSyntheticFixture>;

export function disposeOwnedRoot(
  owner: OwnedRoot,
  options?: {
    childReceipts?: readonly OwnedChildReceipt[];
  },
): OwnedCleanupReceipt;
