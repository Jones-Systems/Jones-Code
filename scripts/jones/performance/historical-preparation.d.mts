import type { CurrentDatabaseSource, QualificationDatabaseSource } from "./sources.mjs";
export interface PreparationBounds {
  readonly maxBytes: number;
  readonly maxEntries: number;
  readonly maxFileBytes: number;
  readonly maxCacheBytes: number;
  readonly maxOutputBytes: number;
  readonly deadlineMs: number;
  readonly terminateGraceMs: number;
  readonly reapTimeoutMs: number;
  readonly maxProcesses: number;
  readonly estimatedPeakBytes: number;
}
export interface PreparationIdentity {
  readonly dev: number;
  readonly ino: number;
  readonly uid: number;
}
export interface PreparationCreation {
  readonly schema: "jones-historical-preparation/v1";
  readonly evidenceRoot: string;
  readonly evidenceIdentity: PreparationIdentity;
  readonly parent: string;
  readonly root: string;
  readonly parentIdentity: PreparationIdentity;
  readonly rootIdentity: PreparationIdentity;
  readonly marker: string;
  readonly taskId: string;
  readonly runId: string;
  readonly helperRevision: string;
  readonly helperSha256: string;
  readonly supervisorSha256: string;
}
declare const ownerBrand: unique symbol;
export interface HistoricalPreparationOwner {
  readonly [ownerBrand]: true;
  readonly creation: PreparationCreation;
}
export interface PreparationOptions {
  readonly parentPath: string;
  readonly evidenceParent: string;
  readonly taskId: string;
  readonly runId: string;
  readonly bounds: PreparationBounds;
  readonly retention: "release-after-closed-evidence" | "retain";
}
export interface PreparationProcess {
  readonly schema: "jones-historical-process/v1";
  readonly outcome: string;
  readonly token: string;
  readonly processOverflow: boolean;
  readonly installerIdentity: readonly [number, number] | null;
  readonly reapedPids: readonly number[];
  readonly familyClosed: boolean;
  readonly streamsClosed: boolean;
  readonly reaped: boolean;
  readonly stdout: string;
  readonly stderr: string;
  readonly outputBytes: number;
  readonly installerExitCode: number | null;
  readonly identities: readonly (readonly [number, number])[];
}
export interface HistoricalInput {
  readonly source: QualificationDatabaseSource;
  readonly tree: string;
  readonly inputs: Readonly<Record<string, string>>;
  readonly prepare: "unexecuted-source-import-route";
  readonly packagedBuild: "unqualified";
}
export interface HistoricalToolchainRequest {
  readonly node: { readonly path: string; readonly sha256: string };
  readonly pnpmCli: { readonly path: string; readonly sha256: string };
  readonly git: { readonly path: string; readonly sha256: string };
  readonly configurationAudit: {
    readonly pnpmCliSha256: string;
    readonly controls: readonly {
      readonly name: string;
      readonly inspected: true;
      readonly evidencePath: string;
      readonly evidenceSha256: string;
    }[];
  };
}
export interface HistoricalPreparationReceipt {
  readonly schema: "jones-historical-preparation/v1";
  readonly creation: PreparationCreation;
  readonly candidate: CurrentDatabaseSource;
  readonly sources: readonly HistoricalInput[];
  readonly toolchain: HistoricalToolchainRequest & {
    readonly nodeVersion: string;
    readonly pnpmVersion: "11.10.0";
  };
  readonly bounds: PreparationBounds;
  readonly sourceParent: string;
  readonly environment: { readonly JONES_PERFORMANCE_SOURCE_PARENT: string };
  readonly children: readonly PreparationProcess[];
  readonly dependencies: "frozen-installed-imports-unproved";
  readonly packagedBuild: "unqualified";
}
export interface PreparationFailure {
  readonly code: string;
  readonly message: string;
}
export interface PreparationCleanup {
  readonly outcome: "complete" | "retained";
  readonly absent: boolean;
  readonly root: string;
  readonly reason?: string;
  readonly error?: PreparationFailure | string;
}
export interface PreparationFinish<A> {
  readonly value?: A;
  readonly originalError?: PreparationFailure;
  readonly postconditionError?: PreparationFailure;
  readonly evidenceError?: PreparationFailure;
  readonly cleanup: PreparationCleanup;
  readonly evidence?: { readonly path: string; readonly sha256: string };
  readonly finalEvidence?: { readonly path: string; readonly sha256: string };
  readonly finalEvidenceError?: PreparationFailure;
  readonly root: string;
}
export const historicalPreparationSchema: "jones-historical-preparation/v1";
export const preparationPins: readonly {
  readonly directory: string;
  readonly sourceRevision: string;
  readonly tree: string;
  readonly lockSha256?: string;
}[];
export function validatePreparationBounds(bounds: PreparationBounds): PreparationBounds;
export function createHistoricalPreparation(
  options: PreparationOptions,
): HistoricalPreparationOwner;
export function assertPreparationIdentity(owner: HistoricalPreparationOwner): PreparationCreation;
export function inspectPreparation(
  owner: HistoricalPreparationOwner,
  options?: { consumption?: boolean },
): { entries: number; bytes: number; cacheBytes: number };
export function admitPreparation(owner: HistoricalPreparationOwner): {
  filesystems: readonly {
    path: string;
    availableBytes: string;
    reserveBytes: number;
    estimatedPeakBytes: number;
  }[];
  observedBytes: number;
  reservation: false;
};
export function runPreparationProcess(
  owner: HistoricalPreparationOwner,
  request: {
    executable: string;
    args: readonly string[];
    cwd: string;
    signal?: AbortSignal;
    env?: Readonly<Record<string, string>>;
  },
): Promise<PreparationProcess>;
export function preparationEvidencePath(owner: HistoricalPreparationOwner): string;
export function cleanupHistoricalPreparation(
  owner: HistoricalPreparationOwner,
  closure: { evidencePreserved: boolean; consumerClosed: boolean },
): PreparationCleanup;
export function preservePreparationEvidence(
  owner: HistoricalPreparationOwner,
  value: unknown,
): { path: string; sha256: string };
export function verifyHistoricalInputs(
  owner: HistoricalPreparationOwner,
  directory: string,
): HistoricalInput;
export function inspectHistoricalToolchain(
  owner: HistoricalPreparationOwner,
  toolchain: HistoricalToolchainRequest,
  signal?: AbortSignal,
): Promise<HistoricalPreparationReceipt["toolchain"]>;
export function assertHistoricalPreparationReceipt(
  path: string,
  trustedSha256: string,
): HistoricalPreparationReceipt;
export function inspectHistoricalPreparationReceipt(receipt: HistoricalPreparationReceipt): {
  entries: number;
  bytes: number;
  cacheBytes: number;
};
export function finishHistoricalPreparation<A>(
  owner: HistoricalPreparationOwner,
  outcome: { value?: A; originalError?: PreparationFailure; consumerClosed: boolean },
): PreparationFinish<A>;
export function withHistoricalPreparation<A>(
  options: PreparationOptions & {
    explicitHistoricalPreparation: true;
    candidate: CurrentDatabaseSource;
    toolchain: HistoricalToolchainRequest;
  },
  use: (prepared: {
    owner: HistoricalPreparationOwner;
    preparation: HistoricalPreparationReceipt;
    receipt: { path: string; sha256: string };
    signal?: AbortSignal;
  }) => Promise<{ consumerClosed: true; value: A; originalError?: PreparationFailure }>,
  signal?: AbortSignal,
): Promise<PreparationFinish<A>>;
