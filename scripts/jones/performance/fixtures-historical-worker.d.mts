import type {
  HistoricalSyntheticFixtureOptions,
  SyntheticFixtureContext,
  SyntheticFixtureCapture,
  SyntheticProfileEvidence,
  HistoricalFixtureEngine,
  HistoricalFixtureSnapshotQuery,
} from "./fixtures.mjs";
import type { OwnedRoot, SyntheticCloseProof, SyntheticFixtureReceipt } from "./guard.mjs";
import type { QualificationDatabaseSource } from "./sources.mjs";
export function captureFixture(context: SyntheticFixtureContext): Promise<SyntheticFixtureCapture>;
export interface HistoricalProductionResult<A> {
  readonly owner: OwnedRoot;
  readonly closeKnown: boolean;
  readonly retainReason?: string;
  readonly value?: A;
  readonly error?: Error;
  readonly capture?: SyntheticFixtureCapture;
  readonly captureSha256?: string;
  readonly receipt?: SyntheticFixtureReceipt;
  readonly receiptSha256?: string;
  readonly profile?: SyntheticProfileEvidence;
}
export interface HistoricalQualificationContext extends Omit<
  SyntheticFixtureContext,
  "databaseSource" | "engine" | "snapshotQuery"
> {
  readonly databaseSource: QualificationDatabaseSource;
  readonly engine?: HistoricalFixtureEngine;
  readonly snapshotQuery?: HistoricalFixtureSnapshotQuery;
}
export function produceFixture<A>(
  options: HistoricalSyntheticFixtureOptions,
  use: (context: SyntheticFixtureContext) => A | Promise<A>,
): Promise<HistoricalProductionResult<A>>;
export function seedQualificationFixture(
  options: Omit<HistoricalSyntheticFixtureOptions, "databaseSource"> & {
    readonly databaseSource: QualificationDatabaseSource;
  },
): Promise<HistoricalProductionResult<HistoricalQualificationContext>>;
export function withQualificationFixture<A>(
  context: HistoricalQualificationContext,
  options: {
    readonly databaseSource: QualificationDatabaseSource;
    readonly mode: "engine" | "migration" | "client";
    readonly owner?: OwnedRoot;
    readonly paths?: SyntheticFixtureContext["paths"];
    readonly signal?: AbortSignal;
  },
  use: (phase: {
    readonly context: HistoricalQualificationContext;
    readonly modules: Readonly<Record<string, unknown>>;
    readonly query: (
      text: string,
      values?: readonly unknown[],
    ) => Promise<readonly Readonly<Record<string, unknown>>[]>;
    readonly capture: () => Promise<unknown>;
  }) => A | Promise<A>,
): Promise<
  HistoricalProductionResult<A> & {
    readonly context?: HistoricalQualificationContext;
    readonly closedProof?: SyntheticCloseProof;
  }
>;
