import type { OwnedCleanupReceipt, PerformanceBinding, StagingPolicy } from "./guard.mjs";
import type { QualificationDatabaseSource } from "./sources.mjs";
export function withPreparedHistoricalSources<A>(
  options: {
    readonly explicitHistoricalRequest: true;
    readonly gitExecutable: string;
    readonly parentPath: string;
    readonly binding: PerformanceBinding;
    readonly policy: StagingPolicy;
    readonly signal?: AbortSignal;
  },
  use: (prepared: {
    readonly root: string;
    readonly sources: readonly QualificationDatabaseSource[];
    readonly environment: Readonly<Record<string, string>>;
    readonly dependencies: "unprepared";
  }) => A | Promise<A>,
): Promise<{
  readonly value: A;
  readonly cleanup: OwnedCleanupReceipt;
  readonly dependencies: "unprepared";
}>;
