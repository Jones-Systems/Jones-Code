import type { SyntheticDatabaseSource } from "./fixtures.mjs";

export const sourceParentEnvironment: "JONES_PERFORMANCE_SOURCE_PARENT";
export const syntheticSourcePins: readonly {
  readonly directory: "baseline" | "live-baseline";
  readonly sourceRevision: SyntheticDatabaseSource["sourceRevision"];
  readonly tree: string;
}[];
export function syntheticSourceParent(
  environment?: Readonly<Record<string, string | undefined>>,
): string;
export function syntheticDatabaseSource(
  sourceRevision: SyntheticDatabaseSource["sourceRevision"],
  environment?: Readonly<Record<string, string | undefined>>,
): SyntheticDatabaseSource;
export function assertSyntheticDatabaseSource(
  source: SyntheticDatabaseSource,
  environment?: Readonly<Record<string, string | undefined>>,
): SyntheticDatabaseSource;
