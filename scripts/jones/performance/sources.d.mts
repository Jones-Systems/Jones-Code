export interface SyntheticDatabaseSource {
  readonly repository: "Jones-Systems/Jones-Code";
  readonly sourceRevision:
    | "e5a31aceec91484b64315c63dcce80f6e7581604"
    | "414bb8da204c3275cd0b76b2ec4d74dfb09a97e4";
  readonly worktreePath: string;
}

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

export interface QualificationDatabaseSource {
  readonly repository: "Jones-Systems/Jones-Code";
  readonly sourceRevision:
    | SyntheticDatabaseSource["sourceRevision"]
    | "c4c68bb0b33eafb72545e6e23b0b7258e49bd613"
    | "da5f4aee0035beec471b38598eaa2857d1e5155c";
  readonly worktreePath: string;
}
export const qualificationSourcePins: readonly {
  readonly directory: "baseline" | "live-baseline" | "history" | "lease";
  readonly sourceRevision: QualificationDatabaseSource["sourceRevision"];
  readonly tree: string;
  readonly lockSha256?: string;
}[];
export function qualificationDatabaseSource(
  sourceRevision: QualificationDatabaseSource["sourceRevision"],
  environment?: Readonly<Record<string, string | undefined>>,
): QualificationDatabaseSource;
export function assertQualificationDatabaseSource(
  source: QualificationDatabaseSource,
  environment?: Readonly<Record<string, string | undefined>>,
): QualificationDatabaseSource;

export interface CurrentDatabaseSource {
  readonly repository: "Jones-Systems/Jones-Code";
  readonly sourceRevision: string;
  readonly tree: string;
  readonly lockSha256: string;
  readonly worktreePath: string;
}
export function currentDatabaseSource(worktreePath: string): CurrentDatabaseSource;
export function assertCurrentDatabaseSource(source: CurrentDatabaseSource): CurrentDatabaseSource;
